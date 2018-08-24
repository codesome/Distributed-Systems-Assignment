#include <iostream>
#include <thread>
#include <atomic>
#include <vector>
#include <mutex>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <signal.h>
using namespace std;

int RECV_BASE_PORT = 8000;

class process {
public:
    int recv_sock, send_sock, listen_port, max_conn;
    int my_id;

    int N;
    mutex vector_mtx;
    int *time_vector;
    atomic_int internal_events, msg_events;    

    atomic_bool end;

    // for sending
    vector<int> to_send;
    int curr_send_pointer;

    // for receiving
    int *recv_vector;

    process(int my_id, int N, vector<int> to_send): 
    my_id(my_id), N(N), to_send(to_send), end(false), internal_events(0), msg_events(0) {
        time_vector = new int[N];
        recv_vector = new int[N];
        for(int i = 0; i < N; i++) {
            time_vector[i] = 0;
            recv_vector[i] = 0;
        }
    }

    void print_counts() {
        printf("%d internal:%d, msg:%d\n", my_id, internal_events.load(), msg_events.load());
        fflush(stdout);
    }

    void print_vector_clock() {
        string s = to_string(my_id) + " [";
        for(int i=0; i<N; i++) {
            s += to_string(time_vector[i]) + " ";
        }
        s += "]\n";

        printf("%s",s.c_str());
        fflush(stdout);
    }

    void end_proc() {
        end.store(true);
    }
    
    float current_ratio() {
        return float(internal_events.load())/float(msg_events.load());
    }

    int listen_recv_server() {
        sockaddr_in server;
        bzero((char *) &server, sizeof(server));
        server.sin_family = AF_INET;
        server.sin_port = htons(listen_port);
        server.sin_addr.s_addr = htonl(INADDR_ANY);
        if(bind(recv_sock, (struct sockaddr *) &server, sizeof(server)) < 0) {
            printf("bind error\n");
            fflush(stdout);
            return -1;
        }
        if(listen(recv_sock, N*5) < 0) {
            printf("listen error\n");
            fflush(stdout);
            return -1;
        }
        printf("Listening on port %d\n", listen_port);
        fflush(stdout);

        timeval option_value;
        option_value.tv_sec = 2;
        option_value.tv_usec = 0;
        setsockopt(recv_sock, SOL_SOCKET, SO_RCVTIMEO, (char *) &option_value, sizeof(timeval));

        return 0;
    }

    void recv_event() {

        // accept connection, receive, and update vector
        // alternatively do blank event
        unsigned int len = sizeof(sockaddr_in);
        sockaddr_in client;
        char recv_msg[1024];
        int size;
        string send_msg;

        // accepting a connection
        int client_sockid = accept(recv_sock, (struct sockaddr *) &client, &len);
        if(errno == EAGAIN || errno == EWOULDBLOCK) {
            // printf("timeout\n");
            // fflush(stdout);
            return;
        }
        if(client_sockid < 0) {
            printf("accept error\n");
            fflush(stdout);
            exit(-1);
        }
        fflush(stdout);

        size = recv(client_sockid, recv_vector, N*sizeof(int), 0);
        close(client_sockid);
        if(size != N*sizeof(int)) {
            return;
        }

        vector_mtx.lock();
        
        time_vector[my_id]++;
        for(int i=0; i<N; i++) {
            if(recv_vector[i] > time_vector[i]) {
                time_vector[i] = recv_vector[i];
            }
        }
        
        vector_mtx.unlock();

    };

    void dummy_event() {
        // just wait for some time with mutex

        vector_mtx.lock();
        
        usleep(rand()%100000);
        time_vector[my_id]++;
        internal_events++;

        vector_mtx.unlock();
    };

    void send_event() {
        send_sock = socket(AF_INET, SOCK_STREAM, 0);

        curr_send_pointer = (curr_send_pointer+1)%to_send.size();
        int server_port = RECV_BASE_PORT + to_send[curr_send_pointer];
        // server info
        sockaddr_in server;
        bzero((char *) &server, sizeof(server));
        server.sin_family = AF_INET;
        server.sin_port = htons(server_port);
        server.sin_addr.s_addr = inet_addr("127.0.0.1");

        // connecting to a server
        int server_sockid;
        if((server_sockid=connect(send_sock, (struct sockaddr *) &server, sizeof(server))) < 0) {
            if(errno != EISCONN) {
                printf("connect error %d %d %d %d\n", my_id, server_port, server_sockid, errno);
                fflush(stdout);
                return;
            }
        }

        vector_mtx.lock();

        time_vector[my_id]++;
        if(send(send_sock, time_vector, N*sizeof(int), 0) > 0) {
            msg_events++;
        } else {
            time_vector[my_id]--;
        }
        
        vector_mtx.unlock();

        close(server_sockid);
        close(send_sock);

    };


};

int main(int argc, const char* argv[]) {

    RECV_BASE_PORT = atoi(argv[1]);

    int N, M;
    float lambda, alpha;
    vector<vector<int>> to_sends;
    cin >> N >> lambda >> alpha >> M;
    to_sends.resize(N);

    int size, val;
    for(int i=0; i<N; i++) {
        cin >> size;
        vector<int>& v = to_sends[i];
        for(int j=0; j<size; j++) {
            cin >> val;
            v.push_back(val);
        }
    }

    auto thread_function = [](int thread_id, int N, int M, vector<int> to_send, float alpha){

        process p(thread_id, N, to_send);

        p.recv_sock = socket(AF_INET, SOCK_STREAM, 0);
        p.listen_port = RECV_BASE_PORT+thread_id;

        if(p.listen_recv_server() < 0) {
            fflush(stdout);
            exit(-1);
        }

        // recv thread
        thread recv_thread([alpha](process *p){
            while(!(p->end.load())) {
                p->recv_event();
                p->print_vector_clock();
            }
        }, &p);
        
        // send
        printf("Sending\n");
        fflush(stdout);
        while(p.msg_events.load() < M && p.internal_events.load() < (alpha*M)) {
            p.send_event();
            p.dummy_event();
        }

        while(p.msg_events.load() < M) {
            p.send_event();
        }

        while(p.internal_events.load() < (alpha*M)) {
            p.dummy_event();
        }
        
        p.end_proc();

        recv_thread.join();

        printf("Ending %d, msg=%d, internal=%d \n", thread_id, p.msg_events.load(), p.internal_events.load());
        fflush(stdout);

    };

    vector<thread> processess;
    for(int i=0; i<N; i++) {
        processess.push_back(thread(thread_function, i, N, M, to_sends[i], alpha));
    }

    for(int i=0; i<N; i++) {
        processess[i].join();
    }

    return 0;
}


