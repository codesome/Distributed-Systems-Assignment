#include <iostream>
#include <thread>
#include <atomic>
#include <vector>
#include <mutex>
#include <random>
#include <chrono>
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

default_random_engine generator;
exponential_distribution<double> *distribution;
uniform_int_distribution<int> toss_coin(0,1);

class process {
public:
    int recv_sock, send_sock, listen_port, max_conn;
    int my_id;

    int N;
    mutex vector_mtx;
    int *time_vector, *last_sent, *last_updated;
    atomic_int internal_events, msg_events, total_events;    

    atomic_bool end;

    // for sending
    vector<int> to_send;
    int curr_send_pointer;

    // for receiving
    int *recv_vector;

    int bytes_sent, no_times_sent;

    process(int my_id, int N, vector<int> to_send): 
    my_id(my_id), N(N), to_send(to_send), end(false), internal_events(0), msg_events(0), total_events(0) {
        time_vector = new int[N];
        last_sent = new int[N];
        last_updated = new int[N];
        recv_vector = new int[2+(N<<1)];
        bytes_sent = 0;
        no_times_sent = 0;
        for(int i = 0; i < N; i++) {
            time_vector[i] = 0;
            last_sent[i] = 0;
            last_updated[i] = 0;
        }
    }

    void print_counts() {
        printf("%d internal:%d, msg:%d\n", my_id, internal_events.load(), msg_events.load());
        fflush(stdout);
    }

    string print_vector_clock() {
        string s = "[";
        for(int i=0; i<N; i++) {
            s += to_string(time_vector[i]) + " ";
        }
        s += "]";
        return s;
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

        sleep(3);

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

        size = recv(client_sockid, recv_vector, (2+(N<<1))*sizeof(int), 0);
        auto timestamp = chrono::duration_cast<std::chrono::milliseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
        close(client_sockid);
        if(size <= 0) {
            return;
        }

        vector_mtx.lock();
        
        total_events++;
        printf("Process%d receives message from process %d in e%d_%d at %ld, vc: %s\n", my_id, recv_vector[0], my_id, total_events.load(), timestamp, print_vector_clock().c_str());
        fflush(stdout);
        time_vector[my_id]++;
        last_updated[my_id] = time_vector[my_id];
        int sz = recv_vector[1];
        for(int i=0, j=2; i<sz; i++, j+=2) {
            if(recv_vector[j+1] > time_vector[recv_vector[j]]) {
                time_vector[recv_vector[j]] = recv_vector[j+1];
                last_updated[recv_vector[j]] = time_vector[my_id];
            }
        }
        
        vector_mtx.unlock();

    };

    void dummy_event() {
        // just wait for some time with mutex

        vector_mtx.lock();
        
        auto timestamp = chrono::duration_cast<std::chrono::milliseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
        usleep((*distribution)(generator)*100000);
        time_vector[my_id]++;
        last_updated[my_id] = time_vector[my_id];
        internal_events++;
        total_events++;

        printf("Process%d executes internal event e%d_%d at %ld, vc: %s\n", my_id, my_id, total_events.load(), timestamp, print_vector_clock().c_str());
        fflush(stdout);

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

        vector<int> in_send_format;
        // in_send_format.reserve(2+(N<<1));
        int ls = last_sent[to_send[curr_send_pointer]];
        in_send_format.push_back(my_id);
        in_send_format.push_back(0);
        for(int i=0; i<N; i++) {
            if(ls < last_updated[i]) {
                in_send_format.push_back(i);
                in_send_format.push_back(time_vector[i]);
            }
        }
        in_send_format[1] = (in_send_format.size()-2)>>1;

        time_vector[my_id]++;
        last_updated[my_id] = time_vector[my_id];
        auto timestamp = chrono::duration_cast<std::chrono::milliseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
        if(send(send_sock, in_send_format.data(), in_send_format.size()*sizeof(int), 0) > 0) {
            total_events++;
            printf("Process%d sends message to process %d from e%d_%d at %ld, vc: %s\n", my_id, to_send[curr_send_pointer], my_id, total_events.load(), timestamp, print_vector_clock().c_str());
            fflush(stdout);
            bytes_sent += in_send_format.size()*sizeof(int);
            no_times_sent++;
            msg_events++;
            last_sent[to_send[curr_send_pointer]] = time_vector[my_id];
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

	distribution = new exponential_distribution<double>(lambda);

    int size, val;
    for(int i=0; i<N; i++) {
        cin >> size;
        vector<int>& v = to_sends[i];
        for(int j=0; j<size; j++) {
            cin >> val;
            v.push_back(val);
        }
    }

    atomic_int bytes_sent(0), no_times_sent(0);

    auto thread_function = [&bytes_sent, &no_times_sent](int thread_id, int N, int M, vector<int> to_send, float alpha){

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
            }
        }, &p);
        
        // send
        printf("Sending\n");
        fflush(stdout);
        while(p.msg_events.load() < M && p.internal_events.load() < (alpha*M)) {
            if(toss_coin(generator)) {
                p.send_event();
            } else {
                p.dummy_event();
            }
        }

        while(p.msg_events.load() < M) {
            p.send_event();
        }

        while(p.internal_events.load() < (alpha*M)) {
            p.dummy_event();
        }
        
        sleep(3);
        p.end_proc();

        recv_thread.join();

        printf("Ending %d, msg=%d, internal=%d \n", thread_id, p.msg_events.load(), p.internal_events.load());
        fflush(stdout);
        
        bytes_sent += p.bytes_sent;
        no_times_sent += p.no_times_sent;
    
    };

    vector<thread> processess;
    for(int i=0; i<N; i++) {
        processess.push_back(thread(thread_function, i, N, M, to_sends[i], alpha));
    }

    for(int i=0; i<N; i++) {
        processess[i].join();
    }

    printf("Bytes:%d, Total msgs:%d, Avg:%f, UnOpt:%lu\n", bytes_sent.load(), no_times_sent.load(), float(bytes_sent.load())/float(no_times_sent.load()),  (2+(N<<1))*sizeof(int));

    return 0;
}