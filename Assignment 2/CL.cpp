/*
    Author: Ganesh Vernekar (CS15BTECH11018)
*/

#include <iostream>
#include <thread>
#include <atomic>
#include <vector>
#include <mutex>
#include <map>
#include <random>
#include <chrono>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <signal.h>
#include <mpi.h>
#include "channel.hpp"
using namespace std;

// RECV_BASE_PORT + PROCESS_ID gives the port for the process to listen.
int RECV_BASE_PORT, PROCESS_ID, MAX_TRANSACTION;

const int max_size = 65536;

default_random_engine generator;
exponential_distribution<double> *distribution; // for sleep.
uniform_int_distribution<int> toss_coin(0,1);

enum COLOR : char {
    WHITE = 0, RED = 1
};

enum MESSAGE_TYPE : char {
    DATA = 0, CONTROL = 1, TERMINATE = 2
};

enum CONTROL_TYPE : char {
    MARKER = 0, SNAPSHOT = 1
};

void increment(void **ptr, int offset) {
    *ptr = ((char*)(*ptr)) + offset;
}

void* shift(void *ptr, int offset) {
    return ((char*)ptr) + offset;
}

void* write_char(void *ptr, char val) {
    *((char*)ptr) = val;
    return (char*)ptr + sizeof(char);
}

void* write_int(void *ptr, int val) {
    *((int*)ptr) = val;
    return (char*)ptr + sizeof(int);
}

char read_char(void **ptr) {
    char v = *((char*)(*ptr));
    *ptr = (char*)(*ptr) + sizeof(char);
    return v;
}

int read_int(void **ptr) {
    int v = *((int*)(*ptr));
    *ptr = (char*)(*ptr) + sizeof(int);
    return v;
}

int message_marshall_size = sizeof(int) + (3*sizeof(char));
struct message {
    char from, to;
    COLOR color;
    int amount;

    message() {}

    message(char from, char to, COLOR color) {
        this->from = from;
        this->to = to;
        this->color = color;
    }

    int marshall(void *original) {
        void *ptr = original;
        ptr = write_char(ptr, from);
        ptr = write_char(ptr, to);
        ptr = write_char(ptr, color);
        ptr = write_int(ptr, amount);
        return message_marshall_size;
    }
    
    void unmarshall(void *original) {
        void *ptr = original;
        from = read_char(&ptr);
        to = read_char(&ptr);
        color = (COLOR)read_char(&ptr);
        amount = read_int(&ptr);
    }
};

struct snapshot {
    int pid;
    int transferred, current;
    vector<message> msgs;

    // 1 int for the number of msgs.
    int marshall_base_size = 4*sizeof(int);

    void clear() {
        msgs.clear();
    }

    int marshall(void *original) {
        void *ptr = original;
        ptr = write_int(ptr, pid);
        ptr = write_int(ptr, transferred);
        ptr = write_int(ptr, current);

        ptr = write_int(ptr, msgs.size());
        for(auto &m: msgs) {
            increment(&ptr, m.marshall(ptr));
        }

        int total_size = marshall_base_size + (message_marshall_size * msgs.size());

        return total_size;
    }

    void unmarshall(void *original) {
        void *ptr = original;
        pid = read_int(&ptr);
        transferred = read_int(&ptr);
        current = read_int(&ptr);
        
        int size = read_int(&ptr);
        msgs.resize(size);
        for(int i=0; i<size; i++) {
            msgs[i].unmarshall(ptr);
            increment(&ptr, message_marshall_size);
        }
    }

    void print() {
        printf("pid=%d, transferred=%d, current=%d, msgs={", pid, transferred, current);
        for(auto &m: msgs) {
            printf(" {from=%d,to=%d,color=%d,amount=%d}", m.from, m.to, m.color, m.amount);
        }
        printf(" }\n");
    }

};

struct buffer_message {
    void *data;
    int size;
    buffer_message(): data(NULL), size(0) {}
    buffer_message(void *data, int size): data(data), size(size) {}
};


int main() {

    snapshot snap;
    snap.pid = 1;
    snap.transferred = 234;
    snap.current = 2346354;

    message m(1,2,WHITE);
    m.amount = 24;
    snap.msgs.push_back(m);
    m.to = 3;
    m.color = RED;
    m.amount = 65;
    snap.msgs.push_back(m);
    m.to = 4;
    m.color = WHITE;
    m.amount = 97;
    snap.msgs.push_back(m);

    snap.print();

    void *ptr = malloc(200);
    int size = snap.marshall(ptr);
    cout << "Marshall size " << size << endl;

    snapshot snap2;
    snap2.unmarshall(ptr);
    snap2.print();


    return 0;
}


// process represents a single instance of a distributed system.
class process {
public:
    int N;
    int my_id;
    atomic_int total_amount, total_amount_sent;
    COLOR color;

    map<int,snapshot*> snapshots;

    int recv_sock, send_sock, listen_port, max_conn;

    // This is made 'true' when the process has to end.
    atomic_bool end;

    // This is the adjacent processess to his process in the graph topology
    // to which it has to send the messages.
    vector<int> to_send, to_recv, to_send_terminate;
    map<int,bool> marker_received;
    int to_send_snapshot;
    int curr_send_pointer;

    void *recv_vector, *send_vector;

    mutex send_mtx;

    Channel<buffer_message> recv_buffer;

    snapshot my_snapshot;

    // coordinator
    vector<snapshot> all_snapshots;
    vector<int> snapshot_received;

    process(int N, vector<int> to_send, vector<int> to_recv, int to_send_snapshot, vector<int> to_send_terminate, int total_amount): 
    my_id(my_id), 
    N(N), 
    to_send(to_send), 
    to_recv(to_recv), 
    to_send_snapshot(to_send_snapshot), 
    to_send_terminate(to_send_terminate),
    total_amount(total_amount), 
    end(false),
    total_amount_sent(0),
    curr_send_pointer(-1) {
        // TODO: fix this
        // 2+(N<<1) is the maximum number of elements possible in a message.
        recv_vector = malloc(max_size);
        send_vector = malloc(max_size);
        listen_port = RECV_BASE_PORT + my_id;
        color = WHITE;
        if(root()) {
            snapshot_received.resize(N);
        }
    }

    void end_proc() {
        if(!end.load()) {
            recv_buffer.close();
            end.store(true);
        }
    }

    bool root() {
        return PROCESS_ID == to_send_snapshot;
    }

    // A single message receive event.
    void recv_event() {

        unsigned int len = sizeof(sockaddr_in);
        sockaddr_in client;

        // accepting a connection
        int client_sockid = accept(recv_sock, (struct sockaddr *) &client, &len);
        if(errno == EAGAIN || errno == EWOULDBLOCK) {
            // timeout
            return;
        }
        if(client_sockid < 0) {
            printf("accept error\n");
            fflush(stdout);
            MPI_Abort(MPI_COMM_WORLD, -1);
        }

        // receive message.
        MPI_Status status;
        if(MPI_Recv(recv_vector, max_size, MPI_BYTE, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status) != 0) {
            return;
        }
        int size;
        MPI_Get_count(&status, MPI_BYTE, &size);
        // auto timestamp = chrono::duration_cast<std::chrono::milliseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
        if(size <= 0) {
            return;
        }

        void *cpy = malloc(size);
        memcpy(cpy, recv_vector, size);
        recv_buffer.add(buffer_message(cpy, size));

    };

    bool all_markers_received() {
        for(auto p: to_recv) {
            if(!marker_received[p]) {
                return false;
            }
        }
        return true;
    }

    bool all_snapshots_received() {
        for(int i=0; i<N; i++) {
            if(!snapshot_received[i]) {
                return false;
            }
        }
        return true;
    }

    void trigger_snapshot() {
        if(color == RED || end.load()) {
            return;
        }
        send_mtx.lock();
        color = RED;
        my_snapshot.clear();
        my_snapshot.pid = PROCESS_ID;
        my_snapshot.transferred = total_amount_sent;
        my_snapshot.current = total_amount;
        for(auto p: to_recv) {
            marker_received[p] = false;
        }
        if(root()) {
            for(int i=0; i<N; i++) {
                snapshot_received[i] = false;
            }
        }

        // send marker to all
        void *cpy = send_vector;
        cpy = write_char(cpy, CONTROL);
        cpy = write_char(cpy, MARKER);
        cpy = write_int(cpy, PROCESS_ID);
        for(auto p: to_send) {
            if(MPI_Send(send_vector, (2*sizeof(char)) + sizeof(int), MPI_BYTE, p, 0, MPI_COMM_WORLD) == 0) {
                printf("Process %d sent marker to %d\n", PROCESS_ID, p);
                fflush(stdout);
            } else {
                // TODO: handle error
            }
        }
        send_mtx.unlock();
    }

    void root_termination_check() {
        int system_total = 0;
        int total_transaction = 0;
        for(auto &s: all_snapshots) {
            system_total += s.current;
            for(auto &m: s.msgs) {
                system_total += m.amount;
            }
            total_transaction += s.transferred;
        }
        printf("SNAPSHOT: system_total=%d, total_transaction=%d\n", system_total, total_transaction);
        fflush(stdout);
        if(total_transaction > MAX_TRANSACTION) {
            send_mtx.lock();
            // send terminate to all
            void *cpy = send_vector;
            cpy = write_char(cpy, CONTROL);
            cpy = write_char(cpy, TERMINATE);
            for(auto p: to_send_terminate) {
                if(MPI_Send(send_vector, 2*sizeof(char), MPI_BYTE, p, 0, MPI_COMM_WORLD) == 0) {
                    printf("Process %d sent terminate to %d\n", PROCESS_ID, p);
                    fflush(stdout);
                } else {
                    // TODO: handle error
                }
            }
            send_mtx.unlock();
            end_proc();
        }
    }

    void process_messages() {
        bool is_closed;
        while(true && !end.load()) {
            buffer_message msg = recv_buffer.retrieve(&is_closed);
            if(is_closed) {
                return;
            }

            void *data = msg.data;

            MESSAGE_TYPE type = MESSAGE_TYPE(read_char(&data));
            switch(type) {
                case DATA: {
                    message m;
                    m.unmarshall(data);
                    total_amount += m.amount;
                    if(color == RED && m.color == WHITE) {
                        my_snapshot.msgs.push_back(m);
                    }
                    break;
                } // DATA
                case CONTROL: {
                    CONTROL_TYPE ctype = CONTROL_TYPE(read_char(&data));
                    int from = read_int(&data);
                    switch(ctype) {
                        case MARKER: {
                            if(color==WHITE) {
                                trigger_snapshot();
                            }
                            marker_received[from] = true;
                            if(all_markers_received()) {
                                printf("Process %d received all markers\n", PROCESS_ID);
                                fflush(stdout);
                                if(root()) {
                                    all_snapshots.push_back(my_snapshot);
                                    snapshot_received[PROCESS_ID] = true;
                                    if(all_snapshots_received()) {
                                        root_termination_check();
                                    }
                                    break;
                                }
                                send_mtx.lock();

                                void *cpy = send_vector;
                                cpy = write_char(cpy, CONTROL);
                                cpy = write_char(cpy, SNAPSHOT);
                                cpy = write_int(cpy, PROCESS_ID);
                                int size = my_snapshot.marshall(cpy) + (2*sizeof(char)) + sizeof(int);

                                if(MPI_Send(send_vector, size, MPI_BYTE, to_send_snapshot, 0, MPI_COMM_WORLD) == 0) {
                                    printf("Process %d successfully captured the snapshot and sent to %d\n", PROCESS_ID, to_send_snapshot);
                                    fflush(stdout);
                                } else {
                                    // TODO: handle error
                                }

                                color = WHITE;
                                send_mtx.unlock();
                            }
                            break;
                        } // MARKER
                        case SNAPSHOT: {
                            if(root()) {
                                snapshot s;
                                s.unmarshall(data);
                                all_snapshots.push_back(s);
                                snapshot_received[s.pid] = true;

                                if(all_snapshots_received()) {
                                    root_termination_check();
                                }

                                break;
                            }
                            send_mtx.lock();
                            if(MPI_Send(msg.data, msg.size, MPI_BYTE, to_send_snapshot, 0, MPI_COMM_WORLD) == 0) {
                                printf("Process %d forwared the snapshot to %d\n", PROCESS_ID, to_send_snapshot);
                                fflush(stdout);
                            } else {
                                // TODO: handle error
                            }
                            send_mtx.unlock();
                            break;
                        } // SNAPSHOT
                        default:
                            printf("Unknown control message type (process=%d): %d\n", PROCESS_ID, type);
                    }
                    break;
                } // CONTROL
                case TERMINATE: {
                    end_proc();
                    break;
                } // TERMINATE
                default:
                    printf("Unknown message type (process=%d): %d\n", PROCESS_ID, type);
            }

            free(msg.data);
        }
    }

    // Internal event.
    void internal_event() {
        auto timestamp = chrono::duration_cast<std::chrono::milliseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
        usleep((*distribution)(generator)*300000); // simulation some process.
        printf("Process%d executes internal event\n", my_id);
        fflush(stdout);
    };

    // A single message send event.
    void send_event() {
        curr_send_pointer = (curr_send_pointer+1)%to_send.size();
        int send_to = to_send[curr_send_pointer];

        send_mtx.lock();
        message m(PROCESS_ID, send_to, color);
        int total = total_amount.load();
        if(total <= 0) {
            send_mtx.unlock();
            return;
        }
        int max = total < 100? total: 100;
        int transaction_amount = rand()%max;
        if(transaction_amount == 0) transaction_amount = 1;
        m.amount = transaction_amount;

        void *ptr = write_char(send_vector, DATA);
        int message_size = m.marshall(ptr) + sizeof(char);
        // auto timestamp = chrono::duration_cast<std::chrono::milliseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
        if(MPI_Send(send_vector, message_size, MPI_BYTE, send_to, 0, MPI_COMM_WORLD) == 0) {
            total_amount -= transaction_amount;
            total_amount_sent += transaction_amount;
            printf("Process%d sends %d to process %d\n", my_id, transaction_amount, send_to);
            fflush(stdout);
        } else {
            // TODO: handle error
        }
        send_mtx.unlock();
    };
};

int main2(int argc, const char* argv[]) {

    MPI_Init(NULL, NULL);
    // Find out rank, size
    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    srand(time(NULL));

    RECV_BASE_PORT = atoi(argv[1]);
    PROCESS_ID = world_rank;

    int N, A, T;
    float lambda;
    vector<int> to_send, to_recv, to_send_terminate;
    cin >> N >> A >> T >> lambda;
    if(!(
        (N > 0) &&
        (N == world_size) &&
        (PROCESS_ID < N) &&
        (A > 0) &&
        (T > 0) &&
        (lambda > 0)
    )) {
        MPI_Abort(MPI_COMM_WORLD, -1);
    }

    MAX_TRANSACTION = T;

	distribution = new exponential_distribution<double>(lambda);

    // input of graph topology.
    int size, val;
    int to_send_snapshot = 0;
    for(int i=0; i<N; i++) {
        cin >> size;
        if(i == PROCESS_ID) {
            for(int j=0; j<size; j++) {
                cin >> val;
                to_send.push_back(val);
            }
        } else {
            for(int j=0; j<size; j++) {
                cin >> val;
                if(val == PROCESS_ID) {
                    to_recv.push_back(val);
                }
            }
        }
    }

    for(int i=0; i<N; i++) {
        cin >> size;
        assert(size == 1);
        cin >> val;
        if(i == PROCESS_ID) {
            to_send_snapshot = val;
            break;
        }
        if(val == PROCESS_ID && i != PROCESS_ID) {
            to_send_terminate.push_back(i);
        }
    }

    process p(N, to_send, to_recv, to_send_snapshot, to_send_terminate, A);

    // recv thread
    thread recv_thread([](process *p){
        while(!(p->end.load())) {
            p->recv_event();
        }
    }, &p);
    thread process_msg_thread([](process *p){
        p->process_messages();
    }, &p);
    
    // send
    printf("Sending\n");
    fflush(stdout);
    while(!p.end.load()) {
        // Randomly select message send or internal event.
        // Do this till we have to perform both the operations.
        if(toss_coin(generator)) {
            p.send_event();
        } else {
            p.internal_event();
        }
    }

    // This sleep is just to make sure that all sending messages are complete before we stop receiving.
    sleep(3);
    p.end_proc();

    recv_thread.join();

    printf("Ending Process%d\n", PROCESS_ID);
    fflush(stdout);
    
    MPI_Finalize();
    return 0;
}