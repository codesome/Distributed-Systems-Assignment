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
#include <fstream>
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

int PROCESS_ID, MAX_TRANSACTION, ROOT;

const int max_size = 65536;

default_random_engine generator;
exponential_distribution<double> *distribution; // for sleep.

MPI_Request request;

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

int message_marshal_size = sizeof(int) + (3*sizeof(char));
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

    int marshal(void *original) {
        void *ptr = original;
        ptr = write_char(ptr, from);
        ptr = write_char(ptr, to);
        ptr = write_char(ptr, color);
        ptr = write_int(ptr, amount);
        return message_marshal_size;
    }
    
    void unmarshal(void *original) {
        void *ptr = original;
        from = read_char(&ptr);
        to = read_char(&ptr);
        color = (COLOR)read_char(&ptr);
        amount = read_int(&ptr);
    }
};

struct snapshot {
    int pid;
    int transferred, current, recv;
    vector<message> msgs;

    // 1 int for the number of msgs.
    int marshal_base_size = 4*sizeof(int);

    void clear() {
        transferred = 0;
        current = 0;
        msgs.clear();
    }

    int marshal(void *original) {
        void *ptr = original;
        ptr = write_int(ptr, pid);
        ptr = write_int(ptr, transferred);
        ptr = write_int(ptr, current);

        ptr = write_int(ptr, msgs.size());
        for(auto &m: msgs) {
            increment(&ptr, m.marshal(ptr));
        }

        int total_size = marshal_base_size + (message_marshal_size * msgs.size());

        return total_size;
    }

    void unmarshal(void *original) {
        void *ptr = original;
        pid = read_int(&ptr);
        transferred = read_int(&ptr);
        current = read_int(&ptr);
        
        int size = read_int(&ptr);
        msgs.resize(size);
        for(int i=0; i<size; i++) {
            msgs[i].unmarshal(ptr);
            increment(&ptr, message_marshal_size);
        }
    }

    void print() {
        printf("pid=%d, transferred=%d, recv=%d, current=%d, msgs={", pid, transferred, recv, current);
        for(auto &m: msgs) {
            printf(" {from=%d,to=%d,color=%d,amount=%d}", m.from, m.to, m.color, m.amount);
        }
        printf(" }\n");
        fflush(stdout);
    }

    void string() {
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


int main2() {

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
    int size = snap.marshal(ptr);
    cout << "marshal size " << size << endl;

    snapshot snap2;
    snap2.unmarshal(ptr);
    snap2.print();


    return 0;
}


// process represents a single instance of a distributed system.
class process {
public:
    int N;
    atomic_int total_amount, total_amount_sent, total_amount_recv;
    COLOR color;

    map<int,snapshot*> snapshots;

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
    ofstream snap_dump;
    int snap_count;

    process(int N, vector<int> to_send, vector<int> to_recv, int to_send_snapshot, vector<int> to_send_terminate, int total_amount): 
    N(N), 
    to_send(to_send), 
    to_recv(to_recv), 
    to_send_snapshot(to_send_snapshot), 
    to_send_terminate(to_send_terminate),
    total_amount(total_amount), 
    end(false),
    total_amount_sent(0),
    total_amount_recv(0),
    curr_send_pointer(-1),
    snap_count(0) {
        // TODO: fix this
        // 2+(N<<1) is the maximum number of elements possible in a message.
        recv_vector = malloc(max_size);
        send_vector = malloc(max_size);
        color = WHITE;
        if(root()) {
            snapshot_received.resize(N);
            snap_dump.open("snapshot_dump.txt");
        }
    }

    void end_proc() {
        if(!end.load()) {
            recv_buffer.close();
            end.store(true);
            snap_dump << "===============================================================================\n";
            snap_dump.close();
        }
    }

    bool root() {
        return PROCESS_ID == ROOT;
    }

    void dump_snapshot() {
        snap_count++;
        snap_dump << "===============================================================================\n";
        snap_dump << "SNAPSHOT " << snap_count << endl;
        int cnt = 0;
        for(auto &s: all_snapshots) {
            snap_dump << "{\n\tpid=" << s.pid << ", transferred=" << s.transferred << ", current=" << s.current << endl;
            for(auto &m: s.msgs) {
                snap_dump << "\t\t{from=" << m.from << ",to=" << m.to << ",amount=" << m.amount << "}\n";
            }
            snap_dump << "}\n";
        }
    }

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
        all_snapshots.clear();
        my_snapshot.clear();
        my_snapshot.pid = PROCESS_ID;
        my_snapshot.transferred = total_amount_sent.load();
        my_snapshot.current = total_amount.load();
        my_snapshot.recv = total_amount_recv.load();
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
            if(MPI_Isend(send_vector, (2*sizeof(char)) + sizeof(int), MPI_BYTE, p, 0, MPI_COMM_WORLD, &request) == 0) {
                printf("MARKER: %d -> %d\n", PROCESS_ID, p);
                fflush(stdout);
            } else {
                printf("### SEND ERROR\n"); fflush(stdout);
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
        dump_snapshot();
        printf("SNAPSHOT: total_process=%lu, system_total=%d, total_transaction=%d\n", all_snapshots.size(), system_total, total_transaction);
        fflush(stdout);
        if(total_transaction > MAX_TRANSACTION) {
            printf("### snapshot termination\n");
            send_all_terminations();
            end_proc();
        }
    }

    void send_all_terminations() {
        send_mtx.lock();
        // send terminate to all
        void *cpy = send_vector;
        // cpy = write_char(cpy, CONTROL);
        cpy = write_char(cpy, TERMINATE);
        for(auto p: to_send_terminate) {
            if(MPI_Isend(send_vector, sizeof(char), MPI_BYTE, p, 0, MPI_COMM_WORLD, &request) == 0) {
                printf("SENT TERMINATE: %d -> %d \n", PROCESS_ID, p);
                fflush(stdout);
            } else {
                printf("### SEND ERROR\n"); fflush(stdout);
            }
        }
        send_mtx.unlock();
    }

    void send_termination_to(int p) {
        send_mtx.lock();
        // send terminate to all
        void *cpy = send_vector;
        // cpy = write_char(cpy, CONTROL);
        cpy = write_char(cpy, TERMINATE);
        if(MPI_Isend(send_vector, sizeof(char), MPI_BYTE, p, 0, MPI_COMM_WORLD, &request) == 0) {
            printf("SENT TERMINATE: %d -> %d \n", PROCESS_ID, p);
            fflush(stdout);
        } else {
            printf("### SEND ERROR\n"); fflush(stdout);
        }
        send_mtx.unlock();
    }

    // A single message receive event.
    bool recv_event() {
        // receive message.
        MPI_Status status;
        if(MPI_Recv(recv_vector, max_size, MPI_BYTE, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status) != 0) {
            return true;
        }
        int size;
        MPI_Get_count(&status, MPI_BYTE, &size);
        if(size <= 0) {
            return true;
        }

        MESSAGE_TYPE type = MESSAGE_TYPE(*((char*)recv_vector));
        if(type != TERMINATE || !root()) {
            void *cpy = malloc(size);
            memcpy(cpy, recv_vector, size);
            recv_buffer.add(buffer_message(cpy, size));
        }

        return type != TERMINATE;
    };

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
                    m.unmarshal(data);
                    total_amount += m.amount;
                    total_amount_recv += m.amount;
                    printf("AMOUNT RECEIVED (%d <- %d): %d\n", PROCESS_ID, m.from, m.amount);
                    fflush(stdout);
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
                            printf("%d received MARKER from %d\n", PROCESS_ID, from);
                            fflush(stdout);
                            if(color==WHITE) {
                                trigger_snapshot();
                            }
                            marker_received[from] = true;
                            if(all_markers_received()) {
                                send_mtx.lock();
                                printf("SNAPSHOT DONE: %d\n", PROCESS_ID);
                                fflush(stdout);
                                if(root()) {
                                    if(!snapshot_received[PROCESS_ID]) {
                                        all_snapshots.push_back(my_snapshot);
                                        snapshot_received[PROCESS_ID] = true;
                                    }
                                    color = WHITE;
                                    if(all_snapshots_received()) {
                                        root_termination_check();
                                    }
                                    send_mtx.unlock();
                                    break;
                                }

                                void *cpy = send_vector;
                                cpy = write_char(cpy, CONTROL);
                                cpy = write_char(cpy, SNAPSHOT);
                                cpy = write_int(cpy, PROCESS_ID);
                                int size = my_snapshot.marshal(cpy) + (2*sizeof(char)) + sizeof(int);

                                my_snapshot.print();
                                if(MPI_Isend(send_vector, size, MPI_BYTE, to_send_snapshot, 0, MPI_COMM_WORLD, &request) == 0) {
                                    printf("SNAPSHOT SENT: %d -> %d\n", PROCESS_ID, to_send_snapshot);
                                    fflush(stdout);
                                } else {
                                    printf("### SEND ERROR\n"); fflush(stdout);
                                }

                                color = WHITE;
                                send_mtx.unlock();
                            }
                            break;
                        } // MARKER
                        case SNAPSHOT: {
                            printf("Process %d got snapshot from %d\n", PROCESS_ID, from);
                            fflush(stdout);
                            if(root()) {
                                snapshot s;
                                s.unmarshal(data);
                                all_snapshots.push_back(s);
                                snapshot_received[s.pid] = true;

                                if(all_snapshots_received()) {
                                    root_termination_check();
                                }

                                break;
                            }
                            send_mtx.lock();
                            if(MPI_Isend(msg.data, msg.size, MPI_BYTE, to_send_snapshot, 0, MPI_COMM_WORLD, &request) == 0) {
                                printf("FORWARD SNAPSHOT: %d -> %d\n", PROCESS_ID, to_send_snapshot);
                                fflush(stdout);
                            } else {
                                printf("### SEND ERROR\n"); fflush(stdout);
                            }
                            send_mtx.unlock();
                            break;
                        } // SNAPSHOT
                        default:
                            printf("Unknown control message type (process=%d): %d\n", PROCESS_ID, type);
                            fflush(stdout);
                    }
                    break;
                } // CONTROL
                case TERMINATE: {
                    printf("TRMINATE: %d\n", PROCESS_ID);
                    fflush(stdout);
                    send_all_terminations();
                    end_proc();
                    break;
                } // TERMINATE
                default:
                    printf("Unknown message type (process=%d): %d\n", PROCESS_ID, type);
                    fflush(stdout);
            }

            free(msg.data);
        }
        while(!is_closed) {
            buffer_message msg = recv_buffer.retrieve(&is_closed);
            if(is_closed) {
                return;
            }
            free(msg.data);
        }
    }

    // Internal event.
    void internal_event() {
        auto timestamp = chrono::duration_cast<std::chrono::milliseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
        usleep((*distribution)(generator)*3000000); // simulation some process.
        printf("Process %d executes internal event\n", PROCESS_ID);
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
        int max = total < 500? total: 500;
        uniform_int_distribution<int> random_amount((max/2)+1,max);
        int transaction_amount = (clock() + random_amount(generator))%max;
        if(transaction_amount == 0) transaction_amount = 1;
        m.amount = transaction_amount;

        void *ptr = write_char(send_vector, DATA);
        int message_size = m.marshal(ptr) + sizeof(char);
        // auto timestamp = chrono::duration_cast<std::chrono::milliseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
        if(MPI_Isend(send_vector, message_size, MPI_BYTE, send_to, 0, MPI_COMM_WORLD, &request) == 0) {
            total_amount -= transaction_amount;
            total_amount_sent += transaction_amount;
            printf("Process %d sends %d to process %d\n", PROCESS_ID, transaction_amount, send_to);
            fflush(stdout);
        } else {
            printf("### SEND ERROR\n"); fflush(stdout);
        }
        send_mtx.unlock();
    };
};

int main(int argc, const char* argv[]) {

    fflush(stdout);
    MPI_Init(NULL, NULL);
    // Find out rank, size
    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    srand(time(NULL));

    PROCESS_ID = world_rank;

    ifstream inFile;
    inFile.open("inp-params.txt");
    int N, A, T;
    float lambda;
    vector<int> to_send, to_recv, to_send_terminate;
    inFile >> N >> A >> T >> lambda;
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
        inFile >> size;
        if(i == PROCESS_ID) {
            for(int j=0; j<size; j++) {
                inFile >> val;
                to_send.push_back(val);
            }
        } else {
            for(int j=0; j<size; j++) {
                inFile >> val;
                if(val == PROCESS_ID) {
                    to_recv.push_back(i);
                }
            }
        }
    }

    for(int i=0; i<N; i++) {
        inFile >> size;
        assert(size == 1);
        inFile >> val;
        if(i == PROCESS_ID) {
            to_send_snapshot = val;
        }
        if(val == PROCESS_ID && i != PROCESS_ID) {
            to_send_terminate.push_back(i);
        }
        if(i == val) {
            ROOT = i;
        }
    }

    process p(N, to_send, to_recv, to_send_snapshot, to_send_terminate, A);

    // recv thread
    thread recv_thread([](process *p){
        while(!(p->end.load())) {
            if(!(p->recv_event()))
                break;
        }
    }, &p);
    thread process_msg_thread([](process *p){
        p->process_messages();
    }, &p);
    thread snapshot_trigger_thread([](process *p){
        if(!p->root()) {
            return;
        }
        while(!(p->end.load())) {
            p->trigger_snapshot();
            sleep(1);
        }
    }, &p);
    
    // send
    fflush(stdout);
    while(!p.end.load()) {
        p.send_event();
        p.internal_event();
    }

    // This sleep is just to make sure that all sending messages are complete before we stop receiving.
    sleep(3);
    p.end_proc();

    process_msg_thread.join();
    snapshot_trigger_thread.join();
    recv_thread.join();

    if(PROCESS_ID == (ROOT+1)%N) {
        p.send_termination_to(ROOT);
    }

    printf("Ending Process %d\n", PROCESS_ID);
    fflush(stdout);

    MPI_Finalize();
    
    return 0;
}