/*
**   Author: Ganesh Vernekar (CS15BTECH11018)
**/

#include <thread>
#include <atomic>
#include <algorithm>
#include <vector>
#include <mutex>
#include <map>
#include <random>
#include <chrono>
#include <fstream>
#include <assert.h>
#include <unistd.h>
#include <stdio.h>
#include <mpi.h>
#include "channel.h"
#include "common.h"
using namespace std;


int PROCESS_ID, // Current process ID. 
    MAX_TRANSACTION, // Max total transaction to be done.
    ROOT, // ID of root.
    SEND_MSG_ID; // Counter of message sent. Used for message id.

// For random numbers.
default_random_engine generator;
exponential_distribution<double> *distribution;

// dummy, just to satisfy function argument.
MPI_Request request; 

// Structure of a single snapshot.
struct snapshot {
    int pid;
    int transferred, // total amount sent. 
        current; // current amount with the process.
    vector<message> msgs; // Messages in transit.

    int marshal_base_size = 4*sizeof(int);

    void clear() {
        transferred = 0;
        current = 0;
        msgs.clear();
    }

    // Encode snapshot into storable format.
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

    // Decode snapshot from bytes.
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

    // Print the snapshot. In-transit message will be printed by the root.
    void print() {
        auto timestamp = chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
        printf("[%ld] pid=%d, transferred=%d, current=%d\n", timestamp, pid, transferred, current);
        fflush(stdout);
    }

};

// process represents a single instance of a distributed system.
class process {
public:
    int N; // Total number of processess.
    atomic_int total_amount, total_amount_sent, total_amount_recv;
    atomic_int total_control_msg_rcvd;
    COLOR color;

    // This is made 'true' when the process has to end.
    atomic_bool end;

    vector<int> to_send, // Processess to which messages and marker has to be sent. 
                to_recv, // Processess from which it has to receive markers. 
                to_send_terminate; // Processess to which it has to sent terminate.
    
    // Map of process id to bool, indicating if marker received or no.
    map<int,bool> marker_received;

    // Process to forward snapshot to.
    int to_send_snapshot;

    // Current pointer in 'to_sent' to which message has to be sent.
    int curr_send_pointer;

    // Memory reused to receive and sent messages.
    void *recv_vector, *send_vector;

    // Lock to be acquired while sending messages.
    mutex send_mtx;

    // A thread safe - go style channel to pass messages 
    // for processing from receive event. 
    Channel<buffer_message> recv_buffer;

    // The current process's snapshot.
    snapshot my_snapshot;

    /// coordinator/root specific variables.

    // Snapshots of all the processess.
    vector<snapshot> all_snapshots;
    // Index i tells if root received snapshot from process i. 
    vector<bool> snapshot_received;
    // Stream to dump the snapshots.
    ofstream snap_dump;
    // Number of snapshots received in total.
    int snap_count;

    process(int N, vector<int> to_send, vector<int> to_recv, int to_send_snapshot, vector<int> to_send_terminate, int total_amount): 
    N(N), to_send(to_send), to_recv(to_recv), to_send_snapshot(to_send_snapshot), 
    to_send_terminate(to_send_terminate), total_amount(total_amount), 
    end(false), total_amount_sent(0), total_amount_recv(0), curr_send_pointer(-1),
    snap_count(0), color(WHITE), total_control_msg_rcvd(0) {
        recv_vector = malloc(max_size);
        send_vector = malloc(max_size);
        if(root()) {
            snapshot_received.resize(N);
            snap_dump.open("snapshot_dump_CL.txt");
        }
    }

    // Used to end all sending and receiving of messages.
    void end_proc() {
        if(!end.load()) {
            end.store(true);
            recv_buffer.close();
            snap_dump << "===============================================================================\n";
            snap_dump.close();
        }
    }

    bool root() {
        return PROCESS_ID == ROOT;
    }

    void dump_snapshot(int system_total, int total_transaction) {
        snap_count++;
        snap_dump << "===============================================================================\n";
        snap_dump << "SNAPSHOT " << snap_count << endl;
        for(auto &s: all_snapshots) {
            snap_dump << "pid=" << s.pid << ", transferred=" << s.transferred << ", current=" << s.current << endl;
            for(auto &m: s.msgs) {
                snap_dump << "\t{from=" << int(m.from) << ",to=" << int(m.to) << ",amount=" << m.amount << "}\n";
            }
            snap_dump << "\n";
        }
        snap_dump << "SNAPSHOT: total_process="<<all_snapshots.size()<<", system_total="<<system_total<<", total_transaction="<<total_transaction<<"\n";
    }

    bool all_markers_received() {
        for(auto p: to_recv) {
            if(!marker_received[p]) {
                return false;
            }
        }
        return true;
    }

    // Used by root.
    bool all_snapshots_received() {
        for(int i=0; i<N; i++) {
            if(!snapshot_received[i]) {
                return false;
            }
        }
        return true;
    }

    // Used to start snapshot process if not started yet.
    void trigger_snapshot() {
        if(color == RED || end.load()) {
            return;
        }
        send_mtx.lock();
        // Taking my snapshot.
        color = RED;
        all_snapshots.clear();
        my_snapshot.clear();
        my_snapshot.pid = PROCESS_ID;
        my_snapshot.transferred = total_amount_sent.load();
        my_snapshot.current = total_amount.load();
        for(auto p: to_recv) {
            marker_received[p] = false;
        }
        if(root()) {
            for(int i=0; i<N; i++) {
                snapshot_received[i] = false;
            }
        }

        // send marker
        void *cpy = send_vector;
        cpy = write_char(cpy, CONTROL);
        cpy = write_char(cpy, MARKER);
        cpy = write_int(cpy, PROCESS_ID);
        for(auto p: to_send) {
            auto timestamp = chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
            if(MPI_Isend(send_vector, (2*sizeof(char)) + sizeof(int), MPI_BYTE, p, 0, MPI_COMM_WORLD, &request) == 0) {
                printf("[%ld] MARKER: %d -> %d\n", timestamp, PROCESS_ID, p);
                fflush(stdout);
            } else {
                printf("### SEND ERROR\n"); fflush(stdout);
            }
        }
        send_mtx.unlock();

        if(root() && all_markers_received()) {
            // Root.
            // Collect own snapshot if received all markers.
            if(!snapshot_received[PROCESS_ID]) {
                all_snapshots.push_back(my_snapshot);
                snapshot_received[PROCESS_ID] = true;
            }
            color = WHITE;
            if(all_snapshots_received()) {
                root_termination_check();
            }
        }
    }

    // Check if termination condition is reached.
    // Used only by root.
    void root_termination_check() {
        int system_total = 0;
        int total_transaction = 0;
        sort(all_snapshots.begin(), all_snapshots.end(), [](const snapshot& lhs, const snapshot& rhs) {
            return lhs.pid < rhs.pid;
        });
        for(auto &s: all_snapshots) {
            system_total += s.current;
            for(auto &m: s.msgs) {
                // Considering in-transit message amount in the total.
                system_total += m.amount;
            }
            total_transaction += s.transferred;
        }
        dump_snapshot(system_total, total_transaction);
        auto timestamp = chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
        printf("[%ld] SNAPSHOT: total_process=%lu, system_total=%d, total_transaction=%d\n", timestamp, all_snapshots.size(), system_total, total_transaction);
        fflush(stdout);
        if(total_transaction > MAX_TRANSACTION) {
            // Transation limit reached.
            printf("[%ld] Snapshot termination\n", timestamp);
            send_all_terminations();
            end_proc();
        }
    }

    void send_all_terminations() {
        send_mtx.lock();
        void *cpy = send_vector;
        cpy = write_char(cpy, TERMINATE);
        for(auto p: to_send_terminate) {
            auto timestamp = chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
            if(MPI_Isend(send_vector, sizeof(char), MPI_BYTE, p, 0, MPI_COMM_WORLD, &request) == 0) {
                printf("[%ld] SENT TERMINATE: %d -> %d \n", timestamp, PROCESS_ID, p);
                fflush(stdout);
            } else {
                printf("### SEND ERROR\n"); fflush(stdout);
            }
        }
        send_mtx.unlock();
    }

    void send_termination_to(int p) {
        send_mtx.lock();
        void *cpy = send_vector;
        cpy = write_char(cpy, TERMINATE);
        auto timestamp = chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
        if(MPI_Isend(send_vector, sizeof(char), MPI_BYTE, p, 0, MPI_COMM_WORLD, &request) == 0) {
            printf("[%ld] SENT TERMINATE: %d -> %d \n", timestamp, PROCESS_ID, p);
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
            // If it a terminate, we only process it if it's not a root.
            void *cpy = malloc(size);
            memcpy(cpy, recv_vector, size);
            // Add the packet into the buffer which will be processed by another thread.
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
            auto timestamp = chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
            switch(type) {
                // DATA
                case DATA: {
                    message m;
                    m.unmarshal(data);
                    total_amount += m.amount;
                    total_amount_recv += m.amount;
                    printf("[%ld] (%d) AMOUNT RECEIVED (%d <- %d): %d\n", timestamp, m.id, PROCESS_ID, m.from, m.amount);
                    fflush(stdout);
                    if(color == RED && m.color == WHITE && !marker_received[m.from]) {
                        my_snapshot.msgs.push_back(m);
                    }
                    break;
                } // DATA
                // CONTROL
                case CONTROL: {
                    total_control_msg_rcvd++;
                    CONTROL_TYPE ctype = CONTROL_TYPE(read_char(&data));
                    int from = read_int(&data);
                    switch(ctype) {
                        // CONTROL, MARKER
                        case MARKER: {
                            printf("[%ld] %d received MARKER from %d\n", timestamp, PROCESS_ID, from);
                            fflush(stdout);
                            if(color==WHITE) {
                                trigger_snapshot();
                            }
                            marker_received[from] = true;
                            if(all_markers_received()) {
                                send_mtx.lock();
                                timestamp = chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
                                printf("[%ld] SNAPSHOT DONE: %d\n", timestamp, PROCESS_ID);
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
                                timestamp = chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
                                if(MPI_Isend(send_vector, size, MPI_BYTE, to_send_snapshot, 0, MPI_COMM_WORLD, &request) == 0) {
                                    printf("[%ld] SNAPSHOT SENT: %d -> %d\n", timestamp, PROCESS_ID, to_send_snapshot);
                                    fflush(stdout);
                                } else {
                                    printf("### SEND ERROR\n"); fflush(stdout);
                                }

                                color = WHITE;
                                send_mtx.unlock();
                            }
                            break;
                        } // MARKER
                        // CONTROL, SNAPSHOT
                        case SNAPSHOT: {
                            printf("[%ld] Process %d got snapshot from %d\n", timestamp, PROCESS_ID, from);
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
                            timestamp = chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
                            if(MPI_Isend(msg.data, msg.size, MPI_BYTE, to_send_snapshot, 0, MPI_COMM_WORLD, &request) == 0) {
                                printf("[%ld] FORWARD SNAPSHOT: %d -> %d\n", timestamp, PROCESS_ID, to_send_snapshot);
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
                // TERMINATE
                case TERMINATE: {
                    printf("[%ld] TERMINATE: %d\n", timestamp, PROCESS_ID);
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
        usleep((*distribution)(generator)*3000000); // simulation some process.
        auto timestamp = chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
        printf("[%ld] Process %d executes internal event\n", timestamp, PROCESS_ID);
        fflush(stdout);
    };

    // A single message send event.
    void send_event() {
        // Process to sent message to.
        curr_send_pointer = (curr_send_pointer+1)%to_send.size();
        int send_to = to_send[curr_send_pointer];

        send_mtx.lock();
        // Randomly selection an amount to send. 
        message m(SEND_MSG_ID++, PROCESS_ID, send_to, color);
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

        // Sending message.
        void *ptr = write_char(send_vector, DATA);
        int message_size = m.marshal(ptr) + sizeof(char);
        auto timestamp = chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
        if(MPI_Isend(send_vector, message_size, MPI_BYTE, send_to, 0, MPI_COMM_WORLD, &request) == 0) {
            total_amount -= transaction_amount;
            total_amount_sent += transaction_amount;
            printf("[%ld] Process %d sends %d to process %d\n", timestamp, PROCESS_ID, transaction_amount, send_to);
            fflush(stdout);
        } else {
            printf("### SEND ERROR\n"); fflush(stdout);
        }
        send_mtx.unlock();
    };
};

int main(int argc, const char* argv[]) {
    srand(time(NULL));

    MPI_Init(NULL, NULL);
    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    PROCESS_ID = world_rank;

    // Input from file.
    ifstream inFile;
    inFile.open("inp-params.txt");
    int N, A, T;
    float lambda;
    vector<int> to_send, to_recv, to_send_terminate;
    inFile >> N >> A >> T >> lambda;
    assert(N > 0); assert(N == world_size); assert(PROCESS_ID < N);
    assert(A > 0); assert(T > 0);           assert(lambda > 0);

    MAX_TRANSACTION = T;
    SEND_MSG_ID = 0;

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

    // Input of spanning tree.
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

    // Receiving messages thread.
    thread recv_thread([](process *p){
        while(!(p->end.load())) {
            if(!(p->recv_event()))
                break;
        }
    }, &p);

    // Processing messages thread.
    thread process_msg_thread([](process *p){
        p->process_messages();
    }, &p);

    // Thread to trigger snapshots.
    thread snapshot_trigger_thread([](process *p){
        if(!p->root()) {
            return;
        }
        while(!(p->end.load())) {
            p->trigger_snapshot();
            sleep(1);
        }
    }, &p);
    
    // Send messages and perform internal events.
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
        // The process with id next to root sends terminate to root.
        // Else root will be stuck at MPI_Recv.
        p.send_termination_to(ROOT);
    }

    printf("Ending Process %d, Total control messages received = %d\n", PROCESS_ID, p.total_control_msg_rcvd.load());
    fflush(stdout);

    if(p.root()) {
        printf("Total snapshots = %d\n", p.snap_count);
        fflush(stdout);
    }

    MPI_Finalize();
    return 0;
}