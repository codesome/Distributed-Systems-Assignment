/*
**   Suzuki-Kasami's Algorithm for Distributed Mutual Exclusion.
**   Author: Ganesh Vernekar (CS15BTECH11018)
**/

#include <thread>
#include <deque>
#include <atomic>
#include <vector>
#include <mutex>
#include <random>
#include <chrono>
#include <fstream>
#include <assert.h>
#include <unistd.h>
#include <stdio.h>
#include <mpi.h>
#include "common.h"
using namespace std;

int PROCESS_ID, // Current process ID.
    TOTAL_PROCS, // Total number of processes.
    MAX_CS_ENTRY; // No. of time the process has to enter the CS.

// For random numbers.
default_random_engine generator;
exponential_distribution<double> *cs_distribution, *internal_distribution;

// dummy, just to satisfy function argument.
MPI_Request request; 

// Structure of a token.
struct token {
    vector<int> LN; // 'LN' as described in the book.
    deque<int> Q; // The queue in the token as described in the book.


    // Encode the token into storable bytes.
    int marshal(void *original) {
        void *ptr = original;

        int total_size = 0;

        ptr = write_int(ptr, LN.size());
        total_size += sizeof(int);
        for(auto v: LN) {
            ptr = write_int(ptr, v);
        }
        total_size += LN.size()*sizeof(int);
        
        ptr = write_int(ptr, Q.size());
        total_size += sizeof(int);
        for(auto v: Q) {
            ptr = write_int(ptr, v);
        }
        total_size += Q.size()*sizeof(int);

        return total_size;
    }

    // Decode the token from bytes.
    void unmarshal(void *original) {
        void *ptr = original;

        int size = read_int(&ptr);
        assert(size == TOTAL_PROCS);
        LN.clear();
        for(int i=0; i<size; i++) {
            int val = read_int(&ptr);
            LN.push_back(val);
        }

        size = read_int(&ptr);
        Q.clear();
        for(int i=0; i<size; i++) {
            int val = read_int(&ptr);
            Q.push_back(val);
        }
    }
};

// Structure of a packet.
struct packet {
    PACKET_TYPE type;
    int from; // the sender of the packet.

    // Used only for REQUEST.
    int sn; // Sequence number.

    // Used only for TOKEN.
    token tkn; // The token.

    packet() {}

    packet(PACKET_TYPE type, int from): type(type), from(from) {}

    string type_string() {
        switch(type) {
            case REQUEST:
                return string("REQUEST");
            case TOKEN:
                return string("TOKEN");
            case TERMINATE:
                return string("TERMINATE");
            default:
                return string("");
        }
    }
    
    // Encode the packet into storable bytes.
    int marshal(void *original) {
        void *ptr = original;
        ptr = write_char(ptr, type);
        ptr = write_int(ptr, from);

        int total_size = sizeof(char) + sizeof(int);

        if(type == REQUEST) {
            ptr = write_int(ptr, sn);
            total_size += sizeof(int);
        } else if(type == TOKEN) {
            total_size += tkn.marshal(ptr);
        }
        return total_size;
    }

    // Decode the packet from bytes.
    void unmarshal(void *original) {
        void *ptr = original;
        type = PACKET_TYPE(read_char(&ptr));
        from = read_int(&ptr);

        if(type == REQUEST) {
            sn = read_int(&ptr);
        } else if(type == TOKEN) {
            tkn.unmarshal(ptr);
        }
    }
};

// process represents a single instance of a distributed system.
class process {
public:

    atomic_bool has_token, // 'true' if the process has the token.
                in_cs, // 'true' if the process is in CS.
                triggered_cs; // 'true' if the process has triggered a request for CS (and not exited CS).

    atomic_int num_cs_entry, // No. of time the process entered CS. 
               total_control_msg; // Total control messages sent for mutex algorithm.
    
    // 'RN' as described in the book.
    vector<int> RN;

    // mutex to protect token related operations.
    mutex mtx;

    // Buffers re-used to receive and send packets. 
    void *recv_vector, *send_vector;

    // Array of size TOTAL_PROCS.
    // ended[i] is true if i'th process has sent TERMINATE message.
    bool *ended;

    // Contains the token when 'has_token' is true.
    token tkn;

    // Used to mark the start of timer for response time.
    int64_t start_trigger;
    // Sum of all response times.
    int64_t total_response_time;


    process():
        has_token(false),
        in_cs(false),
        triggered_cs(false),
        num_cs_entry(0),
        total_control_msg(0)
    {
        recv_vector = malloc(max_packet_size);
        send_vector = malloc(max_packet_size);

        ended = new bool[TOTAL_PROCS];
        RN.resize(TOTAL_PROCS);
        for(int i=0; i<TOTAL_PROCS; i++) {
            ended[i] = false;
            RN[i] = 0;
        }

        if(PROCESS_ID == 0) {
            // Initially the process with id 0 contains the token.
            has_token.store(true);

            // Initializing the token.
            tkn.LN.resize(TOTAL_PROCS);
            for(int i=0; i<TOTAL_PROCS; i++) {
                tkn.LN[i] = 0;
            }
        }
    }

    // End of sending CS requests.
    bool is_send_end() {
        return num_cs_entry.load() >= MAX_CS_ENTRY;
    }

    // Returns 'true' if all the processes other than the current process
    // has sent the terminate.
    bool rest_all_ended() {
        for(int i=0; i<TOTAL_PROCS; i++) {
            if(i == PROCESS_ID) continue;
            if(!ended[i]) {
                return false;
            }
        }
        return true;
    }

    // Sends TERMINATE to all the processes.
    void send_terminate_to_all() {
        packet p(TERMINATE, PROCESS_ID);
        send_to_all(p);
        ended[PROCESS_ID] = true;
    }

    // Sends REQUEST to all the processes.
    void send_request_to_all() {
        packet p(REQUEST, PROCESS_ID);
        p.sn = RN[PROCESS_ID];
        send_to_all(p);
    }

    // Send a packet to all processes.
    void send_to_all(packet p) {
        for(int i=0; i<TOTAL_PROCS; i++) {
            if(i == PROCESS_ID) continue;
            send_packet(p, i);
        }
    }

    // Send the token to a given process.
    void send_token(int to) {
        assert(has_token.load());
        packet p(TOKEN, PROCESS_ID);
        p.tkn = tkn;
        send_packet(p, to);
        has_token.store(false);
    }

    // A single message receive event.
    void recv_event() {
        // receive message.
        MPI_Status status;
        if(MPI_Recv(recv_vector, max_packet_size, MPI_BYTE, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status) != 0) {
            return;
        }
        int size;
        MPI_Get_count(&status, MPI_BYTE, &size);
        if(size <= 0) {
            return;
        }
        process_messages(size);
    };

    // Processess the received message.
    void process_messages(int size) {
        void *data = recv_vector;

        packet p;
        p.unmarshal(data);

        switch(p.type) {
            case REQUEST: {
                mtx.lock();

                auto timestamp = chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
                printf("[%ld] Process %d received REQUEST from %d\n", timestamp, PROCESS_ID, p.from);
                fflush(stdout);

                // Taking the max from my RN and from the packet.
                RN[p.from] = p.sn > RN[p.from] ? p.sn: RN[p.from];

                if(
                    has_token.load() // Has token.
                    && (RN[p.from]==tkn.LN[p.from]+1) // Next unsatisfied request.
                    && !triggered_cs.load() // Has not requested for CS.
                    && !in_cs.load() // And not in CS.
                ) {
                    // Send the token to the one asking.
                    send_token(p.from);
                }

                mtx.unlock();
                break;
            } // REQUEST
            case TOKEN: {
                // Got the token. Enter the CS.
                mtx.lock();

                auto timestamp = chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
                printf("[%ld] Process %d received TOKEN from %d\n", timestamp, PROCESS_ID, p.from);
                fflush(stdout);

                tkn = p.tkn;
                has_token.store(true);
                in_cs.store(true);

                mtx.unlock();

                // CS computation in another thread so that we dont block
                // receiving of packets.
                thread([this](){
                    cs_computation();
                }).detach();
                break;
            } // TOKEN
            case TERMINATE: {
                auto timestamp = chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
                printf("[%ld] Process %d received TERMINATE from %d\n", timestamp, PROCESS_ID, p.from);
                fflush(stdout);

                ended[p.from] = true;
                break;
            } // TERMINATE
            default: {
                break;
            }
        }

    }

    // Trigger a request for CS.
    void trigger_cs() {
        if(triggered_cs.load() || in_cs.load() || is_send_end()) return;
        mtx.lock();

        // Marking the start of request to calculate response time later.
        start_trigger = chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();

        triggered_cs.store(true);
        RN[PROCESS_ID]++;

        if(has_token.load()) {
            // I have the token, hence enter CS.
            in_cs.store(true);
            mtx.unlock();
            cs_computation();
            return;
        }

        // I don't have the token, hence request everyone.        
        send_request_to_all();
        mtx.unlock();
    }

    // Simulates some local computation.
    void local_computation() {
        if(triggered_cs.load() || in_cs.load()) return;
        usleep((*internal_distribution)(generator)*3000000); // simulation some process.
        auto timestamp = chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
        printf("[%ld] Process %d executes internal event\n", timestamp, PROCESS_ID);
        fflush(stdout);
    };

    // Simulates CS computation and calls CS exit tasks at the end.
    void cs_computation() {
        auto timestamp = chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
        printf("[%ld] Process %d starts critical section\n", timestamp, PROCESS_ID);
        fflush(stdout);

        total_response_time += (timestamp - start_trigger);
        num_cs_entry++;

        assert(has_token.load());
        usleep((*cs_distribution)(generator)*2000000); // simulation some process.
        timestamp = chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
        printf("[%ld] Process %d ends critical section\n", timestamp, PROCESS_ID);
        fflush(stdout);

        cs_leaving_task();
    };

    // Tasks to be performed while exiting CS.
    void cs_leaving_task() {
        mtx.lock();
        in_cs.store(false);
        triggered_cs.store(false);
        
        assert(has_token.load());
        // Updating LN of token.
        tkn.LN[PROCESS_ID] = RN[PROCESS_ID];
        // Adding remaining non-outdated requests into the queue.
        for(int i=0; i<TOTAL_PROCS; i++) {
            if(RN[i] == tkn.LN[i]+1) {
                // Not outdated.
                bool present = false;
                for(auto v: tkn.Q) {
                    if(v == i) {
                        present = true;
                        break;
                    }
                }
                if(!present) {
                    // Not present in the queue, hence add.
                    tkn.Q.push_back(i);
                }
            }
        }

        // Queue is not empty, hence send the token.
        if(tkn.Q.size() > 0) {
            int to = tkn.Q[0];
            tkn.Q.pop_front();
            send_token(to);
        }

        mtx.unlock();
    }

    // A single message send event.
    void send_packet(packet p, int to) {
        if(p.type != TERMINATE) {
            // TERMINATE is just an addition of this code 
            // to notify all process that I am ending.
            // Hence it should not be counted as control message 
            // for the mutex algorithm.
            total_control_msg++;
        }
        auto timestamp = chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
        printf("[%ld] sending %s: %d -> %d\n", timestamp, p.type_string().c_str(), PROCESS_ID, to);
        fflush(stdout);
        assert(to >= 0 && to < TOTAL_PROCS);
        MPI_Isend(send_vector, p.marshal(send_vector), MPI_BYTE, to, 0, MPI_COMM_WORLD, &request);
    };
};

int main(int argc, const char* argv[]) {
    srand(time(NULL));

    // Getting process id.
    MPI_Init(NULL, NULL);
    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    PROCESS_ID = world_rank;

    // Input from file.
    ifstream inFile;
    inFile.open("inp-params.txt");
    int n, k;
    float alpha, beta;
    inFile >> n >> k >> alpha >> beta;
    assert(n == world_size); assert(PROCESS_ID < n);
    assert(beta < alpha);    assert(k > 0);

    TOTAL_PROCS = n;
    MAX_CS_ENTRY = k;

	internal_distribution = new exponential_distribution<double>(alpha);
	cs_distribution = new exponential_distribution<double>(beta);

    process p;

    // Receiving messages thread.
    thread recv_thread([](process *p){
        while(!p->rest_all_ended() || (!p->is_send_end() && !p->has_token.load())) {
            p->recv_event();
        }
    }, &p);

    // Send messages and perform internal events.
    while(!p.is_send_end()) {
        p.local_computation();
        p.trigger_cs();
    }

    // I am done entering CS. Hence send TERMINATE to all.
    p.send_terminate_to_all();

    // This sleep is just to make sure that all sending messages are complete before we stop receiving.
    sleep(3);

    recv_thread.join();

    auto timestamp = chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
    printf("[%ld] Ending Process %d {total_control_msg=%d, total_response_time=%ld, count=%d}\n", 
            timestamp, PROCESS_ID, p.total_control_msg.load(), p.total_response_time, p.num_cs_entry.load());
    fflush(stdout);

    MPI_Finalize();
    return 0;
}