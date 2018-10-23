/*
**   Raymond's Algorithm for Distributed Mutual Exclusion.
**   Author: Ganesh Vernekar (CS15BTECH11018)
**/

#include <thread>
#include <atomic>
#include <vector>
#include <mutex>
#include <random>
#include <chrono>
#include <fstream>
#include <queue>
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

// Structure of a packet.
struct packet {
    PACKET_TYPE type;
    int from; // the sender of the packet.

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
        return sizeof(char) + sizeof(int);
    }

    // Decode the packet from bytes.
    void unmarshal(void *original) {
        void *ptr = original;
        type = PACKET_TYPE(read_char(&ptr));
        from = read_int(&ptr);
    }
};

// process represents a single instance of a distributed system.
class process {
public:

    atomic_bool has_token, // 'true' if the process has the token.
                in_cs, // 'true' if the process is in CS.
                triggered_cs; // 'true' if the process has triggered a request for CS (and not exited CS).

    int token_holder; // Neighbour which holds token.
    atomic_int num_cs_entry, // No. of time the process entered CS. 
               total_control_msg; // Total control messages sent for mutex algorithm.
    
    // Queue which holds the requests for the token.
    queue<int> req_queue;
    // mutex to protect the request queue.
    mutex queue_mtx;

    // Buffers re-used to receive and send packets. 
    void *recv_vector, *send_vector;

    // Array of size TOTAL_PROCS.
    // ended[i] is true if i'th process has sent TERMINATE message.
    bool *ended;

    // Used to mark the start of timer for response time.
    int64_t start_trigger;
    // Sum of all response times.
    int64_t total_response_time;

    process():
        has_token(false),
        in_cs(false),
        triggered_cs(false),
        num_cs_entry(0),
        total_control_msg(0) {}

    process(int token_holder):
        has_token(false),
        in_cs(false),
        triggered_cs(false),
        num_cs_entry(0),
        total_control_msg(0),
        token_holder(token_holder)
    {
        recv_vector = malloc(max_packet_size);
        send_vector = malloc(max_packet_size);

        ended = new bool[TOTAL_PROCS];
        for(int i=0; i<TOTAL_PROCS; i++) {
            ended[i] = false;
        }

        if(token_holder == PROCESS_ID) {
            // Root of the tree, hence holds the token.
            auto timestamp = chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
            printf("[%ld] %d has token\n", timestamp, PROCESS_ID); 
            fflush(stdout);
            has_token.store(true);
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
        for(int i=0; i<TOTAL_PROCS; i++) {
            if(i == PROCESS_ID) continue;
            send_packet(p, i);
        }
        ended[PROCESS_ID] = true;
    }

    // A single message receive event.
    void recv_event() {
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
                queue_mtx.lock();

                auto timestamp = chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
                printf("[%ld] Process %d received REQUEST from %d\n", timestamp, PROCESS_ID, p.from);
                fflush(stdout);

                req_queue.emplace(p.from);

                if(req_queue.front() == p.from && has_token.load() && !in_cs.load()) {
                    // The process has ideal token, hence send the token
                    // to the process which requested.
                    req_queue.pop();
                    packet tkn(TOKEN, PROCESS_ID);
                    send_packet(tkn, p.from);

                    token_holder = p.from; // Update token holder.
                    has_token.store(false);
                } else if(!has_token.load() && req_queue.size()==1) {
                    // The process doesn't hold any token, hence request the token holder.
                    request_token();
                }

                queue_mtx.unlock();
                break;
            } // REQUEST
            case TOKEN: {
                queue_mtx.lock();

                auto timestamp = chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
                printf("[%ld] Process %d received TOKEN from %d\n", timestamp, PROCESS_ID, p.from);
                fflush(stdout);
                
                has_token.store(true);
                token_holder = PROCESS_ID;
                int cs_winner = req_queue.front();
                req_queue.pop();

                if(cs_winner == PROCESS_ID) {
                    // I am at the top of the queue, hence enter CS.
                    in_cs.store(true);
                    queue_mtx.unlock();
                    
                    // CS computation in another thread so that we dont block
                    // receiving of packets.
                    thread([this](){
                        cs_computation();
                    }).detach();
                } else {
                    // Forward the token to the process which is at the top of the queue.
                    packet tkn(TOKEN, PROCESS_ID);
                    send_packet(tkn, cs_winner);
                    has_token.store(false);
                    token_holder = cs_winner;
                    if(req_queue.size() > 0) {
                        // More requests exist. Hence request the token again from
                        // the new token holder.
                        request_token();
                    }
                    queue_mtx.unlock();
                }

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
        queue_mtx.lock();

        // Marking the start of request to calculate response time later.
        start_trigger = chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();

        triggered_cs.store(true);
        req_queue.emplace(PROCESS_ID);

        if(req_queue.front() == PROCESS_ID && has_token.load()) {
            // I have the token and I am at the top of the queue.
            // Hence enter the CS.
            req_queue.pop();
            in_cs.store(true);
            queue_mtx.unlock();
            cs_computation();
            return;
        }
        
        if(!has_token.load() && req_queue.size()==1) {
            // I don't have the token, hence request it from token holder.
            request_token();
        }
        queue_mtx.unlock();
    }

    // Requests token from token holder.
    void request_token() {
        packet p(REQUEST, PROCESS_ID);
        send_packet(p, token_holder);
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
        queue_mtx.lock();
        in_cs.store(false);
        triggered_cs.store(false);
        
        assert(has_token.load());
        assert(token_holder == PROCESS_ID);
        if(req_queue.size() > 0) {
            // Token requests are pending. Hence send the token.
            int cs_winner = req_queue.front();
            req_queue.pop();
            assert(cs_winner != PROCESS_ID);

            packet tkn(TOKEN, PROCESS_ID);
            send_packet(tkn, cs_winner);
            
            has_token.store(false);
            token_holder = cs_winner;

            if(req_queue.size() > 0) {
                // More requests in the queue, hence request again.
                request_token();
            }
        }

        queue_mtx.unlock();
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

    // Getting token holder from the tree input.
    int token_holder = 0;
    for(int i=0; i<n; i++) {
        inFile >> token_holder;
        if(i == PROCESS_ID) {
            break;
        }
    }

    process p(token_holder);

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