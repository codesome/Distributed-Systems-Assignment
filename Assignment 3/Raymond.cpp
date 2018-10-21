/*
**   Author: Ganesh Vernekar (CS15BTECH11018)
**/

#include <thread>
#include <atomic>
#include <algorithm>
#include <vector>
#include <string>
#include <mutex>
#include <map>
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

int PROCESS_ID, 
    TOTAL_PROCS,
    MAX_CS_ENTRY;

// For random numbers.
default_random_engine generator;
exponential_distribution<double> *cs_distribution, *internal_distribution;

// dummy, just to satisfy function argument.
MPI_Request request; 

struct packet {
    PACKET_TYPE type;
    int from;

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
    
    int marshal(void *original) {
        void *ptr = original;
        ptr = write_char(ptr, type);
        ptr = write_int(ptr, from);
        return sizeof(char) + sizeof(int);
    }
    void unmarshal(void *original) {
        void *ptr = original;
        type = PACKET_TYPE(read_char(&ptr));
        from = read_int(&ptr);
    }
};

// process represents a single instance of a distributed system.
class process {
public:

    atomic_bool has_token, 
                in_cs,
                triggered_cs;

    int token_holder;
    atomic_int num_cs_entry, total_control_msg;
    
    mutex queue_mtx;
    queue<int> req_queue;

    void *recv_vector, *send_vector;

    bool *ended;

    int64_t total_response_time;
    int64_t start_trigger;

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
            auto timestamp = chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
            printf("[%ld] %d has token\n", timestamp, PROCESS_ID); fflush(stdout);
            has_token.store(true);
        }
    }

    bool is_send_end() {
        return num_cs_entry.load() >= MAX_CS_ENTRY;
    }

    bool rest_all_ended() {
        for(int i=0; i<TOTAL_PROCS; i++) {
            if(i == PROCESS_ID) continue;
            if(!ended[i]) {
                return false;
            }
        }
        return true;
    }

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
                    req_queue.pop();
                    packet tkn(TOKEN, PROCESS_ID);
                    send_packet(tkn, p.from);
                    has_token.store(false);
                    token_holder = p.from;
                } else if(!has_token.load() && req_queue.size()==1) {
                    request_token();
                }

                queue_mtx.unlock();
                break;
            }
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
                    in_cs.store(true);
                    queue_mtx.unlock();
                    thread([this](){
                        cs_computation();
                    }).detach();
                } else {
                    packet tkn(TOKEN, PROCESS_ID);
                    send_packet(tkn, cs_winner);
                    has_token.store(false);
                    token_holder = cs_winner;
                    if(req_queue.size() > 0) {
                        tkn.type = REQUEST;
                        send_packet(tkn, token_holder);
                    }
                    queue_mtx.unlock();
                }

                break;
            }
            case TERMINATE: {
                auto timestamp = chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
                printf("[%ld] Process %d received TERMINATE from %d\n", timestamp, PROCESS_ID, p.from);
                fflush(stdout);

                ended[p.from] = true;
            }
            default: {
                break;
            }
        }

    }

    void trigger_cs() {
        if(triggered_cs.load() || in_cs.load() || is_send_end()) return;
        queue_mtx.lock();

        start_trigger = chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();

        triggered_cs.store(true);
        req_queue.emplace(PROCESS_ID);

        if(req_queue.front() == PROCESS_ID && has_token.load()) {
            req_queue.pop();
            in_cs.store(true);
            queue_mtx.unlock();
            cs_computation();
            return;
        }
        
        if(!has_token.load() && req_queue.size()==1) {
            request_token();
        }
        queue_mtx.unlock();
    }

    void request_token() {
        packet p(REQUEST, PROCESS_ID);
        send_packet(p, token_holder);
    }

    void local_computation() {
        if(triggered_cs.load() || in_cs.load()) return;
        usleep((*internal_distribution)(generator)*3000000); // simulation some process.
        auto timestamp = chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
        printf("[%ld] Process %d executes internal event\n", timestamp, PROCESS_ID);
        fflush(stdout);
    };

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

    void cs_leaving_task() {

        queue_mtx.lock();
        in_cs.store(false);
        triggered_cs.store(false);
        
        assert(has_token.load());
        assert(token_holder == PROCESS_ID);
        if(req_queue.size() > 0) {
            int cs_winner = req_queue.front();
            req_queue.pop();
            assert(cs_winner != PROCESS_ID);

            packet tkn(TOKEN, PROCESS_ID);
            send_packet(tkn, cs_winner);
            has_token.store(false);
            token_holder = cs_winner;
            if(req_queue.size() > 0) {
                tkn.type = REQUEST;
                send_packet(tkn, token_holder);
            }
        }

        queue_mtx.unlock();
    }

    // A single message send event.
    void send_packet(packet p, int to) {
        if(p.type != TERMINATE) {
            total_control_msg++;
        }
        auto timestamp = chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
        printf("[%ld] sending %s: %d -> %d\n", timestamp, p.type_string().c_str(), PROCESS_ID, to);
        fflush(stdout);
        assert(to >= 0 && to < TOTAL_PROCS);
        if(MPI_Isend(send_vector, p.marshal(send_vector), MPI_BYTE, to, 0, MPI_COMM_WORLD, &request) == 0) {
        } else {
            printf("### SEND ERROR\n"); fflush(stdout);
        }
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
    int n, k;
    float alpha, beta;
    inFile >> n >> k >> alpha >> beta;
    assert(n == world_size); assert(PROCESS_ID < n);
    assert(beta < alpha);    assert(k > 0);

    TOTAL_PROCS = n;
    MAX_CS_ENTRY = k;

	internal_distribution = new exponential_distribution<double>(alpha);
	cs_distribution = new exponential_distribution<double>(beta);

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