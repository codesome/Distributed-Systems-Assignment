#ifndef __NETWORK_H__
#define __NETWORK_H__

#include <thread>
#include <functional>
#include <string>

class network_process {
public:
    int recv_sock, send_sock, listen_port, max_conn;
};

class network {

public:

    network_process *np;
    thread *recv_thread;
    
    int max_size;
    void *recv_data;

    // void (int status_code, int errno, void *data_received, int reveived_data_len)
    function<void(int,int,void*,int)> recv_callback;
    // void (int status_code, int errno)
    function<void(int, int)> send_callback;

    bool end;

    network(network_process *np): np(np), end(false) {}

    int start(int ms, function<void(int,int,void*,int)> rc, function<void(int, int)> sc) {
        max_size = ms;
        recv_callback = rc;
        send_callback = sc;
        recv_data = malloc(max_size);

        sockaddr_in server;
        bzero((char *) &server, sizeof(server));
        server.sin_family = AF_INET;
        server.sin_port = htons(np->listen_port);
        server.sin_addr.s_addr = htonl(INADDR_ANY);
        if(bind(np->recv_sock, (struct sockaddr *) &server, sizeof(server)) < 0) {
            printf("bind error\n");
            return -1;
        }
        if(listen(np->recv_sock, (np->max_conn)*5) < 0) {
            printf("listen error\n");
            return -1;
        }
        printf("Listening on port %d\n", np->listen_port);
        
        timeval option_value;
        option_value.tv_sec = 1;
        option_value.tv_usec = 0;
        setsockopt(np->recv_sock, SOL_SOCKET, SO_RCVTIMEO, (char *) &option_value, sizeof(timeval));

        network_recv();

        return 0;
    }

    void network_recv() {
        recv_thread = new thread([this](){
            np->recv_sock = socket(AF_INET, SOCK_STREAM, 0);
            while(!end) {
                // accept connection, receive, and update vector
                // alternatively do blank event
                unsigned int len = sizeof(sockaddr_in);
                sockaddr_in client;
                char recv_msg[1024];
                int size;
                string send_msg;

                // accepting a connection
                int client_sockid = accept(np->recv_sock, (struct sockaddr *) &client, &len);
                if(errno == EAGAIN || errno == EWOULDBLOCK) {
                    printf("timeout\n");
                    fflush(stdout);
                    continue;
                }
                if(client_sockid < 0) {
                    printf("accept error\n");
                    fflush(stdout);
                    recv_callback(client_sockid, errno, NULL, 0);
                }
                fflush(stdout);

                size = recv(client_sockid, recv_data, max_size, 0);
                recv_callback(0, 0, recv_data, size);
            }

        });

    }

    void network_send(string ip, int port, void *msg, int msg_len){

        np->send_sock = socket(AF_INET, SOCK_STREAM, 0);

        // server info
        sockaddr_in server;
        bzero((char *) &server, sizeof(server));
        server.sin_family = AF_INET;
        server.sin_port = htons(port);
        server.sin_addr.s_addr = inet_addr(ip.c_str());

        // connecting to a server
        int server_sockid;
        if((server_sockid=connect(np->send_sock, (struct sockaddr *) &server, sizeof(server))) < 0) {
            if(errno != EISCONN) {
                send_callback(server_sockid, errno);
            }
        }

        int status = send(np->send_sock, msg, msg_len, 0);
        if(status < 0) {
            send_callback(status, errno);
        }
        send_callback(0,0);

        close(server_sockid);
        close(np->send_sock);

    }

    void end() {
        end = true;
        recv_thread->join();
    }

};

#endif