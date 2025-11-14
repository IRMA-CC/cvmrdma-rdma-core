#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdatomic.h>
#include "cvmrdma_objects.h"

struct tcp_client_msg {
    uint64_t qpn;
    uint64_t local_ring_rkey;
};

void start_server(pthread_t* thread, const char* bind_addr);
void stop_server(pthread_t* thread);
void register_qp_with_server(uint32_t qpn, struct cvmrdma_qp *qp);
void unregister_qp_from_server(uint32_t qpn);

int connect_to_server(const char* server_ip, struct tcp_client_msg* message);