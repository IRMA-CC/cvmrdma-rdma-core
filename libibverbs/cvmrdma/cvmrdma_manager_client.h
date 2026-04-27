#include <stdint.h>
#include "cvmrdma_objects.h"

int try_to_start_manager();
int connect_to_manager(uint32_t qpn, int* client_socket);
int send_and_wait_for_key(int client_socket, struct cvmrdma_qp *qp, char* server_ip, uint32_t dest_qpn, uint32_t rkey, uint32_t control_rkey, uint32_t control_qpn, uint32_t control_psn);
// int disconnect_from_manager(int client_socket, uint32_t qpn);
void start_socket_server(pthread_t* thread, int client_socket, struct cvmrdma_qp* qp, uint16_t dlid, uint8_t port_num);


struct tcp_client_msg {
    uint32_t qpn;
    uint32_t local_ring_rkey;
    uint32_t control_rkey;
    uint32_t control_qpn;
    uint32_t control_psn;
    uint32_t send_qp;

};

enum manager_wr_type {
    MANAGER_WR_REGISTER_QP = 0,
    MANAGER_WR_UNREGISTER_QP = 1,
    MANAGER_WR_SEND_KEY = 2
};

struct manager_wr_qpn {
    uint32_t qpn;
};

struct manager_wr_tcp {
    char server_ip[64];
    struct tcp_client_msg msg;
};

struct manager_wr {
    enum manager_wr_type type;
    union {
        struct manager_wr_qpn qpn;
        struct manager_wr_tcp tcp;
    } u;
};

struct server_socket_info {
    int client_socket;
    struct cvmrdma_qp* qp;
    uint16_t dlid;
    uint8_t  port_num;
};

