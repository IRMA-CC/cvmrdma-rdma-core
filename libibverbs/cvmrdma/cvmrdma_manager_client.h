#include <stdint.h>
#include "cvmrdma_objects.h"

int try_to_start_manager();
int connect_to_manager(uint32_t qpn, int* client_socket);
int send_and_wait_for_key(int client_socket, struct cvmrdma_qp *qp, char* server_ip, uint64_t dest_qpn, uint64_t rkey);
int disconnect_from_manager(int client_socket, uint32_t qpn);


struct tcp_client_msg {
    uint32_t qpn;
    uint64_t local_ring_rkey;
};

enum manager_wr_type {
    MANAGER_WR_REGISTER_QP,
    MANAGER_WR_UNREGISTER_QP,
    MANAGER_WR_SEND_KEY
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

