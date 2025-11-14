#include "cvmrdma_tcp.h"

// Global control flag
atomic_int server_running = 1;
// Server socket (needed so stop_server() can shut it down)
int server_fd = -1;

struct qp_entry {
    uint32_t qpn;
    struct cvmrdma_qp *qp;
};

#define MAX_QPS 1024
struct qp_entry qp_table[MAX_QPS];
int num_qps = 0;

struct cvmrdma_qp * lookup_qp(uint32_t qpn) {
    for (int i = 0; i < num_qps; i++) {
        if (qp_table[i].qpn == qpn) {
            return qp_table[i].qp;
        }
    }
    return NULL;
}

int parse_server_address(const char* input, struct sockaddr_in* out) {
    char port_str[16];

    const char* colon = strchr(input, ':');
    if (!colon) return -1;

    strcpy(port_str, colon + 1);
    int port = atoi(port_str);
    if (port <= 0 || port > 65535) return -1;

    out->sin_family = AF_INET;
    out->sin_port = htons(port);

    if (strncmp(input, "*", 1) == 0) {
        out->sin_addr.s_addr = INADDR_ANY;
    } else {
        fprintf(stderr, "Specified IP address is not supported, use '*'\n");
        return -1;
    }

    return 0;
}

// -------------------------------------------------------
//          Gracefully stop the server thread
// -------------------------------------------------------
void stop_server(pthread_t* thread) {
    atomic_store(&server_running, 0);

    // This unblocks accept()
    if (server_fd >= 0)
        shutdown(server_fd, SHUT_RDWR);

    pthread_join(*thread, NULL);
    if (server_fd >= 0) {
        close(server_fd);
        server_fd = -1;
    }
}

// -------------------------------------------------------
//                Server thread
// -------------------------------------------------------
void* server_loop(void* arg) {
    int fd = *(int*)arg;

    while (atomic_load(&server_running)) {
        int client_fd = accept(fd, NULL, NULL);

        if (client_fd < 0) {
            // If stopping, accept will fail after shutdown()
            if (!atomic_load(&server_running))
                break;

            fprintf(stderr, "accept failed\n");
            continue;
        }

        char buffer[1024];
        int bytes = read(client_fd, buffer, sizeof(buffer));
        if (bytes < (int)sizeof(struct tcp_client_msg)) {
            fprintf(stderr, "short read: got %d, need %zu\n", bytes, sizeof(struct tcp_client_msg));
            close(client_fd);
            continue;
        }

        if (bytes > 0) {
            struct tcp_client_msg *msg = (struct tcp_client_msg*)buffer;
            fprintf(stderr, "Received registration for QPN %u with local ring rkey %lu\n", msg->qpn, msg->local_ring_rkey);

            struct cvmrdma_qp *qp = lookup_qp(msg->qpn);
            if (!qp) {
                fprintf(stderr, "No registered QP with QPN %u\n", msg->qpn);
                close(client_fd);
                continue;
            }

            pthread_mutex_lock(&qp->remote_params_lock);
            qp->remote_ring_rkey = msg->local_ring_rkey;
            qp->remote_params_set = true;
            pthread_mutex_unlock(&qp->remote_params_lock);

            pthread_cond_broadcast(&qp->remote_params_set_cv);
        }

        close(client_fd);
    }

    printf("Server loop exited.\n");
    return NULL;
}

// -------------------------------------------------------
//                    Start server
// -------------------------------------------------------
void start_server(pthread_t* thread, const char* bind_addr) {

    struct sockaddr_in addr;
    if (parse_server_address(bind_addr, &addr) != 0) {
        fprintf(stderr, "Invalid bind address: %s\n", bind_addr);
        exit(1);
    }

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); exit(1); }

    if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "bind failed\n");
        exit(1);
    }

    if (listen(server_fd, 10) < 0) {
        fprintf(stderr, "listen failed\n");
        exit(1);
    }

    printf("Server listening on %s\n", bind_addr);

    int err = pthread_create(thread, NULL, server_loop, &server_fd);
    if (err != 0) {
        fprintf(stderr, "pthread_create failed\n");
        exit(1);
    }
}

// -------------------------------------------------------
//                       Register QP
// -------------------------------------------------------
void register_qp_with_server(uint32_t qpn, struct cvmrdma_qp *qp) {

    if (num_qps >= MAX_QPS) {
        fprintf(stderr, "Max QPs reached\n");
        exit(1);
    }

    if (lookup_qp(qpn) != NULL) {
        fprintf(stderr, "QP with QPN %u already registered\n", qpn);
        exit(1);
    }

    qp_table[num_qps].qpn = qpn;
    qp_table[num_qps].qp = qp;
    num_qps++;
}

void unregister_qp_from_server(uint32_t qpn) {

    for (int i = 0; i < num_qps; i++) {
        if (qp_table[i].qpn == qpn) {
            // Shift remaining entries down
            for (int j = i; j < num_qps - 1; j++) {
                qp_table[j] = qp_table[j + 1];
            }
            num_qps--;
            return;
        }
    }

    fprintf(stderr, "No registered QP with QPN %u to unregister\n", qpn);
}

