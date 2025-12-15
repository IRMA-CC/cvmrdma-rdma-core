#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/types.h>
#include <sys/wait.h>
#include "cvmrdma_manager_client.h"
#include "cvmrdma_objects.h"

#define SOCKET_PATH "/tmp/cvmrdma_manager_socket"

/*int start_manager() {
    pid_t pid = fork();
    if (pid < 0) {
        fprintf(stderr, "fork");
        return -1;
    } else if (pid == 0) {
        // Child executes manager
        execl("/usr/bin/cvmrdma_manager", "cvmrdma_manager", NULL);
        fprintf(stderr, "execl failed");
        exit(1);
    } else {
        // Parent waits a short moment for manager to start
        sleep(1);
        return 0;
    }
}

int try_to_start_manager() {
    int sock;
    struct sockaddr_un addr;

    sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock < 0) { fprintf(stderr, "socket failed"); exit(1); }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, SOCKET_PATH, sizeof(addr.sun_path)-1);

    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "Manager not found, starting it...\n");
        start_manager();

        // Retry connecting
        if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            fprintf(stderr, "Failed to connect to manager");
            exit(1);
        }
    }
    close(sock);
    return 0;
}*/

static int send_all(int fd, const void *buf, size_t len) {
    const unsigned char *p = buf;
    while (len) {
        ssize_t n = send(fd, p, len, 0);
        if (n < 0) return -1;
        if (n == 0) return -1; // should not happen on blocking stream
        p += n;
        len -= (size_t)n;
    }
    return 0;
}

static int recv_all(int fd, void *buf, size_t len) {
    unsigned char *p = buf;
    while (len) {
        ssize_t n = recv(fd, p, len, 0);
        if (n < 0) return -1;
        if (n == 0) return -1; // peer closed early
        p += n;
        len -= (size_t)n;
    }
    return 0;
}

int connect_to_manager(uint32_t qpn, int* client_socket) {

    int sock;
    struct sockaddr_un addr;

    sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock < 0) { fprintf(stderr, "socket failed"); exit(1); }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, SOCKET_PATH, sizeof(addr.sun_path)-1);

    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "Failed to connect to manager");
        exit(1);
    }

    struct manager_wr wr;
    wr.type = MANAGER_WR_REGISTER_QP;
    wr.u.qpn.qpn = qpn;

    // Send registration message
    // fprintf(stderr, "Client: Registering QPN %u with manager\n", qpn);
    send_all(sock, &wr, sizeof(wr));

    *client_socket = sock;
    return 0;
}

int send_and_wait_for_key(int client_socket, struct cvmrdma_qp *qp, char* server_ip, uint32_t dest_qpn, uint32_t rkey, uint32_t control_rkey, uint32_t control_qpn, uint32_t control_psn) {

    struct manager_wr wr; 
    wr.type = MANAGER_WR_SEND_KEY;
    memset(&wr.u.tcp, 0, sizeof(wr.u.tcp));
    if (server_ip) {
        strncpy(wr.u.tcp.server_ip, server_ip, sizeof(wr.u.tcp.server_ip) - 1);
        // fprintf(stderr, "Client: Connecting to server IP %s for QPN %u\n", server_ip, dest_qpn);
    }
    wr.u.tcp.msg.qpn = dest_qpn;
    wr.u.tcp.msg.local_ring_rkey = rkey;
    wr.u.tcp.msg.control_rkey = control_rkey;
    wr.u.tcp.msg.control_qpn = control_qpn;
    wr.u.tcp.msg.control_psn = control_psn;

    // Send key request message
    // fprintf(stderr, "Client: Sending key request for QPN %u to manager\n", dest_qpn);
    send_all(client_socket, &wr, sizeof(wr));

    // Wait for response
    /*struct tcp_client_msg response;
    fprintf(stderr, "Client: Waiting for response from manager for QPN %u\n", dest_qpn);
    recv_all(client_socket, &response, sizeof(response));
    fprintf(stderr, "Client: Received remote ring rkey %lu, control rkey %u, control QPN %u, control PSN %u for QPN %u\n",
        response.local_ring_rkey,
        response.control_rkey,
        response.control_qpn,
        response.control_psn,
        response.qpn
    );

    if (response.qpn != qp->user_qp.qp.qp_num) {
        fprintf(stderr, "Received response for wrong qp\n");
        exit(1);
    }

    pthread_mutex_lock(&qp->remote_params_lock);
    qp->remote_ring_rkey = response.local_ring_rkey;
    qp->remote_control_qpn = response.control_qpn;
    qp->remote_control_rkey = response.control_rkey;
    qp->remote_control_psn = response.control_psn;
    qp->remote_params_set = true;
    pthread_mutex_unlock(&qp->remote_params_lock);

    pthread_cond_broadcast(&qp->remote_params_set_cv);*/

    return 0;
}

static void* client_server_thread(void* arg) {
    struct server_socket_info* info = (struct server_socket_info*)arg;
    int client_socket = info->client_socket;
    struct cvmrdma_qp* qp = info->qp;

    struct tcp_client_msg msg;
    int rc = recv_all(client_socket, &msg, sizeof(msg));
    if (rc < 0) {
        fprintf(stderr, "Client: Disconnected from manager\n");
        close(client_socket);
        return NULL;
    }

    /*fprintf(stderr, "Client: Received registration for QPN %u with local ring rkey %u, control rkey %u, control QPN %u, control PSN %u\n",
        msg.qpn,
        msg.local_ring_rkey,
        msg.control_rkey,
        msg.control_qpn,
        msg.control_psn
    );*/

    if (msg.qpn != qp->user_qp.qp.qp_num) {
        fprintf(stderr, "Received response for wrong qp\n");
        exit(1);
    }

    pthread_mutex_lock(&qp->remote_params_lock);
    qp->remote_ring_rkey = msg.local_ring_rkey;
    qp->remote_control_qpn = msg.control_qpn;
    qp->remote_control_rkey = msg.control_rkey;
    qp->remote_control_psn = msg.control_psn;
    qp->remote_params_set = true;
    pthread_mutex_unlock(&qp->remote_params_lock);

    pthread_cond_broadcast(&qp->remote_params_set_cv);

    disconnect_from_manager(client_socket, msg.qpn);

    return NULL;
}

void start_socket_server(pthread_t* thread, int client_socket, struct cvmrdma_qp* qp) {

    struct server_socket_info* info = malloc(sizeof(struct server_socket_info));
    info->client_socket = client_socket;
    info->qp = qp;

    int err = pthread_create(thread, NULL, client_server_thread, info);
    if (err != 0) {
        fprintf(stderr, "Client: pthread_create failed\n");
        exit(1);
    }
    pthread_detach(*thread);
}

int disconnect_from_manager(int client_socket, uint32_t qpn) {
    struct manager_wr wr;
    wr.type = MANAGER_WR_UNREGISTER_QP;
    wr.u.qpn.qpn = qpn;

    // Send unregistration message
    // fprintf(stderr, "Client: Disconnecting QPN %u from manager\n", qpn);
    send_all(client_socket, &wr, sizeof(wr));
    close(client_socket);
    return 0;
}