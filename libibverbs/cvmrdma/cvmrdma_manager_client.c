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

#include "debug.h"
#include "cvmrdma_ack.h"


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
    // this is just send for easier printing
    wr.u.tcp.msg.send_qp = qp->user_qp.qp.qp_num;


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
    uint16_t dlid = info->dlid;
    uint8_t  port_num = info->port_num;

    /*fprintf(stderr, "lib %s: Waiting for response from manager for QPN %u\n",
        lib_get_application_name(),
        qp->user_qp.qp.qp_num
    );*/


    struct tcp_client_msg msg;
    int rc = recv_all(client_socket, &msg, sizeof(msg));
    if (rc < 0) {
        /*fprintf(stderr, "lib %s: Failed to receive response from manager for QPN %u\n",
            lib_get_application_name(),
            qp->user_qp.qp.qp_num
        );*/

        close(client_socket);
        return;
    }

    if (msg.qpn != qp->user_qp.qp.qp_num) {
        fprintf(stderr, "Received response for wrong qp\n");
        exit(1);
    }

    /*fprintf(stderr, "lib %s: Received key for QPN %u from remote QPN %u: remote ring rkey %lu, control rkey %u, control QPN %u, control PSN %u\n",
		lib_get_application_name(),
        msg.qpn,
        msg.send_qp,
        msg.local_ring_rkey,
        msg.control_rkey,
        msg.control_qpn,
        msg.control_psn
    );*/

    pthread_mutex_lock(&qp->remote_params_lock);
    qp->remote_ring_rkey = msg.local_ring_rkey;
    qp->remote_control_qpn = msg.control_qpn;
    qp->remote_control_rkey = msg.control_rkey;
    qp->remote_control_psn = msg.control_psn;
    qp->remote_params_set = true;
    pthread_mutex_unlock(&qp->remote_params_lock);

    pthread_cond_broadcast(&qp->remote_params_set_cv);

    disconnect_from_manager(client_socket, msg.qpn);

    start_control_queue(qp, dlid, port_num);

    return;
}

void start_socket_server(pthread_t* thread, int client_socket, struct cvmrdma_qp* qp, uint16_t dlid, uint8_t port_num) {

    struct server_socket_info* info = malloc(sizeof(struct server_socket_info));
    info->client_socket = client_socket;
    info->qp = qp;
    info->dlid = dlid;
    info->port_num = port_num;


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
    shutdown(client_socket, SHUT_WR);
    close(client_socket);
    return 0;
}

void start_control_queue(struct cvmrdma_qp * qp, uint16_t dlid, uint8_t port_num)
{
	/*struct start_args * start_args = (struct start_args *)args;
	struct cvmrdma_qp * qp = start_args->qp;
	uint16_t dlid = start_args->dlid;
    uint8_t port_num = start_args->port_num;*/

    struct cvmrdma_context * ctxt = cvmrdma_get_ctx(qp->user_qp.qp.context);
    int ret = 0;

    // this is probably not needed anymore, as we only call this function after receiving the remote params
	pthread_mutex_lock(&qp->remote_params_lock);
	while (!qp->remote_params_set) {
		pthread_cond_wait(&qp->remote_params_set_cv, &qp->remote_params_lock);
	}
	pthread_mutex_unlock(&qp->remote_params_lock);

	if (atomic_load(&qp->stop_requested)) {
		fprintf(stderr, "QP %u: stop requested, not starting control queue\n", qp->user_qp.qp.qp_num);
		return;
	}

	// fprintf(stderr, "Remote params: ring rkey %u, control QPN %u, control rkey %u, control PSN %u\n", qp->remote_ring_rkey, qp->remote_control_qpn, qp->remote_control_rkey, qp->remote_control_psn);

	fprintf(stderr, "[MODIFY_QP] QP %u with control QP %u Received remote ring rkey %u, remote control rkey %u, remote control QPN 0x%" PRIx32 ", remote control PSN %u\n",
				qp->user_qp.qp.qp_num,
				qp->control_qp->qp_num,
				qp->remote_ring_rkey,
				qp->remote_control_rkey,
				qp->remote_control_qpn,
				qp->remote_control_psn);

    struct ibv_qp_attr control_qp_attr = {0};
    control_qp_attr.qp_state = IBV_QPS_RTR;
    control_qp_attr.ah_attr.is_global = 0;
    control_qp_attr.ah_attr.dlid = dlid;
    control_qp_attr.ah_attr.port_num = port_num;
    control_qp_attr.path_mtu = IBV_MTU_4096;
    control_qp_attr.dest_qp_num = qp->remote_control_qpn;
    control_qp_attr.rq_psn = qp->remote_control_psn;

    int control_qp_attr_mask = IBV_QP_STATE;
    control_qp_attr_mask |= IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN;

    control_qp_attr.max_dest_rd_atomic = 1;
    control_qp_attr.min_rnr_timer = 12;
    control_qp_attr.timeout = 21;

    control_qp_attr_mask |= IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;

    ret = ctxt->prov_ops.modify_qp(qp->control_qp, &control_qp_attr, control_qp_attr_mask);
    if(ret) {
        fprintf(stderr, "Failed to modify control QP to RTR (%d)\n", ret);
        exit(1);
    }

    for (int i = 0; i < 4096; i++) {
		// Post some dummy recv WRs to be able to receive control messages
		// we can't submit empty WRs, as then post_recv fails
        struct ibv_sge dummy_sge = {
            .addr   = (uintptr_t)qp->control_buf_va + 1 * sizeof(struct control_metadata),          /* any valid MR addr */
            .length = 1,                                      /* minimal non-zero length */
            .lkey   = qp->control_buf_mr->lkey
        };
        struct ibv_recv_wr recv_wr = {
            .wr_id   = 0,
            .sg_list = &dummy_sge,
            .num_sge = 1,
            .next    = NULL
        };
        struct ibv_recv_wr * bad_wr = NULL;
        ret = ctxt->prov_ops.post_recv(qp->control_qp, &recv_wr, &bad_wr);
        if(ret) {
            fprintf(stderr, "Failed to post recv initial RR for control QP (%d)\n", ret);
            exit(1);
        }
    }

    control_qp_attr_mask = IBV_QP_STATE | IBV_QP_SQ_PSN;
    control_qp_attr = (struct ibv_qp_attr){
        .qp_state = IBV_QPS_RTS,
        .sq_psn = 0,
    };
    control_qp_attr.sq_psn = qp->local_control_psn;
    control_qp_attr_mask |= IBV_QP_SQ_PSN;

    control_qp_attr.timeout = 14;
    control_qp_attr.retry_cnt = 7;
    control_qp_attr.rnr_retry = 7;
    control_qp_attr.max_dest_rd_atomic = 1;

    control_qp_attr_mask |= IBV_QP_MAX_QP_RD_ATOMIC | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_TIMEOUT;

    ret = ctxt->prov_ops.modify_qp(qp->control_qp, &control_qp_attr, control_qp_attr_mask);
    if(ret) {
        fprintf(stderr, "Failed to modify control QP belonging to QP %u to RTS (%d)\n", qp->user_qp.qp.qp_num, ret);
        exit(1);
    }

	pthread_mutex_lock(&qp->control_qp_connection_lock);
	qp->control_qp_connected = true;
    pthread_cond_broadcast(&qp->control_qp_connection_cv);
	pthread_mutex_unlock(&qp->control_qp_connection_lock);


    /*fprintf(stderr, "lib %s: Control QP for QPN %u is now in RTS state, control queue started\n",
        lib_get_application_name(),
        qp->user_qp.qp.qp_num,
        qp->control_qp->qp_num,
        dlid,
        port_num
    );*/

	return;
}
