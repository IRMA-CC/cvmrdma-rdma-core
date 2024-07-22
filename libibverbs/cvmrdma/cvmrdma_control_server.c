#include "cvmrdma_control_server.h"

#include <pthread.h>
#include <infiniband/verbs.h>

#include "cvmrdma.h"
#include "debug.h"

void *control_server_send_acks(void *args)
{
	// ACK slots
    struct thread_args *thread_args = (struct thread_args *)args;
    struct cvmrdma_qp * qp = thread_args->qp;
	struct ibv_qp *cqp = qp->control_qp;
	struct ibv_qp_ex *cqpx = ibv_qp_to_qp_ex(cqp);
	struct control_metadata md = { 0 };
	int ret;
	while (true) {
		pthread_mutex_lock(&qp->received_lines_lock);
		while (!(qp->received_lines >= CVMRDMA_ACK_THRESHOLD)) {
			pthread_cond_wait(&qp->recv_lines_ack, &qp->received_lines_lock);
		}
		md.ack_revc_lines = qp->received_lines;
        qp->received_lines = 0;
		memcpy(qp->control_buf_va, &md, sizeof(struct control_metadata));
		pthread_mutex_unlock(&qp->received_lines_lock);

        DEBUG("[CS 0x%x] Sending ACK for %ld lines for dlid %ld\n", qp->control_qp->qp_num, md.ack_revc_lines, qp->dlid);

		ibv_wr_start(cqpx);
		cqpx->wr_id = 0;
		cqpx->wr_flags = IBV_SEND_SIGNALED;

		// Say where we want to write on the remote
		ibv_wr_rdma_write_imm(cqpx, qp->remote_control_rkey, 1 * sizeof(struct control_metadata), 0);
		// What do we want to write
		ibv_wr_set_sge(cqpx, qp->control_buf_mr->lkey, 0, sizeof(struct control_metadata));

		ret = ibv_wr_complete(cqpx);
		if (ret) {
			fprintf(stderr, "[CS] Failed to complete control WR\n");
			exit(1);
		}
	}
}

void *control_server_receive_acks(void *args)
{
    struct thread_args *thread_args = (struct thread_args *)args;
    struct control_metadata md = { 0 };
    struct cvmrdma_qp *qp = thread_args->qp;
    struct ibv_cq * control_cq = qp->control_cq;
    struct verbs_context_ops prov_ops = thread_args->ctxt->prov_ops;
    int err = 0;

	err = pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
	if (err != 0)
	{
		fprintf(stderr, "pthread_setcancelstate failed: %d\n", err);
		exit(1);
	}

	while (true) {
		DEBUG("[CS 0x%x] Waiting for ACK\n", qp->control_qp->qp_num);
		struct ibv_wc wc = { 0 };
		int num_comp = 0;
		do {
			num_comp = prov_ops.poll_cq(control_cq, 1, &wc);

			err = pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
			if (err != 0)
			{
				fprintf(stderr, "pthread_setcancelstate failed: %d\n", err);
				exit(1);
			}
			pthread_testcancel();
			err = pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
			if (err != 0)
			{
				fprintf(stderr, "pthread_setcancelstate failed: %d\n", err);
				exit(1);
			}

		} while (num_comp == 0);

		DEBUG("[CS 0x%x] Control CQ polled: %d, status: %d opcode: %d \n", qp->control_qp->qp_num, num_comp, wc.status, wc.opcode);

		if (num_comp < 0) {
			fprintf(stderr, "[CS] Failed to poll control CQ: %d\n", num_comp);
			exit(1);
		}

		if (wc.status != IBV_WC_SUCCESS) {
			fprintf(stderr, "[CS] Failed status %s (%d) for wr_id %d\n", ibv_wc_status_str(wc.status), wc.status, (int)wc.wr_id);
			exit(1);
		}

		if (wc.opcode == IBV_WC_RDMA_WRITE) {
			continue;
		} else if (wc.opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
            /* immediately refuel the recv queue */
            {
                struct ibv_recv_wr recv_wr[1] = {0};
                struct ibv_recv_wr * bad_wr = NULL;
                err = prov_ops.post_recv(qp->control_qp, recv_wr, &bad_wr);
                if (err) {
                    fprintf(stderr, "[CS] Failed to post recv WR: %d\n", err);
                    exit(1);
                }
            }


            memcpy(&md, qp->control_buf_va + 1 * sizeof(struct control_metadata), sizeof(struct control_metadata));
            
			DEBUG("[CS] Received ACK for %ld lines\n", md.ack_revc_lines);
			// ACK slots
			pthread_mutex_lock(&qp->send_lines_full_lock);
			qp->send_lines_full = qp->send_lines_full - md.ack_revc_lines;
			pthread_mutex_unlock(&qp->send_lines_full_lock);
			err = pthread_cond_signal(&qp->send_lines_avail);
			if (err != 0) {
				fprintf(stderr, "pthread_cond_signal failed: %d\n", err);
				exit(1);
			}
			continue;
		} else {
			fprintf(stderr, "Unexpected opcode %d\n", wc.opcode);
			exit(1);
		}
	}
}