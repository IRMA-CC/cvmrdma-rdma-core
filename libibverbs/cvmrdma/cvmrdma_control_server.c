#include "cvmrdma_control_server.h"

#include <pthread.h>
#include <infiniband/verbs.h>

#include "cvmrdma.h"
#include "debug.h"

void *start_control_server(void *args) {
	struct start_args *start_args = (struct start_args *)args;
	struct cvmrdma_context * ctxt = start_args->ctxt;
	struct cvmrdma_qp * qp = start_args->qp;
	struct ibv_ah_attr * attr = start_args->ah_attr;
	struct ibv_qp * control_qp = qp->control_qp;

	int ret = 0;

	DEBUG("[CS 0x%x] Starting control server\n", control_qp->qp_num);

	pthread_mutex_lock(&qp->remote_params_lock);
	while (!qp->remote_params_set) {
		pthread_cond_wait(&qp->remote_params_set_cv, &qp->remote_params_lock);
	}
	pthread_mutex_unlock(&qp->remote_params_lock);

	DEBUG("[CS 0x%x] remote qp_num: 0x%x, remote rkey: 0x%x, remote psn: 0x%x, local psn 0x%x\n",
		control_qp->qp_num, qp->dest_control_qp_num, qp->remote_control_rkey, qp->control_remote_psn, qp->control_local_psn);

	uint32_t control_remote_psn = qp->control_remote_psn;

	int control_qp_attr_mask = IBV_QP_STATE;
	struct ibv_qp_attr control_qp_attrs = (struct ibv_qp_attr) {
		.qp_state = IBV_QPS_RTR,
	};
	control_qp_attrs.ah_attr.dlid = attr->dlid;
	control_qp_attrs.ah_attr.port_num = attr->port_num;
	control_qp_attrs.ah_attr.is_global = 0;

	control_qp_attrs.path_mtu = IBV_MTU_1024;
	control_qp_attrs.dest_qp_num = qp->dest_control_qp_num;
	control_qp_attrs.rq_psn = control_remote_psn;

	DEBUG("[MODIFY_QP CS 0x%x] control qp: qp->dest_control_qp_num 0x%" PRIx32 " rq_psn 0x%" PRIx32 " dlid %d, port_num %d\n", 
	control_qp->qp_num, qp->dest_control_qp_num, control_remote_psn, attr->dlid, attr->port_num);

	control_qp_attr_mask |= IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN;

	control_qp_attrs.max_dest_rd_atomic = 1; /* ??? */
	control_qp_attrs.min_rnr_timer = 12;     /* ??? */
	control_qp_attrs.timeout = 21;           /* ??? */

	control_qp_attr_mask |= IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;

	ret = ctxt->prov_ops.modify_qp(control_qp, &control_qp_attrs, control_qp_attr_mask);
	if(ret)
	{
		fprintf(stderr, "[CS] Failed to modify control qp to RTR %d\n", ret);
		exit(1);
	}

	for (int i = 0; i < 10; i++)
	{
		struct ibv_recv_wr control_recv_wr[1] = {0};
		struct ibv_recv_wr * control_bad_wr = NULL;
		ret = ctxt->prov_ops.post_recv(control_qp, control_recv_wr, &control_bad_wr);
		if(ret)
		{
			fprintf(stderr, "[CS] Failed to post recv initial RR for control qp (%d)\n", ret);
			exit(1);
		}
	}

	// --> RTS
	uint32_t control_sq_psn = qp->control_local_psn;

	control_qp_attr_mask = IBV_QP_STATE | IBV_QP_SQ_PSN;
	control_qp_attrs = (struct ibv_qp_attr) {
		.qp_state = IBV_QPS_RTS,
		.sq_psn = 0,
	};
	/* SQ PSN is remote's RQ PSN */
	control_qp_attrs.sq_psn = control_sq_psn;
	control_qp_attr_mask |= IBV_QP_SQ_PSN;

	control_qp_attrs.timeout = 14;
	control_qp_attrs.retry_cnt = 7;
	control_qp_attrs.rnr_retry = 7;
	control_qp_attrs.max_dest_rd_atomic = 1;

	control_qp_attr_mask |= IBV_QP_MAX_QP_RD_ATOMIC | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_TIMEOUT;

	ret = ctxt->prov_ops.modify_qp(control_qp, &control_qp_attrs, control_qp_attr_mask);
	if(ret)
	{
		fprintf(stderr, "[CS] Failed to modify control qp to RTS\n");
		exit(1);
	}

	struct thread_args *args1 = calloc(1, sizeof(struct thread_args));
	args1->qp = qp;
	args1->ctxt = ctxt;
	ret = pthread_create(&qp->control_server_send_acks_thread, NULL, &control_server_send_acks, (void *)args1);
	if (ret)
	{
		fprintf(stderr, "[CS] Failed to create control_server_send_acks thread\n");
		exit(1);
	}
	struct thread_args *args2 = calloc(1, sizeof(struct thread_args));
	args2->qp = qp;
	args2->ctxt = ctxt;
	ret = pthread_create(&qp->control_server_receive_acks_thread, NULL, &control_server_receive_acks, (void *)args2);
	if (ret)
	{
		fprintf(stderr, "[CS] Failed to create control_server_receive_acks thread\n");
		exit(1);
	}
}


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
		while (!(qp->received_lines >= 1)) {
			pthread_cond_wait(&qp->recv_lines_ack, &qp->received_lines_lock);
		}
		md.ack_revc_lines = qp->received_lines;
        qp->received_lines = 0;
		memcpy(qp->control_buf_va, &md, sizeof(struct control_metadata));
		pthread_mutex_unlock(&qp->received_lines_lock);

        DEBUG("[CS 0x%x] Sending ACK for %ld lines for dlid %ld and remote qp 0x%x using lkey 0x%x and rkey 0x%x\n", 
		cqp->qp_num, md.ack_revc_lines, qp->dlid, qp->dest_control_qp_num, qp->control_buf_mr->lkey, qp->remote_control_rkey);

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
			DEBUG("[CS 0x%x] Send ACK completed\n", qp->control_qp->qp_num);
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
			if (md.ack_revc_lines > qp->send_lines_full) {
				fprintf(stderr, "Received ACK for %ld lines, but only %d lines are full\n", md.ack_revc_lines, qp->send_lines_full);
				exit(1);
			} else {
				qp->send_lines_full = qp->send_lines_full - md.ack_revc_lines;
			}
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