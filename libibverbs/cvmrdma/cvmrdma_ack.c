#include "cvmrdma_ack.h"

void send_ack(struct cvmrdma_context *ctxt, struct cvmrdma_qp *qp) {

    if (qp->received_lines >= qp->received_lines_threshold){

        qp->received_lines_threshold += (cvmrdma_ring_entries / cvmrdma_ack_notify_divider);
        struct ibv_qp_ex *control_ibqp_ex = ibv_qp_to_qp_ex(qp->control_qp);

        struct control_metadata metadata;
        // send the current number of received lines (we need to subtract the ones we added above)
        metadata.ack_received_lines = qp->received_lines_threshold - (cvmrdma_ring_entries / cvmrdma_ack_notify_divider);

        // fprintf(stderr, "[CQ] notifying remote that we have received %d chunk slots\n", metadata.ack_received_lines);

        memcpy(qp->control_buf_va, &metadata, sizeof(metadata));

        while (qp->ack_send_in_transit_counter >= 4096) {
            poll_for_ack_once(ctxt, qp, false);

            if (atomic_load(&qp->stop_requested)) {
                fprintf(stderr, "Stop requested while save send, exiting save_send\n");
                return 0;
            }
        }

        ibv_wr_start(control_ibqp_ex);
        control_ibqp_ex->wr_id = 0; // can be any value, not used

        ibv_wr_rdma_write_imm(control_ibqp_ex,
            qp->remote_control_rkey,
            1 * sizeof(struct control_metadata),
            0 // no immediate data
        );

        ibv_wr_set_sge(control_ibqp_ex,
            qp->control_buf_mr->lkey,
            0,
            sizeof(struct control_metadata)
        );

        int err = ibv_wr_complete(control_ibqp_ex);
        if (err) {
            fprintf(stderr, "Failed to post ACK WR (%d)\n", err);
            exit(1);
        }

        qp->ack_send_in_transit_counter++;
    }
}

int post_counter = 0;

void poll_for_ack_once(struct cvmrdma_context *ctxt, struct cvmrdma_qp *qp, bool has_lock) {

    struct ibv_cq *control_cq = qp->control_cq;

    struct ibv_wc wc[16];
    int ne = ctxt->prov_ops.poll_cq(control_cq, 16, wc);
    if (ne < 0) {
        fprintf(stderr, "Failed to poll control CQ for ACK\n");
        exit(1);
    } else if (ne == 0) {
        return;
    }

    if (ne > 16) {
        fprintf(stderr, "Polled more than expected ACK WCs\n");
        exit(1);
    }

    bool has_ack_wc = false;
    for (int i = 0; i < ne; i++) {

        if (wc[i].status != IBV_WC_SUCCESS) {
            fprintf(stderr, "ACK WC has error status %d\n", wc[i].status);
            exit(1);
        }

        if (wc[i].opcode == IBV_WC_RDMA_WRITE) {
            qp->ack_send_in_transit_counter--;
            continue; // ignore non-ACK WRs
        }

        // if we got multiple ACK WCs, we only process the last one
        // to avoid unnecessary work
        if (wc[i].opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
            // qp->received_acks += 1;
            has_ack_wc = true;
            
            // we need to create an actual recv wr, even if it isn't by write imm, otherwise the post_recv will fail
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
            int err = ctxt->prov_ops.post_recv(qp->control_qp, &recv_wr, &bad_wr);
            post_counter++;
            if (err) {
                fprintf(stderr, "[CS] Failed to post recv WR: %d (after %d posts)\n", err, post_counter);
                exit(1);
            }
        } else {
            fprintf(stderr, "Received unexpected WC opcode %d in control CQ\n", wc[i].opcode);
            exit(1);
        }
    }

    if (has_ack_wc) {

        // free lines according to the received ACK
        struct control_metadata metadata;
        memcpy(&metadata, qp->control_buf_va + 1 * sizeof(struct control_metadata), sizeof(metadata));

        if (!has_lock) {
            pthread_mutex_lock(&qp->send_lines_full_lock);
        }
        qp->send_lines_full -= (metadata.ack_received_lines - qp->number_of_acked_lines);
        if (!has_lock) {
            pthread_mutex_unlock(&qp->send_lines_full_lock);
        }

        /*fprintf(stderr, "[CQ] received ACK for %d chunk slots, there are now %d send lines full\n",
            metadata.ack_received_lines,
            qp->send_lines_full);*/

        qp->number_of_acked_lines = metadata.ack_received_lines;
    }
}

    
    