#include "cvmrdma_cq_server.h"

#include <assert.h>
#include <stdio.h>
#include <stdbool.h>
#include <pthread.h>
#include <string.h>
#include <poll.h>

#include <linux/userfaultfd.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/syscall.h>

#include <sodium.h>

#include <infiniband/verbs.h>
#include "debug.h"

#include "../ibverbs.h"

#include "cvmrdma.h"
#include "cvmrdma_objects.h"
#include "cvmrdma_network_evloop_priv.h"

#define CQ_SERVER_NUM_WCS CVMRDMA_RING_ENTRIES
static int cq_server_loop_once(struct cvmrdma_context * ctxt, struct ibv_cq * prov_cq)
{
    QPMetadataManager * qp_metadata_manager = ctxt->qp_metadata_manager;

    enum sodium_crypto_aead aead = ctxt->aead;
	unsigned long long header_bytes = cvmrdma_header_bytes(aead);


    int err;

    struct ibv_wc wcs[CQ_SERVER_NUM_WCS];

    int nwcs = ctxt->prov_ops.poll_cq(prov_cq, CQ_SERVER_NUM_WCS, wcs);
    if(nwcs < 0)
    {
        err = nwcs;
        fprintf(stderr, "[CQ] poll provider CQ failed %d\n", err);
        exit(1);
    }
    if(nwcs > 0)
    {
        DEBUG("[CQ] polled %d\n", nwcs);
    }

    for(int i = 0 ; i < nwcs ; i++)
    {
        struct ibv_wc * this_wc = &wcs[i];
        enum ibv_wc_status status = this_wc->status;
        enum ibv_wc_opcode op = this_wc->opcode;
        /* The user-defined immediate is sent on the other channel, the imm in the WC is unused for now! */
        // __be32 protocol_imm = this_wc->imm_data;
        uint32_t wc_length = this_wc->byte_len;
        uint32_t qpn = this_wc->qp_num;
        struct cvmrdma_qp * qp = qp_metadata_manager_get_qp(qp_metadata_manager, qpn);
        assert(qp);

        struct cvmrdma_pd * pd = (struct cvmrdma_pd *)qp->user_qp.qp.pd;
        assert(pd);

        DEBUG("[CQ] WRID = 0x%" PRIx64 " QPN 0x%" PRIx32 "\n", this_wc->wr_id, qpn);

        if(status != IBV_WC_SUCCESS)
        {
            fprintf(stderr, "CQ server WARN: WC reports failure status: %d, vendor err %d\n", status, this_wc->vendor_err);
            if(op == IBV_WC_SEND) {
                struct cvmrdma_mr* mr = NULL;
                int i = 0;
                do {
                    mr = get_bounce_metadata_for_rkey(pd->bounce_metadata, i);
                    if (mr != NULL) {
                        fprintf(stderr, "[CQ] bounce_metadata[%d]: len: %zx, user_mr addr: %p, user_mr hca_va: %p provider_mr len: %zx, provider_mr provider_va: %p, provider_mr lkey: 0x%lx\n", 
                        i, mr->user_mr.ibv_mr.length, mr->user_mr.ibv_mr.addr, mr->hca_va,
                        mr->provider_mr->length, mr->provider_va, mr->provider_mr->lkey);
                    }
                    i++;
                } while (mr != NULL);
            }
        }
        else if(! (op & IBV_WC_RECV))
        {
            /* This is a send completion (SEND, WRITE, WWI) */
            DEBUG("[CQ] send op %d, wrid: 0x%lx done\n", op, this_wc->wr_id);

            qp->wr_chunks_sent++;

            struct send_metadata_entry * send_info = qp_send_metadata_front(qp->send_metadata);
        
            if(send_info->nchunks == qp->wr_chunks_sent)
            {
                uint64_t wrid = send_info->wrid;
                size_t bytes = send_info->bytes;
                bool is_write = (send_info->op == IBV_WR_RDMA_WRITE) || (send_info->op == IBV_WR_RDMA_WRITE_WITH_IMM);

                qp->wr_chunks_sent = 0;
                qp_send_metadata_pop(qp->send_metadata);

                struct ibv_wc send_wc = {
                    .byte_len = bytes,
                    .wc_flags = 0,
                    .imm_data = 0,
                    .opcode = is_write ? IBV_WC_RDMA_WRITE : IBV_WC_SEND,
                    .qp_num = qpn,
                    .status = IBV_WC_SUCCESS,
                    .wr_id = wrid
                };

                /* Notify user CQ of a SEND if IBV_SEND_SIGNALED was set and qp was created with
                   sq_sig_all=0
                */
                if(send_info->send_signaled || qp->user_sq_sig_all) {
                    DEBUG("[CQ] Sending WC to user send_signaled %d user_sq_sig_all %d\n", send_info->send_signaled, qp->user_sq_sig_all);
                    DEBUG("[CQ] User wrid %lx, byte_len %zu, imm_data %x, opcode %d, qp_num %x, status %d\n", send_wc.wr_id, send_wc.byte_len, send_wc.imm_data, send_wc.opcode, send_wc.qp_num, send_wc.status);
                    struct cvmrdma_cq * scq = (struct cvmrdma_cq *)qp->user_qp.qp.send_cq;
                    wcq_push(scq->wc_queue, &send_wc);
                }
            }

        }
        else
        {
            TIMING("cq_serv_wc_recv");
            /* A SEND WR has happened : per CVMRDMA procotol, it has to be a WRITE_WITH_IMM of a chunk in the ring buffer */

            if(op != IBV_WC_RECV_RDMA_WITH_IMM)
            {
                fprintf(stderr, "PROTOCOL ERROR: expected WRITE_WITH_IMM, but the recv op is a WC op type %d\n", op);
                exit(1);
            }

            DEBUG("starting chunk processing for qpn %" PRIx32 "\n", qpn);


            /* immediately refuel the recv queue */
            {
                struct ibv_recv_wr recv_wr[1] = {0};
                struct ibv_recv_wr * bad_wr = NULL;
                err = ctxt->prov_ops.post_recv(qp->provider_qp, recv_wr, &bad_wr);
                if(err)
                {
                    fprintf(stderr, "WARN: failed to refuel recv queue for QPN %x\n", qpn);
                }
            }


            DEBUG("before chunk: rcved 0x%zx bytes of WR\n", qp->wr_bytes_rcved);

            assert(qp->remote_params_set);
            assert(qp->rx_key[0] || qp->rx_key[1] || qp->rx_key[2]);
            uint64_t const * rx_key = qp->rx_key;
            uint64_t const * shared_key = qp->shared_key;

            unsigned char * ringbuf_in = (unsigned char *)qp->ringbuf_va;
            unsigned char * private = (unsigned char *)qp->bounce_private_mem + CVMRDMA_PRIV_CHUNK_ENC_IN * cvmrdma_ring_slot_bytes(ctxt->aead);
            unsigned char * decrypted = (unsigned char *)qp->bounce_private_mem + CVMRDMA_PRIV_CHUNK_SG_IN * cvmrdma_ring_slot_bytes(ctxt->aead);

            int this_recv_slot_id = qp->next_recv_slot;
            unsigned char * slot = ringbuf_in + this_recv_slot_id * cvmrdma_ring_slot_bytes(aead);

            /* And update the slot idx */
            qp->next_recv_slot = (this_recv_slot_id + 1) % CVMRDMA_RING_ENTRIES;


            TIMING("cds_t0");


            /**** DECRYPT *****/
            /* Decrypt the notification, then the payload 
               Based on the header, we decide where to copy the encrypted payload
            */

			/* Get notif nonce */
			uint64_t * qp_nonce = qp->rx_nonce;

			uint64_t nonce_metadata[cvmrdma_aead_NPUB_WORDS];
			memcpy(nonce_metadata, qp_nonce, cvmrdma_aead_NPUBBYTES);
			
			/* and increment nonce */
			qp_nonce[0] += 1;

            struct cq_server_notification notif = {0};

            unsigned char * notif_start = slot;
            unsigned long long notif_clen = header_bytes;
            if(header_bytes > wc_length)
            {
                fprintf(stderr, "Failed to decrypt: chunk too short\n");
                exit(1);
            }

            unsigned long long notif_mlen = sizeof(notif);
            err = cvmrdma_decrypt(
                (unsigned char *)&notif, &notif_mlen,
                NULL,
                notif_start, notif_clen,
                NULL, 0,
                (unsigned char const *)nonce_metadata,
                (unsigned char const *)rx_key,
                aead
            );
            if(err)
            {
                fprintf(stderr, "WARN: Failed to decrypt bounce buffer for notif, exiting.\n");
                exit(1);
            }

            enum ibv_wr_opcode wr_op = notif.op;
            bool is_write = (wr_op == IBV_WR_RDMA_WRITE) || (wr_op == IBV_WR_RDMA_WRITE_WITH_IMM);

            // Decrypt directly to destination !
            bool bypass_scatter = false;
            void * dest = NULL;
            uint64_t offt = 0;
            struct cvmrdma_mr * cur_mr = NULL;
            if(is_write)
            {
                /* The operation is a WRITE: use the remote_addr and rkey provided by the requester. We
                always write in a contiguous block, so always bypass scatter */
                uint32_t rkey = notif.rkey;
                uint64_t hca_addr = notif.hca_addr;

                cur_mr = get_bounce_metadata_for_rkey(pd->bounce_metadata, rkey);
                assert(cur_mr);

                offt = hca_addr - cur_mr->hca_va;
                dest = (unsigned char *)cur_mr->user_mr.ibv_mr.addr + offt;

                if(notif.chunk_len == CVMRDMA_CHUNK_BYTES){
                    decrypted = dest;
                    bypass_scatter = true;
                }
            }
            else
            {
                /* The operation is a SEND: we can only bypass the scatter if the next chunk fits
                    in the current sge */

                struct recv_metadata_entry * recv_info = qp_recv_metadata_front(qp->recv_metadata);
                struct ibv_sge * rcv_sgl = recv_info->sgl;
                struct ibv_sge * cur_sge = &rcv_sgl[qp->cur_sge_idx];

                cur_mr = get_bounce_metadata_for_lkey(pd->bounce_metadata, cur_sge->lkey);
                assert(cur_mr);

                if(notif.chunk_len == CVMRDMA_CHUNK_BYTES 
                    && qp->cur_offt_in_sge + notif.chunk_len <= cur_sge->length)
                {
                    /* can write directly to dest! */
                    unsigned char * mr_addr = cur_mr->user_mr.ibv_mr.addr;
                    offt = (cur_sge->addr - cur_mr->hca_va) + qp->cur_offt_in_sge;

                    decrypted = mr_addr + offt;
                    bypass_scatter = true;
                }
            }

            unsigned long long payload_clen = notif.payload_len + cvmrdma_ABYTES(aead);

            DEBUG("[CQ] notif.op %d notif.payload_nonce %lx %lx\n", notif.op, notif.payload_nonce[0], notif.payload_nonce[1]);
            DEBUG("[CQ] notif.chunk_offt 0x%"PRIx64" and notif.chunk_len %zu\n", notif.chunk_offt, notif.chunk_len);
            DEBUG("[CQ] clen %llu\n", payload_clen);

            size_t chunk_idx = offt / CVMRDMA_CHUNK_BYTES;
            unsigned char * payload_start = NULL;
            struct chunk_tracking_info* chunk_info = &cur_mr->chunk_tracking[chunk_idx];

            if (!bypass_scatter || cvmrdma_disable_cache)
            {
                /* Copy the payload into private memory */
                payload_start = private + header_bytes;
                memcpy(payload_start, slot + header_bytes, payload_clen);
                DEBUG("[CQ] private + offt %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x\n", 
                    payload_start[0+notif.chunk_offt], payload_start[1+notif.chunk_offt], payload_start[2+notif.chunk_offt], payload_start[3+notif.chunk_offt],
                    payload_start[4+notif.chunk_offt], payload_start[5+notif.chunk_offt], payload_start[6+notif.chunk_offt], payload_start[7+notif.chunk_offt],
                    payload_start[8+notif.chunk_offt], payload_start[9+notif.chunk_offt], payload_start[10+notif.chunk_offt], payload_start[11+notif.chunk_offt],
                    payload_start[12+notif.chunk_offt], payload_start[13+notif.chunk_offt], payload_start[14+notif.chunk_offt], payload_start[15+notif.chunk_offt],
                    payload_start[16+notif.chunk_offt], payload_start[17+notif.chunk_offt]);
            } else {
                /* Copy the payload into the private chunk cache */
                // TODO: this is wrong
                assert(offt % CVMRDMA_CHUNK_BYTES == 0);
                DEBUG("[CQ] chunk_idx %zu to chunk cache\n", chunk_idx);
                pthread_mutex_lock(&chunk_info->lock);
                payload_start = cur_mr->provider_va + chunk_idx * cvmrdma_ring_slot_bytes(ctxt->aead) +  cvmrdma_header_bytes(ctxt->aead);

                memcpy(payload_start, slot + header_bytes, cvmrdma_ring_slot_bytes(ctxt->aead) - cvmrdma_header_bytes(ctxt->aead));

                DEBUG("[CQ] payload_start %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x\n", 
                    payload_start[0], payload_start[1], payload_start[2], payload_start[3],
                    payload_start[4], payload_start[5], payload_start[6], payload_start[7],
                    payload_start[8], payload_start[9], payload_start[10], payload_start[11],
                    payload_start[12], payload_start[13], payload_start[14], payload_start[15],
                    payload_start[16], payload_start[17]);

                struct uffdio_writeprotect uffdio_wp = {
                    .range = {
                        .start = decrypted,
                        .len = CVMRDMA_CHUNK_BYTES,
                    },
                    .mode = 0,
                };
                if (ioctl(cur_mr->uffd, UFFDIO_WRITEPROTECT, &uffdio_wp) == -1) {
                    fprintf(stderr, "[CACHE] ioctl-UFFDIO_WRITEPROTECT: %d\n", errno);
                    exit(1);
                }
            }

            unsigned long long payload_mlen = CVMRDMA_CHUNK_BYTES;
            err = cvmrdma_decrypt(
                decrypted, &payload_mlen,
                NULL,
                payload_start, payload_clen,
                NULL, 0,
                (unsigned char const *)notif.payload_nonce,
                (unsigned char const *)shared_key,
                aead
            );
            if(err)
            {
                fprintf(stderr, "WARN: Failed to decrypt bounce buffer for payload, exiting.\n");
                exit(1);
            }

            DEBUG("[CQ] decrypted %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x\n", decrypted[0+notif.chunk_offt],
             decrypted[1+notif.chunk_offt], decrypted[2+notif.chunk_offt], decrypted[3+notif.chunk_offt], 
             decrypted[4+notif.chunk_offt], decrypted[5+notif.chunk_offt], decrypted[6+notif.chunk_offt],
              decrypted[7+notif.chunk_offt], decrypted[8+notif.chunk_offt], decrypted[9+notif.chunk_offt],
               decrypted[10+notif.chunk_offt], decrypted[11+notif.chunk_offt], decrypted[12+notif.chunk_offt],
                decrypted[13+notif.chunk_offt], decrypted[14+notif.chunk_offt], decrypted[15+notif.chunk_offt],
                 decrypted[16+notif.chunk_offt], decrypted[17+notif.chunk_offt]);

            /*** SCATTER ****/
            /* Scatter the plaintext to user memory. If the op is a WRITE, the remote has set the destination address,
                else the op is a SEND and we must use the next chunk in the current recv request */


            if(!bypass_scatter || cvmrdma_disable_cache)
            {
                if (is_write){
                    DEBUG("[CQ] WRITE: writing with notif.chunk_offt 0x%"PRIx64" and notif.chunk_len %zu to dest %p for notif.hca_addr %p\n", 
                          notif.chunk_offt, notif.chunk_len, dest, (void *)notif.hca_addr);
                    memcpy(dest, decrypted + notif.chunk_offt, notif.chunk_len);
                } else {
                    /* The operation is a SEND: use the next chunk on the current recv request */
                    struct recv_metadata_entry * recv_info = qp_recv_metadata_front(qp->recv_metadata);
                    struct ibv_sge * rcv_sgl = recv_info->sgl;
                    int rcv_nsge = recv_info->nsge;
                    
                    size_t bytes_copied = 0;

                    while((bytes_copied < notif.chunk_len) && (qp->cur_sge_idx < rcv_nsge))
                    {
                        struct ibv_sge * cur_sge = &rcv_sgl[qp->cur_sge_idx];
                        assert(cur_mr);
                        unsigned char * mr_addr = cur_mr->user_mr.ibv_mr.addr;


                        size_t remaining = notif.chunk_len - bytes_copied;
                        assert(cur_sge);
                        size_t available = cur_sge->length - qp->cur_offt_in_sge;

                        uint64_t offt = (cur_sge->addr - cur_mr->hca_va) + qp->cur_offt_in_sge;

                        if(remaining < available)
                        {
                            DEBUG("[CQ] SEND: copying %zu bytes from notif.chunk_offt %zu to offt %zu\n", remaining, notif.chunk_offt + bytes_copied, offt);
                            memcpy(mr_addr + offt, decrypted + notif.chunk_offt + bytes_copied, remaining);
                            bytes_copied += remaining;
                            qp->cur_offt_in_sge += remaining;
                        }
                        else
                        {
                            DEBUG("[CQ] SEND: copying %zu bytes from notif.chunk_offt %zu to offt %zu\n", available, notif.chunk_offt + bytes_copied, offt);
                            memcpy(mr_addr + offt, decrypted + notif.chunk_offt + bytes_copied, available);
                            bytes_copied += available;
                            
                            /* move to next sge */
                            qp->cur_sge_idx++;
                            qp->cur_offt_in_sge = 0;
                        }
                    }


                    if(bytes_copied < notif.chunk_len)
                    {
                        /* Early loop exit bc the sglist is empty. This is an error ! */
                        fprintf(stderr, "Incoming chunk would exceed the end of the receive request! (notif.chunk_len 0x%"PRIx64" > recv %zu)\n", notif.chunk_len, bytes_copied);
                        exit(1);
                        /* TODO: emit IBV_WC_LOC_LEN_ERR in WC status for user !*/
                    }
                }
            }
            else
            {
                struct uffdio_writeprotect uffdio_wp = {
                    .range = {
                        .start = (unsigned long) decrypted,
                        .len = CVMRDMA_CHUNK_BYTES,
                    },
                    .mode = UFFDIO_WRITEPROTECT_MODE_WP,
                };
                if (ioctl(cur_mr->uffd, UFFDIO_WRITEPROTECT, &uffdio_wp) == -1) {
                    fprintf(stderr, "[CQ] ioctl-UFFDIO_WRITEPROTECT: %d\n", errno);
                    exit(1);
                }
                memcpy(chunk_info->nonce, notif.payload_nonce, cvmrdma_aead_NPUBBYTES);
                atomic_store(&chunk_info->cached, true);
                pthread_mutex_unlock(&chunk_info->lock);


                /* Record in the QP the number of bytes written during the decrypt (directly to dest) */
                struct recv_metadata_entry * recv_info = qp_recv_metadata_front(qp->recv_metadata);
                struct ibv_sge * rcv_sgl = recv_info->sgl;
                struct ibv_sge * cur_sge = &rcv_sgl[qp->cur_sge_idx];

                qp->cur_offt_in_sge += notif.chunk_len;
                if(qp->cur_offt_in_sge >= cur_sge->length)
                {
                    qp->cur_offt_in_sge = 0;
                    qp->cur_sge_idx++;
                }
            }

            qp->wr_bytes_rcved += notif.chunk_len;

            DEBUG("after chunk: rcved 0x%zx bytes of WR\n", qp->wr_bytes_rcved);

            TIMING("cds_t1");

            /* Is this the end of a WR? */
            if(notif.last_chunk)
            {
                DEBUG("last_chunk!\n");
                if(wr_op == IBV_WR_RDMA_WRITE)
                {
                    /* Do not consume an RR, do not emit a WC. But restore the byte counter */
                    qp->wr_bytes_rcved = 0;
                }
                else
                {
                    /* SEND(_WITH_IMM) and WRITE_WITH_IMM consume an RR: recover its wrid and move to the next one */
                    struct cvmrdma_cq * rcq = (struct cvmrdma_cq *)qp->user_qp.qp.recv_cq;

            
                    struct recv_metadata_entry * recv_info = qp_recv_metadata_front(qp->recv_metadata);
                    uint64_t recv_wrid = recv_info->wrid;
                    qp_recv_metadata_pop(qp->recv_metadata);
                    qp->cur_sge_idx = 0;
                    qp->cur_offt_in_sge = 0;

                    /* The WR may carry an immediate */
                    bool has_imm = (wr_op == IBV_WR_RDMA_WRITE_WITH_IMM) || (wr_op == IBV_WR_SEND_WITH_IMM);
                    uint32_t imm = notif.imm;

                    /* The RR consuming ops also generate a WC. Convert the WR opcode to a WC opcode */
                    enum ibv_wc_opcode wc_op;
                    switch(wr_op)
                    {
                        case IBV_WR_RDMA_WRITE_WITH_IMM: {
                            wc_op = IBV_WC_RECV_RDMA_WITH_IMM;
                            break;
                        }
                        default: {
                            /* the two kinds of SENDs give WC_RECV */
                            wc_op= IBV_WC_RECV;
                            break;
                        }
                    }
                    assert(wc_op != 0);
                    assert(qp->wr_bytes_rcved > 0);

                    struct ibv_wc recv_wc = {
                        .byte_len = qp->wr_bytes_rcved,
                        .wc_flags = has_imm ? IBV_WC_WITH_IMM : 0,
                        .imm_data = imm,
                        .opcode = wc_op,
                        .qp_num = qpn,
                        .status = IBV_WC_SUCCESS,
                        .wr_id = recv_wrid
                    };

                    DEBUG("[CQ] Pushing WC, byte_len %zu, imm %x, opcode %d, qp_num %x, status %d, wr_id %lx\n", recv_wc.byte_len, recv_wc.imm_data, recv_wc.opcode, recv_wc.qp_num, recv_wc.status, recv_wc.wr_id);

                    /* Notify user CQ of a RECV */
                    wcq_push(rcq->wc_queue, &recv_wc);
                    // Pointer to cq struct, see verbs.c:603
                    uint64_t event = (uintptr_t) rcq;
                    
                    bool expected = 0;
                    if (atomic_compare_exchange_strong(&rcq->comp_channel_event_fd_filled, &expected, 1))
                    {
                        DEBUG("[CQ] Writing %p to event fd %d\n", rcq, rcq->comp_channel_event_fd);
                        write(rcq->comp_channel_event_fd, &event, sizeof(event));
                    }

                    /* And reset the byte counter */
                    qp->wr_bytes_rcved = 0;
                }
            }

            /* We can notify the remote thay this chunk can be reused for writing
                XXX: this could be pushed all the way up right after copying into private memory
                But what about failures to decrypt?*/
            TIMING("chunk_notify_recv");

            pthread_mutex_lock(&qp->received_lines_lock);
            if ( ++qp->received_lines >= CVMRDMA_ACK_THRESHOLD)
            {
                pthread_cond_signal(&qp->recv_lines_ack);
            }
            pthread_mutex_unlock(&qp->received_lines_lock);
        }
    }

    return nwcs;
}


int cvmrdma_cq_server_run_once_if_idle_recv(struct cvmrdma_context * ctxt, struct cvmrdma_cq * cq, bool* again)
{
    int ret = 1;

    /*
    ! is_running => (is_running = true && first = true)
        is_running => (is_running = true && first = false)

    If the server is NOT running, mark it as running and run it.
    If it is running, leave it marked as such and do nothing.
	*/

	bool expected = false;
	bool first = atomic_compare_exchange_strong(&cq->is_running_recv, &expected, true);
	if(first)
	{
		ret = cq_server_loop_once(ctxt, cq->prov_cq_recv);
		/* And bring it down */
		atomic_store(&cq->is_running_recv, false);
        if (again != NULL) {
            *again = false;
        }
	} else if(again != NULL) {
        *again = true;
    }
    return ret;
}

int cvmrdma_cq_server_run_once_if_idle_send(struct cvmrdma_context * ctxt, struct cvmrdma_cq * cq, bool* again)
{
    int ret = 1;

	bool expected = false;
	bool first = atomic_compare_exchange_strong(&cq->is_running_send, &expected, true);
	if(first)
	{
		ret = cq_server_loop_once(ctxt, cq->prov_cq_send);
		/* And bring it down */
		atomic_store(&cq->is_running_send, false);
        if (again != NULL) {
            *again = false;
        }
	} else {
        if(again != NULL) {
            *again = true;
        }
    }
    return ret;
}

// This is used if the actual application uses the completion channel

void* cvmrdma_cq_wait_completion_event(void* c_cq) {
    struct cvmrdma_cq * cq = (struct cvmrdma_cq *)c_cq;
    struct cvmrdma_context * context = cvmrdma_get_ctx(cq->user_cq.cq.context);
    bool run_again_recv = false;
    bool run_again_send = false;
    struct ibv_cq *ev_cq;
    void *ev_ctx;
    int ret;

    DEBUG("[CQ] Waiting for CQ event\n");

    for(;;) {
        if(true) {
            ret = ibv_get_cq_event(cq->provider_comp_channel, &ev_cq, &ev_ctx);
            if (ret) {
                fprintf(stderr, "Failed to get CQ event\n");
                exit(1);
            }
            DEBUG("[CQ] Got CQ event\n");
            assert(ev_cq == cq->prov_cq_recv || ev_cq == cq->prov_cq_send);

            /* Request notification upon the next completion event */
            ret = context->prov_ops.req_notify_cq(ev_cq, 0);
            if (ret) {
                fprintf(stderr, "Couldn't request CQ notification\n");
                exit(1);
            }

            ibv_ack_cq_events(ev_cq, 1);

            if(ev_cq == cq->prov_cq_recv) {
                do {
                    cvmrdma_cq_server_run_once_if_idle_recv(context, cq, &run_again_recv);
                } while(run_again_recv);
            } else {
                do {
                    cvmrdma_cq_server_run_once_if_idle_send(context, cq, &run_again_send);
                } while(run_again_send);
            }
        } else {
            cvmrdma_cq_server_run_once_if_idle_recv(context, cq, NULL);
            cvmrdma_cq_server_run_once_if_idle_recv(context, cq, NULL);
        }
    }
}

void print_mr_from_ctxt(struct cvmrdma_context * context) {
    DEBUG("[CQ] MRs for ctxt %p\n", context);
    for (uint64_t i = 0; i < 10; i++) {
        if(context->pd_list[i] != NULL) {
            struct cvmrdma_pd* cpd = context->pd_list[i];
            struct cvmrdma_mr* mr = NULL;
            int k = 0;
            do {
                mr = get_bounce_metadata_for_rkey(cpd->bounce_metadata, k);
                if (mr != NULL) {
                    fprintf(stderr, "[CQ] ctxt: %p, pd[%d]: bounce_metadata[%d]: len: %zx, user_mr addr: %p, user_mr hca_va: %p provider_mr len: %zx, provider_mr provider_va: %p, provider_mr lkey: 0x%lx, provider_mr rkey: 0x%lx\n", 
                    context, i, k, mr->user_mr.ibv_mr.length, mr->user_mr.ibv_mr.addr, mr->hca_va,
                    mr->provider_mr->length, mr->provider_va, mr->provider_mr->lkey, mr->provider_mr->rkey);
                }
                k++;
            } while (mr != NULL);
        }
    }
}