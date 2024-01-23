#include "cvmrdma_cq_server.h"

#include <assert.h>
#include <stdio.h>
#include <stdbool.h>
#include <pthread.h>
#include <string.h>

#include <sodium.h>

#include <infiniband/verbs.h>
#include "debug.h"

#include "cvmrdma.h"
#include "cvmrdma_objects.h"
#include "cvmrdma_network_evloop_priv.h"

void cq_server_context_init(CQServer * server, struct ibv_cq * prov_cq)
{
    server->prov_cq = prov_cq;
    server->qp_metadata_manager = qp_metadata_manager_alloc();

    atomic_store(&server->is_running, false);
}

void cq_server_context_destroy(CQServer * server)
{
    qp_metadata_manager_destroy(server->qp_metadata_manager);
}

#define CQ_SERVER_NUM_WCS 16
int cq_server_loop_once(struct cvmrdma_context * ctxt)
{
    CQServer * server = &ctxt->cvmrdma_cq_server;
    struct ibv_cq * prov_cq = server->prov_cq;
    QPMetadataManager * qp_metadata_manager = server->qp_metadata_manager;

    enum sodium_crypto_aead aead = ctxt->aead;
	unsigned long long tag_bytes = cvmrdma_ABYTES(aead);
	unsigned long long header_bytes = cvmrdma_header_bytes(aead);
	unsigned long long max_payload_bytes = cvmrdma_payload_bytes(aead);


    int err;

    struct ibv_wc wcs[CQ_SERVER_NUM_WCS];

    int nwcs = ctxt->prov_ops.poll_cq(prov_cq, CQ_SERVER_NUM_WCS, wcs);
    if(nwcs < 0)
    {
        err = nwcs;
        fprintf(stderr, "CQ server error: poll CQ failed %d\n", err);
        return err;
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

        DEBUG("[CQ] WRID = 0x%" PRIx64 " QPN %" PRIx32 "\n", this_wc->wr_id, qpn);

        if(status != IBV_WC_SUCCESS)
        {
            fprintf(stderr, "CQ server WARN: WC reports failure %d\n", status);
        }
        else if(! (op & IBV_WC_RECV))
        {
            /* This is a send completion (SEND, WRITE, WWI) */
            DEBUG("[CQ] send op %d done\n", op);

            qp->wr_chunks_sent++;

            struct send_metadata_entry * send_info = qp_send_metadata_front(qp->send_metadata);
        
            if(send_info->nchunks == qp->wr_chunks_sent)
            {
                uint32_t wrid = send_info->wrid;
                size_t bytes = send_info->bytes;

                qp->wr_chunks_sent = 0;
                qp_send_metadata_pop(qp->send_metadata);

                struct ibv_wc send_wc = {
                    .byte_len = bytes,
                    .wc_flags = 0,
                    .imm_data = 0,
                    .opcode = IBV_WC_SEND,
                    .qp_num = qpn,
                    .status = IBV_WC_SUCCESS,
                    .wr_id = wrid
                };

                /* Notify user CQ of a SEND
                    XXX: here we notify always, whereas user can request SEND_SIGNALLED or not.
                */

                struct cvmrdma_cq * scq = (struct cvmrdma_cq *)qp->user_qp.qp.send_cq;
                wcq_push(scq->wc_queue, &send_wc);
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

            struct cvmrdma_pd * pd = (struct cvmrdma_pd *)qp->user_qp.qp.pd;
            assert(qp);


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

            uint64_t const * rx_key = qp->rx_key;

            unsigned char * ringbuf_in = (unsigned char *)qp->ringbuf_va + CVMRDMA_RING_RECV * CVMRDMA_RING_TOTAL_BYTES;
            unsigned char * private = (unsigned char *)qp->bounce_private_mem + CVMRDMA_PRIV_CHUNK_ENC_IN * CVMRDMA_RING_SLOT_BYTES;
            unsigned char * decrypted = (unsigned char *)qp->bounce_private_mem + CVMRDMA_PRIV_CHUNK_SG_IN * CVMRDMA_RING_SLOT_BYTES;

            int this_recv_slot_id = qp->next_recv_slot;
            unsigned char * slot = ringbuf_in + this_recv_slot_id * CVMRDMA_RING_SLOT_BYTES;

            /* And update the slot idx */
            qp->next_recv_slot = (this_recv_slot_id + 1) % CVMRDMA_RING_ENTRIES;


            /**** COPY *****/
            /* Copy the chunk into private memory to prevent TOCTOU */

            TIMING("cds_t0");

            memcpy(private, slot, CVMRDMA_RING_SLOT_BYTES);

            /**** DECRYPT *****/
            /* Decrypt the notification, then the payload */

			/* Prepare the two nonces: for notif and for payload */
			uint64_t * qp_nonce = qp->rx_nonce;

			uint64_t nonce0[cvmrdma_aead_NPUB_WORDS], nonce1[cvmrdma_aead_NPUB_WORDS];
			memcpy(nonce0, qp_nonce, cvmrdma_aead_NPUBBYTES);
			
			memcpy(nonce1, qp_nonce, cvmrdma_aead_NPUBBYTES);
			nonce1[0] += 1;
			
			/* and increment nonce */
			qp_nonce[0] += 2;

            struct cq_server_notification notif = {0};

            unsigned char * notif_start = private;
            unsigned long long notif_clen = header_bytes; /* Q: if I put "wc_length" here, does it work ?*/
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
                (unsigned char const *)nonce0,
                (unsigned char const *)rx_key,
                aead
            );
            if(err)
            {
                fprintf(stderr, "WARN: Failed to decrypt bounce buffer, exiting.\n");
                exit(1);
            }

            enum ibv_wr_opcode wr_op = notif.op;
            bool is_write = (wr_op == IBV_WR_RDMA_WRITE) || (wr_op == IBV_WR_RDMA_WRITE_WITH_IMM);


            unsigned char * payload_start = private + header_bytes;
            unsigned long long payload_clen = wc_length - header_bytes;

            DEBUG("nonce %lx %lx\n", nonce1[0], nonce1[1]);
            DEBUG("private %x %x %x ...\n", private[0], private[1], private[2]);
            DEBUG("clen %llu\n", payload_clen);

            // Decrypt directly to destination !
            bool bypass_scatter = false;
            if(is_write)
            {
                /* The operation is a WRITE: use the remote_addr and rkey provided by the requester. We
                always write in a contiguous block, so always bypass scatter */
                uint32_t rkey = notif.rkey;
                uint64_t hca_addr = notif.hca_addr;

                struct cvmrdma_mr * mr = get_bounce_metadata_for_rkey(pd->bounce_metadata, rkey);

                uint64_t offt = hca_addr - mr->hca_va;
                void * dest = (unsigned char *)mr->user_mr.ibv_mr.addr + offt;

                decrypted = dest;
                bypass_scatter = true;
            }
            else
            {
                /* The operation is a SEND: we can only bypass the scatter if the next chunk fits
                    in the current sge */

                struct recv_metadata_entry * recv_info = qp_recv_metadata_front(qp->recv_metadata);
                struct ibv_sge * rcv_sgl = recv_info->sgl;
                struct ibv_sge * cur_sge = &rcv_sgl[qp->cur_sge_idx];

                /* plen = clen - size(MAC) */
                size_t plen_max = payload_clen - tag_bytes;
                if(qp->cur_offt_in_sge + plen_max <= cur_sge->length)
                {
                    /* can write directly to dest! */
                    struct cvmrdma_mr * cur_mr = get_bounce_metadata_for_lkey(pd->bounce_metadata, cur_sge->lkey);
                    unsigned char * mr_addr = cur_mr->user_mr.ibv_mr.addr;
                    uint64_t offt = (cur_sge->addr - cur_mr->hca_va) + qp->cur_offt_in_sge;

                    decrypted = mr_addr + offt;
                    bypass_scatter = true;
                }
            }

            unsigned long long payload_mlen = CVMRDMA_RING_SLOT_BYTES;
            err = cvmrdma_decrypt(
                decrypted, &payload_mlen,
                NULL,
                payload_start, payload_clen,
                NULL, 0,
                (unsigned char const *)nonce1,
                (unsigned char const *)rx_key,
                aead
            );
            if(err)
            {
                fprintf(stderr, "WARN: Failed to decrypt bounce buffer, exiting.\n");
                exit(1);
            }

            DEBUG("decrypted %x %x %x\n", decrypted[0], decrypted[1], decrypted[2]);

            /*** SCATTER ****/
            /* Scatter the plaintext to user memory. If the op is a WRITE, the remote has set the destination address,
                else the op is a SEND and we must use the next chunk in the current recv request */


            if(! bypass_scatter)
            {
                /* The operation is a SEND: use the next chunk on the current recv request */
                struct recv_metadata_entry * recv_info = qp_recv_metadata_front(qp->recv_metadata);
                struct ibv_sge * rcv_sgl = recv_info->sgl;
                int rcv_nsge = recv_info->nsge;
                
                size_t bytes_copied = 0;

                while((bytes_copied < payload_mlen) && (qp->cur_sge_idx < rcv_nsge))
                {
                    struct ibv_sge * cur_sge = &rcv_sgl[qp->cur_sge_idx];
                    struct cvmrdma_mr * cur_mr = get_bounce_metadata_for_lkey(pd->bounce_metadata, cur_sge->lkey);
                    unsigned char * mr_addr = cur_mr->user_mr.ibv_mr.addr;


                    size_t remaining = payload_mlen - bytes_copied;
                    size_t available = cur_sge->length - qp->cur_offt_in_sge;

                    uint64_t offt = (cur_sge->addr - cur_mr->hca_va) + qp->cur_offt_in_sge;

                    if(remaining < available)
                    {
                        memcpy(mr_addr + offt, decrypted + bytes_copied, remaining);
                        bytes_copied += remaining;
                        qp->cur_offt_in_sge += remaining;
                    }
                    else
                    {
                        memcpy(mr_addr + offt, decrypted + bytes_copied, available);
                        bytes_copied += available;
                        
                        /* move to next sge */
                        qp->cur_sge_idx++;
                        qp->cur_offt_in_sge = 0;
                    }
                }


                if(bytes_copied < payload_mlen)
                {
                    /* Early loop exit bc the sglist is empty. This is an error ! */
                    fprintf(stderr, "Incoming chunk would exceed the end of the receive request! (chunk %llu > recv %zu)\n", payload_mlen, bytes_copied);
                    exit(1);
                    /* TODO: emit IBV_WC_LOC_LEN_ERR in WC status for user !*/
                }
            }
            else
            {
                /* Record in the QP the number of bytes written during the decrypt (directly to dest) */
                struct recv_metadata_entry * recv_info = qp_recv_metadata_front(qp->recv_metadata);
                struct ibv_sge * rcv_sgl = recv_info->sgl;
                struct ibv_sge * cur_sge = &rcv_sgl[qp->cur_sge_idx];

                qp->cur_offt_in_sge += payload_mlen;
                if(qp->cur_offt_in_sge >= cur_sge->length)
                {
                    qp->cur_offt_in_sge = 0;
                    qp->cur_sge_idx++;
                }
            }

            qp->wr_bytes_rcved += payload_mlen;

            DEBUG("after chunk: rcved 0x%zx bytes of WR\n", qp->wr_bytes_rcved);

            TIMING("cds_t1");

            /* Is this the end of a WR? */
            if(notif.last_chunk)
            {
                DEBUG("last_chunk!\n");
                if(op == IBV_WR_RDMA_WRITE)
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
                    switch(op)
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

                    struct ibv_wc recv_wc = {
                        .byte_len = qp->wr_bytes_rcved,
                        .wc_flags = has_imm ? IBV_WC_WITH_IMM : 0,
                        .imm_data = imm,
                        .opcode = wc_op,
                        .qp_num = qpn,
                        .status = IBV_WC_SUCCESS,
                        .wr_id = recv_wrid
                    };

                    /* Notify user CQ of a RECV */
                    wcq_push(rcq->wc_queue, &recv_wc);

                    /* And reset the byte counter */
                    qp->wr_bytes_rcved = 0;
                }
            }

            /* We can notify the remote thay this chunk can be reused for writing
                XXX: this could be pushed all the way up right after copying into private memory
                But what about failures to decrypt?*/
            TIMING("chunk_notify_recv");
            remote_qp_notify_chunk_recv(
                ctxt->network_evloop, qp->remote_qnc
            );

        }
    }

    return 0;
}


/* Every time a QP is added to a CQ, it has to be registered on the CQ server
   Threadsafe, since the NQ manager is threadsafe.
*/
int cq_server_register_qp(CQServer * server, struct cvmrdma_qp * qp)
{
    return qp_metadata_manager_register_qp(server->qp_metadata_manager, qp->user_qp.qp.qp_num, qp);
}