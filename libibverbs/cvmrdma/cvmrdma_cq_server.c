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
// #include "cvmrdma_network_evloop_priv.h"
#include "chunk_queue.h"
#include "cvmrdma_ack.h"
#include "wr_queue.h"

// These variables are for the send side
cvm_time_t wc_send_start; // Tracks time taken to receive the WC for the write request
cvm_time_t wc_send_end; // Tracks time taken to process the received WC for the write request
// The variables are for the recv side
cvm_time_t wc_recv_start; // Tracks the inital time when a recv is noticed
cvm_time_t wc_refuel; // Tracks time taken to refuel the recv queue
cvm_time_t wc_calculations; // Tracks time taken to process the recv WC and get all properties needed for receiving
cvm_time_t wc_get_nonce; // Tracks time taken to get the nonce for the
cvm_time_t wc_decrypt_metadata; // Tracks time taken to decrypt the metadata for the recv WC
// cvm_time_t wc_wait_for_sem; // Tracks time taken to wait for the semaphore
cvm_time_t wc_copy_payload; // Tracks time taken to copy the payload for the recv WC
// cvm_time_t wc_cache_remove_write_protection; // Tracks time taken to remove write protection for the recv WC
cvm_time_t wc_decrypt_payload; // Tracks time taken to decrypt the payload for the recv WC
cvm_time_t wc_scatter_or_bypass; // Tracks time taken to scatter the payload to the user buffers or bypass if possible
cvm_time_t wc_notify_user; // Tracks time taken to notify the user of the received WR
cvm_time_t wc_notify_sender; // Tracks time taken to notify the sender that the recv is done

//_Atomic uint64_t recv_count = 0;

#define CQ_SERVER_NUM_WCS 16

// this loop is never run in parallel
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

        pthread_mutex_lock(&qp->control_qp_connection_lock);
        while (!qp->control_qp_connected && !atomic_load(&qp->stop_requested)) {
            pthread_cond_wait(&qp->control_qp_connection_cv, &qp->control_qp_connection_lock);
        }
        pthread_mutex_unlock(&qp->control_qp_connection_lock);

        struct cvmrdma_pd * pd = (struct cvmrdma_pd *)qp->user_qp.qp.pd;
        assert(pd);

        DEBUG("[CQ] WRID = 0x%" PRIx64 " QPN 0x%" PRIx32 "\n", this_wc->wr_id, qpn);

        if(status != IBV_WC_SUCCESS)
        {
            //fprintf(stderr, "ibv_wc_loc_prot_error %d, ibv_wc_wr_flush_err %d \n", IBV_WC_LOC_PROT_ERR, IBV_WC_WR_FLUSH_ERR);
            if (status != IBV_WC_WR_FLUSH_ERR){
                fprintf(stderr, "CQ server WARN: WC reports failure status: %s, vendor err %d\n", ibv_wc_status_str(status), this_wc->vendor_err);
                fprintf(stderr, "  op %d, wrid 0x%lx, byte_len %u, qp_num %x\n", op, this_wc->wr_id, wc_length, qpn);
            } else 
            {
                DEBUG("CQ server: WC reports flush status: %d, vendor err %d\n", status, this_wc->vendor_err);
            }

            if(op == IBV_WC_SEND) {
                struct cvmrdma_mr* mr = NULL;
                int k = 0;
                do {
                    mr = get_bounce_metadata_for_rkey(pd->bounce_metadata, k);
                    if (mr != NULL) {
                        fprintf(stderr, "[CQ] bounce_metadata[%d]: len: %zx, user_mr addr: %p, user_mr hca_va: %p\n", 
                        k, mr->user_mr.ibv_mr.length, mr->user_mr.ibv_mr.addr, mr->hca_va);
                    }
                    k++;
                } while (mr != NULL);
            }
        }
        else if(! (op & IBV_WC_RECV))
        {
            get_time(&wc_send_start);
            /* This is a send completion (SEND, WRITE, WWI) */
            DEBUG("[CQ] send op %d, wrid: 0x%lx done\n", op, this_wc->wr_id);

            // we track how many wrs are currently in transit, as our underlying nic cannot handle more than 4096
            pthread_mutex_lock(&qp->ibv_send_in_transit_lock);
            qp->send_in_transit_counter--;
            pthread_mutex_unlock(&qp->ibv_send_in_transit_lock);
            qp->wr_chunks_sent++;

            struct send_metadata_entry * send_info = NULL;
            do {
                send_info = qp_send_metadata_front(qp->send_metadata);

                // this is to make sure the thread can exit cleanly
                if (atomic_load(&qp->stop_requested)) {
                    fprintf(stderr, "Stop requested while processing send WC, exiting cq_server_loop_once\n");
                    return -1;
                }
            } while (send_info == NULL);


            DEBUG("[CQ] send op %d, wrid: 0x%lx done, chunks sent %zu send_info->nchunks %zu\n", op, this_wc->wr_id, qp->wr_chunks_sent, send_info->nchunks);
            
            if(send_info->nchunks == qp->wr_chunks_sent || send_info->no_data_send)
            {
                uint64_t wrid = send_info->wrid;
                size_t bytes = send_info->bytes;
                bool is_write = (send_info->op == IBV_WR_RDMA_WRITE) || (send_info->op == IBV_WR_RDMA_WRITE_WITH_IMM);

                DEBUG("[CQ] send op %d, wrid: 0x%lx done, all chunks sent\n", op, wrid);

                qp->wr_chunks_sent = 0;
                qp_send_metadata_pop(qp->send_metadata);

                DEBUG("[CQ] send op %d, wrid: 0x%lx done, popped metadata\n", op, wrid);

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
                if((send_info->send_signaled || qp->user_sq_sig_all) && send_info->op != IBV_WR_RDMA_READ_PING && send_info->op != IBV_WR_RDMA_READ_INTERNAL) {
                    DEBUG("[CQ] Sending WC to user send_signaled %d user_sq_sig_all %d\n", send_info->send_signaled, qp->user_sq_sig_all);
                    DEBUG("[CQ] User wrid %lx, byte_len %zu, imm_data %x, opcode %d, qp_num %x, status %d\n", send_wc.wr_id, send_wc.byte_len, send_wc.imm_data, send_wc.opcode, send_wc.qp_num, send_wc.status);
                    struct cvmrdma_cq * scq = (struct cvmrdma_cq *)qp->user_qp.qp.send_cq;
                    wcq_push(scq->wc_queue, &send_wc);

                    uint64_t event = (uintptr_t) scq;
                    bool expected = 1;
                    if (atomic_compare_exchange_strong(&scq->comp_channel_event_fd_filled, &expected, 0))
                    {
                        DEBUG("[CQ] Writing %p to event fd %d\n", scq, scq->comp_channel_event_fd);
                        write(scq->comp_channel_event_fd, &event, sizeof(event));
                    }

                }
            }
            get_time(&wc_send_end);


            LOG_STRING("SEND WR/WC times:");
            LOG_TIME(wr_submit_start);
            LOG_TIME(wr_submit_end);
            LOG_TIME(wr_calculations);
            LOG_TIME(wr_metadata_push);
            LOG_TIME(wr_send_nonce);
            LOG_TIME(wr_wait_for_recv_slot);
            LOG_TIME(wr_encrypt_metadata);
            LOG_TIME(wr_encrypt_payload);
            LOG_TIME(wr_ibv_post_send);
            LOG_TIME(wc_send_start);
            LOG_TIME(wc_send_end);
        }
        else
        {
            /* A SEND WR has happened : per CVMRDMA procotol, it has to be a WRITE_WITH_IMM of a chunk in the ring buffer */
            // fprintf(stderr, "CQ server: received recv WC for QPN %x, wrid 0x%lx, len %d\n", qpn, this_wc->wr_id, wc_length);

            get_time(&wc_recv_start);

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
            get_time(&wc_refuel);


            /*pthread_mutex_lock(&qp->remote_params_lock);
            if(!qp->remote_params_set) {
                DEBUG("WARN: remote params not set for QPN %x\n", qpn);
            }
            while (!qp->remote_params_set) {
                pthread_cond_wait(&qp->remote_params_set_cv, &qp->remote_params_lock);
            }
            pthread_mutex_unlock(&qp->remote_params_lock);
            DEBUG("remote params set for QPN %x\n", qpn);
            assert(qp->remote_params_set);*/
            uint64_t const * shared_key = qp->shared_key;

            unsigned char * ringbuf_in = (unsigned char *)qp->recv_ringbuf_va;
            unsigned char * private = (unsigned char *)qp->bounce_private_mem + CVMRDMA_PRIV_CHUNK_ENC_IN * cvmrdma_ring_slot_bytes(ctxt->aead);
            unsigned char * decrypted = (unsigned char *)qp->bounce_private_mem + CVMRDMA_PRIV_CHUNK_SG_IN * cvmrdma_ring_slot_bytes(ctxt->aead);

            int this_recv_slot_id = qp->next_recv_slot;
            unsigned char * slot = ringbuf_in + this_recv_slot_id * cvmrdma_ring_slot_bytes(aead);
            DEBUG("[CQ] processing recv slot %d with addr %p (ringslot bytes: 0x%x)\n", 
                this_recv_slot_id, slot, cvmrdma_ring_slot_bytes(aead));

            /* And update the slot idx */
            qp->next_recv_slot = (this_recv_slot_id + 1) % cvmrdma_ring_entries;
            qp->received_lines++;

            /**** DECRYPT *****/
            /* Decrypt the notification, then the payload 
               Based on the header, we decide where to copy the encrypted payload
            */

            struct cq_server_notification notif = {0};

            unsigned char * notif_start = slot;
            unsigned long long notif_clen = header_bytes;
            if(header_bytes > wc_length)
            {
                fprintf(stderr, "Failed to decrypt: chunk too short\n");
                exit(1);
            }

            get_time(&wc_calculations);

            uint64_t nonce_metadata[cvmrdma_aead_NPUB_WORDS] = {0};
            pthread_mutex_lock(&qp->recv_counter_lock);
            nonce_metadata[0] = qp->recv_counter;
            qp->recv_counter += 2;
            pthread_mutex_unlock(&qp->recv_counter_lock);

            get_time(&wc_get_nonce);

            unsigned long long notif_mlen = sizeof(notif);
            err = cvmrdma_decrypt(
                (unsigned char *)&notif, &notif_mlen,
                NULL,
                notif_start, notif_clen,
                NULL, 0,
                (unsigned char const *)nonce_metadata,
                (unsigned char const *)shared_key,
                aead
            );
            if(err)
            {
                fprintf(stderr, "WARN for WC %d: Failed to decrypt bounce buffer for notif with key %lx %lx and nonce %lx %lx %lx notif_start[0] %x %x %x %x notif_start %p notif_clen %llu\n", 
                    this_wc->wr_id, shared_key[0], shared_key[1], nonce_metadata[0], nonce_metadata[1], nonce_metadata[2], notif_start[0], notif_start[1], notif_start[2], notif_start[3], notif_start, notif_clen);
                DEBUG("notif immediate data %d, opcode %d, wc_length %d\n", notif.imm, this_wc->opcode, wc_length);
                    exit(1);
            } else {
                DEBUG("SUCC for WC %d: Decrypted notif with key %lx %lx and nonce %lx %lx %lx\n", 
                    this_wc->wr_id, shared_key[0], shared_key[1], nonce_metadata[0], nonce_metadata[1], nonce_metadata[2]);
                DEBUG("notif contents: %x %x %x %x %x %x %x %x %x %x \n", 
                    notif_start[0], notif_start[1], notif_start[2], notif_start[3],
                    notif_start[4], notif_start[5], notif_start[6], notif_start[7],
                    notif_start[8], notif_start[9]);
                DEBUG("notif immediate data %d, opcode %d, wc_length %d\n", notif.imm, this_wc->opcode, wc_length);
            }
            get_time(&wc_decrypt_metadata);

            enum ibv_wr_opcode wr_op = notif.op;
            bool is_write = (wr_op == IBV_WR_RDMA_WRITE) || (wr_op == IBV_WR_RDMA_WRITE_WITH_IMM);
            bool is_send = (wr_op == IBV_WR_SEND) || (wr_op == IBV_WR_SEND_WITH_IMM);
            bool is_read_internal = (wr_op == IBV_WR_RDMA_READ_INTERNAL);
            bool is_read_ping = (wr_op == IBV_WR_RDMA_READ_PING);

            if (is_read_ping) {

                // fprintf(stderr, "[CQ] Received READ_PING notif. QPN %x, addr %lx, len %lu, rkey %x\n", qpn, notif.hca_addr, notif.chunk_len, notif.rkey);
                // for read pings, we have to issue an internal read to fetch the data
                // and send it back to the requester
                struct ibv_send_wr send_wrs[1] = {0};
                struct ibv_sge send_sgl[1] = {{.addr = (uint64_t) notif.hca_addr, .length = notif.chunk_len, .lkey = notif.rkey}};
                // we don't send a SIGNALED flag, because this wr is internal and on the server side we don't
                // expect a work completion for it
                send_wrs[0].opcode = IBV_WR_RDMA_READ_INTERNAL;
                send_wrs[0].num_sge = 1;
                send_wrs[0].imm_data = 0;
                // wr_id is not used in this case, but we set it to 0 for consistency        
                send_wrs[0].wr_id = 0;
                send_wrs[0].sg_list = send_sgl;
                send_wrs[0].next = NULL;

                send_ack(ctxt, qp);
                // fprintf(stderr, "[CQ] Posting internal read WWI for READ_PING. QPN %x, addr %lx, len %lu, rkey %x\n", qpn, send_sgl[0].addr, send_sgl[0].length, send_sgl[0].lkey);
#ifndef DISABLE_SEND_LOOP
                wrq_push(qp->wr_queue, &send_wrs[0]);
#else
                post_send_do_bounce(qp, &send_wrs[0], NULL);
#endif
                // skip the whole chunk processing for read pings
                // and only trigger the code that notifies the other node that chunk slots can be reused
                goto skip_chunk_processing;
            }

            bool no_data_send = (notif.payload_len == 0);

            if (no_data_send) {
                DEBUG("[CQ] no data send notif contents: hca_addr: %lx, op: %d, rkey: %x, imm: %x, chunk_len: %lu, last_chunk: %d, payload_len: %lu\n",
                    notif.hca_addr, notif.op, notif.rkey, notif.imm,
                    notif.chunk_len, notif.last_chunk, notif.payload_len);
            }

            if (!no_data_send){

                bool skip_scatter = false;
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

                    // the chunk offset needs to be 0, otherwise the encrypted chunk
                    // saved will not match the state of the chunk in the user memory region
                    if(notif.chunk_len == OPT_cvmrdma_chunk_bytes && !OPT_disable_skip_scatter){
                        decrypted = dest;
                        skip_scatter = true;
                    }
                }
                else if (is_send || is_read_internal)
                {
                    /* The operation is a SEND: we can only bypass the scatter if the next chunk fits
                        in the current sge */

                    //fprintf(stderr, "Waiting to get recv_info for SEND/READ_INTERNAL op\n");
                    struct recv_metadata_entry * recv_info = NULL;
                    do {
                        if (is_send) {
                            recv_info = qp_recv_metadata_front(qp->recv_metadata);
                        } else if (is_read_internal) {
                            recv_info = qp_read_metadata_front(qp->read_metadata);
                        }

                        // this is to make sure the thread can exit cleanly
                        if (atomic_load(&qp->stop_requested)) {
                            fprintf(stderr, "Stop requested while processing send WC, exiting cq_server_loop_once\n");
                            return -1;
                        }
                    } while (recv_info == NULL);
                    // fprintf(stderr, "Got recv_info for SEND/READ_INTERNAL op\n");
                    
                    
                    struct ibv_sge * rcv_sgl = recv_info->sgl;
                    struct ibv_sge * cur_sge = &rcv_sgl[qp->cur_sge_idx];

                    cur_mr = get_bounce_metadata_for_lkey(pd->bounce_metadata, cur_sge->lkey);
                    assert(cur_mr);

                    // the chunk offset needs to be 0, otherwise the encrypted chunk
                    // saved will not match the state of the chunk in the user memory region
                    if(notif.chunk_len == OPT_cvmrdma_chunk_bytes 
                        && qp->cur_offt_in_sge + notif.chunk_len <= cur_sge->length && !OPT_disable_skip_scatter)
                    {
                        /* can write directly to dest! */
                        unsigned char * mr_addr = cur_mr->user_mr.ibv_mr.addr;
                        offt = (cur_sge->addr - cur_mr->hca_va) + qp->cur_offt_in_sge;

                        decrypted = mr_addr + offt;
                        skip_scatter = true;
                    }
                } else {
                    fprintf(stderr, "PROTOCOL ERROR: unexpected op %d in CQ server\n", wr_op);
                    exit(1);
                }


                unsigned long long payload_clen = notif.payload_len + cvmrdma_ABYTES(aead);

                DEBUG("[CQ] clen %llu\n", payload_clen);

                unsigned char * payload_start = NULL;

                // copy payload to private memory
                // payload_start = private + header_bytes;
                // memcpy(payload_start, slot + header_bytes, payload_clen);

                get_time(&wc_copy_payload);

                nonce_metadata[0] += 1; // increment nonce for payload

                unsigned long long payload_mlen = OPT_cvmrdma_chunk_bytes;
                err = cvmrdma_decrypt(
                    decrypted, &payload_mlen,
                    NULL,
                    slot+header_bytes, payload_clen,
                    NULL, 0,
                    (unsigned char const *)nonce_metadata,
                    (unsigned char const *)shared_key,
                    aead
                );
                if(err)
                {
                    fprintf(stderr, "WARN: Failed to decrypt bounce buffer for payload, exiting.\n");
                    exit(1);
                }
                get_time(&wc_decrypt_payload);

                /*** SCATTER ****/
                /* Scatter the plaintext to user memory. If the op is a WRITE, the remote has set the destination address,
                    else the op is a SEND and we must use the next chunk in the current recv request */

                if(!skip_scatter)
                {
                    if (is_write){
                        memcpy(dest, decrypted, notif.chunk_len);
                    } else if (is_send || is_read_internal) {
                        /* The operation is a SEND: use the next chunk on the current recv request */

                        struct recv_metadata_entry * recv_info = NULL;
                        do {
                            if (is_send) {
                                recv_info = qp_recv_metadata_front(qp->recv_metadata);
                            } else if (is_read_internal) {
                                recv_info = qp_read_metadata_front(qp->read_metadata);
                            }

                            // this is to make sure the thread can exit cleanly
                            if (atomic_load(&qp->stop_requested)) {
                                fprintf(stderr, "Stop requested while processing send WC, exiting cq_server_loop_once\n");
                                return -1;
                            }
                        } while (recv_info == NULL);

                        struct ibv_sge * rcv_sgl = recv_info->sgl;
                        int rcv_nsge = recv_info->nsge;

                        if (rcv_nsge <= 0) {
                            fprintf(stderr, "PROTOCOL ERROR: recv request has no SGE entries!\n");
                            exit(1);
                        }
                        
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

                        if(bytes_copied < notif.chunk_len)
                        {
                            /* Early loop exit bc the sglist is empty. This is an error ! */
                            fprintf(stderr, "Incoming chunk would exceed the end of the receive request! (notif.chunk_len 0x%"PRIx64" > recv %zu)\n", notif.chunk_len, bytes_copied);
                            exit(1);
                            /* TODO: emit IBV_WC_LOC_LEN_ERR in WC status for user !*/
                        }
                    }
                }
                else // if we use the chunk cache, the chunk was directly decrypted to user memory
                {
                    if (is_send || is_read_internal) {
                        /* Record in the QP the number of bytes written during the decrypt (directly to dest) */
                        struct recv_metadata_entry * recv_info = NULL;

                        do {
                            if (is_send) {
                                recv_info = qp_recv_metadata_front(qp->recv_metadata);
                            } else if (is_read_internal) {
                                recv_info = qp_read_metadata_front(qp->read_metadata);
                            }

                            // this is to make sure the thread can exit cleanly
                            if (atomic_load(&qp->stop_requested)) {
                                fprintf(stderr, "Stop requested while processing send WC, exiting cq_server_loop_once\n");
                                return -1;
                            }
                        } while (recv_info == NULL);

                        struct ibv_sge * rcv_sgl = recv_info->sgl;
                        struct ibv_sge * cur_sge = &rcv_sgl[qp->cur_sge_idx];

                        qp->cur_offt_in_sge += notif.chunk_len;
                        if(qp->cur_offt_in_sge >= cur_sge->length)
                        {
                            qp->cur_offt_in_sge = 0;
                            qp->cur_sge_idx++;
                        }
                    }
                }


                get_time(&wc_scatter_or_bypass);
            } else {
                wc_copy_payload = wc_decrypt_metadata;
                wc_decrypt_payload = wc_decrypt_metadata;
                wc_scatter_or_bypass = wc_decrypt_metadata;
            }

            qp->wr_bytes_rcved += notif.chunk_len;

            DEBUG("after chunk: rcved 0x%zx bytes of WR\n", qp->wr_bytes_rcved);

            /* Is this the end of a WR? */
            if(notif.last_chunk)
            {
                DEBUG("last_chunk!\n");
                if(wr_op == IBV_WR_RDMA_WRITE)
                {
                    /* Do not consume an RR, do not emit a WC. But restore the byte counter */
                    qp->wr_bytes_rcved = 0;
                }
                else if (wr_op == IBV_WR_RDMA_READ_INTERNAL)
                {
                    // get and pop the read entry, because we handeled it
                    // this entry was needed so the client nows where to place the data when it arrives
                    // from the server
                    
                    struct recv_metadata_entry * recv_info = NULL;
                    do {
                        recv_info = qp_read_metadata_front(qp->read_metadata);

                        // this is to make sure the thread can exit cleanly
                        if (atomic_load(&qp->stop_requested)) {
                            fprintf(stderr, "Stop requested while processing send WC, exiting cq_server_loop_once\n");
                            return -1;
                        }
                    } while (recv_info == NULL);
                    uint64_t recv_wrid = recv_info->wrid;
                    int send_signaled = recv_info->send_signaled;
                    qp_read_metadata_pop(qp->read_metadata);
                    qp->cur_sge_idx = 0;
                    qp->cur_offt_in_sge = 0;


                    struct ibv_wc send_wc = {
                        .byte_len = qp->wr_bytes_rcved,
                        .wc_flags = 0,
                        .opcode = IBV_WC_RDMA_READ,
                        .qp_num = qpn,
                        .status = IBV_WC_SUCCESS,
                        .wr_id = recv_wrid
                    };

                    if(send_signaled || qp->user_sq_sig_all) {
                        struct cvmrdma_cq * scq = (struct cvmrdma_cq *)qp->user_qp.qp.send_cq;
                        wcq_push(scq->wc_queue, &send_wc);

                        uint64_t event = (uintptr_t) scq;
                        bool expected = 1;
                        if (atomic_compare_exchange_strong(&scq->comp_channel_event_fd_filled, &expected, 0))
                        {
                            DEBUG("[CQ] Writing %p to event fd %d\n", scq, scq->comp_channel_event_fd);
                            write(scq->comp_channel_event_fd, &event, sizeof(event));
                        }

                    }

                    // Do not write into the fd file as below, because this is a notification
                    // on the scq not on the rcq (we do not notify that a recv happend, because this is a read)

                    qp->wr_bytes_rcved = 0;
                } 
                else
                {
                
                    /* SEND(_WITH_IMM) and WRITE_WITH_IMM consume an RR: recover its wrid and move to the next one */
                    struct cvmrdma_cq * rcq = (struct cvmrdma_cq *)qp->user_qp.qp.recv_cq;

                    struct recv_metadata_entry * recv_info = NULL;
                    do {
                        recv_info = qp_recv_metadata_front(qp->recv_metadata);

                        // this is to make sure the thread can exit cleanly
                        if (atomic_load(&qp->stop_requested)) {
                            fprintf(stderr, "Stop requested while processing send WC, exiting cq_server_loop_once\n");
                            return -1;
                        }
                    } while (recv_info == NULL);
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
                    // assert(qp->wr_bytes_rcved > 0);

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

                    // TODO: This might need to adhere to signaled completions
                    //fprintf(stderr, "[CQ] RECV WC: byte_len %zu, imm %x, opcode %d, qp_num %x, status %d, wr_id %lx\n", recv_wc.byte_len, recv_wc.imm_data, recv_wc.opcode, recv_wc.qp_num, recv_wc.status, recv_wc.wr_id);
                    wcq_push(rcq->wc_queue, &recv_wc);

                    //recv_count++;
                    // Pointer to cq struct, see verbs.c:603
                    uint64_t event = (uintptr_t) rcq;
                    bool expected = 1;
                    if (atomic_compare_exchange_strong(&rcq->comp_channel_event_fd_filled, &expected, 0))
                    {
                        DEBUG("[CQ] Writing %p to event fd %d\n", rcq, rcq->comp_channel_event_fd);
                        write(rcq->comp_channel_event_fd, &event, sizeof(event));
                    }

                    /* And reset the byte counter */
                    qp->wr_bytes_rcved = 0;
                }
            }
            get_time(&wc_notify_user);

            /* We can notify the remote thay this chunk can be reused for writing
                XXX: this could be pushed all the way up right after copying into private memory
                But what about failures to decrypt?*/

            send_ack(ctxt, qp);
skip_chunk_processing:
            get_time(&wc_notify_sender);

            LOG_STRING("RECV WC times:");
            LOG_TIME(wc_recv_start);
            LOG_TIME(wc_refuel);
            LOG_TIME(wc_calculations);
            LOG_TIME(wc_get_nonce);
            LOG_TIME(wc_decrypt_metadata);
            // LOG_TIME(wc_wait_for_sem);
            LOG_TIME(wc_copy_payload);
            // LOG_TIME(wc_cache_remove_write_protection);
            LOG_TIME(wc_decrypt_payload);
            LOG_TIME(wc_scatter_or_bypass);
            LOG_TIME(wc_notify_user);
            LOG_TIME(wc_notify_sender);

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
            if (ret > 0) {
                *again = true;
            } else {
                *again = false;
            }
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
            if (ret > 0) {
                *again = true;
            } else {
                *again = false;
            }
        }
	} else {
        if(again != NULL) {
            *again = true;
        }
    }
    return ret;
}

// This is used if the actual application uses the completion channel



void* cvmrdma_cq_wait_completion_event(void* arg) {
    struct cvmrdma_qp *qp = (struct cvmrdma_qp *)arg;
    struct cvmrdma_cq * cq = (struct cvmrdma_cq *)qp->provider_cq;
    struct cvmrdma_context * context = cvmrdma_get_ctx(cq->user_cq.cq.context);

    DEBUG("[CQ] Waiting for CQ event\n");

    if (OPT_disable_polling) {
        // Event-based polling using libibverbs completion events
        struct ib_uverbs_comp_event_desc ev;
        struct ibv_cq *ev_cq;
        void *ev_ctx;
        bool run_again_recv = false;
        bool run_again_send = false;
        int ret;

        while (!atomic_load(&qp->stop_requested)) {
            // Wait for completion event on the completion channel with a timeout
            // so we can check stop_requested periodically
            struct pollfd pfd = {
                .fd = cq->comp_channel->fd,
                .events = POLLIN,
            };
            
            //fprintf(stderr, "[CQ] Polling on completion channel fd %d\n", cq->comp_channel->fd);
            int poll_ret = poll(&pfd, 1, 100); // 100ms timeout
            if (poll_ret < 0) {
                fprintf(stderr, "poll() failed\n");
                exit(1);
            }
            
            // Check if we should stop before trying to read
            if (atomic_load(&qp->stop_requested)) {
                break;
            }
            
            // If timeout occurred, loop back to check stop_requested again
            if (poll_ret == 0) {
                continue;
            }
            
            //fprintf(stderr, "[CQ] Blocking on completion channel fd %d\n", cq->comp_channel->fd);
            if (read(cq->comp_channel->fd, &ev, sizeof ev) != sizeof ev) {
                fprintf(stderr, "Failed to read CQ event\n");
                exit(1);
            }
            //fprintf(stderr, "[CQ] Received CQ event for cq_handle %lx\n", ev.cq_handle);

            ev_cq = (struct ibv_cq *) (uintptr_t) ev.cq_handle;
            ev_ctx = ev_cq->cq_context;
            context->prov_ops.cq_event(ev_cq);

            DEBUG("[CQ] Got CQ event\n");
            assert(ev_cq == cq->prov_cq_recv || ev_cq == cq->prov_cq_send);


            pthread_mutex_lock(&ev_cq->mutex);
            ev_cq->comp_events_completed += 1;
            pthread_cond_signal(&ev_cq->cond);
            pthread_mutex_unlock(&ev_cq->mutex);

            ret = context->prov_ops.req_notify_cq(ev_cq, 0);
            if (ret) {
                fprintf(stderr, "Couldn't request CQ notification\n");
                exit(1);
            }

            // fprintf(stderr, "[CQ] Processing CQ event for cq_handle %lx\n", ev.cq_handle);
            

            if(ev_cq == cq->prov_cq_recv) {
                do {
                    cvmrdma_cq_server_run_once_if_idle_recv(context, cq, &run_again_recv);
                } while(run_again_recv);
            } else {
                do {
                    cvmrdma_cq_server_run_once_if_idle_send(context, cq, &run_again_send);
                } while(run_again_send);
            }
            //fprintf(stderr, "[CQ] Finished processing CQ event for cq_handle %lx\n", ev.cq_handle);
        }
    } else {
        // Busy polling mode (original behavior)
        while (!atomic_load(&qp->stop_requested)) {
            cvmrdma_cq_server_run_once_if_idle_send(context, cq, NULL);
            cvmrdma_cq_server_run_once_if_idle_recv(context, cq, NULL);
        }
    }

    // fprintf(stderr, "[CQ] Exiting CQ completion event thread\n");

    return NULL;
}

void cvmrdma_stop_cq_thread(struct cvmrdma_qp * qp)
{
    atomic_store(&qp->stop_requested, 1);

    // make sure the cq thread is not blocked on the parameters condition variable
    pthread_mutex_lock(&qp->remote_params_lock);
    if (!qp->remote_params_set) {
        qp->remote_params_set = true; /* or leave false and just broadcast */
        pthread_cond_broadcast(&qp->remote_params_set_cv);
    }
    pthread_mutex_unlock(&qp->remote_params_lock);
}