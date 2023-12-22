#include "cvmrdma_cq_server.h"

#include <assert.h>
#include <stdio.h>
#include <stdbool.h>
#include <pthread.h>

#include <sodium.h>

#include <infiniband/driver.h>
#include "debug.h"

/*
    TODO:
    Extend CQ with a queue that this server will publish to. Implement the user facing poll_cq on
    this queue.
*/

void cq_server_context_init(CQServer * server, struct ibv_cq * cq)
{
    server->cq = cq;
    server->qp_metadata_manager = qp_metadata_manager_alloc();
}

void cq_server_context_destroy(CQServer * server)
{
    qp_metadata_manager_destroy(server->qp_metadata_manager);
}

#define CQ_SERVER_NUM_WCS 16
static void * cq_server_loop(void * args)
{
    CQServer * server = args;
    struct ibv_cq * ibcq = server->cq;
    struct verbs_cq * cq = (struct verbs_cq *)ibcq;
    QPMetadataManager * qp_metadata_manager = server->qp_metadata_manager;
    struct verbs_context * ctxt = verbs_get_ctx(ibcq->context);
    WCQueue * wcq = cq->wc_queue;

    int err;

    struct ibv_wc wcs[CQ_SERVER_NUM_WCS];

    while(true)
    {
        /* TODO: try to switch to poll cq ex */
        int nwcs = ctxt->cvmrdma.poll_cq(ibcq, CQ_SERVER_NUM_WCS, wcs);
        if(nwcs < 0)
        {
            err = nwcs;
            fprintf(stderr, "CQ server error: poll CQ failed %d\n", err);
            return (void*)err;
        }
        if(nwcs > 0)
        {
            DEBUG("[CQ] polled %d\n", nwcs);
        }

        for(int i = 0 ; i < nwcs ; i++)
        {
            TIMING("cqs_wc");
            struct ibv_wc * this_wc = &wcs[i];
            enum ibv_wc_status status = this_wc->status;
            enum ibv_wc_opcode op = this_wc->opcode;
            unsigned int wc_flags = this_wc->wc_flags;
            /* The user-defined immediate is sent on the other channel, the imm in the WC is unused for now! */
            __be32 protocol_imm = this_wc->imm_data;
            uint32_t wc_length = this_wc->byte_len;
            uint32_t qpn = this_wc->qp_num;

            DEBUG("[CQ] WRID = 0x%" PRIx64 "\n", this_wc->wr_id);

            if(status != IBV_WC_SUCCESS)
            {
                fprintf(stderr, "CQ server WARN: WC reports failure\n");
                /* TODO: pass failure to normal poll_cq */
                wcq_push(wcq, this_wc);
                continue;
            }

            if(op == IBV_WC_RDMA_WRITE)
            {
                DEBUG("[CQ] WRITE done\n");
            }
            else if (op == IBV_WC_RECV_RDMA_WITH_IMM)
            {
                DEBUG("[CQ] Recv IMM\n");
            }
            else
            {
                fprintf(stderr, "CQ server ERROR: received op %d, for now only RDMA WRITE is supported\n", op);
                wcq_push(wcq, this_wc);
                continue;
            }

            if(! (wc_flags & IBV_WC_WITH_IMM))
            {
                /* XXX: is this accurate? is WITH_IMM set for sender AND receiver ??*/
                fprintf(stderr, "CQ server WARN: the server expects all WRITEs to report a MAC in the immediate value, "
                                "but this completion has no immedite\n");
            }


            if (op == IBV_WC_RECV_RDMA_WITH_IMM)
            {
                /* A WRITE has happened in a bounce buffer. To have enough information to decrypt this
                payload into the user region, retrieve the RPC notification on the corresponding QP */
                
                NotificationQueue * nq = qp_metadata_manager_get_queue(qp_metadata_manager, qpn);
                assert(nq);

                uint64_t const * rx_key = notification_queue_get_key(nq);
                assert(rx_key);

                /* Read the front of the queue inplace */
                struct cq_server_notification const * notification = notification_queue_front(nq);

                void * bounce_addr = notification->bounce_addr;
                void * user_addr = notification->user_addr;
                size_t length = notification->length;
                uint32_t user_imm = notification->imm;
                bool has_imm = notification->has_imm;
                uint64_t const * nonce = notification->nonce;
                uint64_t const * mac = notification->mac;

                assert(length == wc_length);

                DEBUG("Decrypting %zu B from %p to %p\n", length, bounce_addr, user_addr);
                DEBUG("nonce %lx %lx %lx %lx ; mac = %lx %lx %lx %lx\n", nonce[0], nonce[1], nonce[2], nonce[3], mac[0], mac[1], mac[2], mac[3]);
#ifndef CVMRDMA_DUMMY_BOUNCE
                TIMING("dec_t0");
                err = crypto_aead_aegis256_decrypt_detached(
                    user_addr, NULL,
                    bounce_addr, length,
                    (unsigned char const *)mac,
                    NULL, 0,
                    (unsigned char const *)nonce,
                    (unsigned char const *)rx_key
                );
                TIMING("dec_t1");
                if(err)
                {
                    DEBUG("WARN: Failed to decrypt bounce buffer, ignoring this WC.\n");
                    /* In this case, don't pop from the queue: it might be an injected WR
                       from an attacker. */
                    continue;
                }
#else
                memcpy(user_addr, bounce_addr, length);
#endif

                /* Pop the entry after use */
                notification_queue_pop(nq);


                /* Modify the WC to report correct immediate to user */
                this_wc->imm_data = user_imm;            
            }


            /* Notify the user with the (potentially modified) WC */
            wcq_push(wcq, this_wc);

        }
    }

    __builtin_unreachable();
    return 0;
}


int cq_server_run(CQServer * server, pthread_t * newthread)
{
    return pthread_create(newthread, NULL, cq_server_loop, server);
}


/* Every time a QP is added to a CQ, it has to be registered on the CQ server
   Threadsafe, since the NQ manager is threadsafe.
*/
NotificationQueue * cq_server_add_qp(CQServer * server, uint32_t qpn)
{
    return qp_metadata_manager_create_queue(server->qp_metadata_manager, qpn);
}