#include "cvmrdma_cq_server.h"

#include <assert.h>
#include <stdio.h>
#include <stdbool.h>
#include <pthread.h>

#include "debug.h"

/*
    HACKY: we use the CQ's public interface to do the polling !
    This means that if a user polls the same CQ, everything breaks.

    Better: Move existing CQ as a CQ backend. Extend CQ with a queue that
    this server will publish to. Implement the user facing poll_cq on
    this queue.
*/

void cq_server_context_init(CQServer * server, struct ibv_cq * cq)
{
    server->cq = cq;
    server->nqm = nq_manager_alloc();
}

void cq_server_context_destroy(CQServer * server)
{
    nq_manager_destroy(server->nqm);
}

#define CQ_SERVER_NUM_WCS 16
static void * cq_server_loop(void * args)
{
    CQServer * server = args;
    struct ibv_cq * cq = server->cq;
    NotificationQueueManager * nqm = server->nqm;
    struct verbs_context * ctxt = verbs_get_ctx(cq->context);

    int err;

    struct ibv_wc wcs[CQ_SERVER_NUM_WCS];

    while(true)
    {
        /* TODO: try to switch to poll cq ex */
        int nwcs = ctxt->cvmrdma.poll_cq(cq, CQ_SERVER_NUM_WCS, wcs);
        if(nwcs < 0)
        {
            err = nwcs;
            fprintf(stderr, "CQ server error: poll CQ failed %d\n", err);
            return (void*)err;
        }

        for(int i = 0 ; i < nwcs ; i++)
        {
            struct ibv_wc * this_wc = &wcs[i];
            enum ibv_wc_status status = this_wc->status;
            enum ibv_wc_opcode op = this_wc->opcode;
            unsigned int wc_flags = this_wc->wc_flags;
            /* The user-defined immediate is sent on the other channel,
            the imm in the WC is the mac added by the protocol */
            __be32 protocol_imm = this_wc->imm_data;
            uint32_t wc_length = this_wc->byte_len;
            uint32_t qpn = this_wc->qp_num;

            if(status != IBV_WC_SUCCESS)
            {
                fprintf(stderr, "CQ server WARN: WC reports failure\n");
                /* TODO: pass failure to normal poll_cq */
                continue;
            }

            if(op != IBV_WC_RDMA_WRITE)
            {
                fprintf(stderr, "CQ server ERROR: received op %d, for now only RDMA WRITE is supported\n", op);
                return (void*)EINVAL;
            }

            if(! (wc_flags & IBV_WC_WITH_IMM))
            {
                fprintf(stderr, "CQ server ERROR: the server expects all WRITEs to report a MAC in the immediate value, "
                                "but this completion has no immedite\n");
                return (void*)EINVAL;
            }


            if (op == IBV_WC_RECV_RDMA_WITH_IMM)
            {
            /* A WRITE has happened in a bounce buffer. To have enough information to decrypt this
            payload into the user region, retrieve the RPC notification on the corresponding QP */
            
            NotificationQueue * nq = nq_manager_get_queue(nqm, qpn);
            assert(nq);

            /* Read the front of the queue inplace */
            struct cq_server_notification const * notification = notification_queue_front(nq);

            void * bounce_addr = notification->bounce_addr;
            void * user_addr = notification->user_addr;
            size_t length = notification->length;
                unsigned int nonce_counter = notification->nonce_counter;
            uint32_t user_imm = notification->imm;
                bool has_imm = notification->has_imm;

            unsigned char nonce[3] = {
                0x0000FFu & nonce_counter,
                0x00FF00u & nonce_counter,
                0xFF0000u & nonce_counter
            };

            unsigned char mac[2] = {
                0x00FFu & protocol_imm,
                0xFF00u & protocol_imm
            };

            assert(length == wc_length);

            DEBUG("Decrypting %zu B from %p to %p with nonce %x %x %x ; mac = %x %x\n", length, bounce_addr, user_addr, nonce[0], nonce[1], nonce[2], mac[0], mac[1]);

            /* TODO: decrypt !*/
            memcpy(user_addr, bounce_addr, length);

            /* Pop the entry after use */
            notification_queue_pop(nq);

            /* TODO: push a WC to a queue pollable by user with the user_imm !*/
                DEBUG("WARN: dropping IMM from WRITE notification! Imm = 0x%" PRIx32 " valid = %d\n", user_imm, has_imm);
            }
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
    return nq_manager_create_queue(server->nqm, qpn);
}