#pragma once

/*
 * The completion notifications coming from the device should not be passed
 * directly to the user: a WC_WRITE notification means that the payload has
 * been written into the bounce buffer, not that it is available to the user.
 * 
 * Instead, a dedicated thread receives these notifications, and decrypts the
 * payload from unprotected memory into protected memory. Once this is done,
 * a notification can be queued for the user.
 */

#include "cvmrdma_cq_server.h"


#include <infiniband/verbs.h>


#include "qp_metadata_manager.h"
#include "bounce_metadata.h"
#include "wc_queue.h"


#ifdef __cplusplus
extern "C" {
#endif

typedef struct CQServer
{
    /* ctxt that the CQ server is operating in */
    struct cvmrdma_context * ctxt;

    /* CQ that this server is polling */
    struct ibv_cq * prov_cq;

    /* Map to the notification channels for the QPs using this CQ */
    QPMetadataManager * qp_metadata_manager;

    /* Pass WCs to the upper level through this queue */
    WCQueue * wc_queue;
} CQServer;


/*
    Initialize a new CQ server context.

    @server: storage for the CQServer to be created
    @cq: CQ that this server will poll
*/
void  cq_server_context_init(CQServer * server, struct cvmrdma_context * ctxt, struct ibv_cq * prov_cq);
void cq_server_context_destroy(CQServer * server);
int cq_server_run(CQServer * server, pthread_t * newthread);

int cq_server_register_qp(CQServer * server, struct cvmrdma_qp * qp);


#ifdef __cplusplus
}
#endif