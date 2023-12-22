#pragma once

/*
 * The completion notifications coming from the device should not be passed
 * directly to the user: a WC_WRITE notification means that the payload has
 * been written into the bounce buffer, not that it is available to the user.
 * 
 * Instead, a dedicated thread receives these notifications, and decrypts the
 * payload from unprotected memory into protected memory. Once this is done,
 * a notification can be queue for the user.
 */




/* XXX: for now accept a single CQ, in the future could poll several CQs */

#include <infiniband/verbs.h>


#include "qp_metadata_manager.h"
#include "bounce_metadata.h"

typedef struct CQServer
{
    /* CQ that this server is polling */
    struct ibv_cq * cq;

    /* Map to the notification channels for the QPs using this CQ */
    QPMetadataManager * qp_metadata_manager;

} CQServer;


/*
    Initialize a new CQ server context.

    @server: storage for the CQServer to be created
    @cq: CQ that this server will poll
*/
void cq_server_context_init(CQServer * server, struct ibv_cq * cq);
void cq_server_context_destroy(CQServer * server);
int cq_server_run(CQServer * server, pthread_t * newthread);

NotificationQueue * cq_server_add_qp(CQServer * server, uint32_t qpn);