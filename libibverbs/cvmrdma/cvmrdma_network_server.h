#pragma once

#include <inttypes.h>
#include <infiniband/verbs.h>

#include "notification_queue.h"
#include "bounce_metadata.h"

#ifdef __cplusplus
extern "C" {
#endif


typedef struct CVMRDMAServer CVMRDMAServer;
CVMRDMAServer * cvmrdma_start_network_server(char const * server_bind_addr);
void cvmrdma_stop_network_server(CVMRDMAServer * server);


void cvmrdma_server_register_qp(CVMRDMAServer * cs, PdBounceMetadata * pd_bounce_metadata, NotificationQueue * nq, uint32_t qpn);

#ifdef __cplusplus
}
#endif