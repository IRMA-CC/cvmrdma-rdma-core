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

#include <stdatomic.h>

#include <pthread.h>
#include <infiniband/verbs.h>

#include "cvmrdma_objects.h"
#include "qp_metadata_manager.h"
#include "bounce_metadata.h"
#include "wc_queue.h"


#ifdef __cplusplus
extern "C" {
#endif
int cvmrdma_cq_server_run_once_if_idle_recv(struct cvmrdma_context * ctxt, struct cvmrdma_cq * cq);
int cvmrdma_cq_server_run_once_if_idle_send(struct cvmrdma_context * ctxt, struct cvmrdma_cq * cq);

void* cvmrdma_cq_wait_completion_event(void* c_cq);

#ifdef __cplusplus
}
#endif