#pragma once

#include "cvmrdma_network_client.h"

#include <inttypes.h>
#include <infiniband/verbs.h>

#ifdef __cplusplus

/* C++ API */
kj::Promise<void> remote_qp_notify_chunk_recv_rpc(QPNotificationClient * qnc);

kj::Promise<void> set_remote_params_rpc(QPNotificationClient * qnc, uint32_t local_ring_rkey, uint32_t local_control_rkey, uint32_t control_qpn, uint32_t control_local_psn);

kj::Promise<void> notify_nonce_rpc(QPNotificationClient * qnc, uint64_t * nonce);

QPNotificationClient register_qp_with_remote_qp_rpc(CVMRDMAClient * client, uint32_t remote_qpn);


extern "C" {
#endif

/* C API */

#ifdef __cplusplus
}
#endif