#pragma once

#include "cvmrdma_network_client.h"

#include <inttypes.h>
#include <infiniband/verbs.h>

#ifdef __cplusplus

/* C++ API */
kj::Promise<void> remote_qp_notify_chunk_rpc(
    QPNotificationClient * qnc,
    enum ibv_wr_opcode op,
    uint32_t rkey, uint64_t hca_addr, 
    uint64_t const * nonce,
    bool last_chunk, uint32_t imm
);

kj::Promise<void> remote_qp_notify_chunk_recv_rpc(QPNotificationClient * qnc);

kj::Promise<void> set_remote_params_rpc(QPNotificationClient * qnc, uint64_t const * tx_key, uint32_t local_ring_rkey);


QPNotificationClient register_qp_with_remote_qp_rpc(CVMRDMAClient * client, uint32_t remote_qpn);


extern "C" {
#endif

/* C API */

#ifdef __cplusplus
}
#endif