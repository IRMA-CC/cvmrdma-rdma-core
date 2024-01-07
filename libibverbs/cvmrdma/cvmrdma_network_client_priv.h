#pragma once

#include "cvmrdma_network_client.h"

#ifdef __cplusplus

/* C++ API */
kj::Promise<void> remote_qp_notify_send_rpc(
    QPNotificationClient * qnc,
    uint64_t length,
    uint64_t const * nonce, uint64_t const * mac);
kj::Promise<void> remote_qp_notify_send_with_imm_rpc(
    QPNotificationClient * qnc,
    uint64_t length, uint32_t imm,
    uint64_t const * nonce, uint64_t const * mac);
kj::Promise<void> remote_qp_notify_write_rpc(
    QPNotificationClient * qnc,
    uint64_t hca_addr, uint64_t length, uint32_t rkey,
    uint64_t const * nonce, uint64_t const * mac);
kj::Promise<void> remote_qp_notify_write_with_imm_rpc(
    QPNotificationClient * qnc,
    uint64_t hca_addr, uint64_t length, uint32_t rkey, uint32_t imm,
    uint64_t const * nonce, uint64_t const * mac);

QPNotificationClient get_qp_notification_client_rpc(CVMRDMAClient * client, uint32_t remote_qpn, uint64_t const * tx_key);


extern "C" {
#endif

/* C API */

#ifdef __cplusplus
}
#endif