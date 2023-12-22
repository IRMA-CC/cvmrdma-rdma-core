#pragma once

#include <inttypes.h>
#include <stdbool.h>

#ifdef __cplusplus

#include <capnp/ez-rpc.h>
#include "cvmrdma.capnp.h"

using ClientSession = capnp::EzRpcClient;
using CVMRDMAClient = CVMRDMA::Client;
using QPNotificationClient = CVMRDMA::QpNotificationChannel::Client;


extern "C" {
#else

typedef struct ClientSession ClientSession;
typedef struct CVMRDMAClient CVMRDMAClient;
typedef struct QPNotificationClient QPNotificationClient;

#endif



ClientSession * cvmrdma_start_session(char const * server_addr);
void cvmrdma_stop_session(ClientSession * session);

CVMRDMAClient * cvmrdma_client_init(ClientSession * session);
void cvmrdma_client_destroy(CVMRDMAClient * client);


QPNotificationClient * get_qp_notification_client(ClientSession * session, CVMRDMAClient * client, uint32_t remote_qpn, uint64_t const * tx_key);
void remote_qp_notify_write_rpc(ClientSession * session, QPNotificationClient * qnc, uint64_t hca_addr, uint64_t length, uint32_t rkey, uint32_t imm, bool has_imm, uint64_t const * nonce, uint64_t const * mac);


#ifdef __cplusplus
}
#endif