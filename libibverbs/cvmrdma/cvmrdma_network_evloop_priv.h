#pragma once

#include "cvmrdma_network_evloop.h"

#include "cvmrdma_network_client.h"
#include "cvmrdma_network_server.h"

#include <infiniband/verbs.h>

#ifdef __cplusplus
extern "C" {
#endif


/* Start the event loop thread and the CVMRDMA server */
CapnpEvloopThread * create_capnp_event_loop(char const * local_bind_addr, CVMRDMAServer ** server);
void destroy_capnp_event_loop(CapnpEvloopThread * evloop);

/* Create a new RPC session on the event loop thread */
RPCClientSession * new_client_rpc_session(CapnpEvloopThread * evloop, char const * server_addr);
void destroy_client_rpc_session(const char* remote_addr, CapnpEvloopThread * evloop, RPCClientSession * session);

/* Get a CVMRDMA client on a given RPC session. The client also
   executes on the event loop thread */
CVMRDMAClient * get_cvmrdma_client(const char* remote_addr, CapnpEvloopThread * evloop, RPCClientSession * session);
void destroy_cvmrdma_client(const char* remote_addr, CapnpEvloopThread * evloop, CVMRDMAClient * client);


/* Get a capability to a given remote QP */
QPNotificationClient * get_qp_notification_client(const char* remote_addr, CapnpEvloopThread * evloop, CVMRDMAClient * client, uint32_t remote_qpn);
void set_remote_params(const char* remote_addr, CapnpEvloopThread * evloop, QPNotificationClient *qnc, 
                       uint32_t local_ring_rkey, uint32_t local_control_rkey, uint32_t control_qpn, uint32_t control_local_psn);


void remote_qp_notify_chunk_recv(const char* remote_addr, CapnpEvloopThread *cev, QPNotificationClient *qnc);
void remote_notify_nonce(const char* remote_addr, CapnpEvloopThread *cev, QPNotificationClient *qnc, uint64_t * nonce);
void remote_qp_notify_read(const char * remote_addr, CapnpEvloopThread *cev, QPNotificationClient *qnc, uint64_t addr, uint64_t size);

#ifdef __cplusplus
}
#endif

