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
ClientSession * new_client_rpc_session(CapnpEvloopThread * evloop, char const * server_addr);
void destroy_client_rpc_session(CapnpEvloopThread * evloop, ClientSession * session);

/* Get a CVMRDMA client on a given RPC session. The client also
   executes on the event loop thread */
CVMRDMAClient * get_cvmrdma_client(CapnpEvloopThread * evloop, ClientSession * session);
void destroy_cvmrdma_client(CapnpEvloopThread * evloop, CVMRDMAClient * client);


/* Get a capability to a given remote QP */
QPNotificationClient * get_qp_notification_client(CapnpEvloopThread * evloop, CVMRDMAClient * client, uint32_t remote_qpn);
void set_remote_params(CapnpEvloopThread * evloop, QPNotificationClient *qnc, uint64_t const * tx_key, uint32_t local_ring_rkey);


void remote_qp_notify_chunk(CapnpEvloopThread *cev, QPNotificationClient *qnc,
    enum ibv_wr_opcode op,
    uint32_t rkey, uint64_t hca_addr, 
    bool last_chunk, uint32_t imm
);
void remote_qp_notify_chunk_recv(CapnpEvloopThread *cev, QPNotificationClient *qnc);


#ifdef __cplusplus
}
#endif

