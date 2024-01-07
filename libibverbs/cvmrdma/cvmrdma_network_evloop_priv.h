#pragma once

#include "cvmrdma_network_evloop.h"

#include "cvmrdma_network_client.h"
#include "cvmrdma_network_server.h"

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
QPNotificationClient * get_qp_notification_client(CapnpEvloopThread * evloop, CVMRDMAClient * client, uint32_t remote_qpn, uint64_t const * tx_key);


/* The four notification functions take ownership of nonce and mac and pass it to the RPC function ! */
void remote_qp_notify_send(CapnpEvloopThread *cev, QPNotificationClient *qnc,
			   uint64_t length, uint64_t const *nonce,
			   uint64_t const *mac);
void remote_qp_notify_send_with_imm(CapnpEvloopThread *cev, QPNotificationClient *qnc,
				    uint64_t length, uint32_t imm,
				    uint64_t const *nonce, uint64_t const *mac);
void remote_qp_notify_write(CapnpEvloopThread *cev, QPNotificationClient *qnc,
			    uint64_t hca_addr, uint64_t length, uint32_t rkey,
			    uint64_t const *nonce, uint64_t const *mac);
void remote_qp_notify_write_with_imm(CapnpEvloopThread *cev, QPNotificationClient *qnc,
				     uint64_t hca_addr, uint64_t length,
				     uint32_t rkey, uint32_t imm,
				     uint64_t const *nonce, uint64_t const *mac);


#ifdef __cplusplus
}
#endif

