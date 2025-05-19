#pragma once

#include <pthread.h>

#include <infiniband/driver.h>
#include <stdatomic.h>

#include "crypto.h"
#include "recv_metadata.h"
#include "send_metadata.h"
#include "cvmrdma_network_client.h"
#include "cvmrdma_network_server.h"
#include "cvmrdma_network_evloop.h"
#include "bounce_metadata.h"
#include "qp_metadata_manager.h"
#include "wc_queue.h"
#include "wr_queue.h"
#include "nonce_queue.h"


struct cvmrdma_pd {
	struct ibv_pd user_pd;

	struct ibv_pd *provider_pd;

	/* XXX: we would put this in verbs_pd ... but it doesn't exist */
	PdBounceMetadata *bounce_metadata;
	uint32_t mr_id_ctr;
};


/* the library code modifies the user visible verbs_context quite a bit, it's difficult
   to wrap it! instead, include cvmrdma_context _in_ verbs_context (in verbs.h) */
struct cvmrdma_context {
	/* Save the function pointers to the provider ops here. */
	struct verbs_context_ops prov_ops;

	/* Server and client for IP based RPCs.
        The event loop runs the server by default, and later on CVMRDMA layer can
        open as many sessions as necessary to remote servers on the same event loop.
        From each session, the CVMRDMA layer can get a CVMRDMA client. 
    
        XXX: for now, we only connect a single remote.
    */
	CapnpEvloopThread *network_evloop;
	CVMRDMAServer *network_server;

	RPCClientSession *local_network_client_connection;
	CVMRDMAClient *local_network_client_cap;
	RPCClientSession *remote_network_client_connection;
	CVMRDMAClient *remote_network_client_cap;

	/* Metadata manager maps QPNs to QPs
	   This is shared between all CQ servers.
	*/
	QPMetadataManager * qp_metadata_manager;

	char const *local_addr;
	char const *remote_addr;

	/* dma heap to allocate the public buffers on */
	int dma_heap;

	/* AEAD method to use in sodium */
	enum sodium_crypto_aead aead;

	/* All created PDs with this context*/
	struct cvmrdma_pd *pd_list[10];
};

struct cvmrdma_mr {
	/* Fake user-visible data */
	struct verbs_mr user_mr;

	/* Encrypted provider shadow region */
	struct ibv_mr *provider_mr;
	void *provider_va;
	int provider_dmabuf_fd;

	/* Array to track information about the chunk.
	   The array is indexed by the chunk number.
	 */
	struct chunk_tracking_info *chunk_tracking;

	pthread_t cache_invalidation_thread;
	int uffd;

	uint64_t hca_va;
};

struct cvmrdma_cq {
	struct verbs_cq user_cq;

	/* Provider CQ */
	struct ibv_cq * prov_cq_recv;
	struct ibv_cq * prov_cq_send;

	/* The incomming WRITE_WITH_IMM need to be written to the user_mr in the
	 * they arrive. E.g. when we emulate a SEND, we slowly copy segment by segment
	 * to the address the user specified in the RECV.
	 * In the future we can process different user WQEs in parallel.
	 */
	 _Atomic bool is_running_recv;
	 _Atomic bool is_running_send;

	/* a cvmrdma_cq does not correspond to a backend cq: there is a single CQ server,
       this CQ only gets notification through the WCQueue */

	WCQueue *wc_queue;

	/* Mock for https://www.rdmamojo.com/2012/10/19/ibv_create_comp_channel/ via 
	   https://man7.org/linux/man-pages/man2/eventfd.2.html
	   Created in ibv_create_comp_channel()
	*/
	int comp_channel_event_fd;
	_Atomic bool comp_channel_event_fd_filled;

	/* Provider comp channel
	   This is used when the application also uses a comp channel,
	   since then the applications polls on that channel before calling ibv_poll_cq().
	   Once we receive data on the provider qp/cq we can run the cq_server.
	*/
	struct ibv_comp_channel *provider_comp_channel;
	pthread_t provider_comp_channel_thread;
};

struct cvmrdma_qp {
	/* Fake user-visible data */
	struct verbs_qp user_qp;

	/* Actual provider qp and cq*/
	struct ibv_qp *provider_qp;
	/* Note: multiple cvmrdma_qps can point to the same provider_cq */
	struct cvmrdma_cq *provider_cq;

	/* Second control channel hidden from the user */
	struct ibv_qp *control_qp;
	struct ibv_cq *control_cq;
	uint32_t dest_control_qp_num;
	uint32_t control_remote_psn;
	uint32_t control_local_psn;

	/* Key shared between all endpoints used to encrypt payloads.
	   In contrast, the tx_key and rx_key are unique to the endpoints
	   and are used to encrypt the nonce for the payload. The nonce for this
	   encryption is a counter between the endpoints.
	 */
	uint64_t shared_key[cvmrdma_aead_KEY_WORDS];

	/* Capability of the remote QP to notify on WRITEs */
	QPNotificationClient *remote_qnc;

	/* This thread consumes entries in the wr_queue.
	 * Note that the order of WRs is preserved.	
	 */
	pthread_t send_server_thread;

	/* Protext send_lines_full */
	pthread_mutex_t send_lines_full_lock;
	/* Notifies that new send lines are available */
	pthread_cond_t send_lines_avail;
	/* Protect received_lines */
	pthread_mutex_t received_lines_lock;
	/* Notifies the ACK server to run */
	pthread_cond_t recv_lines_ack;

	/* Reserved chunks to gather, encrypt / scatter, decrypt */
	void *bounce_private_mem;
	/* Ring buffer associated to this QP exposed as an MR. */
	struct ibv_mr *ringbuf_mr;
	void *ringbuf_va;
	int ringbuf_dmabuf_fd;

	int next_send_slot;
	int next_recv_slot;
	/* Amount of entries in the send line
	   increased during post_send, decreased in 
	   the control server
	*/
	int send_lines_full;
	/* During recv the server counts the number of slots processed.
	   If a certain threshold is reached, the server sends an ACK.
	*/
	int received_lines;

	/* qp specific counter of all send messages*/
	_Atomic uint64_t cvmrdma_qp_send_wr_id;

	/* Ringbuffer where ACKs are written to */
	struct ibv_mr *control_buf_mr;
	void *control_buf_va;

	//pthread_t control_server_send_acks_thread;
	//pthread_t control_server_receive_acks_thread;

	/* Remote params */
	uint64_t rx_nonce[cvmrdma_aead_NPUB_WORDS];
	uint32_t remote_ring_rkey;
	uint32_t remote_control_rkey;
	bool remote_params_set;
	pthread_mutex_t remote_params_lock;
	pthread_cond_t remote_params_set_cv;

	/* Save location of future recvs in FIFO. When recving a SEND, the CQ server will write
	  chunk-by-chunk. The QP needs to remember where in the RR it has stopped to be able to write
	  the next chunk */
	QpRecvMetadata *recv_metadata;
	int cur_sge_idx;
	size_t cur_offt_in_sge;

	/* Count the number of bytes recvd in this WR, to report in the WC */
	size_t wr_bytes_rcved;

	/* Save number of chunks of future sends in FIFO. When this many chunks are sent, report to
	   user. */
	QpSendMetadata *send_metadata;

	/* Save ibv_send_wr during ibv_post_send in order to handle the WRs async as 
	 * usually from the library. The user program can only be sure that the WR is 
	 * completed when the WC is received. It _should_ not touch the data before that.
	 */
	WRQueue *wr_queue;

	/* Count the number of chunks sent in this WR, to know when to report WC */
	size_t wr_chunks_sent;

	/* Whether the user requests a SEND_WC by default */
	bool user_sq_sig_all;

	/* Information for debugging purposes */
	u_int64_t lid;
	uint64_t dlid;

	pthread_mutex_t receive_nonce_lock;
	pthread_cond_t receive_nonce_cv;
	NonceQueue *nonce_queue;
	bool nonce_set;
};

struct cvmrdma_srq {
	struct verbs_srq user_srq;

	QpRecvMetadata *recv_metadata;
};

struct control_metadata {
	uint64_t ack_revc_lines;
};

/* Keep track of the nonce used to encrypt a certain payload chunk
   and also whether the chunk must be updated since the changed data
   in the user MR.
*/
struct chunk_tracking_info {
	uint64_t nonce[cvmrdma_aead_NPUB_WORDS]; // 24 bytes
	/* If the chink is not dirty this is true */
	_Atomic bool cached; // 1 byte
	pthread_mutex_t lock;
};

extern bool cvmrdma_disable_cache;