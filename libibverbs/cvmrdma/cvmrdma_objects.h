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
#include "cvmrdma_cq_server.h"
#include "qp_metadata_manager.h"

/* the library code modifies the user visible verbs_context quite a bit, it's difficult
   to wrap it! instead, include cvmrdma_context _in_ verbs_context (in verbs.h) */
struct cvmrdma_context {
	/* Save the function pointers to the provider ops here. */
	struct verbs_context_ops prov_ops;

	/* CQ server for every QP is in this process */
	CQServer cvmrdma_cq_server;

	/* Server and client for IP based RPCs.
        The event loop runs the server by default, and later on CVMRDMA layer can
        open as many sessions as necessary to remote servers on the same event loop.
        From each session, the CVMRDMA layer can get a CVMRDMA client. 
    
        XXX: for now, we only connect a single remote.
    */
	CapnpEvloopThread *network_evloop;
	CVMRDMAServer *network_server;
	ClientSession *network_client_connection;
	CVMRDMAClient *network_client_cap;

	char const *remote_addr;

	/* dma heap to allocate the public buffers on */
	int dma_heap;

	/* AEAD method to use in sodium */
	enum sodium_crypto_aead aead;
};

struct cvmrdma_pd {
	struct ibv_pd user_pd;

	struct ibv_pd *provider_pd;

	/* XXX: we would put this in verbs_pd ... but it doesn't exist */
	PdBounceMetadata *bounce_metadata;
	uint32_t mr_id_ctr;
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

	/* a cvmrdma_cq does not correspond to a backend cq: there is a single CQ server,
       this CQ only gets notification through the WCQueue */

	WCQueue *wc_queue;
};

struct cvmrdma_qp {
	/* Fake user-visible data */
	struct verbs_qp user_qp;

	/* Actual provider qp */
	struct ibv_qp *provider_qp;

	/* Second control channel hidden from the user */
	struct ibv_qp *control_qp;
	struct ibv_cq *control_cq;

	/* Key shared between all endpoints used to encrypt payloads.
	   In contrast, the tx_key and rx_key are unique to the endpoints
	   and are used to encrypt the nonce for the payload. The nonce for this
	   encryption is a counter between the endpoints.
	 */
	uint64_t shared_key[cvmrdma_aead_KEY_WORDS];

	/* Capability of the remote QP to notify on WRITEs */
	QPNotificationClient *remote_qnc;
	/* Transmission key Generate the tx key, and receive the rx key */
	uint64_t tx_key[cvmrdma_aead_KEY_WORDS];
	uint64_t tx_nonce[cvmrdma_aead_NPUB_WORDS];

	/* Preserve order of post_* and the related notifications */
	pthread_mutex_t cvmrdma_send_lock;

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
	   QpNotificationChannel::notifyChunkRecv
	*/
	int send_lines_full;
	/* During recv the server counts the number of slots processed.
	   If a certain threshold is reached, the server sends an ACK.
	*/
	int received_lines;

	/* Ringbuffer where ACKs are written to */
	struct ibv_mr *control_buf_mr;
	void *control_buf_va;

	pthread_t control_server_send_acks_thread;
	pthread_t control_server_receive_acks_thread;

	/* Remote params */
	uint64_t rx_key[cvmrdma_aead_KEY_WORDS];
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

	/* Count the number of chunks sent in this WR, to know when to report WC */
	size_t wr_chunks_sent;

	/* Whether the user requests a SEND_WC by default */
	bool user_sq_sig_all;
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
};