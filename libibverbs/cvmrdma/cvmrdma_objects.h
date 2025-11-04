#pragma once

#include <pthread.h>
#include <semaphore.h>

#include <infiniband/driver.h>
#include <stdatomic.h>

#include "crypto.h"
#include "recv_metadata.h"
#include "read_metadata.h"
#include "send_metadata.h"
#include "chunk_queue.h"
#include "cvmrdma_network_client.h"
#include "cvmrdma_network_server.h"
#include "cvmrdma_network_evloop.h"
#include "bounce_metadata.h"
#include "qp_metadata_manager.h"
#include "wc_queue.h"
#include "wr_queue.h"
#include "nonce_queue.h"
#include "cvmrdma.h"

// enabling this will disable the secure secondary channel
// WARNING: This will very likely lead to decryption errors, so this is for testing only!
// #define CVMRDMA_DISABLE_SECURE_SECONDARY_CHANNEL 0
// WARNING: This will result in the slots no longer being acked
// therefore this can only work if the application uses less slots than available in the ring
// #define CVMRDMA_DISABLE_RECV_NOTIFY 0

#define CVMRDMA_GENERATE_PAYLOAD_NONCE 0

#define SKIP_WR_QUEUE 0


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

	/*RPCClientSession *local_network_client_connection;
	CVMRDMAClient *local_network_client_cap;
	RPCClientSession *remote_network_client_connection;
	CVMRDMAClient *remote_network_client_cap;*/

	/* Metadata manager maps QPNs to QPs
	   This is shared between all CQ servers.
	*/
	QPMetadataManager * qp_metadata_manager;

	/*char const *local_addr;
	char const *remote_addr;*/

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
	pthread_t nonce_thread;
};

struct cvmrdma_qp {
	/* Fake user-visible data */
	struct verbs_qp user_qp;

	/* Actual provider qp and cq*/
	struct ibv_qp *provider_qp;
	/* Note: multiple cvmrdma_qps can point to the same provider_cq */
	struct cvmrdma_cq *provider_cq;

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

	/* Reserved chunks to gather, encrypt / scatter, decrypt */
	void *bounce_private_mem;
	/* Ring buffer associated to this QP exposed as an MR. */
	struct ibv_mr *ringbuf_mr;
	void *ringbuf_va;
	int ringbuf_dmabuf_fd;

	struct ibv_mr *no_data_message_mr;
	void *no_data_message_mr_va;
	int no_data_message_mr_fd;
	sem_t no_data_message_sem;

	uint64_t nonce_metadata_cache[cvmrdma_aead_NPUB_WORDS * NONCE_CACHE_SIZE];
	int nonce_metadata_cache_offset;

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
	uint64_t received_lines;
	uint64_t received_lines_threshold;

	/* qp specific counter of all send messages*/
	_Atomic uint64_t cvmrdma_qp_send_wr_id;

	//pthread_t control_server_send_acks_thread;
	//pthread_t control_server_receive_acks_thread;

	/* Remote params */
	uint64_t rx_nonce[cvmrdma_aead_NPUB_WORDS];
	uint32_t remote_ring_rkey;
	bool remote_params_set;
	pthread_mutex_t remote_params_lock;
	pthread_cond_t remote_params_set_cv;

	/* Save location of future recvs in FIFO. When recving a SEND, the CQ server will write
	  chunk-by-chunk. The QP needs to remember where in the RR it has stopped to be able to write
	  the next chunk */
	QpRecvMetadata *recv_metadata;
	int cur_sge_idx;
	size_t cur_offt_in_sge;

	QpReadMetadata *read_metadata;

	/* Count the number of bytes recvd in this WR, to report in the WC */
	size_t wr_bytes_rcved;

	/* Save number of chunks of future sends in FIFO. When this many chunks are sent, report to
	   user. */
	QpSendMetadata *send_metadata;
	QpChunk *chunk_queue;

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
	QpNonce *nonce_queue;
	bool nonce_set;

	QpNonce *payload_nonce_queue;

	bool is_loopback;

	RPCClientSession *local_network_client_connection;
	CVMRDMAClient *local_network_client_cap;
	RPCClientSession *remote_network_client_connection;
	CVMRDMAClient *remote_network_client_cap;

	char const *local_addr;
	char const *remote_addr;
};

struct cvmrdma_srq {
	struct verbs_srq user_srq;

	QpRecvMetadata *recv_metadata;
};

/* Keep track of the nonce used to encrypt a certain payload chunk
   and also whether the chunk must be updated since the changed data
   in the user MR.
*/
struct chunk_tracking_info {
	uint64_t nonce[cvmrdma_aead_NPUB_WORDS]; // 24 bytes
	/* If the chink is not dirty this is true */
	_Atomic bool cached; // 1 byte

	sem_t in_transit_sem;
};

extern bool cvmrdma_disable_cache;

#ifdef DEBUG_TIMING
typedef struct timespec cvm_time_t;
#else
typedef uint64_t cvm_time_t;
#endif

// These variables are for the send side
extern cvm_time_t wr_submit_start, wr_submit_end; // This tracks the time that it takes between submission from the user application to when it is actually processed
extern cvm_time_t wr_calculations; // Tracks time that is needed to process the wr and get all properties needed for sending ready
extern cvm_time_t wr_metadata_push; // Tracks time taken to push metadata for the write request
extern cvm_time_t wr_send_nonce; // Tracks time taken to send the nonce for the write request
extern cvm_time_t wr_wait_for_recv_slot; // Tracks time taken to wait for a receive slot
extern cvm_time_t wr_wait_for_sem; // Track time that it takes to acquire the semaphore
extern cvm_time_t wr_cache_atomic_load; // Track time that it takes to do the atomic load for cache checking
extern cvm_time_t wr_cache_write_protect; // Track time that it takes to write protect the chunk
extern cvm_time_t wr_cache_atomic_store; // Track time that it takes to do the atomic store for cache marking
extern cvm_time_t wr_generate_nonce; // Track time it takes to generate a new nonce
extern cvm_time_t wr_push_chunk_tracking; // Tracks time taken to push chunk tracking information
extern cvm_time_t wr_encrypt_metadata; // Tracks time taken to encrypt metadata for the write request	
extern cvm_time_t wr_encrypt_payload; // Tracks time taken to encrypt the payload for the write request
extern cvm_time_t wr_ibv_post_send; // Tracks time taken to call ibv_post_send
extern cvm_time_t wc_send_start; // Tracks time taken to receive the WC for the write request
extern cvm_time_t wc_send_end; // Tracks time taken to process the received WC for the write request

// The variables are for the recv side
extern cvm_time_t wc_recv_start; // Tracks the inital time when a recv is noticed
extern cvm_time_t wc_refuel; // Tracks time taken to refuel the recv queue
extern cvm_time_t wc_calculations; // Tracks time taken to process the recv WC and get all properties needed for receiving
extern cvm_time_t wc_get_nonce; // Tracks time taken to get the nonce for the
extern cvm_time_t wc_decrypt_metadata; // Tracks time taken to decrypt the metadata for the recv WC
extern cvm_time_t wc_wait_for_sem; // Tracks time taken to wait for the semaphore
extern cvm_time_t wc_copy_payload; // Tracks time taken to copy the payload for the recv WC
extern cvm_time_t wc_cache_remove_write_protection; // Tracks time taken to remove write protection for the recv WC
extern cvm_time_t wc_decrypt_payload; // Tracks time taken to decrypt the payload for the recv WC
extern cvm_time_t wc_scatter_or_bypass; // Tracks time taken to scatter the payload to the user buffers or bypass if possible
extern cvm_time_t wc_notify_user; // Tracks time taken to notify the user of the received WR
extern cvm_time_t wc_notify_sender; // Tracks time taken to notify the sender that the recv is done

int post_send_do_bounce(struct cvmrdma_qp *qp,
                        struct ibv_send_wr *wrs,
                        struct ibv_send_wr **bad_wr);