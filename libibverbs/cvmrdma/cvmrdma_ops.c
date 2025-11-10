#include "cvmrdma_ops.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

#include <arpa/inet.h> /* ntohl */
#include <unistd.h>

#include <sys/mman.h>
#include <sys/ioctl.h>

#include <linux/userfaultfd.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/syscall.h>

#include <linux/dma-buf.h>

#include <sodium.h>

/* For get_ops */
#include "../ibverbs.h"
#include "../verbs.h"


#include "cvmrdma.h"
#include "cvmrdma_objects.h"
#include "bounce_metadata.h"
#include "cvmrdma_cq_server.h"
#include "cvmrdma_network_client_priv.h"
#include "cvmrdma_network_server_priv.h"
#include "cvmrdma_network_evloop_priv.h"
#include "recv_metadata.h"
#include "read_metadata.h"
#include "wc_queue.h"
#include "wr_queue.h"
#include "dma_heap.h"
#include "cvmrdma_cache.h"
#include "nonce_queue.h"
#include "chunk_queue.h"

#include "debug.h"

void cvmrdma_pregenerate_nonces_loop(void* c_qp){

	struct cvmrdma_qp * qp = (struct cvmrdma_qp *)c_qp;

	for (;;) {
		if (qp_nonce_queue_size(qp->payload_nonce_queue) < 50) {
			while (qp_nonce_queue_size(qp->payload_nonce_queue) <= 100) {
				struct nonce_entry new_nonce_entry = {0};
				randombytes_buf(new_nonce_entry.nonce, sizeof(new_nonce_entry.nonce));
				qp_nonce_queue_push(qp->payload_nonce_queue, &new_nonce_entry);
			}
		} else {
			usleep(100); // Sleep for 100 microseconds to avoid busy-waiting
		}
	}
}

static void wait_in_transit_sem_with_send_progress(struct cvmrdma_context *ctxt,
                                                   struct cvmrdma_qp *qp,
                                                   sem_t *sem) {
    struct cvmrdma_cq *scq = (struct cvmrdma_cq *)qp->user_qp.qp.send_cq;
    for (;;) {
        if (sem_trywait(sem) == 0) return;
        if (errno == EINTR) continue;
        if (errno != EAGAIN) {
            perror("sem_trywait");
            exit(1);
        }
        bool again = false;
        do {
            cvmrdma_cq_server_run_once_if_idle_send(ctxt, scq, &again);
        } while (again);
        sched_yield();
    }
}

void cvmrdma_sem_wait(sem_t *sem, char const * error_msg) {
#ifdef CVMRDMA_TIMED_SEMAPHORE_WAIT
    int waited = 0;

    while (sem_trywait(sem) == -1 && errno == EAGAIN) {
        if (waited >= 100000) {
			fprintf(stderr, "Semaphore wait timeout: %s\n", error_msg);
		}
        waited++;
    }
#else
	sem_wait(sem);
#endif
}

static char * lookup_ip_by_lid(uint64_t lid)
{
	FILE * fp = fopen("/root/lid_to_ip_mlx5", "r");
	if(! fp)
	{
		fprintf(stderr, "Failed to open /root/lid_to_ip_mlx5\n");
		return NULL;
	}

	uint16_t file_lid;
	char file_ip[64]; // Assuming IP addresses are in standard format, max 15 chars + null terminator
    while (fscanf(fp, "%hu %63s", &file_lid, file_ip) == 2) {
        if (file_lid == lid) {
            fclose(fp);
			
			int ip_len = strlen(file_ip);
			char * ip = calloc(ip_len + 1, sizeof(char));
			if (!ip) {
				fprintf(stderr, "Failed to allocate memory for IP address\n");
				return NULL;
			}
			strncpy(ip, file_ip, ip_len);
			ip[ip_len] = '\0'; // Null-terminate the string
			DEBUG(stderr, "Found IP for LID %d: %s\n", lid, ip);
			return ip;
        }
    }
    fclose(fp);
	fprintf(stderr, "LID %d not found in /root/lid_to_ip_mlx5\n", lid);
	return NULL; // LID not found
}


static struct ibv_pd * cvmrdma_alloc_pd(struct ibv_context * ibcontext)
{
    struct cvmrdma_context * ctxt = cvmrdma_get_ctx(ibcontext);

    PdBounceMetadata * bounce_metadata;

    /* Allocate the PD through the provider */
    struct ibv_pd * prov_pd = ctxt->prov_ops.alloc_pd(ibcontext);
    if(! prov_pd)
    {
        fprintf(stderr, "Failed to allocate PD through backend.\n");
        goto err;
    }
	/* see ibv_alloc_pd */
	prov_pd->context = ibcontext;

	bounce_metadata = bounce_metadata_alloc();
	if(! bounce_metadata)
	{
		fprintf(stderr, "Failed to allocated bounce metadata storage.\n");
		goto free_pd;
	}

	struct cvmrdma_pd * pd = calloc(1, sizeof(struct cvmrdma_pd));
	
	pd->user_pd.context = ibcontext;
	pd->user_pd.handle = prov_pd->handle;

	pd->provider_pd = prov_pd;
    pd->bounce_metadata = bounce_metadata;
	pd->mr_id_ctr = 0;

	// for (uint64_t i = 0; i < 10; i++) {
	// 	if(ctxt->pd_list[i] == NULL) {
	// 		ctxt->pd_list[i] = pd;
	// 		break;
	// 	} else if (i == 9) {
	// 		fprintf(stderr, "Failed to allocate PD, no space in pd_list\n");
	// 		exit(1);
	// 	}
	// }

	DEBUG("Allocated PD %p\n", pd);



	return &pd->user_pd;


// free_metadata:
// 	bounce_metadata_destroy(bounce_metadata);
free_pd:
	ctxt->prov_ops.dealloc_pd(prov_pd);
err:
	return NULL;
}


static struct ibv_cq *cvmrdma_create_cq(struct ibv_context *ibcontext, int cqe,
				struct ibv_comp_channel *channel,
				int comp_vector)
{
	struct cvmrdma_cq * cq = calloc(1, sizeof(struct cvmrdma_cq));
	struct cvmrdma_context * ctxt = cvmrdma_get_ctx(ibcontext);
	int ret;

	cq->user_cq.cq.context = ibcontext;
	cq->user_cq.cq.channel = channel;
	cq->user_cq.cq.cqe = cqe;
	pthread_mutex_init(&cq->user_cq.cq.mutex, NULL);

	cq->wc_queue = alloc_wc_queue();

	if(channel) {
		assert(channel->fd >= 0);
		cq->comp_channel_event_fd = channel->fd;

		// emulate verbs_init_cq for completion channel
		pthread_mutex_lock(&ibcontext->mutex);
		++channel->refcnt;
		pthread_mutex_unlock(&ibcontext->mutex);
	}
	// always create a completion channel for the provider cq.
	// This ensures cvmrdma will always get notifications on arrival.
	cq->provider_comp_channel = ibv_create_comp_channel_prov(ibcontext);
	/* Create the recv CQ */
	struct ibv_cq * ibcq_recv = ctxt->prov_ops.create_cq(ibcontext, 16384, cq->provider_comp_channel, 0);
	if(! ibcq_recv)
	{
		fprintf(stderr, "failed to create backend recv CQ for the CQ server\n");
		exit(1);
	}
	/* see ibv_create_cq */
	verbs_init_cq(ibcq_recv, ibcontext, cq->provider_comp_channel, NULL);
	cq->prov_cq_recv = ibcq_recv;

	/*ret = ctxt->prov_ops.req_notify_cq(ibcq_recv, 0);
	if(ret)
	{
		fprintf(stderr, "Failed to request CQ notifications for recv %d\n", ret);
		exit(1);
	}*/

	/* Create send CQ */
	struct ibv_cq * ibcq_send = ctxt->prov_ops.create_cq(ibcontext, 16384, cq->provider_comp_channel, 0);
	if(! ibcq_send)
	{
		fprintf(stderr, "failed to create backend send CQ for the CQ server\n");
		exit(1);
	}
	/* see ibv_create_cq */
	verbs_init_cq(ibcq_send, ibcontext, cq->provider_comp_channel, NULL);
	cq->prov_cq_send = ibcq_send;

	/*ret = ctxt->prov_ops.req_notify_cq(ibcq_send, 0);
	if(ret)
	{
		fprintf(stderr, "Failed to request CQ notifications for send %d\n", ret);
		exit(1);
	}*/

	return &cq->user_cq.cq;
}


static int attach_qp_to_cq_manager(struct cvmrdma_context * ctxt, struct cvmrdma_qp * qp)
{
	/* Add this QP to the CQ server. This gives us one end of the notification
	   channel ; the RPC server will push notifications there.
	 */
	int err;

	uint32_t qpn = qp->user_qp.qp.qp_num;


	err = qp_metadata_manager_register_qp(ctxt->qp_metadata_manager, qpn, qp);
    if(err)
    {
        fprintf(stderr, "failed to register QP on CQ server\n");
        return -1;
    }

	/* Create the corresponding RPC object. Pass the end of the notification channel here.
	   No risk of a race: we haven't returned the QPN to the user yet, the remote cannot possibly
	   know it yet.
	*/
	DEBUG("Create QpNotificationChannel cap QNC using notification queue\n");
	cvmrdma_server_register_qp(
		ctxt->network_server,
		qpn,
		&qp->send_lines_full,
		&qp->send_lines_full_lock,
		&qp->send_lines_avail,
		&qp->remote_ring_rkey,
		&qp->remote_params_set,
		&qp->remote_params_lock,
		&qp->remote_params_set_cv,
		&qp->receive_nonce_lock,
		&qp->receive_nonce_cv,
		&qp->nonce_queue,
		&qp->nonce_set,
		&qp->wr_queue,
		qp
	);

	return 0;
}

/* Allocate a zero-indexed publicly available mr*/
static struct ibv_mr * alloc_public_mr(struct cvmrdma_context * ctxt, struct cvmrdma_pd *pd, size_t size, int * dmabuf_fd, void ** ringbuf_va)
{
	int dma_buf;

#ifndef DUMMY_SYSTEM_HEAP
	DEBUG("Using dma-buf for public MR\n");
	dma_buf = alloc_dma_buf(ctxt->dma_heap, size);
	if(dma_buf < 0)
	{
		fprintf(stderr, "Failed to allocate dma-buf\n");
		goto err;
	}


	/* WARN: MUST attach+dma_map to dma-buf before mmap !! In our hacky implementation, the actual
	   allocation is done in dma_map */
	int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_RELAXED_ORDERING;
	uint64_t offt = 0;
	struct ibv_mr * mr = ctxt->prov_ops.reg_dmabuf_mr(
		pd->provider_pd,
		offt, size, 0,
		dma_buf, access
	);
	if(! mr)
	{
		fprintf(stderr, "Failed to reg dmabuf as mr\n");
		goto free_dma_buf;
	}


	/* direct access to provider ops does NOT set ibv fields ! */
	void * va = mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED_VALIDATE, dma_buf, 0);
	if(va == MAP_FAILED)
	{
		fprintf(stderr, "Failed to mmap dma-buf\n");
		goto dereg_mr;
	}


	mr->addr = (void *)(uintptr_t)offt; /* oddity in libibverb, follow */

#else
	dma_buf = -1;

	void * va = mmap(0, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if(va == MAP_FAILED)
	{
		fprintf(stderr, "Failed to mmap public MR buf\n");
		goto err;
	}

	int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_RELAXED_ORDERING;

	struct ibv_mr * mr = ctxt->prov_ops.reg_mr(
		pd->provider_pd, va, size, 0, access 
	);
	if(! mr)
	{
		fprintf(stderr, "Failed to reg public mr\n");
		munmap(va, size);
		goto err;
	}

	mr->addr = va;
#endif

	/* setup the internal fields of ibv_mr */
	mr->context = pd->user_pd.context;
	mr->pd = &pd->user_pd;
	mr->length = size;

	DEBUG("public mr: dmabuf %d va %p len %zu\n", dma_buf, va, size);

	*dmabuf_fd = dma_buf;
	*ringbuf_va = va;

	return mr;

dereg_mr:
	// TODO: dereg mr on failure ...
#ifndef DUMMY_SYSTEM_HEAP
free_dma_buf:
	close(dma_buf);
#endif
err:
	return NULL;
}

static int free_public_mr(struct cvmrdma_context * ctxt, struct ibv_mr * mr, int dmabuf_fd, void * ringbuf_va)
{
	int ret;

	ret = ctxt->prov_ops.dereg_mr(mr);
	if(ret)
	{
		fprintf(stderr, "Failed to deregister public MR\n");
		return ret;
	}

	ret = munmap(ringbuf_va, mr->length);
	if (ret)
	{
		fprintf(stderr, "Failed to munmap public MR buf\n");
		return ret;
	}

#ifndef DUMMY_SYSTEM_HEAP
	close(dmabuf_fd);
#endif
	return 0;
}


static struct ibv_qp *cvmrdma_create_qp(struct ibv_pd * ibpd,
				struct ibv_qp_init_attr *attr)
{
	struct cvmrdma_pd * pd = (struct cvmrdma_pd *)ibpd;
	struct ibv_context * prov_ctxt = pd->user_pd.context;
    struct cvmrdma_context * ctxt = cvmrdma_get_ctx(prov_ctxt);
	// TODO: in theory we could have different CQs for send and recv, but we don't support that yet
	struct cvmrdma_cq * c_cq = (struct cvmrdma_cq *)attr->send_cq;
	int err;

	struct cvmrdma_qp * qp = calloc(1, sizeof(struct cvmrdma_qp));
	qp->wr_queue = alloc_wr_queue();
	qp->nonce_metadata_cache_offset = 0;

	qp->nonce_metadata_cache = calloc(OPT_cvmrdma_nonce_cache_size * cvmrdma_aead_NPUB_WORDS, sizeof(uint64_t));

	qp->received_lines = 0;
	qp->received_lines_threshold = cvmrdma_ring_entries / 2;

	/* Create the provider QP */
	struct ibv_qp_init_attr_ex attr_ex = {0};
	attr_ex.sq_sig_all = 1;
	/* Direct send and recv WCs to the CQ server */
	attr_ex.send_cq = c_cq->prov_cq_send;
	attr_ex.recv_cq = c_cq->prov_cq_recv;

	assert(attr->qp_type == IBV_QPT_RC || attr->qp_type == IBV_QPT_UC);
	attr_ex.qp_type = attr->qp_type;

	/* Don't keep the user set QP capacity */
	attr_ex.cap.max_recv_sge = 30;
	attr_ex.cap.max_recv_wr = 4096;
	attr_ex.cap.max_send_sge = 30;
	attr_ex.cap.max_send_wr = 4096;
	attr_ex.cap.max_inline_data = 0; /* we're not using inline */

	attr_ex.pd = pd->provider_pd;
	attr_ex.send_ops_flags = IBV_QP_EX_WITH_RDMA_WRITE_WITH_IMM;
	attr_ex.comp_mask = IBV_QP_INIT_ATTR_PD | IBV_QP_INIT_ATTR_SEND_OPS_FLAGS;

    struct ibv_qp * ibqp = ctxt->prov_ops.create_qp_ex(prov_ctxt, &attr_ex);
    if(! ibqp)
    {
		fprintf(stderr, "Unable to create provider QP (%d:%s)\n", errno, strerror(errno));
        goto err;
    }

	qp->provider_qp = ibqp;
	qp->provider_cq = c_cq;

	/* Initialize user visible fields */
	qp->user_qp.qp.context = prov_ctxt;
	qp->user_qp.qp.qp_context = attr->qp_context;
	qp->user_qp.qp.pd = ibpd;
	qp->user_qp.qp.qp_num = ibqp->qp_num;
	qp->user_qp.qp.qp_type = ibqp->qp_type;
	/* user-qp send/recv CQ are the cvmrdma-CQ where the CQ server will push notifications */
	qp->user_qp.qp.recv_cq = attr->recv_cq;
	qp->user_qp.qp.send_cq = attr->send_cq;


	QpRecvMetadata * recv_metadata = NULL;
	if (attr->srq)
	{
		struct cvmrdma_srq * csrq = (struct cvmrdma_srq *)attr->srq;
		assert(csrq->recv_metadata);
		recv_metadata = csrq->recv_metadata;
	} else {
		recv_metadata = alloc_qp_recv_metadata();
		if(! recv_metadata)
		{
			fprintf(stderr, "Failed to allocated recv metadata storage.\n");
			goto free_qp;
		}
	}
	qp->recv_metadata = recv_metadata;
	
	qp->cur_sge_idx = 0;
	qp->cur_offt_in_sge = 0;
	qp->wr_bytes_rcved = 0;

	// this might not be needed, if we send the metadata over ip again?
	QpSendMetadata * send_metadata = alloc_qp_send_metadata();
	if(! send_metadata)
	{
		fprintf(stderr, "Failed to allocated send metadata storage.\n");
		goto free_recv_metadata;
	}
	qp->send_metadata = send_metadata;
	qp->wr_chunks_sent = 0;

	QpChunk * chunk_queue = alloc_qp_chunk_queue();
	if (! chunk_queue)
	{
		fprintf(stderr, "Failed to allocated chunk queue storage.\n");
		goto free_send_metadata;
	}
	qp->chunk_queue = chunk_queue;

	QpReadMetadata * read_metadata = alloc_qp_read_metadata();
	if(! read_metadata)
	{
		fprintf(stderr, "Failed to allocated read metadata storage.\n");
		goto free_chunk_queue;
	}
	qp->read_metadata = read_metadata;


	/* Allocate the bounce regions and ring buffer for this QP */
	DEBUG("Allocating bounce regions with size %zu for QP %" PRIx32 "\n",  (uint64_t) cvmrdma_priv_bounce_total_bytes(ctxt->aead) / (1024*1024), ibqp->qp_num);
	void * bounce_private_mem = mmap(0, cvmrdma_priv_bounce_total_bytes(ctxt->aead), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if (bounce_private_mem == MAP_FAILED) {
		err = errno;
		fprintf(stderr, "Failed to mmap bounce buffer %d\n", err);
		goto free_read_metadata;
	}
	qp->bounce_private_mem = bounce_private_mem;

	// allocate a memory region in which we can encrypt just the header of the no-data message.
	enum sodium_crypto_aead aead = ctxt->aead;
	size_t no_data_message_mr_size = cvmrdma_header_bytes(aead);

	int no_data_message_mr_fd = -1;
	void * no_data_message_mr_va = NULL;
	DEBUG("Allocating no-data message MR with size %zu for QP %" PRIx32 "\n",  (uint64_t) no_data_message_mr_size / (1024*1024), ibqp->qp_num);
	struct ibv_mr * no_data_message_mr = alloc_public_mr(ctxt, pd, no_data_message_mr_size, &no_data_message_mr_fd, &no_data_message_mr_va);
	if(! no_data_message_mr)
	{
		fprintf(stderr, "Failed to allocate no-data message MR\n");
		goto free_bounce_private;
	}

	qp->no_data_message_mr = no_data_message_mr; 
	qp->no_data_message_mr_fd = no_data_message_mr_fd;
	qp->no_data_message_mr_va = no_data_message_mr_va;

	err = sem_init(&qp->no_data_message_sem, 0, 1);
	if(err)
	{
		fprintf(stderr, "Failed to initialize the CVMRDMA no_data_message_sem semaphore %d\n", err);
		goto dereg_ring_mr;
	}



    /* To perform the bounce, we'll need to read and write the ring buffers */
	size_t all_rings_bytes = cvmrdma_ring_slot_bytes(ctxt->aead) * cvmrdma_ring_entries;

	int dmabuf_fd = -1;
	void * ringbuf_va = NULL;
	DEBUG("Allocating ring buffer with size %zu for QP %" PRIx32 "\n",  (uint64_t) all_rings_bytes / (1024*1024), ibqp->qp_num);
	struct ibv_mr * ringbuf_mr = alloc_public_mr(ctxt, pd, all_rings_bytes, &dmabuf_fd, &ringbuf_va);
    if(! ringbuf_mr)
    {
        goto free_no_data_message_mr;
    }

	DEBUG("[CREATE_QP] ringbuf rkey %" PRIx32 ", ringbuf lkey %" PRIx32 "\n", ringbuf_mr->rkey, ringbuf_mr->lkey);

	qp->ringbuf_mr = ringbuf_mr;
	qp->ringbuf_dmabuf_fd = dmabuf_fd;
	qp->ringbuf_va = ringbuf_va;

	qp->next_send_slot = 0;
	qp->next_recv_slot = 0;
	qp->send_lines_full = 0;

	qp->user_sq_sig_all = attr->sq_sig_all;

	/* Initialize the mutex */
	err = pthread_mutex_init(&qp->send_lines_full_lock, NULL);
	if(err)
	{
		fprintf(stderr, "Failed to initialize the CVMRDMA send_lines_full_lock mutex %d\n", err);
		goto dereg_ring_mr;
	}
	err = pthread_cond_init(&qp->send_lines_avail, NULL);
	if(err)
	{
		fprintf(stderr, "Failed to initialize the CVMRDMA send_lines_avail cond var %d\n", err);
		goto dereg_ring_mr;
	}

	err = attach_qp_to_cq_manager(ctxt, qp);
	if(err)
	{
		fprintf(stderr, "Failed to attach QP %" PRIx32 " to its recv cq (%d)\n", ibqp->qp_num, err);
		goto dereg_ring_mr;
	}

	// Since the actual application is likely to not poll the cq all the time,
	// we need another trigger to wake up the cq server.
	err = pthread_create(&c_cq->provider_comp_channel_thread, NULL, &cvmrdma_cq_wait_completion_event, (void *)c_cq);
	if (err)
	{
		fprintf(stderr, "Error creating cvmrdma_cq_wait_completion_event thread: %d\n", err);
		exit(1);
	}

	if (!OPT_cvmrdma_disable_payload_nonce_pregeneration){
		qp->payload_nonce_queue = alloc_qp_nonce_queue();
		err = pthread_create(&c_cq->nonce_thread, NULL, &cvmrdma_pregenerate_nonces_loop, (void *)qp);
		if (err)
		{
			fprintf(stderr, "Error creating cvmrdma_pregenerate_nonces_loop thread: %d\n", err);
			exit(1);
		}
	}

	/* We can only get the capability to the remote QP once we know its QPN, this will be done
	   in modify_qp. We'll also get the rkey to its ring */
	qp->remote_qnc = NULL;
	qp->remote_ring_rkey = 0;
	memset(qp->rx_nonce, 0, cvmrdma_aead_NPUBBYTES);
	qp->remote_params_set = false;
	pthread_mutex_init(&qp->remote_params_lock, NULL);
	pthread_cond_init(&qp->remote_params_set_cv, NULL);

	pthread_mutex_init(&qp->receive_nonce_lock, NULL);
	pthread_cond_init(&qp->receive_nonce_cv, NULL);
	qp->nonce_set = false;
	qp->nonce_queue = alloc_qp_nonce_queue();

#ifdef SKIP_WR_QUEUE
	fprintf(stderr, "CVMRDMA: Skipping WR queue processing\n");
#else
	err = pthread_create(&qp->send_server_thread, NULL, &cvmrdma_do_bounce_loop, (void *)qp);
	if (err)
	{
		fprintf(stderr, "Error creating cvmrdma_cq_wait_completion_event thread: %d\n", err);
		exit(1);
	}
#endif

	char const * local_bind_addr = getenv("CVMRDMA_BIND_ADDR");
	if(! local_bind_addr)
	{
		fprintf(stderr, "ERROR: No environment variable CVMRDMA_BIND_ADDR.\n");
		goto err;
	}

	qp->local_addr = calloc(1, 20);
	sprintf(qp->local_addr, "127.0.0.1:%s", local_bind_addr + 2);

	qp->remote_addr = NULL;
	qp->local_network_client_connection = NULL;
	qp->local_network_client_cap = NULL;
	qp->remote_network_client_connection = NULL;
	qp->remote_network_client_cap = NULL;

	return &qp->user_qp.qp;

dereg_ring_mr:
	ctxt->prov_ops.dereg_mr(verbs_get_mr(ringbuf_mr));
	close(dmabuf_fd);
free_no_data_message_mr:
	ctxt->prov_ops.dereg_mr(verbs_get_mr(no_data_message_mr));
	close(no_data_message_mr_fd);
free_bounce_private:
	munmap(bounce_private_mem, cvmrdma_priv_bounce_total_bytes(ctxt->aead));
free_read_metadata:
	free_qp_read_metadata(read_metadata);
free_chunk_queue:
	free_qp_chunk_queue(qp->chunk_queue);
free_send_metadata:
	free_qp_send_metadata(send_metadata);
free_recv_metadata:
	free_qp_recv_metadata(recv_metadata);
free_qp:
	ctxt->prov_ops.destroy_qp(ibqp);
err:
	return NULL;


}



static int cvmrdma_modify_qp(struct ibv_qp *ibqp, struct ibv_qp_attr *attr,
		     int attr_mask)
{
	struct cvmrdma_qp * qp = (struct cvmrdma_qp *)ibqp;
	struct ibv_qp * prov_qp = qp->provider_qp;

    struct cvmrdma_context * ctxt = cvmrdma_get_ctx(qp->user_qp.qp.context);
    int ret = 0;

    ret = ctxt->prov_ops.modify_qp(prov_qp, attr, attr_mask);
    if(ret)
    {
        goto err;
    }

	/* Update the state on the user-visible qp fields */
	qp->user_qp.qp.state = prov_qp->state;

	/* The RPC object need to be connected to send, but this is not required for receive (the RPC server
	   is already running). However, since the user passes the QPN in the INIT->RTR transition,
	   connect at this step.
	*/
	if((attr_mask & IBV_QP_STATE) && (attr->qp_state == IBV_QPS_RTR))
	{

		struct ibv_port_attr port_attr = {0};
		ret = ctxt->prov_ops.query_port(qp->user_qp.qp.context, /* port num */ 1, &port_attr);
		DEBUG("[MODIFY_QP] lid %d\n", port_attr.lid);
		DEBUG("[MODIFY_QP] attr->ah_attr.dlid %d\n", attr->ah_attr.dlid);
		bool is_local = (port_attr.lid == attr->ah_attr.dlid);
		assert(ret == 0);
		DEBUG("[MODIFY_QP] attr->dest_qp_num 0x%" PRIx32 "\n", attr->dest_qp_num);
		qp->lid = port_attr.lid;
		DEBUG(stderr, "[MODIFY_QP] local lid %d\n", qp->lid);
		qp->dlid = attr->ah_attr.dlid;
		DEBUG(stderr, "[MODIFY_QP] remote lid %d\n", qp->dlid);
		srand48(time(NULL));
		qp->is_loopback = is_local;

		char * remote_addr = lookup_ip_by_lid(qp->dlid);
		if(remote_addr == NULL)
		{
			fprintf(stderr, "Failed to lookup IP for LID %d\n", qp->dlid);
			goto err;
		}
		/* Open a session to the remote using the same event loop */
		/* Only connect to the remote's RPC server when we know that the server is up */
		qp->remote_addr = remote_addr;

		/* Start by connecting to the server if not already connected */
		if(! qp->remote_network_client_connection && !is_local) {
			DEBUG("[MODIFY_QP] Connecting to the remote server on address %s\n", qp->remote_addr);
			qp->remote_network_client_connection = new_client_rpc_session(ctxt->network_evloop, qp->remote_addr);
			qp->remote_network_client_cap = get_cvmrdma_client(qp->remote_addr, ctxt->network_evloop, qp->remote_network_client_connection);
		} else if (! qp->local_network_client_connection && is_local) {
			DEBUG("[MODIFY_QP] Connecting to the local server on address %s\n", qp->local_addr);
			qp->local_network_client_connection = new_client_rpc_session(ctxt->network_evloop, qp->local_addr);
			qp->local_network_client_cap = get_cvmrdma_client(qp->local_addr, ctxt->network_evloop, qp->local_network_client_connection);
		} else {
			DEBUG("[MODIFY_QP] Already connected to the server\n");
		}


		switch(ibqp->qp_type)
		{
			case IBV_QPT_RC:
			case IBV_QPT_UC: {
				assert(attr_mask & IBV_QP_DEST_QPN);
				uint32_t dest_qpn = attr->dest_qp_num;

				DEBUG("[MODIFY_QP] Obtain notification channel cap from remote for its QP with QPN 0x%" PRIx32 "\n", dest_qpn);

				/* Get the qp notifier capability from the remote over the network and share the Tx key with the remote.
				    */
				if(is_local) {
					qp->remote_qnc = get_qp_notification_client(
						qp->local_addr, ctxt->network_evloop, qp->local_network_client_cap, dest_qpn 
					);
					set_remote_params(qp->local_addr,  ctxt->network_evloop, qp->remote_qnc,
									  qp->ringbuf_mr->rkey);
				} else {
					qp->remote_qnc = get_qp_notification_client(
						qp->remote_addr, ctxt->network_evloop, qp->remote_network_client_cap, dest_qpn 
					);
					set_remote_params(qp->remote_addr, ctxt->network_evloop, qp->remote_qnc,
									  qp->ringbuf_mr->rkey);
				}

				break;
			}
			default: {
				fprintf(stderr, "modify_qp: unsupported QP type\n");
				return -1;
			}
		}

		/* Prepare all the slots: need one RR per WRITE_WITH_IMM. Can only post recv  
			after RTR state */
		struct ibv_recv_wr recv_wr[1] = {0};
		for(int i = 0 ; i < 4096 ; i++)
		{
			struct ibv_recv_wr * bad_wr = NULL;
			ret = ctxt->prov_ops.post_recv(prov_qp, recv_wr, &bad_wr);
			if(ret)
			{
				fprintf(stderr, "Failed to post recv initial RR for ring slots (%d)\n", ret);
				exit(1);
			}
		}

		/* Before we can receive anything, we need to wait for the remote to send us its rx key.
			Note that it also sets the ring_rkey, needed later for send */
		const bool skip_wait_for_remote_params = true; //(port_attr.lid == attr->ah_attr.dlid);
		if (!skip_wait_for_remote_params)
		{
			DEBUG("[MODIFY_QP] MODIFY->RTR: wait for rx key...\n");
			pthread_mutex_lock(&qp->remote_params_lock);
			while(! qp->remote_params_set)
			{
				pthread_cond_wait(&qp->remote_params_set_cv, &qp->remote_params_lock);
			}
			pthread_mutex_unlock(&qp->remote_params_lock);
		} else {
			DEBUG("[MODIFY_QP] MODIFY->RTR: skip wait for rx key\n");
		}


		// Get shared key from env
		char * shared_key_str = getenv("CVMRDMA_SHARED_KEY");
		if(shared_key_str == NULL)
		{
			fprintf(stderr, "CVMRDMA_SHARED_KEY not set\n");
			goto err;
		}
		if(sodium_hex2bin((unsigned char *) qp->shared_key, cvmrdma_aead_KEYBYTES, shared_key_str, strlen(shared_key_str), NULL, NULL, NULL) == -1)
		{
			fprintf(stderr, "Failed to parse shared key\n");
			goto err;
		}
	}

    return 0;

err:
    return ret;
}

//_Atomic uint64_t total_polled = 0;

/* Poll the WCs that are passed by the CQ server. */
static int cvmrdma_poll_cq(struct ibv_cq * ibcq, int num_entries, struct ibv_wc *wc)
{
	struct cvmrdma_cq * cq = (struct cvmrdma_cq *)ibcq;
	struct cvmrdma_context * ctxt = cvmrdma_get_ctx(cq->user_cq.cq.context);
	int n_popped;
	int err;

	/* If there are notifications OR an error, return ... */
	n_popped = wcq_pop_into(cq->wc_queue, wc, num_entries);
	// total_polled += n_popped;
	if(n_popped != 0)
	{
		return n_popped;
	}

	err = cvmrdma_cq_server_run_once_if_idle_send(ctxt, cq, NULL);
	if(err < 0)
	{
		return err;
	}

	/* run it ONLY if it is not running yet */
	err = cvmrdma_cq_server_run_once_if_idle_recv(ctxt, cq, NULL);
	if(err < 0)
	{
		return err;
	}

	/* Let tje next poll return */
	return 0;

}


static int cvmrdma_post_recv(struct ibv_qp *ibqp, struct ibv_recv_wr *wr,
		struct ibv_recv_wr **bad_wr)
{
	/*
	  	No SEND happens directly with CVMRDMA: they are emulated over WRITE_WITH_IMMs.
		Record the location of the post_recv to know where to copy the incoming payload later.
	*/
	struct cvmrdma_qp * qp = (struct cvmrdma_qp *)ibqp;

	for(struct ibv_recv_wr * this_wr = wr ; this_wr != NULL ; this_wr = this_wr->next)
	{
		qp_recv_metadata_push(qp->recv_metadata, this_wr->wr_id, this_wr->sg_list, this_wr->num_sge);
	}

	return 0;
}


void send_nonce_cache(struct cvmrdma_qp *qp, struct cvmrdma_context *ctxt, uint64_t *nonce_metadata) {
#ifndef CVMRDMA_DISABLE_SECURE_SECONDARY_CHANNEL
	if (qp->nonce_metadata_cache_offset == 0){
		randombytes_buf(qp->nonce_metadata_cache, cvmrdma_aead_NPUBBYTES * OPT_cvmrdma_nonce_cache_size);

		if (qp->is_loopback) {
			remote_notify_nonce(qp->local_addr, ctxt->network_evloop, qp->remote_qnc, qp->nonce_metadata_cache);
		} else {
			remote_notify_nonce(qp->remote_addr, ctxt->network_evloop, qp->remote_qnc, qp->nonce_metadata_cache);
		}
	}
	size_t word_off = (size_t)(qp->nonce_metadata_cache_offset) * cvmrdma_aead_NPUB_WORDS;
    memcpy(nonce_metadata,
           &qp->nonce_metadata_cache[word_off],
           cvmrdma_aead_NPUBBYTES);	
	qp->nonce_metadata_cache_offset = (qp->nonce_metadata_cache_offset + 1) % OPT_cvmrdma_nonce_cache_size;
#else 
	memset(nonce_metadata, 0, cvmrdma_aead_NPUBBYTES);
#endif
}

void send_nonce(struct cvmrdma_qp *qp, struct cvmrdma_context *ctxt, uint64_t *nonce_metadata) {
#ifndef CVMRDMA_DISABLE_SECURE_SECONDARY_CHANNEL
	randombytes_buf(nonce_metadata, cvmrdma_aead_NPUBBYTES);
	// send the nonce metadata to the remote

	//fprintf(stderr, "[Send] send nonce %lx %lx %lx\n", nonce_metadata[0], nonce_metadata[1], nonce_metadata[2]);

	if (qp->is_loopback) {
		remote_notify_nonce(qp->local_addr, ctxt->network_evloop, qp->remote_qnc, nonce_metadata);
	} else {
		remote_notify_nonce(qp->remote_addr, ctxt->network_evloop, qp->remote_qnc, nonce_metadata);
	}
#else 
	memset(nonce_metadata, 0, cvmrdma_aead_NPUBBYTES);
#endif
}



_Atomic uint64_t cvmrdma_global_send_wr_id = 0;

// these variables are all just used for debugging
// _Atomic uint64_t send_count = 0;
//_Atomic uint64_t no_data_send_count = 0;
//_Atomic uint64_t max_send_size = 0;
//_Atomic uint64_t total_chunks_send = 0;
//_Atomic uint64_t chunk_cache_used = 0;

// These variables are for the send side
cvm_time_t wr_submit_start, wr_submit_end; // This tracks the time that it takes between submission from the user application to when it is actually processed
cvm_time_t wr_calculations; // Tracks time that is needed to process the wr and get all properties needed for sending ready
cvm_time_t wr_metadata_push; // Tracks time taken to push metadata for the write request
cvm_time_t wr_send_nonce; // Tracks time taken to send the nonce for the write request
cvm_time_t wr_wait_for_recv_slot; // Tracks time taken to wait for a receive slot
cvm_time_t wr_wait_for_sem; // Track time that it takes to acquire the semaphore
cvm_time_t wr_cache_atomic_load; // Track time that it takes to do the atomic load for cache checking
cvm_time_t wr_cache_write_protect; // Track time that it takes to write protect the chunk
cvm_time_t wr_cache_atomic_store; // Track time that it takes to do the atomic store for cache marking
cvm_time_t wr_generate_nonce;
cvm_time_t wr_push_chunk_tracking; // Tracks time taken to push chunk tracking information
cvm_time_t wr_encrypt_metadata; // Tracks time taken to encrypt metadata for the write request	
cvm_time_t wr_encrypt_payload; // Tracks time taken to encrypt the payload for the write request
cvm_time_t wr_ibv_post_send; // Tracks time taken to call ibv_post_send


uint64_t REPORT = 1000;

int post_send_do_bounce(struct cvmrdma_qp * qp, struct ibv_send_wr *wrs, struct ibv_send_wr **bad_wr)
{

	/*send_count++;
	if (send_count % REPORT == 0){
		fprintf(stderr, "Send %d\n", send_count);
	}*/

	struct cvmrdma_context * ctxt = cvmrdma_get_ctx(qp->user_qp.qp.context);
	struct cvmrdma_pd * pd = (struct cvmrdma_pd *)qp->user_qp.qp.pd;
    enum sodium_crypto_aead aead = ctxt->aead;

	unsigned long long header_bytes = cvmrdma_header_bytes(aead);

	int err;

	if(qp->user_qp.qp.qp_type != IBV_QPT_RC && qp->user_qp.qp.qp_type != IBV_QPT_UC)
	{
		fprintf(stderr, "ERROR: for now CVMRDMA only supports RC and UC\n");
		return EINVAL;
	}

	struct ibv_qp * prov_qp = qp->provider_qp;
	struct ibv_qp_ex * prov_qpx = ibv_qp_to_qp_ex(prov_qp);


	//for(struct ibv_send_wr * this_wr = wrs ; this_wr != NULL ; this_wr = this_wr->next)
	struct ibv_send_wr * this_wr = wrs;
	{
		enum ibv_wr_opcode this_op = this_wr->opcode;
		uint32_t this_imm = this_wr->imm_data;

		if((this_wr->send_flags & IBV_SEND_INLINE) != 0) {
			fprintf(stderr, "ERROR: inline not supported, sendflags: %d\n", this_wr->send_flags);
			exit(1);
		}


		bool is_send = (this_op == IBV_WR_SEND) || (this_op == IBV_WR_SEND_WITH_IMM);
		bool is_write = (this_op == IBV_WR_RDMA_WRITE) || (this_op == IBV_WR_RDMA_WRITE_WITH_IMM);
		// this is the read send by the user. This read needs to be send over the remote qp, so the server can perform a write to the client
		bool is_read_ping = (this_op == IBV_WR_RDMA_READ); 
		// this is an interal read performed by the server, to write data from the server to the client, emulating a read.
		bool is_read_internal = (this_op == IBV_WR_RDMA_READ_INTERNAL);

		if(! (is_send || is_write || is_read_ping || is_read_internal))
		{
			fprintf(stderr, "ERROR: unsupported infiniband operation %d\n", this_op);
			exit(1);
		}

		/* Prepare the bounce buffer
		   1. Gather the data in a contiguous block

		   XXX: For write, gather 
		 */

		/* TODO:
		   - gather one chunk
		   - encrypt it
		   - notify remote
		   - copy to public space
		   - post send
		
		*/
		uint64_t const * shared_key = qp->shared_key;

		struct ibv_sge * sgl = this_wr->sg_list;
		size_t nsge = this_wr->num_sge;

		/* total bytes sent for this WR */
		size_t bytes_sent = 0;
		size_t chunks_in_wr = 0;

		/* position in sg-list */
		int cur_sge_idx = 0;
		size_t cur_sge_offt = 0;

		uint32_t rkey = this_wr->wr.rdma.rkey;
		uint64_t remote_addr = this_wr->wr.rdma.remote_addr;

		if (rkey <= 0) {
			DEBUG("[SEND QPN %x] rkey is invalid: 0x%x\n", qp->user_qp.qp.qp_num, rkey);
		}
		if (remote_addr < 0) {
			DEBUG("[SEND QPN %x] remote_addr is invalid: 0x%lx\n", qp->user_qp.qp.qp_num, remote_addr);
		}

		bool no_data_send = false;
		if (nsge == 0 || (rkey <= 0 && is_write) || (remote_addr < 0 && is_write)) {
			nsge = 0; // should already be 0, but just to be sure
			no_data_send = true;
		}

		size_t total_bytes = 0;
		size_t total_chunks = 0;
		for(size_t i = 0 ; i < nsge ; i++) {
			total_bytes += sgl[i].length;
			struct cvmrdma_mr * cur_mr = get_bounce_metadata_for_lkey(pd->bounce_metadata, sgl[i].lkey);
			size_t sge_chunks = DIVIDE_ROUND_UP(sgl[i].length + ((sgl[i].addr- cur_mr->hca_va) % OPT_cvmrdma_chunk_bytes), OPT_cvmrdma_chunk_bytes);
			total_chunks += sge_chunks;
		}
		/*if (total_chunks > 1){
			fprintf(stderr, "Send more than one chunks: %d\n", total_chunks);
		}
		total_chunks_send += total_chunks;
		if (total_chunks_send % REPORT == 0){
			fprintf(stderr, "Send %d chunks\n", total_chunks_send);
		}*/

		/*if (total_bytes > max_send_size){
			max_send_size = total_bytes;
			fprintf(stderr, "Found new max send size: %d\n", max_send_size);
		}*/

		if (total_chunks == 0) {
			nsge = 0;
			no_data_send = true;
		}

		if (no_data_send){
			//no_data_send_count++;
			/*if (no_data_send_count % REPORT == 0){
				fprintf(stderr, "Send %d no data messages\n", no_data_send_count);
			}*/
		}


		bool send_signaled = this_wr->send_flags & IBV_SEND_SIGNALED;
		/* XXX: pass the number of chunks to the CQ_server to know when to emit a send WC for user. Do it 
		   BEFORE we start sending, so that CQ server has this informatin when WCs start to arrive
		   This means that each chunk has to send EXACTLY payload_bytes, even if the tag ends up
		   being shorter (bc of the AEAD choice) */

		get_time(&wr_calculations);


		if (is_read_ping){
			qp_read_metadata_push(qp->read_metadata, this_wr->wr_id, sgl, nsge, send_signaled);

			// This results in an empty message being sent to the remote to trigger the read operation
			no_data_send = true;
			nsge = 0;

			// we need a second qp_send_metadata to track the read ping operation
			// WARNING: This currently assumes that between this push and the push above, no other push happens!
			qp_send_metadata_push(qp->send_metadata, this_wr->wr_id, IBV_WR_RDMA_READ_PING, 0, 0, false, no_data_send);

			/*if (qp->is_loopback) {
				remote_qp_notify_read(qp->local_addr, ctxt->network_evloop, qp->remote_qnc, remote_addr, total_bytes, rkey);
			} else {
				remote_qp_notify_read(qp->remote_addr, ctxt->network_evloop, qp->remote_qnc, remote_addr, total_bytes, rkey);
			} 

			return 0;*/
		} else {
			qp_send_metadata_push(qp->send_metadata, this_wr->wr_id, this_op, total_bytes, total_chunks, send_signaled, no_data_send);
		}

		get_time(&wr_metadata_push);

#if defined(DEBUG_TIMING) || defined(DEBUG_CYCLES)
		if (total_chunks > 1) {
			fprintf(stderr, "WARNING: More than one chunk, time mesaurements will be incorrect\n");
		}
#endif

		while(cur_sge_idx < nsge && !no_data_send)
		{
			// generate new metadata nonce for each metadata 
			uint64_t nonce_metadata[cvmrdma_aead_NPUB_WORDS];
			send_nonce_cache(qp, ctxt, nonce_metadata);

			get_time(&wr_send_nonce);


			struct ibv_sge * cur_sge = &sgl[cur_sge_idx];

			struct cvmrdma_mr * cur_mr = get_bounce_metadata_for_lkey(pd->bounce_metadata, cur_sge->lkey);
			uint64_t offt = (cur_sge->addr - cur_mr->hca_va) + cur_sge_offt;

			/* If this is the last chunk, we'll need to notify that it's a WC generating chunk (if op != WRITE) */
			size_t chunks_num = DIVIDE_ROUND_UP(cur_sge->length + ((cur_sge->addr - cur_mr->hca_va) % OPT_cvmrdma_chunk_bytes), OPT_cvmrdma_chunk_bytes);
			bool last_chunk = chunks_in_wr + 1 == chunks_num;

			// TODO find a better way to handle empty sges
			if (chunks_num == 0) {
				last_chunk = true; // no chunks, so this is the last chunk
			}

			DEBUG("[SEND QPN %x] rkey: 0x%x, lkey: 0x%x, remote_addr: 0x%lx, dlid %x, lid %x, op: 0x%x, ctxt: %p\n", 
				qp->user_qp.qp.qp_num, rkey, cur_sge->lkey, remote_addr, qp->dlid, qp->lid, this_op, (void*)ctxt);
			DEBUG("[SEND QPN %x] chunks_in_wr %lu\n", qp->user_qp.qp.qp_num, chunks_in_wr);
			DEBUG("[SEND QPN %x] cur_sge->length 0x%lx\n", qp->user_qp.qp.qp_num, cur_sge->length);
			//fprintf(stderr, "[SEND QPN %x] offt 0x%x\n", qp->user_qp.qp.qp_num, offt % OPT_cvmrdma_chunk_bytes);
			DEBUG("[SEND QPN %x] last_chunk %d\n", qp->user_qp.qp.qp_num, last_chunk);
			DEBUG("[SEND QPN %x] chunks_num %lu\n", qp->user_qp.qp.qp_num, chunks_num);
			DEBUG("[SEND QPN %x] total_chunks %lu\n", qp->user_qp.qp.qp_num, total_chunks);
			/*fprintf(stderr, "[SEND QPN %x] want to send data: %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x\n", 
				qp->user_qp.qp.qp_num,
				((unsigned char *)cur_sge->addr)[0], ((unsigned char *)cur_sge->addr)[1], ((unsigned char *)cur_sge->addr)[2], ((unsigned char *)cur_sge->addr)[3],
				((unsigned char *)cur_sge->addr)[4], ((unsigned char *)cur_sge->addr)[5], ((unsigned char *)cur_sge->addr)[6], ((unsigned char *)cur_sge->addr)[7],
				((unsigned char *)cur_sge->addr)[8], ((unsigned char *)cur_sge->addr)[9], ((unsigned char *)cur_sge->addr)[10], ((unsigned char *)cur_sge->addr)[11],
				((unsigned char *)cur_sge->addr)[12], ((unsigned char *)cur_sge->addr)[13], ((unsigned char *)cur_sge->addr)[14], ((unsigned char *)cur_sge->addr)[15],
				((unsigned char *)cur_sge->addr)[16], ((unsigned char *)cur_sge->addr)[17]
			);*/


			/* offt in MR*/
			uint64_t chunk_idx = offt / OPT_cvmrdma_chunk_bytes;

			uint64_t alinged_offset = offt - (offt % OPT_cvmrdma_chunk_bytes);
			/* location in memory */
			unsigned char * ptxt = cur_mr->user_mr.ibv_mr.addr + alinged_offset;
			//fprintf(stderr, "[SEND QPN %x] chunk_addr %p, chunk aligned_offset 0x%lx, chunk_idx %lu\n", qp->user_qp.qp.qp_num, ptxt, alinged_offset, chunk_idx);
			uint64_t send_len = alinged_offset + OPT_cvmrdma_chunk_bytes > cur_mr->user_mr.ibv_mr.length ? cur_mr->user_mr.ibv_mr.length - alinged_offset : OPT_cvmrdma_chunk_bytes;


            /*uint64_t remaining_in_mr = cur_mr->user_mr.ibv_mr.length - alinged_offset;
            size_t within_chunk_offt = offt % OPT_cvmrdma_chunk_bytes;
            if ( (within_chunk_offt + 0x400 + 18 < OPT_cvmrdma_chunk_bytes) &&
                 (within_chunk_offt + 0x400 + 18 < remaining_in_mr) )
            {
                unsigned char *future_data = ptxt + within_chunk_offt + 0x400;
                fprintf(stderr,
                    "[SEND QPN %x] future data (%p): "
                    "%02x %02x %02x %02x %02x %02x %02x %02x "
                    "%02x %02x %02x %02x %02x %02x %02x %02x %02x %02x\n",
                    qp->user_qp.qp.qp_num,
                    (void*)future_data,
                    future_data[0], future_data[1], future_data[2], future_data[3],
                    future_data[4], future_data[5], future_data[6], future_data[7],
                    future_data[8], future_data[9], future_data[10], future_data[11],
                    future_data[12], future_data[13], future_data[14], future_data[15],
                    future_data[16], future_data[17]
                );
            }*/

			uint64_t true_payload_bytes = 0; 

			if (!last_chunk){
				true_payload_bytes = OPT_cvmrdma_chunk_bytes - (offt % OPT_cvmrdma_chunk_bytes);
				cur_sge_offt += true_payload_bytes;
			} else {
				true_payload_bytes = cur_sge->length - cur_sge_offt;
				cur_sge_idx++;
				cur_sge_offt = 0;
			}

			DEBUG("[SEND QPN %x] chunk_idx %lu\n", qp->user_qp.qp.qp_num, chunk_idx);
			DEBUG("[SEND QPN %x] true_payload_bytes %lu\n", qp->user_qp.qp.qp_num, true_payload_bytes);
			DEBUG("[SEND QPN %x] send_len %lu\n", qp->user_qp.qp.qp_num, send_len);


			/* Get a slot. If all the slots are full, wait for a slot to be acked */
			/* We must get the slot here in order to not also hold any chunk lock which 
			 * would block the CQ server.
			*/
			int slot_id = 0;
			{
				/* LOCK */
				pthread_mutex_lock(&qp->send_lines_full_lock);

				while(qp->send_lines_full >= cvmrdma_ring_entries)
				{
					// fprintf(stderr, "[SEND QPN %x] Waiting for send slot\n", qp->user_qp.qp.qp_num);
					pthread_cond_wait(&qp->send_lines_avail, &qp->send_lines_full_lock);
				}

				slot_id = qp->next_send_slot;

				/*if (slot_id == cvmrdma_ring_entries - 1){
					fprintf(stderr, "Used all slots!!!\n");
				}*/

				qp->next_send_slot = (slot_id + 1) % cvmrdma_ring_entries;
				
				qp->send_lines_full++;
				DEBUG("[SEND QPN %x] slot_id %d\n", qp->user_qp.qp.qp_num, slot_id);
				DEBUG("[SEND QPN %x] send_lines_full %d\n", qp->user_qp.qp.qp_num, qp->send_lines_full);

				pthread_mutex_unlock(&qp->send_lines_full_lock);
				/*  UNLOCK */
			}
			get_time(&wr_wait_for_recv_slot);


			struct chunk_tracking_info* chunk_info = &cur_mr->chunk_tracking[chunk_idx];
			// everything that happend before is just preparation, but does not rely on the content of the chunk
			// from here on we need to make sure that the chunk is not modified until we have sent it.
			// Therefore we need to acquire the chunk lock and wait until the chunk is not in transit anymore.

			// This chunk lock is aquired this way, because there is a special case for read:
			// If two reads to the same chunk happen very fast after each other, it could result in the
			// cvmrdma_cq_server_run_once_if_idle_recv blocking forever, due to the second read ping being received before the
			// send wc of the previous read is processed. In this case the chunk would still be marked as in transit,
			// and the second read would wait forever for the chunk to become free again.
			// We therefore poll the send cq when the in_transit_sem is not available, to make get the wc of the previous read and unlock
			// the chunk.
			wait_in_transit_sem_with_send_progress(ctxt, qp, &chunk_info->in_transit_sem);
			//cvmrdma_sem_wait(&chunk_info->in_transit_sem, "SEND in transit sem");
			get_time(&wr_wait_for_sem);

			bool cached = false;
			if(!OPT_cvmrdma_disable_cache){
				cached = atomic_load(&chunk_info->cached);
				get_time(&wr_cache_atomic_load);
			} else {
				wr_cache_atomic_load = wr_wait_for_sem;
			}

			// after we acquired the in_transit_sem, we set it to be write protected.
			// the cache handler, can only remove the write protection if it can aquire the in_transit_sem.
			// Until the cache can aquire the in_transit_sem, the write will block.
			// There is the possibility, that a write occurs between aquiring the lock and setting the write protection.
			// this is ok, because these writes will be included in the encryption of the chunk.
			size_t wp_len = send_len;
			if(!OPT_cvmrdma_disable_cache && !cached) {
				// Mark chunk in userMR as write protected
				int uffd = cur_mr->uffd;
				struct uffdio_writeprotect uffdio_wp = {
					.range = {
						.start = (unsigned long) ptxt,
						.len = wp_len,
					},
					.mode = UFFDIO_WRITEPROTECT_MODE_WP,
				};
				if (ioctl(uffd, UFFDIO_WRITEPROTECT, &uffdio_wp) == -1) {
					exit(1);
				}


				get_time(&wr_cache_write_protect);
				//fprintf(stderr, "[SEND] Making chunk %llu not writable. addr=%llx, len=%llx\n", chunk_idx, ptxt, wp_len);

				atomic_store(&chunk_info->cached, true);
				get_time(&wr_cache_atomic_store);
				//fprintf(stderr, "[SEND QPN %x] Marked chunk %lu as cached\n", qp->user_qp.qp.qp_num, chunk_idx);
			} else {
				wr_cache_write_protect = wr_wait_for_sem;
				wr_cache_atomic_store = wr_wait_for_sem;
			}


			if(!cached) {
				/* generate new payload nonce */
				DEBUG("[SEND QPN %x] Generating new nonce for chunk %lu\n", qp->user_qp.qp.qp_num, chunk_idx);

				if (!OPT_cvmrdma_disable_payload_nonce_pregeneration) {
					if (qp_nonce_queue_size(qp->payload_nonce_queue) > 0) {
						memcpy(chunk_info->nonce, qp_nonce_queue_pop(qp->payload_nonce_queue), cvmrdma_aead_NPUBBYTES);
					} else {
						// if there is no pre-generated nonce, generate one here, instead of waiting for the queue to be filled.s
						randombytes_buf(chunk_info->nonce, cvmrdma_aead_NPUBBYTES);
					}
				} else {
					randombytes_buf(chunk_info->nonce, cvmrdma_aead_NPUBBYTES);
				}
				DEBUG("[SEND QPN %x] nonce %lx %lx\n", qp->user_qp.qp.qp_num, chunk_info->nonce[0], chunk_info->nonce[1]);
				get_time(&wr_generate_nonce);
			} else {
				wr_generate_nonce = wr_cache_atomic_store;
			}

			

			unsigned char * slot = cur_mr->provider_va + (chunk_idx * cvmrdma_ring_slot_bytes(ctxt->aead));

			// we need to track if a chunk is currently being sent.
			// Otherwise a subsequent send of the same chunk could overwrite the metadata of the previous send.
			// This would result in a decrpytion error on the recivers side, because the nonce would not match the metadata.
			// Before modifying the chunk we therefore need to acquire the lock.
			// Furthermore, the chunk idx is saved in the chunk queue, so that the CQ server can
			// unlock the chunk lock once the chunk is sent.
			struct chunk_entry entry = {
				.chunk_idx = chunk_idx,
				.lkey = cur_sge->lkey,
			};

			qp_chunk_queue_push(qp->chunk_queue, &entry);
			get_time(&wr_push_chunk_tracking);
			DEBUG("[SEND QPN %x] Waiting for chunk lock for chunk %lu\n", qp->user_qp.qp.qp_num, chunk_idx);

			DEBUG("[SEND QPN %x] Acquired chunk lock for chunk %lu\n", qp->user_qp.qp.qp_num, chunk_idx);

			/***** NOTIFY ******/
			unsigned long long notif_clen;
			{
				struct cq_server_notification notif = {0};

				if(is_write)
				{
					/* This chunk will be directly written at the destination */
					notif.hca_addr = remote_addr + bytes_sent; /* include the current offt */
					notif.op = this_op;
					notif.rkey = rkey;
					notif.imm = this_imm;
					memcpy(notif.payload_nonce, chunk_info->nonce, cvmrdma_aead_NPUBBYTES);
					notif.chunk_offt = offt % OPT_cvmrdma_chunk_bytes;
					notif.chunk_len = true_payload_bytes;
					notif.last_chunk = last_chunk;
					notif.payload_len = send_len;
				}
				else // if send or read-internal
				{
					/* All these chunks will be written to the same RR, until last_chunk is reached */
					notif.hca_addr = 0;
					notif.op = this_op;
					notif.rkey = 0;
					notif.imm = this_imm;
					memcpy(notif.payload_nonce, chunk_info->nonce, cvmrdma_aead_NPUBBYTES);
					notif.chunk_offt = offt % OPT_cvmrdma_chunk_bytes;
					notif.chunk_len = true_payload_bytes;
					notif.last_chunk = last_chunk;
					notif.payload_len = send_len;
				}

				notif_clen = header_bytes;
				err = cvmrdma_encrypt(
					slot, &notif_clen,
					(unsigned char const *)&notif, sizeof(notif),
					NULL, 0,
					0,
					(unsigned char *)nonce_metadata,
					(unsigned char const *)shared_key,
					aead
				);
				if(err)
				{
					fprintf(stderr, "Failed to encrypt chunk!\n");
					exit(1);
				}

				assert(notif_clen == header_bytes);
			}

			get_time(&wr_encrypt_metadata);
			/**** ENCRYPT *****/
			if(cached){
				//fprintf(stderr, "[SEND QPN %x] Chunk %lu is cached, skipping\n", qp->user_qp.qp.qp_num, chunk_idx);
				//chunk_cache_used++;
				/*if (chunk_cache_used % 100 == 0){
					fprintf(stderr, "Chunk cached used %d times\n", chunk_cache_used);
				}*/
				goto skip_payload_encryption;
			}

			DEBUG("[SEND QPN %x] Write protect region; addr %p, last_chunk %d, send_len %lu\n", qp->user_qp.qp.qp_num, ptxt, last_chunk, send_len);			
			DEBUG("[SEND QPN %x] ptxt %x %x %x\n", qp->user_qp.qp.qp_num, ptxt[0], ptxt[1], ptxt[2]);
			DEBUG("[SEND QPN %x] chunk_info->nonce %lx %lx\n", qp->user_qp.qp.qp_num, chunk_info->nonce[0], chunk_info->nonce[1]);

			unsigned long long payload_clen = send_len;
			err = cvmrdma_encrypt(
				slot + notif_clen, /* start after the notif */
				&payload_clen,
				ptxt, send_len,
				NULL, 0,
				NULL,
				(unsigned char *)chunk_info->nonce,
				(unsigned char const *)shared_key,
				aead
			);
			if(err)
			{
				fprintf(stderr, "Failed to encrypt chunk!\n");
				exit(1);
			}

			/*fprintf(stderr, "[SEND QPN %x] slot payload bytes:", qp->user_qp.qp.qp_num);
			for (uint64_t i = 0; i < (payload_clen > 16 ? 16 : payload_clen); i++) {
				fprintf(stderr, "%x ", ((unsigned char *)slot)[header_bytes + i + (offt % OPT_cvmrdma_chunk_bytes)] );
			}
			fprintf(stderr, "\n");*/

			DEBUG("[SEND QPN %x] slot[0] %x %x %x ...\n", qp->user_qp.qp.qp_num, slot[0], slot[1], slot[2]);
			DEBUG("[SEND QPN %x] slot[notif_clen + 0] %x %x %x ...\n", qp->user_qp.qp.qp_num, slot[notif_clen + 0], slot[notif_clen + 1], slot[notif_clen + 2]);
			DEBUG("[SEND QPN %x] notif_clen %llu\npayload_clen %llu\n", qp->user_qp.qp.qp_num, notif_clen, payload_clen);

skip_payload_encryption:
			get_time(&wr_encrypt_payload);

			/****** POST SEND WWI ******/
			/* Synchronize the ring buffer with the remote copy using a WWI.
			   Rings MR are 0-indexed. The send ring starts at 0, the recv 
			   ring starts at cvmrdma_ring_entries * CVMRDMA_CHUNK_SIZE.
			*/
			uint32_t local_provider_lkey = cur_mr->provider_mr->lkey;
			uint32_t remote_ring_rkey = qp->remote_ring_rkey;
			assert(remote_ring_rkey != 0);

			uint64_t helper_offt = (offt%OPT_cvmrdma_chunk_bytes)+header_bytes;
			DEBUG("[SEND QPN %x] writing encrypted %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x\n", 
			 	qp->user_qp.qp.qp_num, 
				slot[0+helper_offt], slot[1+helper_offt], slot[2+helper_offt], slot[3+helper_offt],
				slot[4+helper_offt], slot[5+helper_offt], slot[6+helper_offt], slot[7+helper_offt],
				slot[8+helper_offt], slot[9+helper_offt], slot[10+helper_offt], slot[11+helper_offt],
				slot[12+helper_offt], slot[13+helper_offt], slot[14+helper_offt], slot[15+helper_offt],
				slot[16+helper_offt], slot[17+helper_offt]
			);


			ibv_wr_start(prov_qpx);

			prov_qpx->wr_id = cvmrdma_global_send_wr_id++;
			uint64_t send_offt = chunk_idx * cvmrdma_ring_slot_bytes(ctxt->aead);
			DEBUG("[SEND QPN %x] WR ID 0x%lx\n", qp->user_qp.qp.qp_num, prov_qpx->wr_id);
			DEBUG("[SEND QPN %x] provider lkey 0x%lx\n", qp->user_qp.qp.qp_num, local_provider_lkey);
			DEBUG("[SEND QPN %x] remote rkey 0x%lx\n", qp->user_qp.qp.qp_num, remote_ring_rkey);
			DEBUG("[SEND QPN %x] sge data offt 0x%x\n", qp->user_qp.qp.qp_num, send_offt);
			DEBUG("[SEND QPN %x] sge data len 0x%x\n", qp->user_qp.qp.qp_num, (uint32_t) cvmrdma_ring_slot_bytes(ctxt->aead));
            prov_qpx->wr_flags = IBV_SEND_SIGNALED; /* XXX: in the future could avoid signalling all !*/
			ibv_wr_rdma_write_imm(prov_qpx,
				remote_ring_rkey,
				slot_id * cvmrdma_ring_slot_bytes(ctxt->aead),
				0
			);
			ibv_wr_set_sge(prov_qpx,
				local_provider_lkey,
				send_offt,
				(uint32_t) cvmrdma_ring_slot_bytes(ctxt->aead) /* includes mac */
			);

			err = ibv_wr_complete(prov_qpx);
			if(err)
			{
				fprintf(stderr, "Failed to post WWI for a chunk (%d)\n", err);
				exit(1);
			}

			get_time(&wr_ibv_post_send);

			bytes_sent += true_payload_bytes;
			chunks_in_wr++;

			/* try to dequeue some SEND WC from the CQ (if no other thread is polling the CQ) */
			/*if(qp->send_lines_full >= DIVIDE_ROUND_UP(cvmrdma_ack_threshold, 200))
			{
				err = cvmrdma_cq_server_run_once_if_idle_send(ctxt, qp->provider_cq, NULL);
				if(err < 0)
				{
					fprintf(stderr, "Polling the CQ after a post_send failed %d\n", err);
					exit(1);
				}
			}*/
		}

		assert(bytes_sent == total_bytes);
		assert(chunks_in_wr == total_chunks);

		if (no_data_send) {
			DEBUG("[SEND QPN %x] No data send, sending no-data message\n", qp->user_qp.qp.qp_num);
			
			uint64_t nonce_metadata[cvmrdma_aead_NPUB_WORDS];
			
			send_nonce_cache(qp, ctxt, nonce_metadata);

			bool last_chunk = true; // no data send, so this is the last chunk
			get_time(&wr_send_nonce);
			int slot_id = 0;
			{
				/* LOCK */
				pthread_mutex_lock(&qp->send_lines_full_lock);

				while(qp->send_lines_full >= cvmrdma_ring_entries)
				{
					fprintf(stderr, "[SEND QPN %x] Waiting for send slot\n", qp->user_qp.qp.qp_num);
					pthread_cond_wait(&qp->send_lines_avail, &qp->send_lines_full_lock);
				}

				slot_id = qp->next_send_slot;

				/*if (slot_id == cvmrdma_ring_entries - 1){
					fprintf(stderr, "Used all slots!!!\n");
				}*/

				qp->next_send_slot = (slot_id + 1) % cvmrdma_ring_entries;
				
				qp->send_lines_full++;
				DEBUG("[SEND QPN %x] slot_id %d\n", qp->user_qp.qp.qp_num, slot_id);
				DEBUG("[SEND QPN %x] send_lines_full %d\n", qp->user_qp.qp.qp_num, qp->send_lines_full);

				pthread_mutex_unlock(&qp->send_lines_full_lock);
				/*  UNLOCK */
			}
			get_time(&wr_wait_for_recv_slot);

			DEBUG("[SEND QPN %x] Waiting for no-data message lock\n", qp->user_qp.qp.qp_num);
			cvmrdma_sem_wait(&qp->no_data_message_sem, "SEND no-data message");
			get_time(&wr_wait_for_sem);

			wr_cache_atomic_load = wr_wait_for_sem;
			wr_cache_write_protect = wr_wait_for_sem;
			wr_cache_atomic_store = wr_wait_for_sem;
			wr_generate_nonce = wr_wait_for_sem;
			wr_push_chunk_tracking = wr_wait_for_sem;// no chunk tracking for no-data messages
			DEBUG("[SEND QPN %x] Acquired no-data message lock\n", qp->user_qp.qp.qp_num);
			unsigned char * slot = qp->no_data_message_mr_va;

			unsigned long long notif_clen;
			{
				struct cq_server_notification notif = {0};

				if(is_write)
				{
					/* This chunk will be directly written at the destination */
					notif.hca_addr = 0;
					notif.op = this_op;
					notif.rkey = 0;
					notif.imm = this_imm;
					notif.chunk_offt = 0;
					notif.chunk_len = 0;
					notif.last_chunk = true;
					notif.payload_len = 0;
				}
				else if (is_read_ping) {
					/* This chunk will request a write at the remote */
					notif.hca_addr = remote_addr;
					notif.op = IBV_WR_RDMA_READ_PING;
					notif.rkey = rkey;
					notif.imm = this_imm;
					notif.chunk_offt = 0;

					// This is a little cheaky: We reuse this field to pass the total bytes to read
					// since no data is being sent, chunk_len is not used anyway. 
					notif.chunk_len = total_bytes;
					notif.last_chunk = true;
					notif.payload_len = 0;
				}
				else // if send or read-internal
				{
					/* All these chunks will be written to the same RR, until last_chunk is reached */
					notif.hca_addr = 0;
					notif.op = this_op;
					notif.rkey = 0;
					notif.imm = this_imm;
					notif.chunk_offt = 0;
					notif.chunk_len = 0;
					notif.last_chunk = true;
					notif.payload_len = 0;
				}

				DEBUG("[SEND QPN %x] no-data message notif contents: hca_addr: %lx, op: %d, rkey: %x, imm: %x, chunk_offt: %lu, chunk_len: %lu, last_chunk: %d, payload_len: %lu\n",
					qp->user_qp.qp.qp_num,
					notif.hca_addr, notif.op, notif.rkey, notif.imm,
					notif.chunk_offt, notif.chunk_len, notif.last_chunk, notif.payload_len);

				notif_clen = header_bytes;
				err = cvmrdma_encrypt(
					slot, &notif_clen,
					(unsigned char const *)&notif, sizeof(notif),
					NULL, 0,
					0,
					(unsigned char *)nonce_metadata,
					(unsigned char const *)shared_key,
					aead
				);
				if(err)
				{
					fprintf(stderr, "Failed to encrypt chunk!\n");
					exit(1);
				}

				assert(notif_clen == header_bytes);
			}
			get_time(&wr_encrypt_metadata);
			wr_encrypt_payload = wr_encrypt_metadata; // no payload encryption for no-data messages

			uint32_t local_provider_lkey = qp->no_data_message_mr->lkey;
			uint32_t remote_ring_rkey = qp->remote_ring_rkey;
			assert(remote_ring_rkey != 0);

			ibv_wr_start(prov_qpx);

			prov_qpx->wr_id = cvmrdma_global_send_wr_id++;
			DEBUG("[SEND QPN %x] WR ID 0x%lx\n", qp->user_qp.qp.qp_num, prov_qpx->wr_id);
			DEBUG("[SEND QPN %x] provider lkey 0x%lx\n", qp->user_qp.qp.qp_num, local_provider_lkey);
			DEBUG("[SEND QPN %x] remote rkey 0x%lx\n", qp->user_qp.qp.qp_num, remote_ring_rkey);
			prov_qpx->wr_flags = IBV_SEND_SIGNALED; /* XXX: in the future could avoid signalling all !*/
			ibv_wr_rdma_write_imm(prov_qpx,
				remote_ring_rkey,
				slot_id * cvmrdma_ring_slot_bytes(ctxt->aead),
				0
			);
			ibv_wr_set_sge(prov_qpx,
				local_provider_lkey,
				0,
				(uint32_t) notif_clen
			);

			err = ibv_wr_complete(prov_qpx);
			if(err)
			{
				fprintf(stderr, "Failed to post WWI for a chunk (%d)\n", err);
				exit(1);
			}

			get_time(&wr_ibv_post_send);

			/*if(qp->send_lines_full >= DIVIDE_ROUND_UP(cvmrdma_ack_threshold, 200))
			{
				err = cvmrdma_cq_server_run_once_if_idle_send(ctxt, qp->provider_cq, NULL);
				if(err < 0)
				{
					fprintf(stderr, "Polling the CQ after a post_send failed %d\n", err);
					exit(1);
				}
			}*/
		}
	}


	return 0;
}



_Atomic submitted_send_counts = 0;

static int cvmrdma_post_send(struct ibv_qp *ibqp, struct ibv_send_wr *wr,
		     struct ibv_send_wr **bad_wr)
{
	submitted_send_counts++;
	get_time(&wr_submit_start);

    struct cvmrdma_qp * qp = (struct cvmrdma_qp *)ibqp;
	int ret = 0;

#ifdef SKIP_WR_QUEUE
	get_time(&wr_submit_end);
	for(struct ibv_send_wr * this_wr = wr ; this_wr != NULL ; this_wr = this_wr->next)
	{
		if (this_wr->opcode == IBV_WR_RDMA_READ_INTERNAL) {
			fprintf(stderr, "ERROR: RDMA_READ_INTERNAL not allowed to be used externally\n");
			return EINVAL;
		}

		ret = post_send_do_bounce(qp, this_wr, bad_wr);
	}
	return ret;

#else
	// In theory we'd need to do wr validation in order to send rejected wr as bad_wr to the user.
	
	// Just push the WRs here and handle them asynchronously in cvmrdma_do_bounce_loop.
	for(struct ibv_send_wr * this_wr = wr ; this_wr != NULL ; this_wr = this_wr->next)
	{
		if (this_wr->opcode == IBV_WR_RDMA_READ_INTERNAL) {
			fprintf(stderr, "ERROR: RDMA_READ_INTERNAL not allowed to be used externally\n");
			return EINVAL;
		}

		wrq_push(qp->wr_queue, this_wr);
	}

	DEBUG("[SEND QPN %x] WRs posted\n", qp->user_qp.qp.qp_num);
    return ret;
#endif
}

#ifndef SKIP_WR_QUEUE
// This function is started in a thread to process the WRs posted by the user.
void *cvmrdma_do_bounce_loop(void *args) {
	struct cvmrdma_qp * qp = (struct cvmrdma_qp *)args;

	struct ibv_send_wr * wr = calloc(1, sizeof(struct ibv_send_wr));


	pthread_mutex_lock(&qp->remote_params_lock);
	if(!qp->remote_params_set) {
		DEBUG("WARN: remote params not set");
	}
	while (!qp->remote_params_set) {
		pthread_cond_wait(&qp->remote_params_set_cv, &qp->remote_params_lock);
	}
	pthread_mutex_unlock(&qp->remote_params_lock);

	while(1) {
		// If there are no entries to pop, this blocks non-busy until there are.
		wr = wrq_front(qp->wr_queue);
		get_time(&wr_submit_end);

		struct ibv_send_wr * bad_wr = NULL;
		int ret = post_send_do_bounce(qp, wr, &bad_wr);
		if(ret < 0)
		{
			fprintf(stderr, "failed to send WR with wr_id %lu, ret %d\n", wr->wr_id, ret);
			exit(1);
		}
		wrq_pop(qp->wr_queue);
	}
}
#endif

static int cvmrdma_query_device_ex(struct ibv_context *ibctxt,
			const struct ibv_query_device_ex_input *input,
			struct ibv_device_attr_ex *attr,
			size_t attr_size)
{
	struct cvmrdma_context * ctxt = cvmrdma_get_ctx(ibctxt);
	return ctxt->prov_ops.query_device_ex(ibctxt, input, attr, attr_size);
}



static int cvmrdma_query_port(struct ibv_context *ibctxt, uint8_t port_num,
		struct ibv_port_attr *port_attr)
{
	struct cvmrdma_context * ctxt = cvmrdma_get_ctx(ibctxt);
	return ctxt->prov_ops.query_port(ibctxt, port_num, port_attr);
}


// the 0 iod is not given to a mr in default libibverbs, we
// therefore start counting from 1 for cvmrdma_mr_id
_Atomic uint64_t cvmrdma_mr_id = 1;
static struct ibv_mr *cvmrdma_reg_mr(struct ibv_pd *ibpd, void *addr, size_t length,
        uint64_t hca_va, int access)
{

	struct cvmrdma_pd * pd = (struct cvmrdma_pd *)ibpd;
	struct ibv_context * ibctxt = pd->user_pd.context;
	struct cvmrdma_context * ctxt = cvmrdma_get_ctx(ibctxt);
	assert(length > 0);

	if (!OPT_cvmrdma_disable_cache){
		// if the cache is on we need to make sure that the addr and length are page aligned
		if (((uintptr_t)addr % sysconf(_SC_PAGESIZE)) != 0 || (length % sysconf(_SC_PAGESIZE)) != 0) {
			fprintf(stderr, "ERROR: addr (%p) and length (%zu) must be page aligned\n", addr, length);

			// exit(1);
		}
	}
	

    /* CVM
		
		We cannot directly expose the user region to the device. Instead, we will allocate a
		bounce buffer to encrypt/decrypt the payloads into/from in public memory. To prevent
		TOCTOU attacks during decryption/encryption, we'll use a secundary bounce buffer in
		private memory.

		HACKY: libsodium only supports encrypting contiguous blocks of memory into contiguous
		blocks of memory. To overcome this limitation, we allocate a third buffer where we can
		gather/scatter the payload before encryption/after decryption.

		On send, we'll chunk up the payload to allow pipelining and to limit the size of the
		intermediate buffers. This means we need

		- for send:
			+ 1 page to gather into
			+ 1 page to encrypt into
			+ N public pages of ring buffer to copy into
		- for recv:
			+ N public pages of ring buffer to recv into
			+ 1 page to copy into
			+ 1 page to decrypt into

		The private pages are already allocated on the QP.

		We'll keep track of the mappings (lkey -> MR metadata) and (rkey -> MR metadata), as upon
		send/recv, the lkey/rkey will be communicated to us by the user and we'll need to access 
		the user buffer and the 3 intermediate buffers. 

        Allocating in public memory can be done with a dma-heap.
    */

	// Allocate encrypted provider shadow MR
	
	struct cvmrdma_mr * mr = calloc(1, sizeof(struct cvmrdma_mr));

	uint64_t num_chunks = DIVIDE_ROUND_UP((length + (((uint64_t)addr) % OPT_cvmrdma_chunk_bytes)), OPT_cvmrdma_chunk_bytes);
	uint64_t provider_mr_size = num_chunks * cvmrdma_ring_slot_bytes(ctxt->aead);

	DEBUG("[MR] Allocating provider buffer MR of size %lu for user MR of size %lu\n", provider_mr_size, length);

	int dmabuf_fd = -1;
	void * provider_va = NULL;
	DEBUG("[MR] Allocating provider MR with size %lu\n", (uint64_t) provider_mr_size / (1024*1024));
	struct ibv_mr * provider_mr = alloc_public_mr(ctxt, pd, provider_mr_size, &dmabuf_fd, &provider_va);
    if(! provider_mr)
    {
        fprintf(stderr, "Failed to allocate provider MR with size %lu\n", provider_mr_size);
		exit(1);
    }

	DEBUG("[MR] rkey 0x%x, lkey 0x%x\n", provider_mr->rkey, provider_mr->lkey);

	mr->provider_mr = provider_mr;
	mr->provider_va = provider_va;
	mr->provider_dmabuf_fd = dmabuf_fd;

	// Allocate chunk tracking array
	mr->chunk_tracking = calloc(num_chunks, sizeof(struct chunk_tracking_info));
	if (!mr->chunk_tracking)
	{
		fprintf(stderr, "Failed to allocate chunk tracking array\n");
		exit(1);
	}
	for (uint64_t i = 0; i < num_chunks; i++)
	{
		sem_init(&mr->chunk_tracking[i].in_transit_sem, 0, 1);
		memset(&mr->chunk_tracking[i].nonce, 0, cvmrdma_aead_NPUBBYTES);
	}


	if(!OPT_cvmrdma_disable_cache) {
		// Register userMR and Spin up userfaultfd thread
		// See: man 2 userfaultfd
		long uffd = syscall(__NR_userfaultfd, O_CLOEXEC | O_NONBLOCK);
		if (uffd == -1) {
			fprintf(stderr, "Failed to create userfaultfd %d\n", errno);
			exit(1);
		}

		struct uffdio_api uffdio_api = {
			.api = UFFD_API,
			.features = UFFD_FEATURE_PAGEFAULT_FLAG_WP,
		};
		if (ioctl(uffd, UFFDIO_API, &uffdio_api) == -1) {
			fprintf(stderr, "Failed to ioctl-UFFDIO_API %d\n", errno);
			exit(1);
		}

		size_t reg_len = length;
		/* Register the memory range of the mapping we just created for
		handling by the userfaultfd object. */
		struct uffdio_register uffdio_register = {
			.range = {
				.start = (unsigned long) addr,
				.len = reg_len,
			},
			.mode = UFFDIO_REGISTER_MODE_WP | UFFDIO_REGISTER_MODE_MISSING,
			//.mode = UFFDIO_REGISTER_MODE_WP,
		};
		if (ioctl(uffd, UFFDIO_REGISTER, &uffdio_register) == -1) {
			fprintf(stderr, "Failed to ioctl-UFFDIO_REGISTER %d with addr %p and len %lu\n", errno, addr, reg_len);
			exit(1);
		}

		/* Create a thread that will process the userfaultfd events. */
		struct fault_handler_thread_args *args = calloc(1, sizeof(struct fault_handler_thread_args));
		args->mr = mr;
		args->uffd = uffd;
		int ret = pthread_create(&mr->cache_invalidation_thread, NULL, &fault_handler_thread, (void*) args);
		if (ret) {
			fprintf(stderr, "Failed to create fault handler thread\n");
			exit(1);
		}

		mr->uffd = uffd;
	}

	/*
		The CVMRDMA MR exposes ibv_mr fields to the user containing data (eg the address)
		related to the user region, whereas the buffer actually registered as an MR is kept
		in a private field.
	*/

	uint32_t mr_id = pd->mr_id_ctr++;
	mr_id = atomic_load_explicit(&cvmrdma_mr_id, memory_order_relaxed);
	atomic_fetch_add_explicit(&cvmrdma_mr_id, 1, memory_order_relaxed);

	/* Expose some information to the user */
	//fprintf(stderr, "[MR] Exposing user MR with addr %p, length %lu, lkey/rkey 0x%x\n", addr, length, mr_id);
	mr->user_mr.ibv_mr.addr = addr;
	mr->user_mr.ibv_mr.context = ibctxt;
	mr->user_mr.ibv_mr.handle = 0;
	mr->user_mr.ibv_mr.length = length;
	mr->user_mr.ibv_mr.lkey = mr_id;
	mr->user_mr.ibv_mr.pd = &pd->user_pd;
	mr->user_mr.ibv_mr.rkey = mr_id;
	mr->user_mr.access = access;
	mr->user_mr.mr_type = IBV_MR_TYPE_MR;

	mr->hca_va = hca_va;

	/* Make this bounce MR's metadata available through its lkey or rkey */
	register_bounce_mr(pd->bounce_metadata, mr_id, mr);

	return &mr->user_mr.ibv_mr;
}

/*int cvmrdma_destroy_qp(struct ibv_qp *ibqp)
{
	struct cvmrdma_qp * qp = (struct cvmrdma_qp *)ibqp;
	// TODO: find a way to shutdown the server cleanly
	usleep(100000);

	DEBUG("Dummy destroy QP\n");

	return 0;
}*/

int cvmrdma_destroy_qp(struct ibv_qp *ibqp)
{
	struct cvmrdma_qp * qp = (struct cvmrdma_qp *)ibqp;
	struct cvmrdma_context * ctxt = cvmrdma_get_ctx(qp->user_qp.qp.context);
	int ret;

	// todo stop threads
	// TODO detach qp from cq manager

	ret = free_public_mr(ctxt, qp->ringbuf_mr, qp->ringbuf_dmabuf_fd, qp->ringbuf_va);
	if (ret) {
		fprintf(stderr, "Failed to free ringbuf_mr\n");
		return ret;
	}

	ret = free_public_mr(ctxt, qp->no_data_message_mr, qp->no_data_message_mr_fd, qp->no_data_message_mr_va);
	if (ret) {
		fprintf(stderr, "Failed to free no_data_message_mr\n");
		return ret;
	}

	ret = munmap(qp->bounce_private_mem, cvmrdma_priv_bounce_total_bytes(ctxt->aead));
	if (ret) {
		fprintf(stderr, "Failed to unmap bounce private memory\n");
		return ret;
	}

	free_qp_send_metadata(qp->send_metadata);
	free_qp_chunk_queue(qp->chunk_queue);
	free_qp_read_metadata(qp->read_metadata);

	// TODO: destroy recv_metadata depending on if it is shared or not

	ret = ctxt->prov_ops.destroy_qp(qp->provider_qp);
	if (ret) {
		fprintf(stderr, "Failed to destroy provider QP\n");
		return ret;
	}

	free(qp);

	return 0;
}

int cvmrdma_dereg_mr(struct verbs_mr *vmr){

	fprintf(stderr, "Dereg mr\n");

	struct ibv_mr * ibmr = &vmr->ibv_mr;
	struct cvmrdma_mr * cur_mr = (struct cvmrdma_mr *)ibmr;
	struct cvmrdma_context * ctxt = cvmrdma_get_ctx(ibmr->context);
	int ret;

	ret = free_public_mr(ctxt, cur_mr->provider_mr, cur_mr->provider_dmabuf_fd, cur_mr->provider_va);
	if (ret) {
		fprintf(stderr, "Failed to free provider_mr\n");
		return ret;
	}

	return 0;

}

static struct ibv_srq *cvmrdma_create_srq(struct ibv_pd *ibpd, struct ibv_srq_init_attr *attr)
{
	struct cvmrdma_pd * pd = (struct cvmrdma_pd *)ibpd;
	struct ibv_context * ibctxt = pd->user_pd.context;
	struct cvmrdma_context * ctxt = cvmrdma_get_ctx(ibctxt);

	QpRecvMetadata * recv_metadata = alloc_qp_recv_metadata();
	if(! recv_metadata)
	{
		fprintf(stderr, "Failed to allocated recv metadata storage for srq.\n");
		exit(1);
	}

	struct cvmrdma_srq * srq = calloc(1, sizeof(struct cvmrdma_srq));
	srq->user_srq.srq = (struct ibv_srq) {
		.context = ibctxt,
		.pd = ibpd,
		.srq_context = attr->srq_context,
		.handle = 0,
	};
	srq->recv_metadata = recv_metadata;

	DEBUG("Created SRQ\n");

	return &srq->user_srq.srq;
}

static int cvmrdma_req_notify_cq(struct ibv_cq *ibcq, int solicited_only)
{
	struct cvmrdma_cq * cq = (struct cvmrdma_cq *)ibcq;
	struct cvmrdma_context * ctxt = cvmrdma_get_ctx(cq->user_cq.cq.context);
	bool expected = 0;
	atomic_compare_exchange_strong(&cq->comp_channel_event_fd_filled, &expected, 1);

	return 0;
}

static void cvmrdma_cq_event(struct ibv_cq *ibcq)
{
	// we don't need to handle anything special when the event is detected by the application.
	return;
}

static int cvmrdma_query_qp(struct ibv_qp *ibqp, struct ibv_qp_attr *attr,
		int attr_mask, struct ibv_qp_init_attr *init_attr)
{
	// Note that we might want to fake certain capabilieties
	// Since e.g. imm is not used directly but rather emulated
	struct cvmrdma_qp * qp = (struct cvmrdma_qp *)ibqp;
	struct cvmrdma_context * ctxt = cvmrdma_get_ctx(qp->user_qp.qp.context);
	int ret = ctxt->prov_ops.query_qp(qp->provider_qp, attr, attr_mask, init_attr);
	assert(ret == 0);
	// Disable inlining data as we need to bounce and encrypt data anyway
	init_attr->cap.max_inline_data = 0;
	return ret;
}

static int cvmrdma_post_srq_recv(struct ibv_srq *srq, struct ibv_recv_wr *recv_wr,
			     struct ibv_recv_wr **bad_recv_wr)
{
	/*
	  	No SEND happens directly with CVMRDMA: they are emulated over WRITE_WITH_IMMs.
		Record the location of the post_recv to know where to copy the incoming payload later.
	*/
	struct cvmrdma_srq * csrq = (struct cvmrdma_srq *)srq;

	for(struct ibv_recv_wr * this_wr = recv_wr ; this_wr != NULL ; this_wr = this_wr->next)
	{
		qp_recv_metadata_push(csrq->recv_metadata, this_wr->wr_id, this_wr->sg_list, this_wr->num_sge);
	}

	return 0;
}

/*static int cvmrdma_destroy_cq(struct ibv_cq *ibcq) {

	struct cvmrdma_cq *cq = (struct cvmrdma_cq *)ibcq;
	struct cvmrdma_context * ctxt = cvmrdma_get_ctx(cq->user_cq.cq.context);

	int ret;

	// join cq provider channel thread
	if (cq->provider_comp_channel_thread) {
		atomic_store(&cq->stop_thread, true);
		if (cq->provider_comp_channel && cq->provider_comp_channel->fd >= 0) {
			// this will unblock read if necessary
			close(cq->provider_comp_channel->fd);
			cq->provider_comp_channel->fd = -1;
		}
		pthread_join(cq->provider_comp_channel_thread, NULL);
	}

	// destroy recv cq
	ret = ctxt->prov_ops.destroy_cq(cq->prov_cq_recv);
	if (ret) {
		goto out;
	}

	//destroy send cq
	ret = ctxt->prov_ops.destroy_cq(cq->prov_cq_send);
	if (ret) {
		goto out;
	}

	free(cq);

	// destroy completion channel after cq, as otherwise channel cannot be destroyed
	ret = ibv_destroy_comp_channel(cq->provider_comp_channel);
	if (ret) {
		goto out;
	}

	// emulate ibv_destroy_cq for the channel provided by
	struct ibv_comp_channel *channel = cq->user_cq.cq.channel;
	if (channel) {
		pthread_mutex_lock(&channel->context->mutex);
		--channel->refcnt;
		pthread_mutex_unlock(&channel->context->mutex);
	}

out:
	return ret;
}*/

static int cvmrdma_dealloc_pd(struct ibv_pd *pd) {
	fprintf(stderr, "Dealloc pd called\n");
	return 0;
}

static void cvmrdma_free_context(struct ibv_context *context){

	fprintf(stderr, "Free context called\n");
}

struct verbs_context_ops cvmrdma_ops = {
    .alloc_pd = cvmrdma_alloc_pd,
    .create_cq = cvmrdma_create_cq,
    .create_qp = cvmrdma_create_qp,
    //.dealloc_pd = cvmrdma_dealloc_pd,
    .dereg_mr = cvmrdma_dereg_mr,
    // .destroy_cq = cvmrdma_destroy_cq,
    .destroy_qp = cvmrdma_destroy_qp,
    .free_context = cvmrdma_free_context,
    .modify_qp = cvmrdma_modify_qp,
    .poll_cq = cvmrdma_poll_cq,
    .post_recv = cvmrdma_post_recv,
    .post_send = cvmrdma_post_send,
	.query_device_ex = cvmrdma_query_device_ex,
	.query_port = cvmrdma_query_port,
    // .reg_dmabuf_mr = cvmrdma_reg_dmabuf_mr,
	.req_notify_cq = cvmrdma_req_notify_cq,
    .reg_mr = cvmrdma_reg_mr,
	.create_srq = cvmrdma_create_srq,
	.cq_event = cvmrdma_cq_event,
	.query_qp = cvmrdma_query_qp,
	.post_srq_recv = cvmrdma_post_srq_recv,
};

static int find_in_array(char const ** array, size_t entries, char const * str)
{
	for(int i = 0 ; i < entries ; i++)
	{
		if(strcmp(str, array[i]) == 0)
		{
			return i;
		}
	}
	return -1;
}

static void print_process_name_debug() {
    char proc_name[256] = {0};
    FILE *f = fopen("/proc/self/comm", "r");
    if (f) {
        if (fgets(proc_name, sizeof(proc_name), f)) {
            // Remove trailing newline
            size_t len = strlen(proc_name);
            if (len > 0 && proc_name[len - 1] == '\n') {
                proc_name[len - 1] = '\0';
            }
            DEBUG("Process name: %s\n", proc_name);
        }
        fclose(f);
    } else {
        DEBUG("Could not read process name\n");
    }
}

_Atomic bool tcp_server_running = false;
CapnpEvloopThread * evloop_server = NULL;
CVMRDMAServer * tcp_server = NULL;

bool OPT_cvmrdma_disable_cache = false;
bool OPT_cvmrdma_enable_decrypt_skip = false;
uint64_t OPT_cvmrdma_chunk_bytes = 1024 * 4;
uint64_t cvmrdma_ring_entries = -1;
uint64_t cvmrdma_ack_threshold = -1;
uint64_t OPT_cvmrdma_nonce_cache_size = 100;
bool OPT_cvmrdma_disable_payload_nonce_pregeneration = false;
// it should always hold (OPT_cvmrdma_chunk_bytes * cvmrdma_ring_entries) < 64 * 2048 * 4096 = 536870912 bytes = 512 MiB
static struct verbs_context *cvmrdma_alloc_context(struct ibv_device *ibdev,
						  int cmd_fd,
						  void *private_data)
{
    struct verbs_device * vdev = verbs_get_device(ibdev);
	int ret;

	DEBUG("cvmrdma_alloc_context\n");

	/* eg 192.4.0.3:3456 */
	/*char const * remote_addr = getenv("CVMRDMA_REMOTE_ADDR");
	if(! remote_addr)
	{
		fprintf(stderr, "ERROR: No environment variable CVMRDMA_REMOTE_ADDR.\n");
		goto err;
	}*/

	/* eg *:3456 */
	char const * local_bind_addr = getenv("CVMRDMA_BIND_ADDR");
	if(! local_bind_addr)
	{
		fprintf(stderr, "ERROR: No environment variable CVMRDMA_BIND_ADDR.\n");
		goto err;
	}

	char const * aead_str = getenv("CVMRDMA_AEAD");
	enum sodium_crypto_aead aead = LIBAEGIS_AEAD_AEGIS128X4;
	char const * sodium_aead_str[__SODIUM_AEAD_NR] = {
		[SODIUM_AEAD_NONE] = "none",
		[SODIUM_AEAD_XCHACHAPOLY] = "xchachapoly",
		[SODIUM_AEAD_AES256GCM] = "aes256gcm",
		[SODIUM_AEAD_AEGIS128L] = "aegis128l",
		[LIBAEGIS_AEAD_AEGIS128X2] = "aegis128x2",
		[LIBAEGIS_AEAD_AEGIS128X4] = "aegis128x4",
	};
	if(aead_str)
	{
		int i = find_in_array(sodium_aead_str, __SODIUM_AEAD_NR, aead_str);
		if(i < 0)
		{
			fprintf(stderr, "ERROR: unknown AEAD '%s'\n", aead_str);
			goto err;
		}
		else
		{
		}
		aead = (enum sodium_crypto_aead)i;
	}

	DEBUG("AEAD is %s\n", sodium_aead_str[aead]);


	char const * cvmrdma_disable_cache_str = getenv("CVMRDMA_DISABLE_CACHE");
	if(cvmrdma_disable_cache_str != NULL)
	{
		OPT_cvmrdma_disable_cache = true;
	}
	fprintf(stderr, "CVMRDMA_DISABLE_CACHE set to %d\n", OPT_cvmrdma_disable_cache);

	char const* cvmrdma_chunk_bytes_str = getenv("CVMRDMA_CHUNK_BYTES");
	if(cvmrdma_chunk_bytes_str != NULL)
	{
		OPT_cvmrdma_chunk_bytes = atoi(cvmrdma_chunk_bytes_str);
		if (OPT_cvmrdma_chunk_bytes == 0)
		{
			fprintf(stderr, "ERROR: CVMRDMA_CHUNK_BYTES must be a positive integer\n");
			goto err;
		}
	} 

	// the size of our ring buffer is limited to 512 MiB
	// we need to claculate how many entries we can fit into the ring buffer depending on the chunk size
	cvmrdma_ring_entries = CVMRDMA_RING_BUFFER_SIZE / (OPT_cvmrdma_chunk_bytes);
	fprintf(stderr, "CVMRDMA_CHUNK_BYTES set to %lu, ring entries set to %lu\n", OPT_cvmrdma_chunk_bytes, cvmrdma_ring_entries);
	cvmrdma_ack_threshold = DIVIDE_ROUND_UP(cvmrdma_ring_entries, 2);

	char const * cvmrdma_nonce_cache_size_str = getenv("CVMRDMA_NONCE_CACHE_SIZE");
	if(cvmrdma_nonce_cache_size_str != NULL)
	{
		OPT_cvmrdma_nonce_cache_size = atoi(cvmrdma_nonce_cache_size_str);
		if (OPT_cvmrdma_nonce_cache_size == 0)
		{
			fprintf(stderr, "ERROR: CVMRDMA_NONCE_CACHE_SIZE must be a positive integer\n");
			goto err;
		}
	}
	fprintf(stderr, "CVMRDMA_NONCE_CACHE_SIZE set to %lu\n", OPT_cvmrdma_nonce_cache_size);

	char const * cvmrdma_disable_payload_nonce_pregeneration_str = getenv("CVMRDMA_DISABLE_PAYLOAD_NONCE_PREGENERATION");
	if(cvmrdma_disable_payload_nonce_pregeneration_str != NULL)
	{
		OPT_cvmrdma_disable_payload_nonce_pregeneration = true;
	}
	fprintf(stderr, "CVMRDMA_DISABLE_PAYLOAD_NONCE_PREGENERATION set to %d\n", OPT_cvmrdma_disable_payload_nonce_pregeneration);

	char const * cvmrdma_enable_decrypt_skip_str = getenv("CVMRDMA_ENABLE_DECRYPT_SKIP");
	if(cvmrdma_enable_decrypt_skip_str != NULL)
	{
		OPT_cvmrdma_enable_decrypt_skip = true;
	}
	fprintf(stderr, "CVMRDMA_ENABLE_DECRYPT_SKIP set to %d\n", OPT_cvmrdma_enable_decrypt_skip);

	/* Initialize cryptographic APIs (can be called multiple times) */
	if (sodium_init() < 0) {
		fprintf(stderr, "ERROR: Failed to initialize cryptographic APIs.\n");
		goto err;
	}

    /* Allocate provider context */
    struct verbs_context * prov_verbs_context = vdev->cvmrdma.alloc_context(ibdev, cmd_fd, private_data);
    if(! prov_verbs_context) {
		goto err;
    }
	struct ibv_context * prov_ctx = &prov_verbs_context->context;

	struct cvmrdma_context * cvmrdma_ctx = cvmrdma_get_ctx(prov_ctx);

	/* Save the provider ops */
	memcpy(&cvmrdma_ctx->prov_ops, get_ops(prov_ctx), sizeof(struct verbs_context_ops));

	/* and replace the ops in the user facing context with the CVMRDMA wrapper ops */
	verbs_set_ops(prov_verbs_context, &verbs_dummy_ops);
	verbs_set_ops(prov_verbs_context, &cvmrdma_ops);

	print_process_name_debug();

	bool expected = false;
	bool first = atomic_compare_exchange_strong(&tcp_server_running, &expected, true);
	if(first)
	{
		/* Start a new thread with a capnp event loop running the RPC server */
		cvmrdma_ctx->network_evloop = create_capnp_event_loop(local_bind_addr, &cvmrdma_ctx->network_server);
		
		// wait for server to be up
		DEBUG("Waiting for server to be up\n");
		while(!cvmrdma_ctx->network_server)
		{
			usleep(1000);
		}
		evloop_server = cvmrdma_ctx->network_evloop;
		tcp_server = cvmrdma_ctx->network_server;
		DEBUG("Server started on %s\n", local_bind_addr);
		assert(evloop_server != NULL);
		assert(tcp_server != NULL);
	} else {
		DEBUG("Server already running on %s\n", local_bind_addr);
		assert(evloop_server != NULL);
		assert(tcp_server != NULL);
		cvmrdma_ctx->network_evloop = evloop_server;
		cvmrdma_ctx->network_server = tcp_server;
	}

	// TODO: qp_metadata_manager_destroy(server->qp_metadata_manager); when destroying the context
	cvmrdma_ctx->qp_metadata_manager = qp_metadata_manager_alloc();

#ifndef DUMMY_SYSTEM_HEAP
	int system_heap_fd = open_system_heap();
	if(system_heap_fd < 0)
	{
		ret = (int)errno;
		fprintf(stderr, "Failed to open system DMA heap (%d)\n", ret);
		exit(1);
	}
	cvmrdma_ctx->dma_heap = system_heap_fd;
#endif

	cvmrdma_ctx->aead = aead;

	return prov_verbs_context;

// uninit_and_free:
// 	context->cvmrdma.free_context(&context->context);
err:
	return NULL;
}

struct verbs_device_ops cvmrdma_device_ops = {
    .alloc_context = cvmrdma_alloc_context,
};
