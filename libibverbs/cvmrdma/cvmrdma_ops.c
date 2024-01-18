#include "cvmrdma_ops.h"

#include <stdio.h>
#include <stdlib.h>

#include <arpa/inet.h> /* ntohl */
#include <unistd.h>

#include <sys/mman.h>
#include <sys/ioctl.h>

#include <linux/dma-buf.h>

#include <sodium.h>

/* For get_ops */
#include "../ibverbs.h"


#include "cvmrdma.h"
#include "cvmrdma_objects.h"
#include "bounce_metadata.h"
#include "cvmrdma_cq_server.h"
#include "cvmrdma_network_client_priv.h"
#include "cvmrdma_network_server_priv.h"
#include "cvmrdma_network_evloop_priv.h"
#include "notification_queue.h"
#include "recv_metadata.h"
#include "wc_queue.h"
#include "dma_heap.h"

#include "debug.h"

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

	pd->provider_pd = prov_pd;
    pd->bounce_metadata = bounce_metadata;
	pd->mr_id_ctr = 0;

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

	cq->user_cq.cq.context = ibcontext;
	cq->user_cq.cq.cqe = cqe;
	pthread_mutex_init(&cq->user_cq.cq.mutex, NULL);

	cq->wc_queue = alloc_wc_queue();

	return &cq->user_cq.cq;
}


static int attach_qp_to_cq_server(struct cvmrdma_context * ctxt, struct cvmrdma_qp * qp)
{
	/* Add this QP to the CQ server. This gives us one end of the notification
	   channel ; the RPC server will push notifications there.
	 */
	int err;

	uint32_t qpn = qp->user_qp.qp.qp_num;
	DEBUG("Create notification queue ->CQ server for QPN %" PRIx32 "\n", qpn);
	qp->notification_queue = alloc_notification_queue();


	err = cq_server_register_qp(&ctxt->cvmrdma_cq_server, qp);
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
		qp->notification_queue,
		qpn,
		&qp->send_lines_full,
		&qp->send_lines_full_lock,
		&qp->send_lines_avail,
		qp->rx_key,
		&qp->remote_ring_rkey,
		&qp->remote_params_set,
		&qp->remote_params_lock,
		&qp->remote_params_set_cv
	);

	return 0;
}

/* Allocate a zero-indexed publicly available mr*/
static struct ibv_mr * alloc_public_mr(struct cvmrdma_context * ctxt, struct cvmrdma_pd *pd, size_t size, int * dmabuf_fd, void ** ringbuf_va)
{
	int ret;
	int dma_buf;

#ifndef DUMMY_SYSTEM_HEAP
	dma_buf = alloc_dma_buf(ctxt->dma_heap, size);
	if(dma_buf < 0)
	{
		fprintf(stderr, "Failed to allocate dma-buf\n");
		goto err;
	}


	/* WARN: MUST attach+dma_map to dma-buf before mmap !! In our hacky implementation, the actual
	   allocation is done in dma_map */
	int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_RELAXED_ORDERING;
	size_t offt = 0;
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

	DEBUG("public mr: dmabuf %d va %p len %zu\n", *dmabuf_fd, va, size);

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

static struct ibv_qp *cvmrdma_create_qp(struct ibv_pd * ibpd,
				struct ibv_qp_init_attr *attr)
{
	struct cvmrdma_pd * pd = (struct cvmrdma_pd *)ibpd;
	struct ibv_context * prov_ctxt = pd->user_pd.context;
    struct cvmrdma_context * ctxt = cvmrdma_get_ctx(prov_ctxt);
	int err;

	struct cvmrdma_qp * qp = calloc(1, sizeof(struct cvmrdma_qp));

	/* Create the provider QP */
	struct ibv_qp_init_attr_ex attr_ex = {0};
	attr_ex.sq_sig_all = 1;
	/* Direct send and recv WCs to the CQ server */
	attr_ex.send_cq = ctxt->cvmrdma_cq_server.prov_cq;
	attr_ex.recv_cq = ctxt->cvmrdma_cq_server.prov_cq;

	assert(attr->qp_type == IBV_QPT_RC);
	attr_ex.qp_type = IBV_QPT_RC;

	/* Keep the user set QP capacity */
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
        goto err;
    }

	qp->provider_qp = ibqp;

	/* Initialize user visible fields */
	qp->user_qp.qp.context = prov_ctxt;
	qp->user_qp.qp.qp_context = attr->qp_context;
	qp->user_qp.qp.pd = ibpd;
	qp->user_qp.qp.qp_num = ibqp->qp_num;
	qp->user_qp.qp.qp_type = ibqp->qp_type;
	/* user-qp send/recv CQ are the cvmrdma-CQ where the CQ server will push notifications */
	qp->user_qp.qp.recv_cq = attr->recv_cq;
	qp->user_qp.qp.send_cq = attr->send_cq;


	QpRecvMetadata * recv_metadata = alloc_qp_recv_metadata();
	if(! recv_metadata)
	{
		fprintf(stderr, "Failed to allocated recv metadata storage.\n");
		goto free_qp;
	}
	qp->recv_metadata = recv_metadata;
	qp->cur_sge_idx = 0;
	qp->cur_offt_in_sge = 0;
	qp->wr_bytes_rcved = 0;

	QpSendMetadata * send_metadata = alloc_qp_send_metadata();
	if(! send_metadata)
	{
		fprintf(stderr, "Failed to allocated send metadata storage.\n");
		goto free_recv_metadata;
	}
	qp->send_metadata = send_metadata;
	qp->wr_chunks_sent = 0;


	/* Allocate the bounce regions and ring buffer for this QP */
	void * bounce_private_mem = mmap(0, CVMRDMA_PRIV_BOUNCE_TOTAL_BYTES, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if (bounce_private_mem == MAP_FAILED) {
		err = errno;
		fprintf(stderr, "Failed to mmap bounce buffer %d\n", err);
		goto free_send_metadata;
	}
	qp->bounce_private_mem = bounce_private_mem;


    /* To perform the bounce, we'll need to read and write the ring buffers */
	size_t all_rings_bytes = __CVMRDMA_RING_NR * CVMRDMA_RING_TOTAL_BYTES;

	int dmabuf_fd = -1;
	void * ringbuf_va = NULL;
	struct ibv_mr * ringbuf_mr = alloc_public_mr(ctxt, pd, all_rings_bytes, &dmabuf_fd, &ringbuf_va);
    if(! ringbuf_mr)
    {
        goto free_bounce_private;
    }

	qp->ringbuf_mr = ringbuf_mr;
	qp->ringbuf_dmabuf_fd = dmabuf_fd;
	qp->ringbuf_va = ringbuf_va;

	qp->next_send_slot = 0;
	qp->next_recv_slot = 0;
	qp->send_lines_full = 0;
	qp->last_line_acked = CVMRDMA_RING_ENTRIES;

	qp->user_sq_sig_all = attr->sq_sig_all;

	/* Initialize the send mutex */
	err = pthread_mutex_init(&qp->cvmrdma_send_lock, NULL);
	if(err)
	{
		fprintf(stderr, "Failed to initialize the CVMRDMA recv mutex %d\n", err);
		goto dereg_ring_mr;
	}


	err = attach_qp_to_cq_server(ctxt, qp);
	if(err)
	{
		fprintf(stderr, "Failed to attach QP %" PRIx32 " to its recv cq (%d)\n", ibqp->qp_num, err);
		goto free_send_lock;
	}

	/* We can only get the capability to the remote QP once we know its QPN, this will be done
	   in modify_qp. We'll also get the rkey to its ring */
	qp->remote_qnc = NULL;
	qp->remote_ring_rkey = 0;
	memset(qp->rx_key, 0, sizeof(qp->rx_key));
	qp->remote_params_set = false;
	pthread_mutex_init(&qp->remote_params_lock, NULL);
	pthread_cond_init(&qp->remote_params_set_cv, NULL);


	/* Generate a random tx key */
	crypto_aead_xchacha20poly1305_ietf_keygen((unsigned char *)qp->tx_key);


	return &qp->user_qp.qp;

free_send_lock:
	pthread_mutex_destroy(&qp->cvmrdma_send_lock);
dereg_ring_mr:
	ctxt->prov_ops.dereg_mr(verbs_get_mr(ringbuf_mr));
	close(dmabuf_fd);
free_bounce_private:
	munmap(bounce_private_mem, CVMRDMA_PRIV_BOUNCE_TOTAL_BYTES);
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

		/* Start by connecting to the server if not already connected */
		if(! ctxt->network_client_connection)
		{
			ctxt->network_client_connection = new_client_rpc_session(ctxt->network_evloop, ctxt->remote_addr);
			ctxt->network_client_cap = get_cvmrdma_client(ctxt->network_evloop, ctxt->network_client_connection);
		}


		switch(ibqp->qp_type)
		{
			case IBV_QPT_RC:
			case IBV_QPT_UC: {
				assert(attr_mask & IBV_QP_DEST_QPN);
				uint32_t dest_qpn = attr->dest_qp_num;

				DEBUG("Obtain notification channel cap from remote for its QP with QPN 0x%" PRIx32 "\n", dest_qpn);

				/* Get the qp notifier capability from the remote over the network and share the Tx key with the remote.
				    */
				qp->remote_qnc = get_qp_notification_client(
					ctxt->network_evloop, ctxt->network_client_cap, dest_qpn 
				);

				/* Blocking: set the tx_key and the qp's ring rkey. Block because recv cannot proceed until
				   remote has our rkey */
				   set_remote_params(ctxt->network_evloop, qp->remote_qnc, qp->tx_key, qp->ringbuf_mr->rkey);
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
			for(int i = 0 ; i < CVMRDMA_RING_ENTRIES ; i++)
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
			DEBUG("INIT->RECV: wait for rx key...\n");
			pthread_mutex_lock(&qp->remote_params_lock);
			while(! qp->remote_params_set)
			{
				pthread_cond_wait(&qp->remote_params_set_cv, &qp->remote_params_lock);
			}
			pthread_mutex_unlock(&qp->remote_params_lock);

	}

    return 0;

err:
    return ret;
}


/* Poll the WCs that are passed by the CQ server. */
static int cvmrdma_poll_cq(struct ibv_cq * ibcq, int num_entries, struct ibv_wc *wc)
{
	struct cvmrdma_cq * cq = (struct cvmrdma_cq *)ibcq;
	return wcq_pop_into(cq->wc_queue, wc, num_entries);
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


static int post_send_do_bounce(struct cvmrdma_qp * qp, struct ibv_send_wr *wrs, struct ibv_send_wr **bad_wr, bool write_for_read)
{
	struct cvmrdma_context * ctxt = cvmrdma_get_ctx(qp->user_qp.qp.context);
	struct cvmrdma_pd * pd = (struct cvmrdma_pd *)qp->user_qp.qp.pd;

	int err;

	if(qp->user_qp.qp.qp_type != IBV_QPT_RC)
	{
		fprintf(stderr, "ERROR: for now CVMRDMA only supports RC\n");
		return EINVAL;
	}

	struct ibv_qp * prov_qp = qp->provider_qp;
	struct ibv_qp_ex * prov_qpx = ibv_qp_to_qp_ex(prov_qp);

	uint32_t local_ring_lkey = qp->ringbuf_mr->lkey;
	uint32_t remote_ring_rkey = qp->remote_ring_rkey;

	for(struct ibv_send_wr * this_wr = wrs ; this_wr != NULL ; this_wr = this_wr->next)
	{
		enum ibv_wr_opcode this_op = this_wr->opcode;
		uint32_t this_imm = this_wr->imm_data;


		bool is_send = (this_op == IBV_WR_SEND) || (this_op == IBV_WR_SEND_WITH_IMM);
		bool is_write = (this_op == IBV_WR_RDMA_WRITE) || (this_op == IBV_WR_RDMA_WRITE_WITH_IMM);

		if(! (is_send || is_write))
		{
			fprintf(stderr, "ERROR: unsupported infiniband operation\n");
			*bad_wr = this_wr;
			return EINVAL;
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
		uint64_t const * tx_key = qp->tx_key;
		unsigned char * gather = (unsigned char *)qp->bounce_private_mem + CVMRDMA_PRIV_CHUNK_SG_OUT * CVMRDMA_RING_SLOT_BYTES;
		unsigned char * encrypted = (unsigned char *)qp->bounce_private_mem + CVMRDMA_PRIV_CHUNK_ENC_OUT * CVMRDMA_RING_SLOT_BYTES;

		unsigned char * ringbuf_out = qp->ringbuf_va + CVMRDMA_RING_SEND * CVMRDMA_RING_TOTAL_BYTES;;

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


		size_t total_bytes = 0;
		for(size_t i = 0 ; i < nsge ; i++)
			total_bytes += sgl[i].length;
		size_t total_chunks = DIVIDE_ROUND_UP(total_bytes, CVMRDMA_PAYLOAD_BYTES);
		/* XXX: pass the number of chunks to the CQ_server to know when to emit a send WC for user. Do it 
		   BEFORE we start sending, so that CQ server has this informatin when WCs start to arrive  */
		qp_send_metadata_push(qp->send_metadata, this_wr->wr_id, total_bytes, total_chunks, write_for_read);

		while(cur_sge_idx < nsge)
		{
			struct ibv_sge * cur_sge = &sgl[cur_sge_idx];

			/* If this is the last chunk, we'll need to notify that it's a WC generating chunk (if op != WRITE) */
			bool last_chunk = ((cur_sge_idx + 1) == nsge) && ((cur_sge_offt + CVMRDMA_PAYLOAD_BYTES) >= cur_sge->length);


			/* Store the nonce word aligned to make serialization easier
			Store on the heap  */
			uint64_t nonce[crypto_aead_xchacha20poly1305_ietf_NPUB_WORDS];


			/* Generate the nonce to use for the encryption */
			randombytes_buf(nonce, sizeof(nonce));

			/***** NOTIFY ******/
			/* The notification happens off-thread, and will happen in parallel to the GATHER, ENCRYPT, COPY and SEND */
			TIMING("chunk_notify");
			{
				/* Make a copy of the nonce on the heap to pass ownership to another thread */
				uint64_t * nonce_for_notif = malloc(sizeof(nonce));
				memcpy(nonce_for_notif, &nonce, sizeof(nonce));

				if(is_write)
				{
					/* This chunk will be directly written at the destination */
					remote_qp_notify_chunk(
						ctxt->network_evloop,
						qp->remote_qnc,
						this_op,
						rkey,
						remote_addr + bytes_sent,
						nonce_for_notif,
						last_chunk,
						this_imm
					);
				}
				else
				{
					/* All these chunks will be written to the same RR, until last_chunk is reached */
					remote_qp_notify_chunk(
						ctxt->network_evloop,
						qp->remote_qnc,
						this_op,
						0,
						0,
						nonce_for_notif,
						last_chunk,
						this_imm
					);

				}
				/* on-heap nonce ownership is passed to capnp client */
				nonce_for_notif = NULL;
			}

			TIMING("gec_t0");
			/***** GATHER ******/

			/* bytes already copied in the chunk */
			size_t chunk_bytes = 0;


			unsigned char * ptxt = NULL;

			struct cvmrdma_mr * cur_mr = get_bounce_metadata_for_lkey(pd->bounce_metadata, cur_sge->lkey);

			/* We can avoid the GATHER step if current (sge, offt) gives a contiguous chunk */
			if(cur_sge_offt + CVMRDMA_PAYLOAD_BYTES <= cur_sge->length)
			{
				/* offt in MR*/
				uint64_t offt = (cur_sge->addr - cur_mr->hca_va) + cur_sge_offt;
				/* location in memory */
				ptxt = cur_mr->user_mr.ibv_mr.addr + offt;

				cur_sge_offt += CVMRDMA_PAYLOAD_BYTES;
				chunk_bytes = CVMRDMA_PAYLOAD_BYTES;

				if(cur_sge_offt >= cur_sge->length)
				{
					cur_sge_idx++;
					cur_sge_offt = 0;
				}
			}
			/* Or if this is the last sge */
			else if(cur_sge_idx + 1 >= nsge)
			{
				/* bytes available in this sge */
				size_t available = cur_sge->length - cur_sge_offt;

				/* offt in MR */
				uint64_t offt = (cur_sge->addr - cur_mr->hca_va) + cur_sge_offt;
				/* location in memory */
				ptxt = cur_mr->user_mr.ibv_mr.addr + offt;

				chunk_bytes = available;

				cur_sge_offt = 0;
				cur_sge_idx++;
			}
			/* Else we need to gather bytes from different sges in a contiguous block */
			else
			{
				while((chunk_bytes < CVMRDMA_PAYLOAD_BYTES) && (cur_sge_idx < nsge))
				{
					cur_sge = &sgl[cur_sge_idx];
					cur_mr = get_bounce_metadata_for_lkey(pd->bounce_metadata, cur_sge->lkey);

					unsigned char * mr_addr = cur_mr->user_mr.ibv_mr.addr;

					/* bytes needed to fill up chunk */
					size_t remaining = CVMRDMA_PAYLOAD_BYTES - chunk_bytes;
					/* bytes still available in this sge */
					size_t available = cur_sge->length - cur_sge_offt;

					/* offt in MR */
					uint64_t offt = (cur_sge->addr - cur_mr->hca_va) + cur_sge_offt;


					if(remaining < available)
					{
						memcpy(gather + chunk_bytes, mr_addr + offt, remaining);
						chunk_bytes += remaining;
						cur_sge_offt += remaining;
					}
					else
					{
						memcpy(gather + chunk_bytes, mr_addr + offt, available);
						chunk_bytes += available;
						/* move to next sge */
						cur_sge_idx++;
						cur_sge_offt = 0;
					}
				}

				ptxt = gather;
			}
			/**** ENCRYPT *****/

			DEBUG("ptxt %x %x %x\n", ptxt[0], ptxt[1], ptxt[2]);
			DEBUG("plen %zu\n", chunk_bytes);
			DEBUG("nonce %lx %lx\n", nonce[0], nonce[1]);

			unsigned long long clen = CVMRDMA_RING_SLOT_BYTES;
			err = crypto_aead_xchacha20poly1305_ietf_encrypt(
				encrypted,
				&clen,
				ptxt, chunk_bytes,
				NULL, 0,
				NULL,
				(unsigned char *)nonce,
				(unsigned char const *)tx_key
			);
			if(err)
			{
				fprintf(stderr, "Failed to encrypt chunk!\n");
				exit(1);
			}


			DEBUG("encrypted %x %x %x ...\n", encrypted[0], encrypted[1], encrypted[2]);
			DEBUG("clen %llu\n", clen);

			/* The ciphertext has length (plen + maclen) */
			assert(clen == (chunk_bytes + crypto_aead_xchacha20poly1305_ietf_ABYTES));


			/***** COPY ******/
			/* Copy ciphertext (with MAC) from private memory into a ring buffer slot */

			/* Get a slot. If all the slots are full, wait for a slot to be acked */
			int slot_id = 0;
			{
				/* LOCK */
				pthread_mutex_lock(&qp->send_lines_full_lock);

				while(qp->send_lines_full >= CVMRDMA_RING_ENTRIES)
				{
					TIMING("wait_free_sendlines!");
					pthread_cond_wait(&qp->send_lines_avail, &qp->send_lines_full_lock);
				}

				slot_id = qp->next_send_slot;

				qp->next_send_slot = (slot_id + 1) % CVMRDMA_RING_ENTRIES;
				qp->send_lines_full++;

				pthread_mutex_unlock(&qp->send_lines_full_lock);
				/*  UNLOCK */
			}

			unsigned char * slot = ringbuf_out + slot_id * CVMRDMA_RING_SLOT_BYTES;
			memcpy(slot, encrypted, clen);


			TIMING("gec_t1");

			/****** POST SEND WWI ******/
			/* Synchronize the ring buffer with the remote copy using a WWI.
			   Rings MR are 0-indexed. The send ring starts at 0, the recv 
			   ring starts at CVMRDMA_RING_ENTRIES * CVMRDMA_CHUNK_SIZE.
			*/

			ibv_wr_start(prov_qpx);

			prov_qpx->wr_id = 0;
            prov_qpx->wr_flags = IBV_SEND_SIGNALED; /* XXX: in the future could avoid signalling all !*/
			ibv_wr_rdma_write_imm(prov_qpx,
				remote_ring_rkey,
				(CVMRDMA_RING_ENTRIES + slot_id) * CVMRDMA_RING_SLOT_BYTES,
				0	
			);
			ibv_wr_set_sge(prov_qpx,
				local_ring_lkey,
				slot_id * CVMRDMA_RING_SLOT_BYTES,
				clen /* includes mac */
			);

			TIMING("send_chunk_wr");
			err = ibv_wr_complete(prov_qpx);
			if(err)
			{
				fprintf(stderr, "Failed to post WWI for a chunk (%d)\n", err);
			}

			bytes_sent += chunk_bytes;
			chunks_in_wr++;
		}


		assert(bytes_sent == total_bytes);
		assert(chunks_in_wr == total_chunks);
	}

	return 0;
}


static int cvmrdma_post_send(struct ibv_qp *ibqp, struct ibv_send_wr *wr,
		     struct ibv_send_wr **bad_wr)
{
    struct cvmrdma_qp * qp = (struct cvmrdma_qp *)ibqp;
	int ret = 0;

	TIMING("post_send_t0");

	/* We need to lock so that the notifications and the send actually
	   match (i.e. no interleaved send)*/

	pthread_mutex_lock(&qp->cvmrdma_send_lock);


    /*
	CVMRDMA: do the bounce and modify the WRs to use the bounce buffers
    
    on WRITE
	- find the local (base_addr, bounce_addr) for lkey
	- xchachapoly1305 the plaintext region to the bounce buffer,
	  keeping the same offset
	- change WRITE into WRITE_WITH_IMM to notify the remote.
	- RPC send the dest hca_addr, rkey, imm, MAC, nonce
	*/

	ret = post_send_do_bounce(qp, wr, bad_wr, false);
	if(ret < 0)
	{
		fprintf(stderr, "Failed to bounce before send!\n");
		goto ret;
	}

	TIMING("post_send_t1");


ret:
	pthread_mutex_unlock(&qp->cvmrdma_send_lock);
    return ret;

/* XXX: do error handling here, if the send fails then things are
   a bit tricky: the remote has already been notified of the incoming
   WRITE, but the WRITE will not happen ... The remote will wait forever.

    TODO:
    1. either send the capnp notif only in case of success
    2. or send a notification cancelling the WRITE

   The bounce will be overwritten later and do not need to be rolled back
   */
}

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



static struct ibv_mr *cvmrdma_reg_mr(struct ibv_pd *ibpd, void *addr, size_t length,
        uint64_t hca_va, int access)
{
	struct cvmrdma_pd * pd = (struct cvmrdma_pd *)ibpd;
	struct ibv_context * ibctxt = pd->user_pd.context;
	struct cvmrdma_context * ctxt = cvmrdma_get_ctx(ibctxt);

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

	struct cvmrdma_mr * mr = calloc(1, sizeof(struct cvmrdma_mr));
	/*
		The CVMRDMA MR exposes ibv_mr fields to the user containing data (eg the address)
		related to the user region, whereas the buffer actually registered as an MR is kept
		in a private field.
	*/

	uint32_t mr_id = pd->mr_id_ctr++;

	/* Expose some information to the user */
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


struct verbs_context_ops cvmrdma_ops = {
    .alloc_pd = cvmrdma_alloc_pd,
    .create_cq = cvmrdma_create_cq,
    .create_qp = cvmrdma_create_qp,
    // .dealloc_pd = cvmrdma_dealloc_pd,
    // .dereg_mr = cvmrdma_dereg_mr,
    // .destroy_cq = cvmrdma_destroy_cq,
    // .destroy_qp = cvmrdma_destroy_qp,
    // .free_context = cvmrdma_free_context,
    .modify_qp = cvmrdma_modify_qp,
    .poll_cq = cvmrdma_poll_cq,
    .post_recv = cvmrdma_post_recv,
    .post_send = cvmrdma_post_send,
	.query_device_ex = cvmrdma_query_device_ex,
	.query_port = cvmrdma_query_port,
    // .reg_dmabuf_mr = cvmrdma_reg_dmabuf_mr,
	// .req_notify_cq = cvmrdma_req_notify_cq
    .reg_mr = cvmrdma_reg_mr
};



static struct verbs_context *cvmrdma_alloc_context(struct ibv_device *ibdev,
						  int cmd_fd,
						  void *private_data)
{
    struct verbs_device * vdev = verbs_get_device(ibdev);
	int ret;

	DEBUG("cvmrdma_alloc_context\n");

	/* eg 192.4.0.3:3456 */
	char const * remote_addr = getenv("CVMRDMA_REMOTE_ADDR");
	if(! remote_addr)
	{
		fprintf(stderr, "ERROR: No environment variable CVMRDMA_REMOTE_ADDR.\n");
		goto err;
	}

	/* eg *:3456 */
	char const * local_bind_addr = getenv("CVMRDMA_BIND_ADDR");
	if(! local_bind_addr)
	{
		fprintf(stderr, "ERROR: No environment variable CVMRDMA_BIND_ADDR.\n");
		goto err;
	}

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



	DEBUG("Start IP based server on %s\n", local_bind_addr);
	/* Start a new thread with a capnp event loop running the RPC server */
	cvmrdma_ctx->network_evloop = create_capnp_event_loop(local_bind_addr, &cvmrdma_ctx->network_server);
	

	/* Open a session to the remote using the same event loop */
	DEBUG("Remote's IP is %s\n", remote_addr);
	/* Only connect to the remote's RPC server when we know that the server is up */
	cvmrdma_ctx->remote_addr = remote_addr;
	cvmrdma_ctx->network_client_connection = NULL;
	cvmrdma_ctx->network_client_cap = NULL;

	/* Get the root capability to the remote's RPC's API */
	
	/* Create a single CQ to get the send and recv WC for the chunks */
	struct ibv_cq * ibcq = cvmrdma_ctx->prov_ops.create_cq(prov_ctx, 16384, NULL, 0);
	if(! ibcq)
	{
		fprintf(stderr, "failed to create backend CQ for the CQ server\n");
		exit(1);
	}
	/* see ibv_create_cq */
	verbs_init_cq(ibcq, prov_ctx, NULL, NULL);


	/* Initialize the CQ polling server */
	cq_server_context_init(&cvmrdma_ctx->cvmrdma_cq_server, cvmrdma_ctx, ibcq);

	/* And run the server loop in another thread */
	ret = cq_server_run(&cvmrdma_ctx->cvmrdma_cq_server, &cvmrdma_ctx->cvmrdma_cq_server_thread);
	if(ret)
	{
		fprintf(stderr, "Failed to start CQ server %d\n", ret);
		exit(1);
	}

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

	return prov_verbs_context;

// uninit_and_free:
// 	context->cvmrdma.free_context(&context->context);
err:
	return NULL;
}

struct verbs_device_ops cvmrdma_device_ops = {
    .alloc_context = cvmrdma_alloc_context,
};
