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


#include "cvmrdma.h"
#include "cvmrdma_objects.h"
#include "bounce_metadata.h"
#include "cvmrdma_cq_server.h"
#include "cvmrdma_network_client_priv.h"
#include "cvmrdma_network_server_priv.h"
#include "cvmrdma_network_evloop_priv.h"
#include "cvmrdma_control_server.h"
#include "recv_metadata.h"
#include "wc_queue.h"
#include "dma_heap.h"
#include "cvmrdma_cache.h"

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
	pd->user_pd.handle = prov_pd->handle;

	pd->provider_pd = prov_pd;
    pd->bounce_metadata = bounce_metadata;
	pd->mr_id_ctr = 0;

	for (uint64_t i = 0; i < 10; i++) {
		if(ctxt->pd_list[i] == NULL) {
			ctxt->pd_list[i] = pd;
			break;
		} else if (i == 9) {
			fprintf(stderr, "Failed to allocate PD, no space in pd_list\n");
			exit(1);
		}
	}

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

	cq->user_cq.cq.context = ibcontext;
	cq->user_cq.cq.cqe = cqe;
	pthread_mutex_init(&cq->user_cq.cq.mutex, NULL);

	cq->wc_queue = alloc_wc_queue();

	if(channel) {
		assert(channel->fd >= 0);
		cq->comp_channel_event_fd = channel->fd;
	}

	return &cq->user_cq.cq;
}


static int attach_qp_to_cq_server(struct cvmrdma_context * ctxt, struct cvmrdma_qp * qp)
{
	/* Add this QP to the CQ server. This gives us one end of the notification
	   channel ; the RPC server will push notifications there.
	 */
	int err;

	uint32_t qpn = qp->user_qp.qp.qp_num;


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
		qpn,
		&qp->send_lines_full,
		&qp->send_lines_full_lock,
		&qp->send_lines_avail,
		qp->rx_key,
		&qp->remote_ring_rkey,
		&qp->remote_control_rkey,
		&qp->dest_control_qp_num,
		&qp->control_remote_psn,
		&qp->remote_params_set,
		&qp->remote_params_lock,
		&qp->remote_params_set_cv
	);

	return 0;
}

/* Allocate a zero-indexed publicly available mr*/
static struct ibv_mr * alloc_public_mr(struct cvmrdma_context * ctxt, struct cvmrdma_pd *pd, size_t size, int * dmabuf_fd, void ** ringbuf_va)
{
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
        goto err;
    }

	qp->provider_qp = ibqp;

	/* Create the control Completion Queue */
	struct ibv_cq * control_ibcq = ctxt->prov_ops.create_cq(prov_ctxt, 16384, NULL, 0);
	if(! control_ibcq)
	{
		fprintf(stderr, "failed to create backend CQ for the control CQ\n");
		exit(1);
	}
	/* see ibv_create_cq */
	verbs_init_cq(control_ibcq, prov_ctxt, NULL, NULL);
	qp->control_cq = control_ibcq;

	


	/* Create the control Queue Pair */
	struct ibv_qp_init_attr_ex control_attr_ex = {0};
	control_attr_ex.sq_sig_all = 1;
	control_attr_ex.send_cq = qp->control_cq;
	control_attr_ex.recv_cq = qp->control_cq;

	control_attr_ex.qp_type = IBV_QPT_RC;

	control_attr_ex.cap.max_recv_sge = 16;
	control_attr_ex.cap.max_recv_wr = CVMRDMA_RING_ENTRIES;
	control_attr_ex.cap.max_send_sge = 16;
	control_attr_ex.cap.max_send_wr = CVMRDMA_RING_ENTRIES;
	control_attr_ex.cap.max_inline_data = 0; /* we're not using inline */

	control_attr_ex.pd = pd->provider_pd;
	control_attr_ex.send_ops_flags = IBV_QP_EX_WITH_RDMA_WRITE_WITH_IMM;
	control_attr_ex.comp_mask = IBV_QP_INIT_ATTR_PD | IBV_QP_INIT_ATTR_SEND_OPS_FLAGS;

    struct ibv_qp * control_ibqp = ctxt->prov_ops.create_qp_ex(prov_ctxt, &control_attr_ex);
    if(! control_ibqp)
    {
        goto err;
    }

	qp->control_qp = control_ibqp;

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

	QpSendMetadata * send_metadata = alloc_qp_send_metadata();
	if(! send_metadata)
	{
		fprintf(stderr, "Failed to allocated send metadata storage.\n");
		goto free_recv_metadata;
	}
	qp->send_metadata = send_metadata;
	qp->wr_chunks_sent = 0;


	/* Allocate the bounce regions and ring buffer for this QP */
	void * bounce_private_mem = mmap(0, cvmrdma_priv_bounce_total_bytes(ctxt->aead), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if (bounce_private_mem == MAP_FAILED) {
		err = errno;
		fprintf(stderr, "Failed to mmap bounce buffer %d\n", err);
		goto free_send_metadata;
	}
	qp->bounce_private_mem = bounce_private_mem;


    /* To perform the bounce, we'll need to read and write the ring buffers */
	size_t all_rings_bytes = cvmrdma_ring_slot_bytes(ctxt->aead) * CVMRDMA_RING_ENTRIES;

	int dmabuf_fd = -1;
	void * ringbuf_va = NULL;
	struct ibv_mr * ringbuf_mr = alloc_public_mr(ctxt, pd, all_rings_bytes, &dmabuf_fd, &ringbuf_va);
    if(! ringbuf_mr)
    {
        goto free_bounce_private;
    }

	DEBUG("[CREATE_QP] ringbuf rkey %" PRIx32 ", ringbuf lkey %" PRIx32 "\n", ringbuf_mr->rkey, ringbuf_mr->lkey);

	qp->ringbuf_mr = ringbuf_mr;
	qp->ringbuf_dmabuf_fd = dmabuf_fd;
	qp->ringbuf_va = ringbuf_va;

	qp->next_send_slot = 0;
	qp->next_recv_slot = 0;
	qp->send_lines_full = 0;

	qp->user_sq_sig_all = attr->sq_sig_all;

	/* Allocate rungbuffer where the other side writes the ACKs to*/
	size_t control_rings_bytes = PAGE_SIZE;

	int control_dmabuf_fd = -1;
	void * control_buf_va = NULL;
	struct ibv_mr * control_buf_mr = alloc_public_mr(ctxt, pd, control_rings_bytes, &control_dmabuf_fd, &control_buf_va);
    if(! control_buf_mr)
    {
        goto free_bounce_private;
    }

	qp->control_buf_mr = control_buf_mr;
	qp->control_buf_va = control_buf_va;


	/* Initialize the mutex */
	err = pthread_mutex_init(&qp->cvmrdma_send_lock, NULL);
	if(err)
	{
		fprintf(stderr, "Failed to initialize the CVMRDMA cvmrdma_send_lock mutex %d\n", err);
		goto dereg_ring_mr;
	}
	err = pthread_mutex_init(&qp->send_lines_full_lock, NULL);
	if(err)
	{
		fprintf(stderr, "Failed to initialize the CVMRDMA send_lines_full_lock mutex %d\n", err);
		goto dereg_ring_mr;
	}
	err = pthread_mutex_init(&qp->received_lines_lock, NULL);
	if(err)
	{
		fprintf(stderr, "Failed to initialize the CVMRDMA received_lines_lock mutex %d\n", err);
		goto dereg_ring_mr;
	}
	err = pthread_cond_init(&qp->send_lines_avail, NULL);
	if(err)
	{
		fprintf(stderr, "Failed to initialize the CVMRDMA send_lines_avail cond var %d\n", err);
		goto dereg_ring_mr;
	}
	err = pthread_cond_init(&qp->recv_lines_ack, NULL);
	if(err)
	{
		fprintf(stderr, "Failed to initialize the CVMRDMA recv_lines_ack cond var %d\n", err);
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
	memset(qp->rx_nonce, 0, cvmrdma_aead_NPUBBYTES);
	qp->remote_params_set = false;
	pthread_mutex_init(&qp->remote_params_lock, NULL);
	pthread_cond_init(&qp->remote_params_set_cv, NULL);


	/* Generate a random tx key */
	randombytes_buf(qp->tx_key, cvmrdma_aead_KEYBYTES);
	memset(qp->tx_nonce, 0, cvmrdma_aead_NPUBBYTES);

	return &qp->user_qp.qp;

free_send_lock:
	pthread_mutex_destroy(&qp->cvmrdma_send_lock);
dereg_ring_mr:
	ctxt->prov_ops.dereg_mr(verbs_get_mr(ringbuf_mr));
	close(dmabuf_fd);
free_bounce_private:
	munmap(bounce_private_mem, cvmrdma_priv_bounce_total_bytes(ctxt->aead));
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
	struct ibv_qp * control_qp = qp->control_qp;

    struct cvmrdma_context * ctxt = cvmrdma_get_ctx(qp->user_qp.qp.context);
    int ret = 0;

    ret = ctxt->prov_ops.modify_qp(prov_qp, attr, attr_mask);
    if(ret)
    {
        goto err;
    }

	/* Update the state on the user-visible qp fields */
	qp->user_qp.qp.state = prov_qp->state;


	/* control QP to INIT*/
	if((attr_mask & IBV_QP_STATE) && (attr->qp_state == IBV_QPS_INIT)){
		if (control_qp->state != IBV_QPS_RESET)
		{
			fprintf(stderr, "control QP to init is not in RESET state!\n");
			goto err;
		}


		assert(attr_mask & IBV_QP_PORT);
		uint8_t myport = attr->port_num;
		DEBUG("control QP to INIT on port %d\n", myport);

		int qp_attr_mask = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT;
		struct ibv_qp_attr control_qp_attrs = {
			.qp_state = IBV_QPS_INIT,
			.pkey_index = 0,
			.port_num = myport,
		};
		control_qp_attrs.qp_access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
		qp_attr_mask |= IBV_QP_ACCESS_FLAGS;

		ret = ctxt->prov_ops.modify_qp(control_qp, &control_qp_attrs, qp_attr_mask);
		if(ret)
		{
			fprintf(stderr, "Failed to modify control QP to INIT\n");
			goto err;
		}
	}

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
		qp->dlid = attr->ah_attr.dlid;
		srand48(time(NULL));
		qp->control_local_psn = lrand48() & 0xffffff;

		/* Start by connecting to the server if not already connected */
		if(! ctxt->remote_network_client_connection && !is_local) {
			DEBUG("[MODIFY_QP] Connecting to the remote server on address %s\n", ctxt->remote_addr);
			ctxt->remote_network_client_connection = new_client_rpc_session(ctxt->network_evloop, ctxt->remote_addr);
			ctxt->remote_network_client_cap = get_cvmrdma_client(ctxt->remote_addr, ctxt->network_evloop, ctxt->remote_network_client_connection);
		} else if (! ctxt->local_network_client_connection && is_local) {
			DEBUG("[MODIFY_QP] Connecting to the local server on address %s\n", ctxt->local_addr);
			ctxt->local_network_client_connection = new_client_rpc_session(ctxt->network_evloop, ctxt->local_addr);
			ctxt->local_network_client_cap = get_cvmrdma_client(ctxt->local_addr, ctxt->network_evloop, ctxt->local_network_client_connection);
		} else {
			DEBUG("[MODIFY_QP] Already connected to the server\n");
		}

		/*
			Setup control qp
		*/
		struct start_args * start_args = calloc(1, sizeof(struct start_args));
		struct ibv_ah_attr * ah_attr = calloc(1, sizeof(struct ibv_ah_attr));
		start_args->ctxt = ctxt;
		start_args->qp = qp;
		memcpy(ah_attr, &attr->ah_attr, sizeof(struct ibv_ah_attr));
		start_args->ah_attr = ah_attr;
		pthread_t control_start_thread;
		ret = pthread_create(&control_start_thread, NULL, start_control_server, start_args);
		if(ret)
		{
			fprintf(stderr, "Failed to start control server thread\n");
			exit(1);
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
						ctxt->local_addr, ctxt->network_evloop, ctxt->local_network_client_cap, dest_qpn 
					);
					set_remote_params(ctxt->local_addr,  ctxt->network_evloop, qp->remote_qnc, qp->tx_key, 
									  qp->ringbuf_mr->rkey, qp->control_buf_mr->rkey, control_qp->qp_num, qp->control_local_psn);
				} else {
					qp->remote_qnc = get_qp_notification_client(
						ctxt->remote_addr, ctxt->network_evloop, ctxt->remote_network_client_cap, dest_qpn 
					);
					set_remote_params(ctxt->remote_addr, ctxt->network_evloop, qp->remote_qnc, qp->tx_key, 
									  qp->ringbuf_mr->rkey, qp->control_buf_mr->rkey, control_qp->qp_num, qp->control_local_psn);
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


/* Poll the WCs that are passed by the CQ server. */
static int cvmrdma_poll_cq(struct ibv_cq * ibcq, int num_entries, struct ibv_wc *wc)
{
	struct cvmrdma_cq * cq = (struct cvmrdma_cq *)ibcq;
	struct cvmrdma_context * ctxt = cvmrdma_get_ctx(cq->user_cq.cq.context);
	int n_popped;
	int err;

	/* If there are notifications OR an error, return ... */
	n_popped = wcq_pop_into(cq->wc_queue, wc, num_entries);
	if(n_popped != 0)
	{
		return n_popped;
	}

	/* run it ONLY if it is not running yet */
	err = cvmrdma_cq_server_run_once_if_idle(ctxt);
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



_Atomic uint64_t cvmrdma_qp_send_wr_id = 0;
static int post_send_do_bounce(struct cvmrdma_qp * qp, struct ibv_send_wr *wrs, struct ibv_send_wr **bad_wr, bool write_for_read)
{
	struct cvmrdma_context * ctxt = cvmrdma_get_ctx(qp->user_qp.qp.context);
	struct cvmrdma_pd * pd = (struct cvmrdma_pd *)qp->user_qp.qp.pd;
    enum sodium_crypto_aead aead = ctxt->aead;

	unsigned long long header_bytes = cvmrdma_header_bytes(aead);

	int err;

	if(qp->user_qp.qp.qp_type != IBV_QPT_RC)
	{
		fprintf(stderr, "ERROR: for now CVMRDMA only supports RC\n");
		return EINVAL;
	}

	struct ibv_qp * prov_qp = qp->provider_qp;
	struct ibv_qp_ex * prov_qpx = ibv_qp_to_qp_ex(prov_qp);


	for(struct ibv_send_wr * this_wr = wrs ; this_wr != NULL ; this_wr = this_wr->next)
	{
		enum ibv_wr_opcode this_op = this_wr->opcode;
		uint32_t this_imm = this_wr->imm_data;

		if((this_wr->send_flags & IBV_SEND_INLINE) != 0) {
			fprintf(stderr, "ERROR: inline not supported, sendflags: %d\n", this_wr->send_flags);
			exit(1);
		}


		bool is_send = (this_op == IBV_WR_SEND) || (this_op == IBV_WR_SEND_WITH_IMM);
		bool is_write = (this_op == IBV_WR_RDMA_WRITE) || (this_op == IBV_WR_RDMA_WRITE_WITH_IMM);

		if(! (is_send || is_write))
		{
			fprintf(stderr, "ERROR: unsupported infiniband operation\n");
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
		uint64_t const * tx_key = qp->tx_key;
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


		size_t total_bytes = 0;
		size_t total_chunks = 0;
		for(size_t i = 0 ; i < nsge ; i++) {
			total_bytes += sgl[i].length;
			struct cvmrdma_mr * cur_mr = get_bounce_metadata_for_lkey(pd->bounce_metadata, sgl[i].lkey);
			size_t sge_chunks = DIVIDE_ROUND_UP(sgl[i].length + ((sgl[i].addr- cur_mr->hca_va) % CVMRDMA_CHUNK_BYTES), CVMRDMA_CHUNK_BYTES);
			total_chunks += sge_chunks;
		}

		bool send_signaled = this_wr->send_flags & IBV_SEND_SIGNALED;
		/* XXX: pass the number of chunks to the CQ_server to know when to emit a send WC for user. Do it 
		   BEFORE we start sending, so that CQ server has this informatin when WCs start to arrive
		   This means that each chunk has to send EXACTLY payload_bytes, even if the tag ends up
		   being shorter (bc of the AEAD choice) */
		qp_send_metadata_push(qp->send_metadata, this_wr->wr_id, total_bytes, total_chunks, write_for_read, send_signaled);

		while(cur_sge_idx < nsge)
		{
			struct ibv_sge * cur_sge = &sgl[cur_sge_idx];

			struct cvmrdma_mr * cur_mr = get_bounce_metadata_for_lkey(pd->bounce_metadata, cur_sge->lkey);
			uint64_t offt = (cur_sge->addr - cur_mr->hca_va) + cur_sge_offt;

			/* If this is the last chunk, we'll need to notify that it's a WC generating chunk (if op != WRITE) */
			size_t chunks_num = DIVIDE_ROUND_UP(cur_sge->length + ((cur_sge->addr - cur_mr->hca_va) % CVMRDMA_CHUNK_BYTES), CVMRDMA_CHUNK_BYTES);
			
			

			bool last_chunk = chunks_in_wr + 1 == chunks_num;

			DEBUG("[SEND QPN %x] rkey: 0x%x, lkey: 0x%x, remote_addr: 0x%lx, dlid %x, lid %x, op: 0x%x, ctxt: %p\n", 
				qp->user_qp.qp.qp_num, rkey, cur_sge->lkey, remote_addr, qp->dlid, qp->lid, this_op, (void*)ctxt);
			DEBUG("[SEND QPN %x] chunks_in_wr %lu\n", qp->user_qp.qp.qp_num, chunks_in_wr);
			DEBUG("[SEND QPN %x] cur_sge->length 0x%lx\n", qp->user_qp.qp.qp_num, cur_sge->length);
			DEBUG("[SEND QPN %x] offt 0x%x\n", qp->user_qp.qp.qp_num, offt);
			DEBUG("[SEND QPN %x] last_chunk %d\n", qp->user_qp.qp.qp_num, last_chunk);
			DEBUG("[SEND QPN %x] chunks_num %lu\n", qp->user_qp.qp.qp_num, chunks_num);
			DEBUG("[SEND QPN %x] total_chunks %lu\n", qp->user_qp.qp.qp_num, total_chunks);
			DEBUG("[SEND QPN %x] want to send data: %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x\n", 
				qp->user_qp.qp.qp_num,
				((unsigned char *)cur_sge->addr)[0], ((unsigned char *)cur_sge->addr)[1], ((unsigned char *)cur_sge->addr)[2], ((unsigned char *)cur_sge->addr)[3],
				((unsigned char *)cur_sge->addr)[4], ((unsigned char *)cur_sge->addr)[5], ((unsigned char *)cur_sge->addr)[6], ((unsigned char *)cur_sge->addr)[7],
				((unsigned char *)cur_sge->addr)[8], ((unsigned char *)cur_sge->addr)[9], ((unsigned char *)cur_sge->addr)[10], ((unsigned char *)cur_sge->addr)[11],
				((unsigned char *)cur_sge->addr)[12], ((unsigned char *)cur_sge->addr)[13], ((unsigned char *)cur_sge->addr)[14], ((unsigned char *)cur_sge->addr)[15],
				((unsigned char *)cur_sge->addr)[16], ((unsigned char *)cur_sge->addr)[17]
			);

			// if(cvmrdma_qp_send_wr_id == 0xc) {
			// 	struct cvmrdma_mr* mr = NULL;
            //     int i = 0;
            //     do {
            //         mr = get_bounce_metadata_for_rkey(pd->bounce_metadata, i);
            //         if (mr != NULL) {
            //             fprintf(stderr, "[SEND] bounce_metadata[%d]: len: 0x%zx, user_mr addr: %p, user_mr hca_va: %p provider_mr len: 0x%zx, provider_mr provider_va: %p, provider_mr lkey: 0x%lx\n", 
            //             i, mr->user_mr.ibv_mr.length, mr->user_mr.ibv_mr.addr, mr->hca_va,
            //             mr->provider_mr->length, mr->provider_va, mr->provider_mr->lkey);
            //         }
            //         i++;
            //     } while (mr != NULL);
			// }


			/* Prepare the two nonces: for notif and for payload */
			uint64_t * qp_nonce = qp->tx_nonce;

			uint64_t nonce_metadata[cvmrdma_aead_NPUB_WORDS], nonce_payload[cvmrdma_aead_NPUB_WORDS];
			memcpy(nonce_metadata, qp_nonce, cvmrdma_aead_NPUBBYTES);
			/* and increment nonce */
			qp_nonce[0] += 1;


			/* offt in MR*/
			uint64_t chunk_idx = offt / CVMRDMA_CHUNK_BYTES;

			uint64_t alinged_offset = offt - (offt % CVMRDMA_CHUNK_BYTES);
			/* location in memory */
			unsigned char * ptxt = cur_mr->user_mr.ibv_mr.addr + alinged_offset;

			uint64_t true_payload_bytes = 0; 

			if (!last_chunk){
				true_payload_bytes = CVMRDMA_CHUNK_BYTES - (offt % CVMRDMA_CHUNK_BYTES);
				cur_sge_offt += true_payload_bytes;
			} else {
				true_payload_bytes = cur_sge->length - cur_sge_offt;
				cur_sge_idx++;
				cur_sge_offt = 0;
			}

			DEBUG("[SEND QPN %x] chunk_idx %lu\n", qp->user_qp.qp.qp_num, chunk_idx);
			DEBUG("[SEND QPN %x] true_payload_bytes %lu\n", qp->user_qp.qp.qp_num, true_payload_bytes);

			struct chunk_tracking_info* chunk_info = &cur_mr->chunk_tracking[chunk_idx];
			
			/* generate new payload nonce */
			if (!chunk_info->cached){
				DEBUG("[SEND QPN %x] Generating new nonce for chunk %lu\n", qp->user_qp.qp.qp_num, chunk_idx);
				randombytes_buf(chunk_info->nonce, cvmrdma_aead_NPUBBYTES);
				DEBUG("[SEND QPN %x] nonce %lx %lx\n", qp->user_qp.qp.qp_num, chunk_info->nonce[0], chunk_info->nonce[1]);
			}

			unsigned char * slot = cur_mr->provider_va + (chunk_idx * cvmrdma_ring_slot_bytes(ctxt->aead));

			/***** NOTIFY ******/
			TIMING("chunk_notify");
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
					notif.chunk_offt = offt % CVMRDMA_CHUNK_BYTES;
					notif.chunk_len = true_payload_bytes;
					notif.last_chunk = last_chunk;
				}
				else
				{
					/* All these chunks will be written to the same RR, until last_chunk is reached */
					notif.hca_addr = 0;
					notif.op = this_op;
					notif.rkey = 0;
					notif.imm = this_imm;
					memcpy(notif.payload_nonce, chunk_info->nonce, cvmrdma_aead_NPUBBYTES);
					notif.chunk_offt = offt % CVMRDMA_CHUNK_BYTES;
					notif.chunk_len = true_payload_bytes;
					notif.last_chunk = last_chunk;
				}

				notif_clen = header_bytes;
				err = cvmrdma_encrypt(
					slot, &notif_clen,
					(unsigned char const *)&notif, sizeof(notif),
					NULL, 0,
					0,
					(unsigned char *)nonce_metadata,
					(unsigned char const *)tx_key,
					aead
				);
				if(err)
				{
					fprintf(stderr, "Failed to encrypt chunk!\n");
					exit(1);
				}

				assert(notif_clen == header_bytes);
			}

			TIMING("gec_t0");
			/**** ENCRYPT *****/
			if(chunk_info->cached){
				DEBUG("[SEND QPN %x] Chunk %lu is cached\n", qp->user_qp.qp.qp_num, chunk_idx);
				goto skip_payload_encryption;
			}

			// Mark chunk in userMR as write protected
			atomic_store(&chunk_info->cached, true);
			int uffd = cur_mr->uffd;
			struct uffdio_writeprotect uffdio_wp = {
				.range = {
					.start = (unsigned long) ptxt,
					.len = CVMRDMA_CHUNK_BYTES,
				},
				.mode = UFFDIO_WRITEPROTECT_MODE_WP,
			};
			if (ioctl(uffd, UFFDIO_WRITEPROTECT, &uffdio_wp) == -1) {
				fprintf(stderr, "Failed to write protect the memory region; addr %p, len %lu, err: %d\n", ptxt, DIVIDE_ROUND_UP(true_payload_bytes, PAGE_SIZE) * PAGE_SIZE, errno);
				exit(1);
			}

			DEBUG("[SEND QPN %x] ptxt %x %x %x\n", qp->user_qp.qp.qp_num, ptxt[0], ptxt[1], ptxt[2]);
			DEBUG("[SEND QPN %x] nonce %lx %lx\n", qp->user_qp.qp.qp_num, chunk_info->nonce[0], chunk_info->nonce[1]);

			unsigned long long payload_clen = CVMRDMA_CHUNK_BYTES;
			err = cvmrdma_encrypt(
				slot + notif_clen, /* start after the notif */
				&payload_clen,
				ptxt, CVMRDMA_CHUNK_BYTES,
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

			DEBUG("[SEND QPN %x] slot[0] %x %x %x ...\n", qp->user_qp.qp.qp_num, slot[0], slot[1], slot[2]);
			DEBUG("[SEND QPN %x] slot[notif_clen + 0] %x %x %x ...\n", qp->user_qp.qp.qp_num, slot[notif_clen + 0], slot[notif_clen + 1], slot[notif_clen + 2]);
			DEBUG("[SEND QPN %x] notif_clen %llu\npayload_clen %llu\n", qp->user_qp.qp.qp_num, notif_clen, payload_clen);

skip_payload_encryption:

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
				DEBUG("[SEND QPN %x] slot_id %d\n", qp->user_qp.qp.qp_num, slot_id);
				DEBUG("[SEND QPN %x] send_lines_full %d\n", qp->user_qp.qp.qp_num, qp->send_lines_full);

				pthread_mutex_unlock(&qp->send_lines_full_lock);
				/*  UNLOCK */
			}

			TIMING("gec_t1");

			/****** POST SEND WWI ******/
			/* Synchronize the ring buffer with the remote copy using a WWI.
			   Rings MR are 0-indexed. The send ring starts at 0, the recv 
			   ring starts at CVMRDMA_RING_ENTRIES * CVMRDMA_CHUNK_SIZE.
			*/
			uint32_t local_provider_lkey = cur_mr->provider_mr->lkey;
			uint32_t remote_ring_rkey = qp->remote_ring_rkey;
			assert(remote_ring_rkey != 0);

			uint64_t helper_offt = (offt%CVMRDMA_CHUNK_BYTES)+header_bytes;
			DEBUG("[SEND QPN %x] writing encrypted %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x\n", 
			 	qp->user_qp.qp.qp_num, 
				slot[0+helper_offt], slot[1+helper_offt], slot[2+helper_offt], slot[3+helper_offt],
				slot[4+helper_offt], slot[5+helper_offt], slot[6+helper_offt], slot[7+helper_offt],
				slot[8+helper_offt], slot[9+helper_offt], slot[10+helper_offt], slot[11+helper_offt],
				slot[12+helper_offt], slot[13+helper_offt], slot[14+helper_offt], slot[15+helper_offt],
				slot[16+helper_offt], slot[17+helper_offt]
			);


			ibv_wr_start(prov_qpx);

			prov_qpx->wr_id = cvmrdma_qp_send_wr_id++;
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

			TIMING("send_chunk_wr");
			err = ibv_wr_complete(prov_qpx);
			if(err)
			{
				fprintf(stderr, "Failed to post WWI for a chunk (%d)\n", err);
				exit(1);
			}

			bytes_sent += true_payload_bytes;
			chunks_in_wr++;

			/* after each send, try to dequeue some SEND WC from the CQ (if no other thread is polling the CQ)
			   XXX: for performance, ideally don't dequeue after every send, and don't signal every send
			*/
			err = cvmrdma_cq_server_run_once_if_idle(ctxt);
			if(err)
			{
				fprintf(stderr, "Polling the CQ after a post_send failed %d\n", err);
				exit(1);
			}
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
	- AEAD the plaintext region to the bounce buffer,
	  keeping the same offset
	- change WRITE into WRITE_WITH_IMM to notify the remote.
	- RPC send the dest hca_addr, rkey, imm
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


_Atomic uint64_t cvmrdma_mr_id = 0;
static struct ibv_mr *cvmrdma_reg_mr(struct ibv_pd *ibpd, void *addr, size_t length,
        uint64_t hca_va, int access)
{
	struct cvmrdma_pd * pd = (struct cvmrdma_pd *)ibpd;
	struct ibv_context * ibctxt = pd->user_pd.context;
	struct cvmrdma_context * ctxt = cvmrdma_get_ctx(ibctxt);
	assert(length > 0);

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

	uint64_t num_chunks = DIVIDE_ROUND_UP((length + (((uint64_t)addr) % CVMRDMA_CHUNK_BYTES)), CVMRDMA_CHUNK_BYTES);
	uint64_t provider_mr_size = num_chunks * cvmrdma_ring_slot_bytes(ctxt->aead);

	DEBUG("[MR] Allocating provider buffer MR of size %lu for user MR of size %lu\n", provider_mr_size, length);

	int dmabuf_fd = -1;
	void * provider_va = NULL;
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

	/* Register the memory range of the mapping we just created for
       handling by the userfaultfd object. */
    struct uffdio_register uffdio_register = {
        .range = {
            .start = (unsigned long) addr,
            .len = length,
        },
        .mode = UFFDIO_REGISTER_MODE_WP | UFFDIO_REGISTER_MODE_MISSING,
        //.mode = UFFDIO_REGISTER_MODE_WP,
    };
    if (ioctl(uffd, UFFDIO_REGISTER, &uffdio_register) == -1) {
		fprintf(stderr, "Failed to ioctl-UFFDIO_REGISTER %d with addr %p and len %lu\n", errno, addr, length);
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

	/*
		The CVMRDMA MR exposes ibv_mr fields to the user containing data (eg the address)
		related to the user region, whereas the buffer actually registered as an MR is kept
		in a private field.
	*/

	uint32_t mr_id = pd->mr_id_ctr++;
	mr_id = atomic_load_explicit(&cvmrdma_mr_id, memory_order_relaxed);
	atomic_fetch_add_explicit(&cvmrdma_mr_id, 1, memory_order_relaxed);

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

int cvmrdma_destroy_qp(struct ibv_qp *ibqp)
{
	struct cvmrdma_qp * qp = (struct cvmrdma_qp *)ibqp;
	void* t_ret;
	int ret = pthread_cancel(qp->control_server_send_acks_thread);
	if(ret)
	{
		fprintf(stderr, "Failed to cancel control_server_send_acks_thread\n");
	}
	ret = pthread_join(qp->control_server_send_acks_thread, &t_ret);
	if(ret)
	{
		fprintf(stderr, "Failed to join control_server_send_acks_thread\n");
	}
	if (t_ret != PTHREAD_CANCELED)
	{
		fprintf(stderr, "control_server_send_acks_thread was not canceled\n");
	}

	ret = pthread_cancel(qp->control_server_receive_acks_thread);
	if(ret)
	{
		fprintf(stderr, "Failed to cancel control_server_receive_acks_thread\n");
	}
	ret = pthread_join(qp->control_server_receive_acks_thread, &t_ret);
	if(ret)
	{
		fprintf(stderr, "Failed to join control_server_receive_acks_thread\n");
	}
	if (t_ret != PTHREAD_CANCELED)
	{
		fprintf(stderr, "control_server_receive_acks_thread was not canceled\n");
	}

	DEBUG("Successfully joined control_server_send_acks_thread and control_server_receive_acks_thread: %ld %ld\n", (long)qp->control_server_send_acks_thread, (long)qp->control_server_receive_acks_thread);

	// TODO(@ANONYMIZED): This should likely do more cleanup

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
	return 0;
}

static void cvmrdma_cq_event(struct ibv_cq *ibcq)
{
	struct cvmrdma_cq * cq = (struct cvmrdma_cq *)ibcq;
	struct cvmrdma_context * ctxt = cvmrdma_get_ctx(cq->user_cq.cq.context);
	bool expected = 1;
	atomic_compare_exchange_strong(&cq->comp_channel_event_fd_filled, &expected, 0);
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

struct verbs_context_ops cvmrdma_ops = {
    .alloc_pd = cvmrdma_alloc_pd,
    .create_cq = cvmrdma_create_cq,
    .create_qp = cvmrdma_create_qp,
    // .dealloc_pd = cvmrdma_dealloc_pd,
    // .dereg_mr = cvmrdma_dereg_mr,
    // .destroy_cq = cvmrdma_destroy_cq,
    .destroy_qp = cvmrdma_destroy_qp,
    // .free_context = cvmrdma_free_context,
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

_Atomic bool tcp_server_running = false;
CapnpEvloopThread * evloop_server = NULL;
CVMRDMAServer * tcp_server = NULL;

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

	char const * aead_str = getenv("CVMRDMA_AEAD");
	enum sodium_crypto_aead aead = SODIUM_AEAD_XCHACHAPOLY;
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


	bool expected = false;
	bool first = atomic_compare_exchange_strong(&tcp_server_running, &expected, true);
	if(first)
	{
		DEBUG("Start IP based server on %s\n", local_bind_addr);
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
	cvmrdma_ctx->local_addr = calloc(1, 20);
	sprintf(cvmrdma_ctx->local_addr, "127.0.0.1:%s", local_bind_addr + 2);	
	

	/* Open a session to the remote using the same event loop */
	DEBUG("Remote's IP is %s\n", remote_addr);
	/* Only connect to the remote's RPC server when we know that the server is up */
	cvmrdma_ctx->remote_addr = remote_addr;
	cvmrdma_ctx->local_network_client_connection = NULL;
	cvmrdma_ctx->local_network_client_cap = NULL;
	cvmrdma_ctx->remote_network_client_connection = NULL;
	cvmrdma_ctx->remote_network_client_cap = NULL;

	/* Get the root capability to the remote's RPC's API */
	
	/* Create a single CQ to get the send and recv WC for the chunks */
	struct ibv_cq * ibcq = cvmrdma_ctx->prov_ops.create_cq(prov_ctx, 16384, cvmrdma_ctx->provider_comp_channel, 0);
	if(! ibcq)
	{
		fprintf(stderr, "failed to create backend CQ for the CQ server\n");
		exit(1);
	}
	/* see ibv_create_cq */
	verbs_init_cq(ibcq, prov_ctx, NULL, NULL);

	if(cvmrdma_ctx->provider_comp_channel) {
		ret = cvmrdma_ctx->prov_ops.req_notify_cq(ibcq, 0);
		if(ret)
		{
			fprintf(stderr, "Failed to request CQ notifications %d\n", ret);
			goto err;
		}
	}


	/* Initialize the CQ polling metadata */
	cq_server_context_init(&cvmrdma_ctx->cvmrdma_cq_server, ibcq);


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
