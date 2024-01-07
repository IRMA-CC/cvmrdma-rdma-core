#include "cvmrdma_ops.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <arpa/inet.h> /* ntohl */

#include <sodium.h>

#include "bounce_metadata.h"
#include "cvmrdma_cq_server.h"
#include "cvmrdma_network_client_priv.h"
#include "cvmrdma_network_server_priv.h"
#include "cvmrdma_network_evloop_priv.h"
#include "notification_queue.h"
#include "recv_metadata.h"
#include "taskset_wrapper.h"
#include "wc_queue.h"

#include "debug.h"

static struct ibv_pd * cvmrdma_alloc_pd(struct ibv_context *context)
{
    struct verbs_context * vctxt = verbs_get_ctx(context);
    PdBounceMetadata * bounce_metadata;

    /* Allocate the PD through the provider */
    struct ibv_pd * ibpd = vctxt->cvmrdma.alloc_pd(context);
    if(! ibpd)
    {
        verbs_err(vctxt, "Failed to allocate PD through backend.\n");
        goto err;
    }


	bounce_metadata = bounce_metadata_alloc();
	if(! bounce_metadata)
	{
		fprintf(stderr, "Failed to allocated bounce metadata storage.\n");
		goto free_pd;
	}

    ibpd->bounce_metadata = bounce_metadata;

	return ibpd;


// free_metadata:
// 	bounce_metadata_destroy(bounce_metadata);
free_pd:
	vctxt->cvmrdma.dealloc_pd(ibpd);
err:
	return NULL;
}


static struct ibv_cq *cvmrdma_create_cq(struct ibv_context *context, int cqe,
				struct ibv_comp_channel *channel,
				int comp_vector)
{
    struct verbs_context * vctxt = verbs_get_ctx(context);
	int ret;

    /* Create the CQ on the provider */
    struct ibv_cq * ibcq = vctxt->cvmrdma.create_cq(context, cqe, channel, comp_vector);
    if(! ibcq)
    {
        goto err;
    }

	struct verbs_cq * cq = (struct verbs_cq *)ibcq;

	cq->wc_queue = alloc_wc_queue();

	/* Initialize the CQ polling server */
	cq_server_context_init(&cq->cvmrdma_cq_server, ibcq);

	/* And run the server loop in another thread */
	ret = cq_server_run(&cq->cvmrdma_cq_server, &cq->cvmrdma_cq_server_thread);
	if(ret)
	{
		fprintf(stderr, "Failed to start CQ server %d\n", ret);
		goto cq_server_context_destroy;
	}

	return &cq->cq;

cq_server_context_destroy:
	cq_server_context_destroy(&cq->cvmrdma_cq_server);
// destroy_cq:
    vctxt->cvmrdma.destroy_cq(ibcq);
err:
    return NULL;
}


static int attach_qp_to_cq(struct verbs_context * ctxt, struct ibv_pd *pd, struct verbs_qp * qp, struct ibv_cq * ibcq)
{
	/* Add this QP to the CQ server. This gives us one end of the notification
	   channel ; the RPC server will push notifications there.
	    The    
	 */
	assert(ibcq);
	struct verbs_cq * cq = (struct verbs_cq *)ibcq;

	struct ibv_qp * ibqp = &qp->qp;
	DEBUG("Create notification queue ->CQ server for QPN %" PRIx32 "\n", ibqp->qp_num);
	NotificationQueue * nq = cq_server_add_qp(&cq->cvmrdma_cq_server, ibqp->qp_num);
    if(! nq)
    {
        fprintf(stderr, "failed to register QP on CQ server\n");
        return -1;
    }

	/* Create the corresponding RPC object. Pass the end of the notification channel here.
	   No risk of a race: we haven't returned the QPN to the user yet, the remote cannot possibly
	   know it yet.
	*/
	DEBUG("Create QpNotificationChannel cap QNC using notification queue\n");
	cvmrdma_server_register_qp(ctxt->cvmrdma.network_server, pd->bounce_metadata, qp->recv_metadata, nq, ibqp->qp_num);

	return 0;
}

static struct ibv_qp *cvmrdma_create_qp(struct ibv_pd *pd,
				struct ibv_qp_init_attr *attr)
{
    struct verbs_context * ctxt = verbs_get_ctx(pd->context);
	int err;

    struct ibv_qp * ibqp = ctxt->cvmrdma.create_qp(pd, attr);
    if(! ibqp)
    {
        goto err;
    }

	struct verbs_qp * qp = (struct verbs_qp *)ibqp;

	struct ibv_cq * rcq = attr->recv_cq;
	struct ibv_cq * scq = attr->send_cq;

	/*  recv_cq is necessary: we need to decrypt on recv.
	    send cq is optionnal.
	    XXX: if a CQ is only used for sends (even for multiple QPs), then it could bypass
	    the CQ server    
	*/

	QpRecvMetadata * recv_metadata = alloc_qp_recv_metadata();
	if(! recv_metadata)
	{
		fprintf(stderr, "Failed to allocated recv metadata storage.\n");
		goto free_qp;
	}
	qp->recv_metadata = recv_metadata;

	/* Initialize the send and recv mutex */
	err = pthread_mutex_init(&qp->cvmrdma_recv_lock, NULL);
	if(err)
	{
		fprintf(stderr, "Failed to initialize the CVMRDMA recv mutex %d\n", err);
		goto free_recv_metadata;
	}

	err = pthread_mutex_init(&qp->cvmrdma_send_lock, NULL);
	if(err)
	{
		fprintf(stderr, "Failed to initialize the CVMRDMA recv mutex %d\n", err);
		goto free_recv_lock;
	}


	err = attach_qp_to_cq(ctxt, pd, qp, rcq);
	if(err)
	{
		fprintf(stderr, "Failed to attach QP %" PRIx32 " to its recv cq (%d)\n", ibqp->qp_num, err);
		goto free_send_lock;
	}
	if(scq && (scq != rcq))
	{
		err = attach_qp_to_cq(ctxt, pd, qp, scq);
		if(err)
		{
			fprintf(stderr, "Failed to attach QP %" PRIx32 " to its send cq (%d)\n", ibqp->qp_num, err);
			goto free_send_lock;
		}
	}

	/* We can only get the capability to the remote QP once we know its QPN, this will be done
	   in modify_qp */
	qp->remote_qnc = NULL;

	/* Generate a random key */
	randombytes_buf(qp->tx_key, sizeof(qp->tx_key));


	return ibqp;

free_send_lock:
	pthread_mutex_destroy(&qp->cvmrdma_send_lock);
free_recv_lock:
	pthread_mutex_destroy(&qp->cvmrdma_send_lock);
free_recv_metadata:
	free_qp_recv_metadata(recv_metadata);
free_qp:
    ctxt->cvmrdma.destroy_qp(ibqp);
err:
	return NULL;


}

static int cvmrdma_modify_qp(struct ibv_qp *ibqp, struct ibv_qp_attr *attr,
		     int attr_mask)
{
    struct verbs_context * vctxt = verbs_get_ctx(ibqp->context);
	struct verbs_qp * qp = (struct verbs_qp *)ibqp;
    int ret = 0;

    ret = vctxt->cvmrdma.modify_qp(ibqp, attr, attr_mask);
    if(ret)
    {
        goto err;
    }


	/* The RPC object need to be connected to send, but this is not required for receive (the RPC server
	   is already running). However, since the user passes the QPN in the INIT->RTR transition,
	   connect at this step.
	*/
	if((attr_mask & IBV_QP_STATE) && (attr->qp_state == IBV_QPS_RTR))
	{
		switch(ibqp->qp_type)
		{
			case IBV_QPT_RC:
			case IBV_QPT_UC: {
				assert(attr_mask & IBV_QP_DEST_QPN);
				uint32_t dest_qpn = attr->dest_qp_num;

				DEBUG("Obtain notification channel cap from remote for its QP with QPN 0x%" PRIx32 "\n", dest_qpn);

				/* Blocking: get the qp notifier capability from the remote over the network and share the Tx key with the remote */
				qp->remote_qnc = get_qp_notification_client(
					vctxt->cvmrdma.network_evloop, vctxt->cvmrdma.network_client_cap,
					dest_qpn, qp->tx_key
				);
				break;
			}
			default: {
				fprintf(stderr, "modify_qp: unsupported QP type\n");
				return -1;
			}
		}
	}

    return 0;

err:
    return ret;
}


/* Poll the WCs that are passed by the CQ server. */
static int cvmrdma_poll_cq(struct ibv_cq * ibcq, int num_entries, struct ibv_wc *wc)
{
	struct verbs_cq * cq = (struct verbs_cq *)ibcq;
	return wcq_pop_into(cq->wc_queue, wc, num_entries);
}


static int cvmrdma_post_recv(struct ibv_qp *ibqp, struct ibv_recv_wr *wr,
		struct ibv_recv_wr **bad_wr)
{
	/*
	   Since the bounce buffer is registered with the hca_va, a valid post recv for the original buffer
	   is a valid post recv for the bounce buffer. We can simply pass the request unmodified.

	   Since the CVMRDMA wrapper also needs to know where recvs end up, we need to keep the sg-list in case
	   of success
	*/
	int err;
	struct verbs_qp * qp = (struct verbs_qp *)ibqp;
	struct verbs_context * ctx = verbs_get_ctx(ibqp->context);


	/* We need to lock so that the recv queue and the recvs actually
	   match (i.e. no interleaved recv and push)*/

	pthread_mutex_lock(&qp->cvmrdma_recv_lock);


	err = ctx->cvmrdma.post_recv(ibqp, wr, bad_wr);
	if(err)
	{
		goto ret;
	}

	for(struct ibv_recv_wr * this_wr = wr ; this_wr != NULL ; this_wr = this_wr->next)
	{
		qp_recv_metadata_push(qp->recv_metadata, this_wr->wr_id, this_wr->sg_list, this_wr->num_sge);
	}

ret:
	pthread_mutex_unlock(&qp->cvmrdma_recv_lock);
	return err;
}


static int post_send_do_bounce(struct ibv_qp *ibqp, struct ibv_send_wr *wrs, struct ibv_send_wr **bad_wr)
{
	struct verbs_context * ctxt = verbs_get_ctx(ibqp->context);
    struct verbs_qp * qp = (struct verbs_qp *)ibqp;
	struct ibv_pd * pd = ibqp->pd;
	int err;

	if(ibqp->qp_type != IBV_QPT_RC)
	{
		fprintf(stderr, "ERROR: for now CVMRDMA only supports RC\n");
		return EINVAL;
	}


	for(struct ibv_send_wr * this_wr = wrs ; this_wr != NULL ; this_wr = this_wr->next)
	{
		enum ibv_wr_opcode this_op = this_wr->opcode;

		switch(this_op)
		{
		case IBV_WR_SEND:
		case IBV_WR_SEND_WITH_IMM:
		case IBV_WR_RDMA_WRITE:
		case IBV_WR_RDMA_WRITE_WITH_IMM: {
			break;
		}
		default: {
			fprintf(stderr, "ERROR: unsupported infiniband operation\n");
			*bad_wr = this_wr;
			return EINVAL;
		}
		}

		/* Prepare the bounce buffer. We have to encrypt the data
		from the user regions into the bounce MRs. */

		if(this_wr->num_sge != 1)
		{
			fprintf(stderr, "ERROR: for now CVMRDMA only supports 1 sge per WR\n");
			*bad_wr = this_wr;
			return EINVAL;
		}

		struct ibv_sge sge = this_wr->sg_list[0];
		
		/* Send rkey s.t. remote knows which MR has been written to */
		uint64_t remote_addr = this_wr->wr.rdma.remote_addr;
		uint32_t rkey = this_wr->wr.rdma.rkey;

		/* Send the imm ONLY over the capnp channel to keep it confidential */
		uint32_t imm = ntohl(this_wr->imm_data);
		this_wr->imm_data = 0;

		/* Identify the local MR from the lkey and find the corresponding bounce buffer */
		struct bounce_entry bounce_info = get_bounce_metadata_for_lkey(pd->bounce_metadata, sge.lkey);
		size_t offt = sge.addr - bounce_info.hca_va;

		void * user_start = bounce_info.user_buffer + offt;
		void * bounce_start = bounce_info.bounce_buffer + offt;

		/* Store the nonce and the mac word aligned to make serialization easier
		   Store on the heap to pass ownership to another thread */
		uint64_t * nonce = malloc(crypto_aead_aegis256_NPUBBYTES);
		uint64_t * mac = malloc(crypto_aead_aegis256_ABYTES);

		/* Generate the nonce to use for the encryption */
		randombytes_buf(nonce, sizeof(nonce));

		uint64_t const * tx_key = qp->tx_key;

		DEBUG("Encrypting 0x%" PRIx32 " B from %p to %p with nonce %lx %lx %lx %lx\n", sge.length, user_start, bounce_start, nonce[0], nonce[1], nonce[2], nonce[3]);
#ifndef CVMRDMA_DUMMY_BOUNCE
		/* TODO TOCTOU bounce !!!!*/
		unsigned long long maclen = sizeof(mac);
		
		TIMING("enc_t0");
		err = crypto_aead_aegis256_encrypt_detached(
			bounce_start,
			(unsigned char *)mac, &maclen,
			user_start, sge.length,
			NULL, 0,
			NULL,
			(unsigned char *)nonce,
			(unsigned char const *)tx_key
		);
		TIMING("enc_t1");
		if(err)
		{
			fprintf(stderr, "ERROR: Failed to encrypt WR.\n");
			*bad_wr = this_wr;
			return -1;
		}

#else
		memcpy(bounce_start, user_start, sge.length);
		memset(mac, 0x01, sizeof(mac));
		mac[0] = 0x11223344;
		unsigned long long maclen = sizeof(mac);
#endif
		assert(maclen == crypto_aead_aegis256_ABYTES);
		DEBUG("mac = %lx %lx %lx %lx\n", mac[0], mac[1], mac[2], mac[3]);

		/* After the bounce, the sg-list in the WR is valid.
			We need to notify the remote of the incoming payload.
		*/
		

		TIMING("notif_lazy");
		/* Before doing anything, we can already notify the remote of the upcoming write
			We use kj::Executor to start the send off-thread, on the capnp event loop thread.
		*/

		switch (this_op)
		{
		case IBV_WR_SEND:
			remote_qp_notify_send(ctxt->cvmrdma.network_evloop,
				qp->remote_qnc,
				sge.length,
				nonce,
				mac
			);
			break;
		case IBV_WR_SEND_WITH_IMM:
			remote_qp_notify_send_with_imm(ctxt->cvmrdma.network_evloop,
				qp->remote_qnc,
				sge.length,
				imm,
				nonce,
				mac
			);
			break;
		case IBV_WR_RDMA_WRITE:
			remote_qp_notify_write(ctxt->cvmrdma.network_evloop,
				qp->remote_qnc,
				remote_addr,
				sge.length,
				rkey,
				nonce,
				mac
			);
			break;
		case IBV_WR_RDMA_WRITE_WITH_IMM:
			remote_qp_notify_write_with_imm(ctxt->cvmrdma.network_evloop,
				qp->remote_qnc,
				remote_addr,
				sge.length,
				rkey,
				imm,
				nonce,
				mac
			);
			break;
		default: {
			fprintf(stderr, "ERROR: for now CVMRDMA only supports SEND (w/ imm) and WRITE (W/ imm)\n");
			assert(false);
			exit(1);
		}
		}

		/* mac and nonce ownership is passed to capnp client */
		mac = NULL;
		nonce = NULL;
	}

	return 0;
}


static int cvmrdma_post_send(struct ibv_qp *ibqp, struct ibv_send_wr *wr,
		     struct ibv_send_wr **bad_wr)
{
    struct verbs_context * vctxt = verbs_get_ctx(ibqp->context);
    struct verbs_qp * qp = (struct verbs_qp *)ibqp;
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

	ret = post_send_do_bounce(ibqp, wr, bad_wr);
	if(ret < 0)
	{
		fprintf(stderr, "Failed to bounce before send!\n");
		goto ret;
	}

	TIMING("send_prov");
    ret = vctxt->cvmrdma.post_send(ibqp, wr, bad_wr);

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
	struct verbs_context * vctxt = verbs_get_ctx(ibctxt);
	return vctxt->cvmrdma.query_device_ex(ibctxt, input, attr, attr_size);
}



static int cvmrdma_query_port(struct ibv_context *ibctxt, uint8_t port_num,
		struct ibv_port_attr *port_attr)
{
	struct verbs_context * vctxt = verbs_get_ctx(ibctxt);
	return vctxt->cvmrdma.query_port(ibctxt, port_num, port_attr);
}



static struct ibv_mr *cvmrdma_reg_mr(struct ibv_pd *pd, void *addr, size_t length,
        uint64_t hca_va, int access)
{
    struct verbs_context * vctxt = verbs_get_ctx(pd->context);

	/* TODO: add a secondary buffer in public memory where the encrypted payload will be
	   copied from/to to avoid TOCTOU attacks. register _this secundary buffer_ as an MRs */

	int bounce_prots;
	void *bounce;
    struct ibv_mr * ibmr;
    /* CVM
       - allocate memory here for bounce buf: should be in unprotected memory! eg with special
	     syscall get and fd and mmap it
	   - register it as MR
	   - keep track of (lkey, bounce addr)
    
        e.g: with a dmabuf
    */

    /* `access` is access to the original buffer, wheraes bounce_prots is for the bounce buffer:
	   - to read from the original buffer, we must write to the bounce buffer
	   - to write to the original buffer, we must read the bounce buffer
	*/

    /* LOCAL_READ is always enabled (as source of SEND/WRITE)*/
	bounce_prots = PROT_WRITE;

	/* LOCAL_WRITE: for destination of recv
	   REMOTE_WRITE: for destination of write */
	if ((access & IBV_ACCESS_LOCAL_WRITE) || (access & IBV_ACCESS_REMOTE_WRITE))
	{
		DEBUG("Make bounce buffer read/write\n");
		bounce_prots |= PROT_READ;
	}

	/* TODO: allocate the bounce buffer in unencrypted memory */
	bounce = mmap(0, length, bounce_prots, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if (bounce == MAP_FAILED) {
		goto err;
	}

	DEBUG("Allocated bounce buffer for user region: UR %p   bounce MR %p\n", addr, bounce);

    /* reg_mr the bounce buffer as the MR through the provider */
    ibmr = vctxt->cvmrdma.reg_mr(pd, bounce, length, hca_va, access);
    if(! ibmr)
    {
        goto free_bounce;
    }

	/* Make this bounce MR's metadata available through its lkey or rkey */
	register_bounce_mr(pd->bounce_metadata,
		ibmr->lkey,
		ibmr->rkey,
		hca_va,
		addr,
		bounce
	);

	return ibmr;

free_bounce:
	munmap(bounce, length);
err:
    return NULL;
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
    struct verbs_context * context = vdev->cvmrdma.alloc_context(ibdev, cmd_fd, private_data);
    if(! context) {
		goto err;
    }

	DEBUG("Start IP based server on %s\n", local_bind_addr);
	/* Start a new thread with a capnp event loop running the RPC server */
	context->cvmrdma.network_evloop = create_capnp_event_loop(local_bind_addr, &context->cvmrdma.network_server);
	

	/* Open a session to the remote using the same event loop */

	DEBUG("Connect to IP based remote on %s\n", remote_addr);
	/* Connect to the remote's RPC server */
	context->cvmrdma.network_client_connection = new_client_rpc_session(context->cvmrdma.network_evloop, remote_addr);

	/* Get the root capability to the remote's RPC's API */
	context->cvmrdma.network_client_cap = get_cvmrdma_client(context->cvmrdma.network_evloop, context->cvmrdma.network_client_connection);
	
	return context;

// uninit_and_free:
// 	context->cvmrdma.free_context(&context->context);
err:
	return NULL;
}

struct verbs_device_ops cvmrdma_device_ops = {
    .alloc_context = cvmrdma_alloc_context,
};
