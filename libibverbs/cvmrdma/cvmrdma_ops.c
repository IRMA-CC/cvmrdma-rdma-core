#include "cvmrdma_ops.h"


#include "bounce_metadata.h"
#include "cvmrdma_cq_server.h"
#include "cvmrdma_network_client.h"
#include "cvmrdma_network_server.h"
#include "notification_queue.h"

#include "debug.h"

struct ibv_pd * cvmrdma_alloc_pd(struct ibv_context *context)
{
    struct verbs_context * vctxt = verbs_get_ctx(pd->context);
    PdBounceMetadata * bounce_metadata;

    /* Allocate the PD through the provider */
    struct ibv_pd * ibpd = vctxt->cvmrdma.alloc_pd();
    if(! ibpd)
    {
        verbs_err(vctxt, "Failed to allocate PD through backend.")
        goto err;
    }


	bounce_metadata = bounce_metadata_alloc();
	if(! bounce_metadata)
	{
		fprintf(stderr, "Failed to allocated bounce metadata storage.\n");
		goto free_pd;
	}

    ibpd->bounce_metadata = bounce_metadata;


free_metadata:
	bounce_metadata_destroy(bounce_metadata);
free_pd:
	vctxt->cvmrdma.dealloc_pd(pd);
err:
	return NULL;
}


struct ibv_cq *cvmrdma_create_cq(struct ibv_context *context, int cqe,
				struct ibv_comp_channel *channel,
				int comp_vector)
{
    struct verbs_context * vctxt = verbs_get_ctx(pd->context);

    /* Create the CQ on the provider */
    struct ibv_cq * cq = vctxt->cvmrdma.create_cq(context, cqe, channel, comp_vector);
    if(! cq)
    {
        goto err;
    }

	/* Initialize the CQ polling server */
	cq_server_context_init(&cq->cvmrdma_cq_server, &cq->ibv_cq);

	/* And run the server loop in another thread */
	ret = cq_server_run(&cq->cvmrdma_cq_server, &cq->cvmrdma_cq_server_thread);
	if(ret)
	{
		fprintf(stderr, "Failed to start CQ server %d\n", ret);
		goto cq_server_context_destroy;
	}

	return &cq->ibv_cq;

cq_server_context_destroy:
	cq_server_context_destroy(&cq->cvmrdma_cq_server);
destroy_cq:
    vctxt->cvmrdma.destroy_cq(cq);
err:
    return NULL;
}


struct ibv_qp *cvmrdma_create_qp(struct ibv_pd *pd,
				struct ibv_qp_init_attr *attr)
{
    struct verbs_context * vctxt = verbs_get_ctx(pd->context);

    struct ibv_qp * qp = vctxt->cvmrdma.create_qp(pd, attr);
    if(! qp)
    {
        goto err;
    }

	struct verbs_qp *vqp = (struct verbs_qp *)qp;

	/* Add this QP to the CQ server. This gives us one end of the notification
	   channel ; the RPC server will push notifications there */
	struct ibv_cq * send_cq = attr->send_cq;
	assert(send_cq);
	struct pvrdma_cq * send_pcq = to_vcq(send_cq);

	NotificationQueue * nq = cq_server_add_qp(&send_pcq->cvmrdma_cq_server, qp->ibv_qp.qp_num);
    if(! nq)
    {
        fprintf(stderr, "failed to register QP on CQ server\n");
        goto free_qp;
    }

	/* Create the corresponding RPC object. Pass the end of the notification channel here.
	   No risk of a race: we haven't returned the QPN to the user yet, the remote cannot possibly
	   know it yet.
	*/
	cvmrdma_server_register_qp(ctxt->network_server, ppd->bounce_metadata, nq, qp->ibv_qp.qp_num);

	/* We can only get the capability to the remote QP once we know its QPN, this will be done
	   in modify_qp */
	vqp->remote_qnc = NULL;
	vqp->tx_ctr = 0;

	return qp;

free_qp:
    vctxt->cvmrdma.destroy_qp(qp);
err:
	return NULL;


}

int cvmrdma_modify_qp(struct ibv_qp *ibqp, struct ibv_qp_attr *attr,
		     int attr_mask)
{
    struct verbs_context * vctxt = verbs_get_ctx(ibqp->context);
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

				/* Blocking: get the qp notifier capability from the remote over the network */
				qp->remote_qnc = get_qp_notification_client(vctxt->cvmrdma.network_client_connection, vctxt->cvmrdma.network_client_cap, dest_qpn);

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



// HACKY: very ad-hoc message, at least use a header
struct cvmrdma_ring_message {
   uint64_t hca_va;
   uint64_t length;
   uint32_t rkey;
};
static int post_send_do_bounce(struct ibv_qp *ibqp, struct ibv_send_wr *wr, struct ibv_send_wr **bad_wr)
{
    struct verbs_qp * qp = (struct verbs_qp *)ibqp;
	struct ibv_pd * pd = ibqp->pd;
	struct verbs_context * ctx = verbs_get_ctx(ibqp->context);

	if(ibqp->qp_type != IBV_QPT_RC)
	{
		fprintf(stderr, "ERROR: for now CVMRDMA only supports RC\n");
		return EINVAL;
	}


	for(struct ibv_send_wr * this_wr = wr ; this_wr != NULL ; this_wr = this_wr->next)
	{
		switch(this_wr->opcode)
		{
			case IBV_WR_RDMA_WRITE:{
				/* Prepare the bounce buffer. We have to encrypt the data
				from the user regions into the bounce MRs. */

				if(wr->num_sge != 1)
				{
					fprintf(stderr, "ERROR: for now CVMRDMA only supports 1 sge per WR\n");
					*bad_wr = this_wr;
					return EINVAL;
				}

				struct ibv_sge sge = wr->sg_list[0];
				uint32_t rkey = this_wr->wr.rdma.rkey;

				/* Before doing anything, we can already notify the remote of the upcoming write */
				remote_qp_notify_write_rpc(
					ctx->cvmrdma.network_client_connection, qp->remote_qnc,
					ntohl(this_wr->imm_data),
					sge.addr,
					sge.length,
					rkey
				);

				/* Identify the local MR from the lkey and find the corresponding bounce buffer */
				struct bounce_entry bounce_info = get_bounce_metadata_for_lkey(pd->bounce_metadata, sge.lkey);
				size_t offt = sge.addr - bounce_info.hca_va;

				void * user_start = bounce_info.user_buffer + offt;
				void * bounce_start = bounce_info.bounce_buffer + offt;

				/* Compute the nonce to use for the encryption from the counter*/
				uint32_t nonce_counter = qp->tx_ctr++;
				unsigned char nonce[3] = {
					0x0000FFu & nonce_counter,
					0x00FF00u & nonce_counter,
					0xFF0000u & nonce_counter
				};

				/* HACK: fake mac to check transmission */
				unsigned char mac[2] = {0x20, 0x10};


        		DEBUG("Encrypting 0x%" PRIx32 " B from %p to %p with nonce %x %x %x\n", sge.length, user_start, bounce_start, nonce[0], nonce[1], nonce[2]);

				/* TODO: encrypt instead of copy! Populate mac */
				memcpy(bounce_start, user_start, sge.length);

				DEBUG("mac = %x %x\n", mac[0], mac[1]);

				/* After the bounce, the sg-list in the WR is valid.
				   We need to notify the remote of the incoming payload and send the MAC.

				   Do both by
				   - for IMM: changing WRITE to WRITE_WITH_IMM and send the MAC as immediate.
				   - for WRITE_WITH_IMM: replace the immediate with the MAC
				
					The actual immediate has already been sent with the RPC.
				*/
				
				__be32 mac_imm = mac[0] & (mac[1] << 8);

				this_wr->opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
				this_wr->imm_data = mac_imm;


				break;
			}
			default: {
				fprintf(stderr, "ERROR: for now CVMRDMA only supports WRITE\n");
				*bad_wr = this_wr;
				return EINVAL;
			}
		}

	}

	return 0;
}


int pvrdma_post_send(struct ibv_qp *ibqp, struct ibv_send_wr *wr,
		     struct ibv_send_wr **bad_wr)
{
    struct verbs_context * vctxt = verbs_get_ctx(ibqp->context);
    int ret = 0;

    /*
	CVMRDMA: do the bounce and modify the WRs to use the bounce buffers
    
    on WRITE
	- RPC send the dest hca_addr, rkey, imm (before starting encryption, to
	  give IP based RPC a head start)
	- find the local (base_addr, bounce_addr) for lkey
	- xchachapoly1305 the plaintext region to the bounce buffer,
	  keeping the same offset
	- change WRITE into WRITE_WITH_IMM to notify the remote.
	- send the MAC as the immediate.
	*/

	ret = post_send_do_bounce(qp, wr, bad_wr);
	if(ret < 0)
	{
		return ret;
	}

    ret = vctxt->cvmrdma.post_send(ibqp, wr, bad_wr);

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



struct ibv_mr *cvmrdma_reg_mr(struct ibv_pd *pd, void *addr, size_t length,
        uint64_t hca_va, int access)
{
    struct verbs_context * vctxt = verbs_get_ctx(pd->context);

	int bounce_prots;
	void *bounce;
    struct ibv_mr * ibmr;
    struct verbs_mr * vmr;
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
	if ((access & IBV_ACCESS_LOCAL_WRITE) ||
	    (access & IBV_ACCESS_REMOTE_WRITE))
		bounce_prots |= PROT_READ;

	/* TODO: allocate the bounce buffer in unencrypted memory */
	bounce = mmap(0, length, bounce_prots,
		      MAP_ANONYMOUS | MAP_SHARED_VALIDATE, -1, 0);
	if (bounce == MAP_FAILED) {
		goto err;
	}

    /* reg_mr the bounce buffer as the MR through the provider */
    ibmr = vctxt->cvmrdma.reg_mr(pd, bounce, length, hca_va, access);
    if(! ibmr)
    {
        goto free_bounce;
    }

    vmr = verbs_get_mr(ibmr);

	/* Make this bounce MR's metadata available through its lkey or rkey */
	register_bounce_mr(pd->bounce_metadata,
		ibmr.lkey,
		ibmr.rkey,
		hca_va,
		addr,
		bounce
	);



free_bounce:
	munmap(bounce, length);
err:
    return NULL;

}


struct verbs_context_ops cvmrdma_ops {
    .alloc_pd = cvmrdma_alloc_pd,
    .create_cq = cvmrdma_create_cq,
    .create_qp = cvmrdma_create_qp,
    // .dealloc_pd = cvmrdma_dealloc_pd,
    // .dereg_mr = cvmrdma_dereg_mr,
    // .destroy_cq = cvmrdma_destroy_cq,
    // .destroy_qp = cvmrdma_destroy_qp,
    // .free_context = cvmrdma_free_context,
    .modify_qp = cvmrdma_modify_qp,
    // .poll_cq = cvmrdma_poll_cq,
    // .post_recv = cvmrdma_post_recv,
    .post_send = cvmrdma_post_send,
    // .reg_dmabuf_mr = cvmrdma_reg_dmabuf_mr,
    .reg_mr = cvmrdma_reg_mr
};



static struct verbs_context *cvmrdma_alloc_context(struct ibv_device *ibdev,
						  int cmd_fd,
						  void *private_data)
{
    struct verbs_device * vdev = verbs_get_device(ibdev);

    /* Allocate provider context */
    struct verbs_context * context = vdev->cvmrdma.alloc_context(ibdev, cmd_fd, private_data);
    if(! context) {
		goto err;
    }
	/* Start an RPC server here,  */
	context->cvmrdma.network_server = cvmrdma_start_network_server();

	/* Connect to the remote's RPC server */
	char const * server_addr = getenv("PVRDMA_REMOTE_ADDR");
	if(! server_addr)
	{
		fprintf(stderr, "ERROR: No environment variable PVRDMA_REMOTE_ADDR.\n");
		goto uninit_and_free;
	}
	context->cvmrdma.network_client_connection = cvmrdma_start_session(server_addr);

	/* Get the root capability to the remote's RPC's API */
	context->cvmrdma.network_client_cap = cvmrdma_client_init(context->cvmrdma.network_client_connection);
	

	return &context->context;

// close_server:
//    TODO: close network server
uninit_and_free:
	context->cvmrdma.free_context(context->context);
err:
	return NULL;
}

struct verbs_device_ops cvmrdma_device_ops {
    .alloc_context = cvmrdma_alloc_context,
}