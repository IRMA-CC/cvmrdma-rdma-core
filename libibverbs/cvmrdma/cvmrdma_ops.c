#include "cvmrdma_ops.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <arpa/inet.h> /* ntohl */

#include "bounce_metadata.h"
#include "cvmrdma_cq_server.h"
#include "cvmrdma_network_client.h"
#include "cvmrdma_network_server.h"
#include "notification_queue.h"

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


static struct ibv_qp *cvmrdma_create_qp(struct ibv_pd *pd,
				struct ibv_qp_init_attr *attr)
{
    struct verbs_context * ctxt = verbs_get_ctx(pd->context);

    struct ibv_qp * ibqp = ctxt->cvmrdma.create_qp(pd, attr);
    if(! ibqp)
    {
        goto err;
    }

	struct verbs_qp * qp = (struct verbs_qp *)ibqp;

	/* Add this QP to the CQ server. This gives us one end of the notification
	   channel ; the RPC server will push notifications there */
	struct ibv_cq * send_ibcq = attr->send_cq;
	assert(send_ibcq);
	struct verbs_cq * send_cq = (struct verbs_cq *)send_ibcq;

	NotificationQueue * nq = cq_server_add_qp(&send_cq->cvmrdma_cq_server, ibqp->qp_num);
    if(! nq)
    {
        fprintf(stderr, "failed to register QP on CQ server\n");
        goto free_qp;
    }

	/* Create the corresponding RPC object. Pass the end of the notification channel here.
	   No risk of a race: we haven't returned the QPN to the user yet, the remote cannot possibly
	   know it yet.
	*/
	cvmrdma_server_register_qp(ctxt->cvmrdma.network_server, pd->bounce_metadata, nq, ibqp->qp_num);

	/* We can only get the capability to the remote QP once we know its QPN, this will be done
	   in modify_qp */
	qp->remote_qnc = NULL;
	qp->tx_ctr = 0;

	return ibqp;

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

static int cvmrdma_post_recv(struct ibv_qp *ibqp, struct ibv_recv_wr *wr,
		struct ibv_recv_wr **bad_wr)
{
	/* WRITE_WITH_IMM consumes an RR even though it doesn't write a payload to the MR! So the user
	   must be able to post_recvs.

	   Since the bounce buffer is registered with the hca_va, a valid post recv for the original buffer
	   is a valid post recv for the bounce buffer. We can simply pass the request unmodified.   
	*/
	struct verbs_context * ctx = verbs_get_ctx(ibqp->context);
	return ctx->cvmrdma.post_recv(ibqp, wr, bad_wr);
}


static int post_send_do_bounce(struct ibv_qp *ibqp, struct ibv_send_wr *wrs, struct ibv_send_wr **bad_wr)
{
    struct verbs_qp * qp = (struct verbs_qp *)ibqp;
	struct ibv_pd * pd = ibqp->pd;
	struct verbs_context * ctx = verbs_get_ctx(ibqp->context);

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
			case IBV_WR_RDMA_WRITE:
			case IBV_WR_RDMA_WRITE_WITH_IMM: {
				/* Prepare the bounce buffer. We have to encrypt the data
				from the user regions into the bounce MRs. */

				if(this_wr->num_sge != 1)
				{
					fprintf(stderr, "ERROR: for now CVMRDMA only supports 1 sge per WR\n");
					*bad_wr = this_wr;
					return EINVAL;
				}

				struct ibv_sge sge = this_wr->sg_list[0];
				uint32_t rkey = this_wr->wr.rdma.rkey;

				uint32_t imm = ntohl(this_wr->imm_data);
				bool has_imm = (this_op == IBV_WR_RDMA_WRITE_WITH_IMM);

				/* Before doing anything, we can already notify the remote of the upcoming write */
				remote_qp_notify_write_rpc(
					ctx->cvmrdma.network_client_connection, qp->remote_qnc,
					sge.addr,
					sge.length,
					rkey,
					imm,
					has_imm
				);

				/* Identify the local MR from the lkey and find the corresponding bounce buffer */
				struct bounce_entry bounce_info = get_bounce_metadata_for_lkey(pd->bounce_metadata, sge.lkey);
				size_t offt = sge.addr - bounce_info.hca_va;

				void * user_start = bounce_info.user_buffer + offt;
				void * bounce_start = bounce_info.bounce_buffer + offt;

				/* Compute the nonce to use for the encryption from the counter*/
				uint32_t nonce_counter = qp->tx_ctr++;
				unsigned char nonce[3] = {
                    0xFFu & (nonce_counter >> 0),
                    0xFFu & (nonce_counter >> 8),
                    0xFFu & (nonce_counter >> 16),
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
				
					The actual immediate (if any) has already been sent with the RPC.
				*/
				
				__be32 mac_imm = mac[0] | (mac[1] << 8);

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


static int cvmrdma_post_send(struct ibv_qp *ibqp, struct ibv_send_wr *wr,
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

	ret = post_send_do_bounce(ibqp, wr, bad_wr);
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
    // .poll_cq = cvmrdma_poll_cq,
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
	

	return context;

// close_server:
//    TODO: close network server
uninit_and_free:
	context->cvmrdma.free_context(&context->context);
err:
	return NULL;
}

struct verbs_device_ops cvmrdma_device_ops = {
    .alloc_context = cvmrdma_alloc_context,
};
