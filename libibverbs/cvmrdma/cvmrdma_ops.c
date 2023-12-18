#include "cvmrdma_ops.h"


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
	free(pd);
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
    if(! mr)
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
    // .dealloc_pd = cmvrdma_dealloc_pd,
    // .dereg_mr = cmvrdma_dereg_mr,
    // .destroy_cq = cmvrdma_destroy_cq,
    // .destroy_qp = cmvrdma_destroy_qp,
    // .free_context = cmvrdma_free_context,
    .modify_qp = cvmrdma_modify_qp,
    // .poll_cq = cmvrdma_poll_cq,
    // .post_recv = cmvrdma_post_recv,
    // .post_send = cmvrdma_post_send,
    // .reg_dmabuf_mr = cmvrdma_reg_dmabuf_mr,
    .reg_mr = cmvrdma_reg_mr
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