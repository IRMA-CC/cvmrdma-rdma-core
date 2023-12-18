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


struct ibv_mr *cmvrdma_reg_mr(struct ibv_pd *pd, void *addr, size_t length,
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
    // .create_qp = cvmrdma_create_qp,
    // .dealloc_pd = cmvrdma_dealloc_pd,
    // .dereg_mr = cmvrdma_dereg_mr,
    // .destroy_cq = cmvrdma_destroy_cq,
    // .destroy_qp = cmvrdma_destroy_qp,
    // .free_context = cmvrdma_free_context,
    // .modify_qp = cmvrdma_modify_qp,
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