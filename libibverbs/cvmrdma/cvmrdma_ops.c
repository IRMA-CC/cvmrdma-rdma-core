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


struct verbs_context_ops cvmrdma_ops {
    .alloc_pd = cvmrdma_alloc_pd,
    // .create_cq = cvmrdma_create_cq,
    // .create_qp = cvmrdma_create_qp,
    // .dealloc_pd = cmvrdma_dealloc_pd,
    // .dereg_mr = cmvrdma_dereg_mr,
    // .destroy_qp = cmvrdma_destroy_qp,
    // .free_context = cmvrdma_free_context,
    // .modify_qp = cmvrdma_modify_qp,
    // .poll_cq = cmvrdma_poll_cq,
    // .post_recv = cmvrdma_post_recv,
    // .post_send = cmvrdma_post_send,
    // .reg_dmabuf_mr = cmvrdma_reg_dmabuf_mr,
    // .reg_mr = cmvrdma_reg_mr
};