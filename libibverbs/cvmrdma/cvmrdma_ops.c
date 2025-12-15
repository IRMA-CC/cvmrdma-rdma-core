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
//#include "cvmrdma_network_client_priv.h"
//#include "cvmrdma_network_server_priv.h"
//#include "cvmrdma_network_evloop_priv.h"
#include "recv_metadata.h"
#include "read_metadata.h"
#include "wc_queue.h"
#include "wr_queue.h"
#include "dma_heap.h"
#include "nonce_queue.h"
#include "chunk_queue.h"
#include "cvmrdma_manager_client.h"
#include "cvmrdma_ack.h"

#include "debug.h"


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

	DEBUG("Allocated PD %p\n", pd);

	//fprintf(stderr, "Trying to start manager\n");
	// try_to_start_manager();

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
	/* Create the recv CQ */
	struct ibv_cq * ibcq_recv = ctxt->prov_ops.create_cq(ibcontext, 16384, NULL, 0);
	if(! ibcq_recv)
	{
		fprintf(stderr, "failed to create backend recv CQ for the CQ server\n");
		exit(1);
	}
	/* see ibv_create_cq */
	verbs_init_cq(ibcq_recv, ibcontext, NULL, NULL);
	cq->prov_cq_recv = ibcq_recv;

	/*ret = ctxt->prov_ops.req_notify_cq(ibcq_recv, 0);
	if(ret)
	{
		fprintf(stderr, "Failed to request CQ notifications for recv %d\n", ret);
		exit(1);
	}*/

	/* Create send CQ */
	struct ibv_cq * ibcq_send = ctxt->prov_ops.create_cq(ibcontext, 16384, NULL, 0);
	if(! ibcq_send)
	{
		fprintf(stderr, "failed to create backend send CQ for the CQ server\n");
		exit(1);
	}
	/* see ibv_create_cq */
	verbs_init_cq(ibcq_send, ibcontext, NULL, NULL);
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

	return 0;
}

static int detach_qp_from_cq_manager(struct cvmrdma_context * ctxt, struct cvmrdma_qp * qp)
{
	uint32_t qpn = qp->user_qp.qp.qp_num;

	int err = qp_metadata_manager_unregister_qp(ctxt->qp_metadata_manager, qpn);
	if(err)
	{
		fprintf(stderr, "failed to unregister QP from CQ server\n");
		return -1;
	}

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


struct start_args {
    struct cvmrdma_qp *qp;
	uint16_t dlid;
    uint8_t  port_num;
};



void *start_control_queue(void *args)
{
	struct start_args * start_args = (struct start_args *)args;
	struct cvmrdma_qp * qp = start_args->qp;
	uint16_t dlid = start_args->dlid;
    uint8_t port_num = start_args->port_num;

    struct cvmrdma_context * ctxt = cvmrdma_get_ctx(qp->user_qp.qp.context);
    int ret = 0;

	pthread_mutex_lock(&qp->remote_params_lock);
	while (!qp->remote_params_set) {
		pthread_cond_wait(&qp->remote_params_set_cv, &qp->remote_params_lock);
	}
	pthread_mutex_unlock(&qp->remote_params_lock);

	// fprintf(stderr, "Remote params: ring rkey %u, control QPN %u, control rkey %u, control PSN %u\n", qp->remote_ring_rkey, qp->remote_control_qpn, qp->remote_control_rkey, qp->remote_control_psn);


    struct ibv_qp_attr control_qp_attr = {0};
    control_qp_attr.qp_state = IBV_QPS_RTR;
    control_qp_attr.ah_attr.is_global = 0;
    control_qp_attr.ah_attr.dlid = dlid;
    control_qp_attr.ah_attr.port_num = port_num;
    control_qp_attr.path_mtu = IBV_MTU_4096;
    control_qp_attr.dest_qp_num = qp->remote_control_qpn;
    control_qp_attr.rq_psn = qp->remote_control_psn;

    int control_qp_attr_mask = IBV_QP_STATE;
    control_qp_attr_mask |= IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN;

    control_qp_attr.max_dest_rd_atomic = 1;
    control_qp_attr.min_rnr_timer = 12;
    control_qp_attr.timeout = 21;

    control_qp_attr_mask |= IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;

    ret = ctxt->prov_ops.modify_qp(qp->control_qp, &control_qp_attr, control_qp_attr_mask);
    if(ret) {
        fprintf(stderr, "Failed to modify control QP to RTR (%d)\n", ret);
        exit(1);
    }

    for (int i = 0; i < 4096; i++) {
		// Post some dummy recv WRs to be able to receive control messages
		// we can't submit empty WRs, as then post_recv fails
        struct ibv_sge dummy_sge = {
            .addr   = (uintptr_t)qp->control_buf_va + 1 * sizeof(struct control_metadata),          /* any valid MR addr */
            .length = 1,                                      /* minimal non-zero length */
            .lkey   = qp->control_buf_mr->lkey
        };
        struct ibv_recv_wr recv_wr = {
            .wr_id   = 0,
            .sg_list = &dummy_sge,
            .num_sge = 1,
            .next    = NULL
        };
        struct ibv_recv_wr * bad_wr = NULL;
        ret = ctxt->prov_ops.post_recv(qp->control_qp, &recv_wr, &bad_wr);
        if(ret) {
            fprintf(stderr, "Failed to post recv initial RR for control QP (%d)\n", ret);
            exit(1);
        }
    }

    control_qp_attr_mask = IBV_QP_STATE | IBV_QP_SQ_PSN;
    control_qp_attr = (struct ibv_qp_attr){
        .qp_state = IBV_QPS_RTS,
        .sq_psn = 0,
    };
    control_qp_attr.sq_psn = qp->local_control_psn;
    control_qp_attr_mask |= IBV_QP_SQ_PSN;

    control_qp_attr.timeout = 14;
    control_qp_attr.retry_cnt = 7;
    control_qp_attr.rnr_retry = 7;
    control_qp_attr.max_dest_rd_atomic = 1;

    control_qp_attr_mask |= IBV_QP_MAX_QP_RD_ATOMIC | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_TIMEOUT;

    ret = ctxt->prov_ops.modify_qp(qp->control_qp, &control_qp_attr, control_qp_attr_mask);
    if(ret) {
        fprintf(stderr, "Failed to modify control QP to RTS (%d)\n", ret);
        exit(1);
    }

	pthread_mutex_lock(&qp->control_qp_connection_lock);
	qp->control_qp_connected = true;
	pthread_cond_signal(&qp->control_qp_connection_cv);
	pthread_mutex_unlock(&qp->control_qp_connection_lock);

	return NULL;
}



static int cvmrdma_modify_qp(struct ibv_qp *ibqp, struct ibv_qp_attr *attr,
		     int attr_mask)
{
	struct cvmrdma_qp * qp = (struct cvmrdma_qp *)ibqp;
	struct ibv_qp * prov_qp = qp->provider_qp;

    struct cvmrdma_context * ctxt = cvmrdma_get_ctx(qp->user_qp.qp.context);
    int ret = 0;

	attr->qp_access_flags |= IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE;
	attr_mask |= IBV_QP_ACCESS_FLAGS;

    ret = ctxt->prov_ops.modify_qp(prov_qp, attr, attr_mask);
    if(ret)
    {
        goto err;
    }

	/* Update the state on the user-visible qp fields */
	qp->user_qp.qp.state = prov_qp->state;

	if((attr_mask & IBV_QP_STATE) && (attr->qp_state == IBV_QPS_INIT)){
		if (qp->control_qp->state != IBV_QPS_RESET)
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

		ret = ctxt->prov_ops.modify_qp(qp->control_qp, &control_qp_attrs, qp_attr_mask);
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

		qp->local_control_psn = lrand48() & 0xFFFFFF;
		// fprintf(stderr, "[MODIFY_QP] local control PSN %u\n", qp->local_control_psn);

		switch(ibqp->qp_type)
		{
			case IBV_QPT_RC:
			case IBV_QPT_UC: {
				assert(attr_mask & IBV_QP_DEST_QPN);
				uint32_t dest_qpn = attr->dest_qp_num;

				DEBUG("[MODIFY_QP] Obtain notification channel cap from remote for its QP with QPN 0x%" PRIx32 "\n", dest_qpn);

				/*fprintf(stderr, "[MODIFY_QP] Sending connection request to remote QPN %" PRIx32 " with values: local ring rkey %u, local control rkey %u, local control QPN 0x%" PRIx32 ", local control PSN %u\n",
						dest_qpn,
						qp->recv_ringbuf_mr->rkey,
						qp->control_buf_mr->rkey,
						qp->control_qp->qp_num,
						qp->local_control_psn);*/

				pthread_t client_thread;
				start_socket_server(&client_thread, qp->client_socket, qp);
				if (is_local) {
					send_and_wait_for_key(qp->client_socket, qp, qp->local_addr, dest_qpn, qp->recv_ringbuf_mr->rkey, qp->control_buf_mr->rkey, qp->control_qp->qp_num, qp->local_control_psn);
				} else {
					send_and_wait_for_key(qp->client_socket, qp, qp->remote_addr, dest_qpn, qp->recv_ringbuf_mr->rkey, qp->control_buf_mr->rkey, qp->control_qp->qp_num, qp->local_control_psn);
				}

				// disconnect_from_manager(qp->client_socket, qp->user_qp.qp.qp_num);

				/*fprintf(stderr, "[MODIFY_QP] Received remote ring rkey %u, remote control rkey %u, remote control QPN 0x%" PRIx32 ", remote control PSN %u\n",
						qp->remote_ring_rkey,
						qp->remote_control_rkey,
						qp->remote_control_qpn,
						qp->remote_control_psn);*/

				// start control queue
				pthread_t control_thread;
				struct start_args * start_args = calloc(1, sizeof(struct start_args));
				start_args->qp = qp;
				start_args->dlid = attr->ah_attr.dlid;
				start_args->port_num = attr->ah_attr.port_num;
				pthread_create(&control_thread, NULL, start_control_queue, (void *)start_args);
				pthread_detach(control_thread);

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

int wait_for_slot(struct cvmrdma_context * ctxt, struct cvmrdma_qp *qp){
	/* LOCK */
	pthread_mutex_lock(&qp->send_lines_full_lock);

	int counter = 0;
	while(qp->send_lines_full >= cvmrdma_ring_entries)
	{
		/*counter++;
		if (counter == 1){
			fprintf(stderr, "All send slots full, waiting for ack...\n");
		}*/
		poll_for_ack_once(ctxt, qp, true);

		if (atomic_load(&qp->stop_requested)) {
			fprintf(stderr, "Stop requested while polling for send line, exiting save_send\n");
			return -1;
		}
	}

	/*if (counter > 1){
		fprintf(stderr, "Got send slot after %d polls\n", counter);
	}*/

	// qp->next_send_slot = (slot_id + 1) % cvmrdma_ring_entries;
	qp->send_lines_full++;
	pthread_mutex_unlock(&qp->send_lines_full_lock);

	return 0;
}

int save_send(struct cvmrdma_context * ctxt, struct cvmrdma_qp * qp, struct ibv_qp_ex * prov_qpx,
		uint64_t send_offt,
		uint32_t remote_ring_rkey,
		uint32_t local_provider_lkey,
		uint32_t size){

	int err = 0;

	// try to dequeue if we have too many sends in transit
	// and only submit again when we have space
	// int retry = 0;
	while (qp->send_in_transit_counter >= 4096) {
		/*if (retry == 0) {
			fprintf(stderr, "Too many sends in transit, try to dequeue...\n");
			retry = 1;
		}*/
		cvmrdma_cq_server_run_once_if_idle_send(ctxt, qp->provider_cq, NULL);

		if (atomic_load(&qp->stop_requested)) {
			fprintf(stderr, "Stop requested while save send, exiting save_send\n");
			return 0;
		}
	}

	ibv_wr_start(prov_qpx);
	ibv_wr_rdma_write_imm(prov_qpx,
		remote_ring_rkey,
		send_offt,
		0
	);
	ibv_wr_set_sge(prov_qpx,
		local_provider_lkey,
		send_offt,
		size
	);

	err = ibv_wr_complete(prov_qpx);
	if (!err) {
		pthread_mutex_lock(&qp->ibv_send_in_transit_lock);
		qp->send_in_transit_counter += 1;
		pthread_mutex_unlock(&qp->ibv_send_in_transit_lock);
	} else 
	{
		if (err == EINVAL) {
            struct ibv_qp_attr qa = {0};
            struct ibv_qp_init_attr qia = {0};
            ctxt->prov_ops.query_qp(qp->provider_qp, &qa, IBV_QP_STATE, &qia);
            fprintf(stderr,
                "[SEND QPN %x] EINVAL posting WWI: qp_state=%d type=%d len=%u lkey=0x%x "
                "remote_offt=0x%lx rkey=0x%x mr_base=%p mr_len=%zu\n",
                qp->user_qp.qp.qp_num, qa.qp_state, qp->provider_qp->qp_type, size, local_provider_lkey,
                (unsigned long)send_offt, remote_ring_rkey,
                qp->send_ringbuf_mr->addr, qp->send_ringbuf_mr->length);
        }
        fprintf(stderr, "Failed to post RDMA WRITE WITH IMM in save_send (%d)\n", err);
        exit(1);
	}

	return err;
}



_Atomic uint64_t cvmrdma_global_send_wr_id = 0;

// these variables are all just used for debugging
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
// cvm_time_t wr_wait_for_sem; // Track time that it takes to acquire the semaphore
// cvm_time_t wr_cache_atomic_load; // Track time that it takes to do the atomic load for cache checking
// cvm_time_t wr_cache_write_protect; // Track time that it takes to write protect the chunk
// cvm_time_t wr_cache_atomic_store; // Track time that it takes to do the atomic store for cache marking
// cvm_time_t wr_generate_nonce;
// cvm_time_t wr_push_chunk_tracking; // Tracks time taken to push chunk tracking information
cvm_time_t wr_encrypt_metadata; // Tracks time taken to encrypt metadata for the write request	
cvm_time_t wr_encrypt_payload; // Tracks time taken to encrypt the payload for the write request
cvm_time_t wr_ibv_post_send; // Tracks time taken to call ibv_post_send


uint64_t REPORT = 1000;

int post_send_do_bounce(struct cvmrdma_qp * qp, struct ibv_send_wr *wrs, struct ibv_send_wr **bad_wr)
{

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

		// sanitize the sge list by removing 0-length sges
		struct ibv_sge sgl_sanitized[nsge];
		size_t sanitized_nsge = 0;
		for(size_t i = 0 ; i < nsge ; i++) {
			if (sgl[i].length == 0) {
				continue;
			}
			sgl_sanitized[sanitized_nsge] = sgl[i];
			sanitized_nsge++;
		}

		sgl = sgl_sanitized;
		nsge = sanitized_nsge;

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
			size_t sge_chunks = DIVIDE_ROUND_UP(sgl[i].length, OPT_cvmrdma_chunk_bytes);
			total_chunks += sge_chunks;
		}

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

			uint64_t nonce_metadata[cvmrdma_aead_NPUB_WORDS] = {0};
			pthread_mutex_lock(&qp->send_counter_lock);
			nonce_metadata[0] = qp->send_counter;
			qp->send_counter += 2;
			pthread_mutex_unlock(&qp->send_counter_lock);

			get_time(&wr_send_nonce);

			struct ibv_sge * cur_sge = &sgl[cur_sge_idx];

			struct cvmrdma_mr * cur_mr = get_bounce_metadata_for_lkey(pd->bounce_metadata, cur_sge->lkey);
			uint64_t offt = (cur_sge->addr - cur_mr->hca_va) + cur_sge_offt;

			/* If this is the last chunk, we'll need to notify that it's a WC generating chunk (if op != WRITE) */
			size_t chunks_num = DIVIDE_ROUND_UP(cur_sge->length, OPT_cvmrdma_chunk_bytes);
			
			// is this the last chunk in this sge?
			bool last_chunk_sge = chunks_in_wr + 1 == chunks_num;
			// is this the last chunk in the whole wr?
			bool last_chunk = (chunks_in_wr + 1 == total_chunks);

			/* location in memory */
			unsigned char * ptxt = cur_mr->user_mr.ibv_mr.addr + offt;
			//fprintf(stderr, "[SEND QPN %x] chunk_addr %p, chunk aligned_offset 0x%lx, chunk_idx %lu\n", qp->user_qp.qp.qp_num, ptxt, alinged_offset, chunk_idx);

			// the sge could border the end of the mr, so we need to make sure we do not read past the end if the chunk is larger than the remaining mr size
			uint64_t send_len = offt + OPT_cvmrdma_chunk_bytes > cur_mr->user_mr.ibv_mr.length ? cur_mr->user_mr.ibv_mr.length - offt : OPT_cvmrdma_chunk_bytes;
			uint64_t true_payload_bytes = 0; 

			if (!last_chunk_sge){
				true_payload_bytes = OPT_cvmrdma_chunk_bytes;
				cur_sge_offt += true_payload_bytes;
			} else {
				// we could fill the rest of the chunk with data from the next sge, but this would introduce a memory region to stage the data, adding copies.
				// therefore we just send the remaining data of an sge in a chunk, and leave the rest unused.
				true_payload_bytes = cur_sge->length - cur_sge_offt;
				cur_sge_idx++;
				cur_sge_offt = 0;
			}

			unsigned long long notif_clen;
			struct cq_server_notification notif = {0};

			if(is_write)
			{
				/* This chunk will be directly written at the destination */
				notif.hca_addr = remote_addr + bytes_sent; /* include the current offt */
				notif.op = this_op;
				notif.rkey = rkey;
				notif.imm = this_imm;
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
				notif.chunk_len = true_payload_bytes;
				notif.last_chunk = last_chunk;
				notif.payload_len = send_len;
			}

			/* Get a slot. If all the slots are full, wait for a slot to be acked */
			/* We must get the slot here in order to not also hold any chunk lock which 
			 * would block the CQ server.
			*/
			int slot_id = (nonce_metadata[0]/2) % cvmrdma_ring_entries;
			{
				int ret = wait_for_slot(ctxt, qp);
				if (ret < 0) {
					return 0;
				}
			}
			get_time(&wr_wait_for_recv_slot);


			unsigned char * slot = qp->send_ringbuf_va + (slot_id * cvmrdma_ring_slot_bytes(ctxt->aead));

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

			get_time(&wr_encrypt_metadata);

			nonce_metadata[0] += 1; // increment nonce for payload

			unsigned long long payload_clen = send_len;
			err = cvmrdma_encrypt(
				slot + notif_clen, /* start after the notif */
				&payload_clen,
				ptxt, send_len,
				NULL, 0,
				NULL,
				(unsigned char *)nonce_metadata,
				(unsigned char const *)shared_key,
				aead
			);
			if(err)
			{
				fprintf(stderr, "Failed to encrypt chunk!\n");
				exit(1);
			}

			get_time(&wr_encrypt_payload);

			/****** POST SEND WWI ******/
			/* Synchronize the ring buffer with the remote copy using a WWI.
			   Rings MR are 0-indexed. The send ring starts at 0, the recv 
			   ring starts at cvmrdma_ring_entries * CVMRDMA_CHUNK_SIZE.
			*/
			uint32_t local_provider_lkey = qp->send_ringbuf_mr->lkey;
			uint32_t remote_ring_rkey = qp->remote_ring_rkey;
			assert(remote_ring_rkey != 0);

			uint64_t send_offt = slot_id * cvmrdma_ring_slot_bytes(ctxt->aead);
			prov_qpx->wr_id = cvmrdma_global_send_wr_id++;

			while (nonce_metadata[0] - 1 != qp->next_send_counter) {
				// this is needed to ensure send exits when stop is requested
				if (atomic_load(&qp->stop_requested)) {
					fprintf(stderr, "Stop requested while waiting for order, exiting save_send\n");
					return 0;
				}
				// pthread_cond_wait(&qp->ibv_cv, &qp->ibv_lock);
			}
			pthread_mutex_lock(&qp->ibv_lock);


			// only send the encrypted metadata + encrypted payload
			// the rest of the chunk is left uninitialized and not sent
			// this saves bandwidth and reduces the amount of data that needs to be encrypted
			int ret = save_send(ctxt, qp, prov_qpx, send_offt, remote_ring_rkey, local_provider_lkey, (uint32_t) cvmrdma_ring_slot_bytes(ctxt->aead));
			if (ret < 0) {
				return -1;
			}

			qp->next_send_counter += 2;
			pthread_mutex_unlock(&qp->ibv_lock);
			// pthread_cond_broadcast(&qp->ibv_cv);

			get_time(&wr_ibv_post_send);

			bytes_sent += true_payload_bytes;
			chunks_in_wr++;
		}

		assert(bytes_sent == total_bytes);
		assert(chunks_in_wr == total_chunks);

		if (no_data_send) {
			DEBUG("[SEND QPN %x] No data send, sending no-data message\n", qp->user_qp.qp.qp_num);
			
			uint64_t nonce_metadata[cvmrdma_aead_NPUB_WORDS] = {0};
			pthread_mutex_lock(&qp->send_counter_lock);
			nonce_metadata[0] = qp->send_counter;
			qp->send_counter += 2;
			pthread_mutex_unlock(&qp->send_counter_lock);

			get_time(&wr_send_nonce);

			bool last_chunk = true; // no data send, so this is the last chunk

			unsigned long long notif_clen;
			struct cq_server_notification notif = {0};

			if(is_write)
			{
				/* This chunk will be directly written at the destination */
				notif.hca_addr = 0;
				notif.op = this_op;
				notif.rkey = 0;
				notif.imm = this_imm;
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

				// This is a little cheaky: We reuse this field to pass the total bytes to read
				// since no data is being sent, chunk_len is not used anyway. 
				notif.chunk_len = total_bytes;
				notif.last_chunk = true;
				notif.payload_len = 0;
			}
			else // if send or read-internal or ack
			{
				/* All these chunks will be written to the same RR, until last_chunk is reached */
				notif.hca_addr = 0;
				notif.op = this_op;
				notif.rkey = 0;
				notif.imm = this_imm;
				notif.chunk_len = 0;
				notif.last_chunk = true;
				notif.payload_len = 0;
			}
			notif_clen = header_bytes;

			int slot_id = (nonce_metadata[0]/2) % cvmrdma_ring_entries;
			{
				int ret = wait_for_slot(ctxt, qp);
				if (ret < 0) {
					return 0;
				}
			}

			get_time(&wr_wait_for_recv_slot);

			unsigned char * slot = qp->send_ringbuf_va + (slot_id * cvmrdma_ring_slot_bytes(ctxt->aead));

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
			get_time(&wr_encrypt_metadata);
			wr_encrypt_payload = wr_encrypt_metadata; // no payload encryption for no-data messages

			uint32_t local_provider_lkey = qp->send_ringbuf_mr->lkey;
			uint32_t remote_ring_rkey = qp->remote_ring_rkey;
			assert(remote_ring_rkey != 0);

			prov_qpx->wr_id = cvmrdma_global_send_wr_id++;
			uint64_t send_offt = slot_id * cvmrdma_ring_slot_bytes(ctxt->aead);

			while (nonce_metadata[0] != qp->next_send_counter) {
				// fprintf(stderr, "[SEND QPN %x] Waiting for nonce %lx, got %lx\n", qp->user_qp.qp.qp_num, nonce_metadata[0], qp->next_send_counter);
				if (atomic_load(&qp->stop_requested)) {
					fprintf(stderr, "Stop requested while waiting for order, exiting save_send\n");
					return 0;
				}
				// pthread_cond_wait(&qp->ibv_cv, &qp->ibv_lock);
			}
			pthread_mutex_lock(&qp->ibv_lock);

			int ret = save_send(ctxt, qp, prov_qpx, send_offt, remote_ring_rkey, local_provider_lkey, (uint32_t) cvmrdma_ring_slot_bytes(ctxt->aead));
			if (ret < 0) {
				return -1;
			}

			qp->next_send_counter += 2;
			pthread_mutex_unlock(&qp->ibv_lock);
			// pthread_cond_broadcast(&qp->ibv_cv);

			get_time(&wr_ibv_post_send);
			
		}
	}

	/* try to dequeue some ACKs if we have many send lines full */
	if(qp->send_lines_full >= cvmrdma_ring_entries / cvmrdma_ack_notify_divider)
	{
		poll_for_ack_once(ctxt, qp, false);
	}
	return 0;
}




static int cvmrdma_post_send(struct ibv_qp *ibqp, struct ibv_send_wr *wr,
		     struct ibv_send_wr **bad_wr)
{
	get_time(&wr_submit_start);

	struct cvmrdma_qp * qp = (struct cvmrdma_qp *)ibqp;
	int ret = 0;

	pthread_mutex_lock(&qp->control_qp_connection_lock);
	while (!qp->control_qp_connected) {
		pthread_cond_wait(&qp->control_qp_connection_cv, &qp->control_qp_connection_lock);
	}
	pthread_mutex_unlock(&qp->control_qp_connection_lock);

	get_time(&wr_submit_end);
	for(struct ibv_send_wr * this_wr = wr ; this_wr != NULL ; this_wr = this_wr->next)
	{
		if (this_wr->opcode == IBV_WR_RDMA_READ_INTERNAL) {
			fprintf(stderr, "ERROR: RDMA_READ_INTERNAL not allowed to be used externally\n");
			return EINVAL;
		}

#ifndef DISABLE_SEND_LOOP
		wrq_push(qp->wr_queue, this_wr);
#else
		ret = post_send_do_bounce(qp, this_wr, bad_wr);
#endif
	}
	return ret;
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
	
	struct cvmrdma_mr * mr = calloc(1, sizeof(struct cvmrdma_mr));

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

#ifndef DISABLE_SEND_LOOP
int loop_send_do_bounce(void* arg) {
    struct cvmrdma_qp *qp = (struct cvmrdma_qp *)arg;

	while (!atomic_load(&qp->stop_requested)) {
		struct ibv_send_wr * wrs = wrq_front(qp->wr_queue);
		if(! wrs)
		{
			continue;
		}

		int err = post_send_do_bounce(qp, wrs, NULL);
		if(err)
		{
			fprintf(stderr, "post_send_do_bounce exited (%d)\n", err);
			break;
		}
		wrq_pop(qp->wr_queue);
	}
	return 0;
}
#endif

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


	qp->received_lines = 0;
	qp->received_lines_threshold = cvmrdma_ring_entries / cvmrdma_ack_notify_divider;

	qp->send_counter = 0;
	qp->next_send_counter = 0;
	qp->recv_counter = 0;
	pthread_mutex_init(&qp->send_counter_lock, NULL);
	pthread_mutex_init(&qp->recv_counter_lock, NULL);
	pthread_mutex_init(&qp->ibv_lock, NULL);
	pthread_cond_init(&qp->ibv_cv, NULL);

	/* Create the provider QP */
	struct ibv_qp_init_attr_ex attr_ex = {0};
	attr_ex.sq_sig_all = 1;
	/* Direct send and recv WCs to the CQ server */
	attr_ex.send_cq = c_cq->prov_cq_send;
	attr_ex.recv_cq = c_cq->prov_cq_recv;

	if (attr->qp_type != IBV_QPT_RC && attr->qp_type != IBV_QPT_UC) {
		fprintf(stderr, "Unsupported QP type %d\n", attr->qp_type);
		exit(1);
	}
	
	attr_ex.qp_type = attr->qp_type;

	/*struct ibv_device_attr dev_attr;
	err = ibv_query_device(ctxt, &dev_attr);
	if (err) {
		fprintf(stderr, "ibv_query_device() failed\n");
		exit(1);
	}*/

	/* Don't keep the user set QP capacity */
	attr_ex.cap.max_recv_sge = 30; /* max supported by device */
	attr_ex.cap.max_recv_wr = 4096; /* max supported by device */
	attr_ex.cap.max_send_sge = 30; /* max supported by device */
	attr_ex.cap.max_send_wr = 4096; /* max supported by device */
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


	/* Create the control Completion Queue */
	struct ibv_cq * control_ibcq = ctxt->prov_ops.create_cq(prov_ctxt, 16384, NULL, 0);
	if(!control_ibcq)
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
	control_attr_ex.cap.max_recv_wr = 4096;
	control_attr_ex.cap.max_send_sge = 16;
	control_attr_ex.cap.max_send_wr = 4096;
	control_attr_ex.cap.max_inline_data = 0; /* we're not using inline */

	control_attr_ex.pd = pd->provider_pd;
	control_attr_ex.send_ops_flags = IBV_QP_EX_WITH_RDMA_WRITE_WITH_IMM;
	control_attr_ex.comp_mask = IBV_QP_INIT_ATTR_PD | IBV_QP_INIT_ATTR_SEND_OPS_FLAGS;

    struct ibv_qp * control_qp = ctxt->prov_ops.create_qp_ex(prov_ctxt, &control_attr_ex);
    if(! control_qp)
    {
		fprintf(stderr, "Unable to create control provider QP (%d:%s)\n", errno, strerror(errno));
        goto err;
    }

	qp->control_qp = control_qp;


	/* Initialize user visible fields */
	qp->user_qp.qp.context = prov_ctxt;
	qp->user_qp.qp.qp_context = attr->qp_context;
	qp->user_qp.qp.pd = ibpd;
	qp->user_qp.qp.qp_num = ibqp->qp_num;
	qp->user_qp.qp.qp_type = ibqp->qp_type;
	/* user-qp send/recv CQ are the cvmrdma-CQ where the CQ server will push notifications */
	qp->user_qp.qp.recv_cq = attr->recv_cq;
	qp->user_qp.qp.send_cq = attr->send_cq;

	WRQueue * wr_queue = alloc_wr_queue();
	if(! wr_queue)
	{
		fprintf(stderr, "Failed to allocated WR queue storage.\n");
		goto free_qp;
	}
	qp->wr_queue = wr_queue;

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
			goto free_wr_queue;
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



    /* To perform the bounce, we'll need to read and write the ring buffers */
	size_t all_rings_bytes = cvmrdma_ring_slot_bytes(ctxt->aead) * cvmrdma_ring_entries;

	int recv_dmabuf_fd = -1;
	void * recv_ringbuf_va = NULL;
	DEBUG("Allocating ring buffer with size %zu for QP %" PRIx32 "\n",  (uint64_t) all_rings_bytes / (1024*1024), ibqp->qp_num);
	struct ibv_mr * recv_ringbuf_mr = alloc_public_mr(ctxt, pd, all_rings_bytes, &recv_dmabuf_fd, &recv_ringbuf_va);
    if(! recv_ringbuf_mr)
    {
        goto dereg_send_ring_mr;
    }

	qp->recv_ringbuf_mr = recv_ringbuf_mr;
	qp->recv_ringbuf_dmabuf_fd = recv_dmabuf_fd;
	qp->recv_ringbuf_va = recv_ringbuf_va;

	// fprintf(stderr, "QP %" PRIx32 " recv ring buffer rkey 0x%x, lkey 0x%x\n", ibqp->qp_num, recv_ringbuf_mr->rkey, recv_ringbuf_mr->lkey);

	int send_dmabuf_fd = -1;
	void * send_ringbuf_va = NULL;
	struct ibv_mr * send_ringbuf_mr = alloc_public_mr(ctxt, pd, all_rings_bytes, &send_dmabuf_fd, &send_ringbuf_va);
	if(! send_ringbuf_mr)
	{
		goto free_bounce_private;
	}

	qp->send_ringbuf_mr = send_ringbuf_mr;
	qp->send_ringbuf_dmabuf_fd = send_dmabuf_fd;
	qp->send_ringbuf_va = send_ringbuf_va;

	// fprintf(stderr, "QP %" PRIx32 " send ring buffer rkey 0x%x, lkey 0x%x\n", ibqp->qp_num, send_ringbuf_mr->rkey, send_ringbuf_mr->lkey);

	size_t control_buf_size = PAGE_SIZE;
	int control_dmabuf_fd = -1;
	void * control_buf_va = NULL;
	struct ibv_mr * control_buf_mr = alloc_public_mr(ctxt, pd, control_buf_size, &control_dmabuf_fd, &control_buf_va);
	if(! control_buf_mr)
	{
		goto dereg_send_ring_mr;
	}

	qp->control_buf_mr = control_buf_mr;
	qp->control_dmabuf_fd = control_dmabuf_fd;
	qp->control_buf_va = control_buf_va;


	qp->next_send_slot = 0;
	qp->next_recv_slot = 0;
	qp->send_lines_full = 0;
	qp->number_of_acked_lines = 0;

	qp->send_in_transit_counter = 0;
	qp->ack_send_in_transit_counter = 0;
	pthread_mutex_init(&qp->ibv_send_in_transit_lock, NULL);

	qp->user_sq_sig_all = attr->sq_sig_all;

	/* Initialize the mutex */
	err = pthread_mutex_init(&qp->send_lines_full_lock, NULL);
	if(err)
	{
		fprintf(stderr, "Failed to initialize the CVMRDMA send_lines_full_lock mutex %d\n", err);
		goto dereg_control_mr;
	}
	err = pthread_cond_init(&qp->send_lines_avail, NULL);
	if(err)
	{
		fprintf(stderr, "Failed to initialize the CVMRDMA send_lines_avail cond var %d\n", err);
		goto dereg_control_mr;
	}

	err = attach_qp_to_cq_manager(ctxt, qp);
	if(err)
	{
		fprintf(stderr, "Failed to attach QP %" PRIx32 " to its recv cq (%d)\n", ibqp->qp_num, err);
		goto dereg_control_mr;
	}

	// Since the actual application is likely to not poll the cq all the time,
	// we need another trigger to wake up the cq server.
	qp->stop_requested = 0;
	err = pthread_create(&qp->provider_comp_channel_thread, NULL, &cvmrdma_cq_wait_completion_event, (void *)qp);
	if (err)
	{
		fprintf(stderr, "Error creating cvmrdma_cq_wait_completion_event thread: %d\n", err);
		exit(1);
	}

#ifndef DISABLE_SEND_LOOP
	err = pthread_create(&qp->provider_send_channel_thread, NULL, &loop_send_do_bounce, (void *)qp);
	if (err)
	{
		fprintf(stderr, "Error creating cvmrdma_cq_wait_send_event thread: %d\n", err);
		exit(1);
	}
#endif

	/* We can only get the capability to the remote QP once we know its QPN, this will be done
	   in modify_qp. We'll also get the rkey to its ring */
	qp->remote_ring_rkey = 0;
	qp->remote_control_qpn = 0;
	qp->remote_control_rkey = 0;
	qp->remote_control_psn = 0;
	qp->remote_params_set = false;
	pthread_mutex_init(&qp->remote_params_lock, NULL);
	pthread_cond_init(&qp->remote_params_set_cv, NULL);

	qp->control_qp_connected = false;
	pthread_mutex_init(&qp->control_qp_connection_lock, NULL);
	pthread_cond_init(&qp->control_qp_connection_cv, NULL);

	char const * local_bind_addr = getenv("CVMRDMA_BIND_ADDR");
	if(! local_bind_addr)
	{
		fprintf(stderr, "ERROR: No environment variable CVMRDMA_BIND_ADDR.\n");
		goto err;
	} 

	qp->local_addr = calloc(1, 20);
	sprintf(qp->local_addr, "127.0.0.1:%s", local_bind_addr + 2);

	qp->remote_addr = NULL;

	connect_to_manager(qp->user_qp.qp.qp_num, &qp->client_socket);

	return &qp->user_qp.qp;

dereg_control_mr:
	free_public_mr(ctxt, control_buf_mr, control_dmabuf_fd, control_buf_va);
dereg_send_ring_mr:
	free_public_mr(ctxt, send_ringbuf_mr, send_dmabuf_fd, send_ringbuf_va);
dereg_recv_ring_mr:
	free_public_mr(ctxt, recv_ringbuf_mr, recv_dmabuf_fd, recv_ringbuf_va);
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
free_wr_queue:
	free_wr_queue(wr_queue);
free_qp:
	ctxt->prov_ops.destroy_qp(ibqp);
err:
	return NULL;
}

int cvmrdma_destroy_qp(struct ibv_qp *ibqp)
{

	struct cvmrdma_qp * qp = (struct cvmrdma_qp *)ibqp;
	struct cvmrdma_context * ctxt = cvmrdma_get_ctx(qp->user_qp.qp.context);
	int ret;

	cvmrdma_stop_cq_thread(qp);
#ifndef DISABLE_SEND_LOOP
	fprintf(stderr, "Joining provider send channel thread\n");
	pthread_join(qp->provider_send_channel_thread, NULL);
	fprintf(stderr, "Joined provider send channel thread\n");
#endif
	pthread_join(qp->provider_comp_channel_thread, NULL);

	free(qp->local_addr);

	pthread_cond_destroy(&qp->remote_params_set_cv);
	pthread_mutex_destroy(&qp->remote_params_lock);

	pthread_cond_destroy(&qp->control_qp_connection_cv);
	pthread_mutex_destroy(&qp->control_qp_connection_lock);

	detach_qp_from_cq_manager(ctxt, qp);

	pthread_cond_destroy(&qp->send_lines_avail);
	pthread_mutex_destroy(&qp->send_lines_full_lock);

	// TODO detach qp from cq 
	
	ret = free_public_mr(ctxt, qp->control_buf_mr, qp->control_dmabuf_fd, qp->control_buf_va);
	if (ret) {
		fprintf(stderr, "Failed to free control_buf_mr\n");
		return ret;
	}

	ret = free_public_mr(ctxt, qp->recv_ringbuf_mr, qp->recv_ringbuf_dmabuf_fd, qp->recv_ringbuf_va);
	if (ret) {
			fprintf(stderr, "Failed to free recv_ringbuf_mr\n");
		return ret;
	}

	ret = free_public_mr(ctxt, qp->send_ringbuf_mr, qp->send_ringbuf_dmabuf_fd, qp->send_ringbuf_va);
	if (ret) {
		fprintf(stderr, "Failed to free send_ringbuf_mr\n");
		return ret;
	}

	ret = munmap(qp->bounce_private_mem, cvmrdma_priv_bounce_total_bytes(ctxt->aead));
	if (ret) {
		fprintf(stderr, "Failed to unmap bounce private memory\n");
		return ret;
	}

	free_wr_queue(qp->wr_queue);
	free_qp_send_metadata(qp->send_metadata);
	free_qp_chunk_queue(qp->chunk_queue);
	free_qp_read_metadata(qp->read_metadata);

	pthread_cond_destroy(&qp->ibv_cv);
	pthread_mutex_destroy(&qp->ibv_lock);
	pthread_mutex_destroy(&qp->send_counter_lock);
	pthread_mutex_destroy(&qp->recv_counter_lock);

	// TODO: destroy recv_metadata depending on if it is shared or not

	ret = ctxt->prov_ops.destroy_qp(qp->provider_qp);
	if (ret) {
		fprintf(stderr, "Failed to destroy provider QP\n");
		return ret;
	}

	ret = ctxt->prov_ops.destroy_qp(qp->control_qp);
	if (ret) {
		fprintf(stderr, "Failed to destroy control QP\n");
		return ret;
	}

	ret = ctxt->prov_ops.destroy_cq(qp->control_cq);
	if (ret) {
		fprintf(stderr, "Failed to destroy control CQ\n");
		return ret;
	}

	free(qp);

	return 0;
}

int cvmrdma_dereg_mr(struct verbs_mr *vmr){

	struct cvmrdma_mr * mr = (struct cvmrdma_mr *)vmr;
	struct cvmrdma_pd * pd = (struct cvmrdma_pd *)vmr->ibv_mr.pd;

	unregister_bounce_mr(pd->bounce_metadata, mr->user_mr.ibv_mr.lkey);
	free(mr);
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

static int cvmrdma_destroy_cq(struct ibv_cq *ibcq) {

	struct cvmrdma_cq *cq = (struct cvmrdma_cq *)ibcq;
	struct cvmrdma_context * ctxt = cvmrdma_get_ctx(cq->user_cq.cq.context);

	int ret;

	// destroy recv cq
	ret = ctxt->prov_ops.destroy_cq(cq->prov_cq_recv);
	if (ret) {
		fprintf(stderr, "Failed to destroy recv cq\n");
		goto out;
	}

	//destroy send cq
	ret = ctxt->prov_ops.destroy_cq(cq->prov_cq_send);
	if (ret) {
		fprintf(stderr, "Failed to destroy send cq\n");
		goto out;
	}

	// emulate ibv_destroy_cq for the channel provided by
	struct ibv_comp_channel *channel = cq->user_cq.cq.channel;
	if (channel) {
		pthread_mutex_lock(&channel->context->mutex);
		--channel->refcnt;
		pthread_mutex_unlock(&channel->context->mutex);
	}

	free(cq);

out:
	return ret;
}

static int cvmrdma_dealloc_pd(struct ibv_pd *pd) {

	struct cvmrdma_context * ctxt = cvmrdma_get_ctx(pd->context);
	struct cvmrdma_pd * cpd = (struct cvmrdma_pd *)pd;
	bounce_metadata_destroy(cpd->bounce_metadata);

	ctxt->prov_ops.dealloc_pd(cpd->provider_pd);
	free(pd);

	return 0;
}

static void cvmrdma_free_context(struct ibv_context *context)
{
	struct cvmrdma_context * ctxt = cvmrdma_get_ctx(context);

#ifndef DUMMY_SYSTEM_HEAP
	close_system_heap(ctxt->dma_heap);
#endif

	ctxt->prov_ops.free_context(context);

	// sodium does not provide a way to free the allocated resources

}

struct verbs_context_ops cvmrdma_ops = {
    .alloc_pd = cvmrdma_alloc_pd,
    .create_cq = cvmrdma_create_cq,
    .create_qp = cvmrdma_create_qp,
    .dealloc_pd = cvmrdma_dealloc_pd,
    .dereg_mr = cvmrdma_dereg_mr,
    .destroy_cq = cvmrdma_destroy_cq,
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


uint64_t OPT_cvmrdma_ring_buffer_size = 1024 * 1024 * 512; // 512 MiB
uint64_t OPT_cvmrdma_chunk_bytes = 1024 * 4;
uint64_t cvmrdma_ring_entries = -1;
uint64_t cvmrdma_ack_notify_divider = 16;
uint64_t cvmrdma_ack_threshold = -1;
// bool OPT_cvmrdma_disable_payload_nonce_pregeneration = false;
// it should always hold (OPT_cvmrdma_chunk_bytes * cvmrdma_ring_entries) < 64 * 2048 * 4096 = 536870912 bytes = 512 MiB
static struct verbs_context *cvmrdma_alloc_context(struct ibv_device *ibdev,
						  int cmd_fd,
						  void *private_data)
{
    struct verbs_device * vdev = verbs_get_device(ibdev);
	int ret;

	DEBUG("cvmrdma_alloc_context\n");

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

	fprintf(stderr, "CVMRDMA_CHUNK_BYTES set to %lu\n", OPT_cvmrdma_chunk_bytes);

	char const * cvmrdma_ring_buffer_size_str = getenv("CVMRDMA_RING_BUFFER_SIZE");
	if(cvmrdma_ring_buffer_size_str != NULL)
	{
		OPT_cvmrdma_ring_buffer_size = atoi(cvmrdma_ring_buffer_size_str);
		if (OPT_cvmrdma_ring_buffer_size == 0)
		{
			fprintf(stderr, "ERROR: CVMRDMA_RING_BUFFER_SIZE must be a positive integer\n");
			goto err;
		}
	}

	// the size of our ring buffer is limited to 512 MiB
	// we need to claculate how many entries we can fit into the ring buffer depending on the chunk size
	cvmrdma_ring_entries = OPT_cvmrdma_ring_buffer_size / (OPT_cvmrdma_chunk_bytes);
	// fprintf(stderr, "CVMRDMA_CHUNK_BYTES set to %lu, ring entries set to %lu\n", OPT_cvmrdma_chunk_bytes, cvmrdma_ring_entries);
	cvmrdma_ack_threshold = DIVIDE_ROUND_UP(cvmrdma_ring_entries, 2);

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

	// TODO: qp_metadata_manager_destroy(server->qp_metadata_manager); when destroying the context
	cvmrdma_ctx->qp_metadata_manager = qp_metadata_manager_alloc();

	fprintf(stderr, "Actual chunk size (including overhead) is %lu\n", cvmrdma_ring_slot_bytes(cvmrdma_ctx->aead));
	fprintf(stderr, "Needs %lu MTUs without metadata per chunk\n", DIVIDE_ROUND_UP(OPT_cvmrdma_chunk_bytes, 4096));
	fprintf(stderr, "Needs %lu MTUs per chunk\n", DIVIDE_ROUND_UP(cvmrdma_ring_slot_bytes(cvmrdma_ctx->aead), 4096));

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
