#pragma once

#include <infiniband/verbs.h>


/*
    After each recv, the recver must find where the recv happened
    in the bounce buffer. This information is not made available in
    the notification, it is therefore necessary to save it in a queue
    that can be read from the 
*/

struct recv_metadata_entry
{
    uint64_t wrid;
    struct ibv_sge * sgl;
    int nsge;
};


#ifdef __cplusplus
#include "ThreadsafeQueue.hpp"

struct QpRecvMetadata {
    ThreadsafeQueue<struct recv_metadata_entry> queue;
};

extern "C" {
#endif

typedef struct QpRecvMetadata QpRecvMetadata;

QpRecvMetadata * alloc_qp_recv_metadata(void);
void free_qp_recv_metadata(QpRecvMetadata * recv_metadata);

void qp_recv_metadata_push(QpRecvMetadata * recv_metadata, uint64_t wrid, struct ibv_sge * sgl, int nsge);
struct recv_metadata_entry * qp_recv_metadata_front(QpRecvMetadata * recv_metadata);
void qp_recv_metadata_pop(QpRecvMetadata * recv_metadata);

#ifdef __cplusplus
}
#endif
