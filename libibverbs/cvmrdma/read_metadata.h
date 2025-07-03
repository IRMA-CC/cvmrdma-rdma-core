#pragma once

#include <infiniband/verbs.h>
#include "recv_metadata.h"


/*
    This is basically just a receive queue, but for reads.
*/


#ifdef __cplusplus
#include "ThreadsafeQueue.hpp"

struct QpReadMetadata {
    ThreadsafeQueue<struct recv_metadata_entry> queue;
};

extern "C" {
#endif

typedef struct QpReadMetadata QpReadMetadata;

QpReadMetadata * alloc_qp_read_metadata(void);
void free_qp_read_metadata(QpReadMetadata * read_metadata);

void qp_read_metadata_push(QpReadMetadata * read_metadata, uint64_t wrid, struct ibv_sge * sgl, int nsge);
struct recv_metadata_entry * qp_read_metadata_front(QpReadMetadata * read_metadata);
void qp_read_metadata_pop(QpReadMetadata * read_metadata);

#ifdef __cplusplus
}
#endif
