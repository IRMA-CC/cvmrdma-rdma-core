#pragma once

#include <infiniband/verbs.h>

struct chunk_entry
{
    uint64_t chunk_idx;
    uint64_t lkey;
};


#ifdef __cplusplus
#include "ThreadsafeQueue.hpp"

struct QpChunk {
    ThreadsafeQueue<struct chunk_entry> queue;
};

extern "C" {
#endif

typedef struct QpChunk QpChunk;

QpChunk * alloc_qp_chunk_queue(void);
void free_qp_chunk_queue(QpChunk * chunk_queue);

void qp_chunk_queue_push(QpChunk * chunk_queue, struct chunk_entry * entry);
struct chunk_entry * qp_chunk_queue_front(QpChunk * chunk_queue);
void qp_chunk_queue_pop(QpChunk * chunk_queue);

#ifdef __cplusplus
}
#endif
