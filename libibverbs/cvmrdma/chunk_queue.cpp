#include "chunk_queue.h"

#include <cstring>
#include <cassert>
#include "debug.h"

QpChunk * alloc_qp_chunk_queue(void)
{
    return new QpChunk{};
}

void free_qp_chunk_queue(QpChunk * chunk_queue)
{
    delete chunk_queue;
}

void qp_chunk_queue_push(QpChunk * chunk_queue, struct chunk_entry * entry)
{
    struct chunk_entry entry_copy = {};
    memcpy(&entry_copy, entry, sizeof(struct chunk_entry));
    chunk_queue->queue.push(entry_copy);
}

void qp_chunk_queue_pop(QpChunk * chunk_queue)
{
    DEBUG("[RM] pop chunk queue: currently has %zu entries\n", chunk_queue->queue.size());
    chunk_queue->queue.pop();
}

struct chunk_entry * qp_chunk_queue_front(QpChunk * chunk_queue)
{
    return &chunk_queue->queue.front();
}