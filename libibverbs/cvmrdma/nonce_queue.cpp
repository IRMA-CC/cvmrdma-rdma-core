#include "nonce_queue.h"

#include <cstring>
#include <cassert>
#include "debug.h"

QpNonce * alloc_qp_nonce_queue(void)
{
    return new QpNonce{};
}

void free_qp_nonce_queue(QpNonce * nonce_queue)
{
    delete nonce_queue;
}

void qp_nonce_queue_push(QpNonce * nonce_queue, struct nonce_entry * entry)
{
    DEBUG("[RM] push nonce queue: currently has %zu entries; nonce: %lx %lx %lx\n", 
        nonce_queue->queue.size(), entry->nonce[0], entry->nonce[1], entry->nonce[2]);
    struct nonce_entry entry_copy = {};
    memcpy(&entry_copy, entry, sizeof(struct nonce_entry));
    nonce_queue->queue.push(entry_copy);
}

void qp_nonce_queue_pop(QpNonce * nonce_queue)
{
    DEBUG("[RM] pop nonce queue: currently has %zu entries\n", nonce_queue->queue.size());
    nonce_queue->queue.pop();
}

struct nonce_entry * qp_nonce_queue_front(QpNonce * nonce_queue)
{
    DEBUG("[RM] nonce queue front: currently has %zu entries, nonce: %lx %lx %lx\n", 
        nonce_queue->queue.size(), 
        nonce_queue->queue.front().nonce[0], 
        nonce_queue->queue.front().nonce[1], 
        nonce_queue->queue.front().nonce[2]);
    return &nonce_queue->queue.front();
}