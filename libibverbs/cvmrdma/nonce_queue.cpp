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
    struct nonce_entry entry_copy = {};
    memcpy(&entry_copy, entry, sizeof(struct nonce_entry));
    nonce_queue->queue.push(entry_copy);
}

void qp_nonce_queue_pop(QpNonce * nonce_queue)
{
    nonce_queue->queue.pop();
}

struct nonce_entry * qp_nonce_queue_front(QpNonce * nonce_queue)
{
    return &nonce_queue->queue.front();
}