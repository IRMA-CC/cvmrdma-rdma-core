#include "nonce_queue.h"

#include <cstring>
#include <cassert>
#include "debug.h"
#include <deque>

NonceQueue * alloc_qp_nonce_queue(void)
{
    return new NonceQueue{};
}

void free_qp_nonce_queue(NonceQueue * nonce_queue)
{
    delete nonce_queue;
}

void qp_nonce_queue_push(NonceQueue * nonce_queue, uint64_t nonce0, uint64_t nonce1, uint64_t nonce2)
{
    DEBUG("[NQ] push NONCE: currently has %zu entries\n", nonce_queue->queue.size());

    struct nonce_t entry = {
        .nonce = {nonce0, nonce1, nonce2}
    };

    nonce_queue->queue.push_back(entry);
}

void qp_nonce_queue_pop(NonceQueue * nonce_queue)
{
    DEBUG("[NQ] pop NONCE: currently has %zu entries\n", nonce_queue->queue.size());
    nonce_queue->queue.pop_front();
}

struct nonce_t * qp_nonce_queue_front(NonceQueue * nonce_queue)
{
    DEBUG("[NQ] NONCE front: currently has %zu entries\n", nonce_queue->queue.size());
    return &nonce_queue->queue.front();
}

int qp_nonce_queue_size(NonceQueue * nonce_queue)
{
    return nonce_queue->queue.size();
}