#pragma once

#include <stdint.h>

struct nonce_t {
    uint64_t nonce[3];
};

#ifdef __cplusplus
#include <deque>

struct NonceQueue {
    std::deque<struct nonce_t> queue;
};

extern "C" {
#endif

typedef struct NonceQueue NonceQueue;

NonceQueue * alloc_qp_nonce_queue(void);
void free_qp_nonce_queue(NonceQueue * nonce_queue);

void qp_nonce_queue_push(NonceQueue * nonce_queue, uint64_t nonce0, uint64_t nonce1, uint64_t nonce2);
struct nonce_t * qp_nonce_queue_front(NonceQueue * nonce_queue);
void qp_nonce_queue_pop(NonceQueue * nonce_queue);

int qp_nonce_queue_size(NonceQueue * nonce_queue);

#ifdef __cplusplus
}
#endif