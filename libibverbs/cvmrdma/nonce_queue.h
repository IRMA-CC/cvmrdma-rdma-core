#pragma once

#include <infiniband/verbs.h>

struct nonce_entry
{
    uint64_t nonce[3]; // 3 x 64 bits
};


#ifdef __cplusplus
#include "ThreadsafeQueue.hpp"

struct QpNonce {
    ThreadsafeQueue<struct nonce_entry> queue;
};

extern "C" {
#endif

typedef struct QpNonce QpNonce;

QpNonce * alloc_qp_nonce_queue(void);
void free_qp_nonce_queue(QpNonce * nonce_queue);

void qp_nonce_queue_push(QpNonce * nonce_queue, struct nonce_entry * entry);
struct nonce_entry * qp_nonce_queue_front(QpNonce * nonce_queue);
void qp_nonce_queue_pop(QpNonce * nonce_queue);

#ifdef __cplusplus
}
#endif
