#pragma once

#include <infiniband/verbs.h>

#include "crypto.h"

#define PAGE_SIZE 4096
#define DIVIDE_ROUND_UP(x,y) ((x + y - 1) / y)

struct cq_server_notification
{
    uint64_t hca_addr; // 8
    enum ibv_wr_opcode op; // 12
    uint32_t rkey; // 16
    uint32_t imm; // 20
    bool last_chunk; // 24
};



#define CVMRDMA_RING_SLOT_BYTES ((unsigned long long)32 * PAGE_SIZE)

static inline unsigned long long cvmrdma_header_bytes(enum sodium_crypto_aead aead)
{
	return sizeof(struct cq_server_notification) + cvmrdma_ABYTES(aead);
}
static inline unsigned long long cvmrdma_payload_bytes(enum sodium_crypto_aead aead)
{
	return CVMRDMA_RING_SLOT_BYTES - (cvmrdma_header_bytes(aead) + cvmrdma_ABYTES(aead));  
}

#define CVMRDMA_RING_ENTRIES 64
#define CVMRDMA_RING_TOTAL_BYTES (CVMRDMA_RING_ENTRIES * CVMRDMA_RING_SLOT_BYTES)

#define CVMRDMA_ACK_THRESHOLD DIVIDE_ROUND_UP(CVMRDMA_RING_ENTRIES, 4)

enum cvmrdma_ring_types {
	CVMRDMA_RING_SEND,
	CVMRDMA_RING_RECV,
	__CVMRDMA_RING_NR,
};

enum cvmrdma_priv_chunk_id {
	CVMRDMA_PRIV_CHUNK_SG_OUT,
	CVMRDMA_PRIV_CHUNK_ENC_OUT,
	CVMRDMA_PRIV_CHUNK_ENC_IN,
	CVMRDMA_PRIV_CHUNK_SG_IN,
	__CVMRDMA_PRIV_CHUNK_NR,
};

#define CVMRDMA_PRIV_BOUNCE_TOTAL_BYTES (__CVMRDMA_PRIV_CHUNK_NR * CVMRDMA_RING_SLOT_BYTES)
