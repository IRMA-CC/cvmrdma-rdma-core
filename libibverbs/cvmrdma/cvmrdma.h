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
	uint64_t payload_nonce[cvmrdma_aead_NPUB_WORDS]; // 44
	uint64_t payload_len; // 52
	uint64_t chunk_offt; // 60
	uint64_t chunk_len; // 68
    bool last_chunk; // 69
};

#define CVMRDMA_SHARED_KEY_ENV "CVMRDMA_SHARED_KEY"

#define CVMRDMA_CHUNK_BYTES ((uint64_t)32 * PAGE_SIZE)

static inline uint64_t cvmrdma_ring_slot_bytes(enum sodium_crypto_aead aead)
{
	return CVMRDMA_CHUNK_BYTES + cvmrdma_ABYTES(aead) 
			+ sizeof(struct cq_server_notification) + cvmrdma_ABYTES(aead);
}


static inline uint64_t cvmrdma_header_bytes(enum sodium_crypto_aead aead)
{
	return sizeof(struct cq_server_notification) + cvmrdma_ABYTES(aead);
}

#define CVMRDMA_RING_ENTRIES 512

#define CVMRDMA_ACK_THRESHOLD DIVIDE_ROUND_UP(CVMRDMA_RING_ENTRIES, 2)

enum cvmrdma_priv_chunk_id {
	CVMRDMA_PRIV_CHUNK_SG_OUT,
	CVMRDMA_PRIV_CHUNK_ENC_OUT,
	CVMRDMA_PRIV_CHUNK_ENC_IN,
	CVMRDMA_PRIV_CHUNK_SG_IN,
	__CVMRDMA_PRIV_CHUNK_NR,
};

static inline uint64_t cvmrdma_priv_bounce_total_bytes(enum sodium_crypto_aead aead)
{
	return __CVMRDMA_PRIV_CHUNK_NR * cvmrdma_ring_slot_bytes(aead);
}