#pragma once

#include <infiniband/verbs.h>

#include "crypto.h"

#define PAGE_SIZE 4096
#define DIVIDE_ROUND_UP(x,y) ((x + y - 1) / y)

struct cq_server_notification
{
    uint64_t hca_addr; // 8
    enum ibv_wr_opcode op; // 16
    uint32_t rkey; // 16
    uint32_t imm; // 24
	// uint64_t payload_nonce[cvmrdma_aead_NPUB_WORDS]; // 48
	uint64_t payload_len; // 56
	uint64_t chunk_len; // 72
    bool last_chunk; // 80
};

#define CVMRDMA_SHARED_KEY_ENV "CVMRDMA_SHARED_KEY"

// Defined in cvmrdma_ops.c
extern uint64_t OPT_cvmrdma_ring_buffer_size;
extern uint64_t OPT_cvmrdma_chunk_bytes;
extern uint64_t cvmrdma_ring_entries;
extern uint64_t cvmrdma_ack_threshold;
extern uint64_t cvmrdma_ack_notify_divider;


static inline uint64_t cvmrdma_ring_slot_bytes(enum sodium_crypto_aead aead)
{
	assert(OPT_cvmrdma_chunk_bytes != 0);
	return OPT_cvmrdma_chunk_bytes + cvmrdma_ABYTES(aead) 
			+ sizeof(struct cq_server_notification) + cvmrdma_ABYTES(aead);
}


static inline uint64_t cvmrdma_header_bytes(enum sodium_crypto_aead aead)
{
	return sizeof(struct cq_server_notification) + cvmrdma_ABYTES(aead);
}

// originally 32 * 1048576
// #define CVMRDMA_RING_BUFFER_SIZE (512 * 1048576) //536870912
// #define CVMRDMA_RING_ENTRIES 64

// This must be smaller than the max_recv_wr/max_send_wr for control_ibqp in cvmrdma_create_qp.
//#define CVMRDMA_ACK_THRESHOLD DIVIDE_ROUND_UP(CVMRDMA_RING_ENTRIES, 2)

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