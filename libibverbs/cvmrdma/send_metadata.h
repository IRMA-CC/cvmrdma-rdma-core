#pragma once

#include <infiniband/verbs.h>


/*
    For each send, store the number of chunks in that WR to the CQ server, so that
    it knows when to emit a SEND WC back to the user.
*/

struct send_metadata_entry
{
    uint64_t wrid;
    enum ibv_wr_opcode op;
    size_t bytes;
    size_t nchunks;
    /* Whether this SEND was requested by the remote as a READ */
    bool write_for_read;
    // If user has set SEND_SIGNALED, we need to send a WC back to the user
    bool send_signaled;
    bool no_data_send; // if true, this OP does not send any data, just metadata
};


#ifdef __cplusplus
#include "ThreadsafeQueue.hpp"

struct QpSendMetadata {
    ThreadsafeQueue<struct send_metadata_entry> queue;
};

extern "C" {
#endif

typedef struct QpSendMetadata QpSendMetadata;

QpSendMetadata * alloc_qp_send_metadata(void);
void free_qp_send_metadata(QpSendMetadata * send_metadata);

void qp_send_metadata_push(QpSendMetadata * send_metadata_manager, uint64_t wrid, enum ibv_wr_opcode op, size_t bytes, size_t nchunks, bool write_for_read, bool send_signaled, bool no_data_send);
struct send_metadata_entry * qp_send_metadata_front(QpSendMetadata * send_metadata);
void qp_send_metadata_pop(QpSendMetadata * send_metadata);

#ifdef __cplusplus
}
#endif
