#include "send_metadata.h"

#include <assert.h>
#include <cstring>
#include "debug.h"

QpSendMetadata * alloc_qp_send_metadata(void)
{
    return new QpSendMetadata{};
}

void free_qp_send_metadata(QpSendMetadata * send_metadata)
{
    delete send_metadata;
}

void qp_send_metadata_push(QpSendMetadata * send_metadata_manager, uint64_t wrid, enum ibv_wr_opcode op, size_t bytes, size_t nchunks, bool send_signaled, bool no_data_send)
{
    struct send_metadata_entry entry = {
        .wrid = wrid,
        .op = op,
        .bytes = bytes,
        .nchunks = nchunks,
        .send_signaled = send_signaled,
        .no_data_send = no_data_send,
    };

    send_metadata_manager->queue.push(entry);
}

void qp_send_metadata_pop(QpSendMetadata * send_metadata_manager)
{
    send_metadata_manager->queue.pop();
}

struct send_metadata_entry * qp_send_metadata_front(QpSendMetadata * send_metadata_manager)
{
    assert(send_metadata_manager);
    send_metadata_entry * entry = &send_metadata_manager->queue.front();
    assert(entry);
    return entry;
}