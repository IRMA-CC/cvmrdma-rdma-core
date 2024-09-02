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

void qp_send_metadata_push(QpSendMetadata * send_metadata_manager, uint64_t wrid, enum ibv_wr_opcode op, size_t bytes, size_t nchunks, bool write_for_read, bool send_signaled)
{
    DEBUG("[SM] push SEND metadata: currently has %zu entries\n", send_metadata_manager->queue.size());
    struct send_metadata_entry entry = {
        .wrid = wrid,
        .op = op,
        .bytes = bytes,
        .nchunks = nchunks,
        .write_for_read = write_for_read,
        .send_signaled = send_signaled
    };

    send_metadata_manager->queue.push(entry);
}

void qp_send_metadata_pop(QpSendMetadata * send_metadata_manager)
{
    DEBUG("[SM] pop SEND metadata: currently has %zu entries\n", send_metadata_manager->queue.size());
    send_metadata_manager->queue.pop();
}

struct send_metadata_entry * qp_send_metadata_front(QpSendMetadata * send_metadata_manager)
{
    DEBUG("[SM] SEND metadata front: currently has %zu entries\n", send_metadata_manager->queue.size());
    assert(send_metadata_manager);
    send_metadata_entry * entry = &send_metadata_manager->queue.front();
    assert(entry);
    return entry;
}