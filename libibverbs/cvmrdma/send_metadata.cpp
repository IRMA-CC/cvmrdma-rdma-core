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
    DEBUG("push SEND metadata!\n");
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
    DEBUG("pop SEND metadata!\n");
    send_metadata_manager->queue.pop();
}

struct send_metadata_entry * qp_send_metadata_front(QpSendMetadata * send_metadata_manager)
{
    DEBUG("SEND metadata front...!\n");
    assert(send_metadata_manager);
    return &send_metadata_manager->queue.front();
}