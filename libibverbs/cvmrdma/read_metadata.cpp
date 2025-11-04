#include "read_metadata.h"

#include <cstring>
#include <cassert>
#include "debug.h"

QpReadMetadata * alloc_qp_read_metadata(void)
{
    return new QpReadMetadata{};
}

void free_qp_read_metadata(QpReadMetadata * read_metadata)
{
    delete read_metadata;
}

void qp_read_metadata_push(QpReadMetadata * read_metadata_manager, uint64_t wrid, struct ibv_sge * sgl, int nsge, int signaled)
{
    // The sgl is a _list_ not an array that simply can be copied. It contains a next pointer after all.
    assert(nsge == 1);

    struct ibv_sge * sgl_entry = new ibv_sge[nsge];


    memcpy(sgl_entry, sgl, nsge * sizeof(*sgl));

    struct recv_metadata_entry entry = {
        .wrid = wrid,
        .sgl = sgl_entry,
        .nsge = nsge,
        .send_signaled = signaled
    };

    read_metadata_manager->queue.push(entry);

}

void qp_read_metadata_pop(QpReadMetadata * read_metadata_manager)
{
    struct recv_metadata_entry & entry = read_metadata_manager->queue.front();
    delete [] entry.sgl;
    read_metadata_manager->queue.pop();
}

struct recv_metadata_entry * qp_read_metadata_front(QpReadMetadata * read_metadata_manager)
{
    return &read_metadata_manager->queue.front();
}