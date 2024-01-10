#include "recv_metadata.h"

#include <cstring>

QpRecvMetadata * alloc_qp_recv_metadata(void)
{
    return new QpRecvMetadata{};
}

void free_qp_recv_metadata(QpRecvMetadata * recv_metadata)
{
    delete recv_metadata;
}

void qp_recv_metadata_push(QpRecvMetadata * recv_metadata_manager, uint64_t wrid, struct ibv_sge * sgl, int nsge)
{

    struct ibv_sge * sgl_entry = new ibv_sge[nsge];


    memcpy(sgl_entry, sgl, nsge * sizeof(*sgl));

    struct recv_metadata_entry entry = {
        .wrid = wrid,
        .sgl = sgl_entry,
        .nsge = nsge
    };

    recv_metadata_manager->queue.push(entry);
}

void qp_recv_metadata_pop(QpRecvMetadata * recv_metadata_manager)
{
    struct recv_metadata_entry & entry = recv_metadata_manager->queue.front();
    delete entry.sgl;
    recv_metadata_manager->queue.pop();
}

struct recv_metadata_entry * qp_recv_metadata_front(QpRecvMetadata * recv_metadata_manager)
{
    return &recv_metadata_manager->queue.front();
}