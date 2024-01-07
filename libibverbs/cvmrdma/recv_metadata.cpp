#include "recv_metadata.hpp"

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
    sge_vec vec(nsge);
    memcpy(vec.data(), sgl, nsge * sizeof(*sgl));

    recv_metadata_manager->queue.push(recv_metadata_entry{wrid, std::move(vec)});
}