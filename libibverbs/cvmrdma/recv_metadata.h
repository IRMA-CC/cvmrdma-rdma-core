#pragma once

#include <infiniband/verbs.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct QpRecvMetadata QpRecvMetadata;

QpRecvMetadata * alloc_qp_recv_metadata(void);
void free_qp_recv_metadata(QpRecvMetadata * recv_metadata);

void qp_recv_metadata_push(QpRecvMetadata * recv_metadata, uint64_t wrid, struct ibv_sge * sgl, int nsge);

#ifdef __cplusplus
}
#endif
