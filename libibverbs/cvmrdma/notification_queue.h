#pragma once

#include <assert.h>
#include <stdbool.h>
#include <sys/types.h>
#include <inttypes.h>

#include <infiniband/verbs.h>

#ifdef __cplusplus
extern "C" {
#endif

struct cq_server_notification
{
    uint64_t nonce[4];
    uint64_t hca_addr;
    enum ibv_wr_opcode op;
    uint32_t rkey;
    uint32_t imm;
    bool last_chunk;
};

typedef struct NotificationQueue NotificationQueue;

NotificationQueue * alloc_notification_queue(void);

struct cq_server_notification const * notification_queue_front(NotificationQueue * q);
void notification_queue_pop(NotificationQueue * q);

#ifdef __cplusplus
}
#endif