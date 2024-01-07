#pragma once

#include <assert.h>
#include <stdbool.h>
#include <sys/types.h>
#include <inttypes.h>

#include <infiniband/verbs.h>

#ifdef __cplusplus
extern "C" {
#endif

/* WRITE notification, could be extended for
   other opcodes */
struct cq_server_notification
{
    uint64_t nonce[4];
    uint64_t mac[4];
    void * bounce_addr;
    void * user_addr;
    size_t length;
    enum ibv_wr_opcode op;
    uint32_t imm; /* imm is only valid for ops that contain one */
};

typedef struct NotificationQueue NotificationQueue;

struct cq_server_notification const * notification_queue_front(NotificationQueue * q);
void notification_queue_pop(NotificationQueue * q);

uint64_t const * notification_queue_get_key(NotificationQueue const * q);

#ifdef __cplusplus
}
#endif