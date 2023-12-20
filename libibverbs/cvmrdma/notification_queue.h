#pragma once

#include <stdbool.h>
#include <sys/types.h>
#include <inttypes.h>

#ifdef __cplusplus
extern "C" {
#endif

/* WRITE notification, could be extended for
   other opcodes */
struct cq_server_notification
{
    void * bounce_addr;
    void * user_addr;
    size_t length;
    unsigned int nonce_counter;
    uint32_t imm;
    bool has_imm;
};

typedef struct NotificationQueue NotificationQueue;
typedef struct NotificationQueueManager NotificationQueueManager;

NotificationQueueManager * nq_manager_alloc(void);
void nq_manager_destroy(NotificationQueueManager * nqm);

NotificationQueue * nq_manager_create_queue(NotificationQueueManager * nqm, uint32_t qpn);
NotificationQueue * nq_manager_get_queue(NotificationQueueManager * nqm, uint32_t qpn);

void notification_queue_push(NotificationQueue * q, struct cq_server_notification const * notif);
struct cq_server_notification const * notification_queue_front(NotificationQueue * q);
void notification_queue_pop(NotificationQueue * q);


#ifdef __cplusplus
}
#endif