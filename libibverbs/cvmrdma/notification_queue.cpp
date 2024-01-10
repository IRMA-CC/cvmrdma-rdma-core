#include "notification_queue.hpp"

#include <chrono>

#include "debug.h"

NotificationQueue * alloc_notification_queue(void)
{
    return new NotificationQueue{};
}


void notification_queue_push(NotificationQueue * q, std::unique_ptr<struct cq_server_notification> notif)
{
    q->queue.push(std::move(notif));
}

struct cq_server_notification const * notification_queue_front(NotificationQueue * q)
{
    return q->queue.front().get();
}

void notification_queue_pop(NotificationQueue * q)
{
    q->queue.pop();
}