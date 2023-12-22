#include "notification_queue.hpp"

#include <chrono>

#include "debug.h"

void notification_queue_set_rx_key(NotificationQueue * q, aegis_key_t const & rx_key)
{
    q->rx_key = rx_key;
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


uint64_t const * notification_queue_get_key(NotificationQueue const * q)
{
    return q->rx_key.cbegin();
}