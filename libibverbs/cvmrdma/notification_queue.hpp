#pragma once

/* C++ interface to the notification queue. All producers are C++ objects,
   so we can use some STL types in the interface */

#include "notification_queue.h"

#include <memory>

#include "ThreadsafeQueue.hpp"
#include "aegis.hpp"

struct NotificationQueue
{
    ThreadsafeQueue<std::unique_ptr<struct cq_server_notification>> queue;

    /* RX key for the QP associated with this queue. */
    aegis_key_t rx_key;
};

void notification_queue_push(NotificationQueue * q, std::unique_ptr<struct cq_server_notification> notif);
void notification_queue_set_rx_key(NotificationQueue * q, aegis_key_t const & rx_key);