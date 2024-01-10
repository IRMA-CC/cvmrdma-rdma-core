#pragma once

/* C++ interface to the notification queue. All producers are C++ objects,
   so we can use some STL types in the interface */

#include "notification_queue.h"

#include <memory>

#include "ThreadsafeQueue.hpp"

struct NotificationQueue
{
    ThreadsafeQueue<std::unique_ptr<struct cq_server_notification>> queue;
};

void notification_queue_push(NotificationQueue * q, std::unique_ptr<struct cq_server_notification> notif);