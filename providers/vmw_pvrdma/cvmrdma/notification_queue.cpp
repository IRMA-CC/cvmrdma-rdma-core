#include "notification_queue.h"

#include <chrono>
#include <deque>
#include <mutex>
#include <condition_variable>
#include <unordered_map>

#include "debug.h"

struct NotificationQueue
{
    std::deque<struct cq_server_notification> queue;
    std::condition_variable queue_has_data_cv;
    std::mutex lock;
};

void notification_queue_push(NotificationQueue * q, struct cq_server_notification const * notif)
{
    auto locked = std::scoped_lock{q->lock};
    q->queue.emplace_back(*notif);

    q->queue_has_data_cv.notify_one();
}

struct cq_server_notification const * notification_queue_front(NotificationQueue * q)
{
    auto locked = std::unique_lock{q->lock};
    if(q->queue.empty())
    {
        auto t0 = std::chrono::steady_clock::now();

        DEBUG("WARN: IP notification was slower than the RDMA payload!\n");
        while(q->queue.empty())
        {
            q->queue_has_data_cv.wait(locked);
        }
        auto dt = std::chrono::steady_clock::now() - t0;
        auto dt_ms = std::chrono::duration<float, std::milli>{dt};

        DEBUG("WARN: CQ waited %f ms for niotification\n", dt_ms.count());
    }

    /* Reference stays valid even without lock because there is a
        single consumer */
    return &q->queue.front();
}

void notification_queue_pop(NotificationQueue * q)
{
    auto locked = std::scoped_lock{q->lock};
    q->queue.pop_front();

    /* No need to notify_one after pop, there is a single consummer, no one
        else is waiting. */
}


struct NotificationQueueManager
{
    std::unordered_map<uint32_t, std::unique_ptr<NotificationQueue>> notification_queue_for_qpn;
    std::mutex lock;
};


NotificationQueueManager * nq_manager_alloc(void)
{
    return new NotificationQueueManager{};
}

void nq_manager_destroy(NotificationQueueManager * nqm)
{
    delete nqm;
}

NotificationQueue * nq_manager_create_queue(NotificationQueueManager * nqm, uint32_t qpn)
{
    auto locked = std::scoped_lock{nqm->lock};

    auto present = nqm->notification_queue_for_qpn.find(qpn);
    if(present == nqm->notification_queue_for_qpn.end())
    {
        DEBUG("ERROR: qpn already has a registered notification queue!\n");
        return nullptr;
    }

    auto && [it, inserted] = nqm->notification_queue_for_qpn.insert({qpn, std::make_unique<NotificationQueue>()});
    return it->second.get();
}

NotificationQueue * nq_manager_get_queue(NotificationQueueManager * nqm, uint32_t qpn)
{
    auto locked = std::scoped_lock{nqm->lock};
    
    auto it = nqm->notification_queue_for_qpn.find(qpn);
    if(it == nqm->notification_queue_for_qpn.end())
    {
        DEBUG("ERROR: qpn already has a registered notification queue!\n");
        return nullptr;
    }

    return it->second.get();
}