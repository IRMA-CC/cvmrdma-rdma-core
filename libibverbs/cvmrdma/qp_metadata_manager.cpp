#include "qp_metadata_manager.h"

#include <unordered_map>
#include <mutex>
#include <memory>

#include "aegis.hpp"
#include "notification_queue.hpp"
#include "debug.h"


struct QPMetadataManager
{
    std::unordered_map<uint32_t, std::unique_ptr<NotificationQueue>> notification_queue_for_qpn;
    std::mutex lock;
};


QPMetadataManager * qp_metadata_manager_alloc(void)
{
    return new QPMetadataManager{};
}

void qp_metadata_manager_destroy(QPMetadataManager * qp_metadata_manager)
{
    delete qp_metadata_manager;
}

NotificationQueue * qp_metadata_manager_create_queue(QPMetadataManager * qp_metadata_manager, uint32_t qpn)
{
    auto locked = std::scoped_lock{qp_metadata_manager->lock};

    auto present = qp_metadata_manager->notification_queue_for_qpn.find(qpn);
    if(present != qp_metadata_manager->notification_queue_for_qpn.end())
    {
        DEBUG("ERROR: a notification queue is already registered for QPN 0x%" PRIx32 "!\n", qpn);
        return nullptr;
    }

    auto && [it, inserted] = qp_metadata_manager->notification_queue_for_qpn.insert({qpn, std::make_unique<NotificationQueue>()});
    return it->second.get();
}

NotificationQueue * qp_metadata_manager_get_queue(QPMetadataManager * qp_metadata_manager, uint32_t qpn)
{
    auto locked = std::scoped_lock{qp_metadata_manager->lock};
    
    auto it = qp_metadata_manager->notification_queue_for_qpn.find(qpn);
    if(it == qp_metadata_manager->notification_queue_for_qpn.end())
    {
        DEBUG("ERROR: no notification queue found for QPN 0x%" PRIx32 "!\n", qpn);
        return nullptr;
    }

    return it->second.get();
}