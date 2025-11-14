#include "qp_metadata_manager.h"

#include <unordered_map>
#include <mutex>
#include <memory>

#include "debug.h"

struct QPMetadataManager
{
    std::unordered_map<uint32_t, struct cvmrdma_qp *> qp_for_qpn;
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

int qp_metadata_manager_register_qp(QPMetadataManager * qp_metadata_manager, uint32_t qpn, struct cvmrdma_qp * qp)
{
    auto locked = std::scoped_lock{qp_metadata_manager->lock};

    assert(qp != nullptr);

    auto present = qp_metadata_manager->qp_for_qpn.find(qpn);
    if(present != qp_metadata_manager->qp_for_qpn.end())
    {
        DEBUG("ERROR: a qp is already registered for QPN 0x%" PRIx32 "!\n", qpn);
        return -1;
    }

    auto && [it, inserted] = qp_metadata_manager->qp_for_qpn.insert({qpn, qp});
    return 0;
}

int qp_metadata_manager_unregister_qp(QPMetadataManager * qp_metadata_manager, uint32_t qpn)
{
    auto locked = std::scoped_lock{qp_metadata_manager->lock};

    auto present = qp_metadata_manager->qp_for_qpn.find(qpn);
    if(present == qp_metadata_manager->qp_for_qpn.end())
    {
        DEBUG("ERROR: no qp found to unregister for QPN 0x%" PRIx32 "!\n", qpn);
        return -1;
    }

    qp_metadata_manager->qp_for_qpn.erase(present);
    return 0;
}

struct cvmrdma_qp * qp_metadata_manager_get_qp(QPMetadataManager * qp_metadata_manager, uint32_t qpn)
{
    auto locked = std::scoped_lock{qp_metadata_manager->lock};
    
    auto it = qp_metadata_manager->qp_for_qpn.find(qpn);
    if(it == qp_metadata_manager->qp_for_qpn.end())
    {
        DEBUG("ERROR: no qp found for QPN 0x%" PRIx32 "!\n", qpn);
        return nullptr;
    }

    assert(it->second != nullptr);

    return it->second;
}