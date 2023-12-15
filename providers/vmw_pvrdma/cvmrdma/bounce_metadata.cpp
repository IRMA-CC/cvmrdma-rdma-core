#include "bounce_metadata.h"

#include <unordered_map>
#include <vector>
#include <mutex>

/*
 * To take the bounce buffer into account, the rdma backend will need to map
 * an MR's rkey or lkey to
 * - its hca_va
 * - the address of the original user buffer passed for registration
 * - the address of the bounce buffer effectively registered as the MR
 */


struct PdBounceMetadata
{
    std::unordered_map<uint32_t,struct bounce_entry *> rkey_metadata;
    std::unordered_map<uint32_t,struct bounce_entry *> lkey_metadata;
    std::mutex metadata_lock;
};


PdBounceMetadata * bounce_metadata_alloc(void)
{
    return new PdBounceMetadata{};
}

void bounce_metadata_destroy(PdBounceMetadata * bm)
{
    delete bm;
}

void register_bounce_mr(PdBounceMetadata * metadata, uint32_t lkey, uint32_t rkey, uint64_t hca_va, void * user_addr, void * bounce_addr)
{
    struct bounce_entry * entry = new  bounce_entry{
        .hca_va = hca_va,
        .user_buffer = user_addr,
        .bounce_buffer = bounce_addr
    };

    auto locked = std::scoped_lock{metadata->metadata_lock};
    metadata->rkey_metadata.insert_or_assign(rkey, entry);
    metadata->lkey_metadata.insert_or_assign(lkey, entry);
}

struct bounce_entry get_bounce_metadata_for_rkey(PdBounceMetadata * metadata, uint32_t rkey)
{
    auto locked = std::scoped_lock{metadata->metadata_lock};
    return *metadata->rkey_metadata.at(rkey);
}

struct bounce_entry get_bounce_metadata_for_lkey(PdBounceMetadata * metadata, uint32_t lkey)
{
    auto locked = std::scoped_lock{metadata->metadata_lock};
    return *metadata->lkey_metadata.at(lkey);
}