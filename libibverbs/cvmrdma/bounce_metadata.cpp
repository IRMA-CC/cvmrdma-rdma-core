#include "bounce_metadata.h"

#include <unordered_map>
#include <vector>
#include <mutex>

/*
 * To take the bounce buffer into account, the rdma backend will need to map
 * an MR's rkey or lkey back to the MR itself during send/recv operations.
 */


PdBounceMetadata * bounce_metadata_alloc(void)
{
    return new PdBounceMetadata{};
}

void bounce_metadata_destroy(PdBounceMetadata * bm)
{
    delete bm;
}

void register_bounce_mr(PdBounceMetadata * metadata, uint32_t mr_id, struct cvmrdma_mr * mr)
{
    metadata->insert_or_assign(mr_id, mr);
}

struct cvmrdma_mr * get_bounce_metadata_for_rkey(PdBounceMetadata * metadata, uint32_t rkey)
{
    return metadata->at(rkey);
}

struct cvmrdma_mr * get_bounce_metadata_for_lkey(PdBounceMetadata * metadata, uint32_t lkey)
{
    return metadata->at(lkey);
}