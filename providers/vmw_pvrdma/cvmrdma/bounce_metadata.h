#pragma once

#include <inttypes.h>

#ifdef __cplusplus
extern "C" {
#endif

/* XXX: could also map to the pvrdma_mr, but it's annoying because we need:
   pd -> bounce_metadata -> mr
   so we would need to break up pvrdma.h 
*/
struct bounce_entry
{
    uint64_t hca_va;
    void * user_buffer;
    void * bounce_buffer;
};

typedef struct PdBounceMetadata PdBounceMetadata;

PdBounceMetadata * bounce_metadata_alloc(void);
void bounce_metadata_destroy(PdBounceMetadata * bm);


void register_bounce_mr(PdBounceMetadata * metadata, uint32_t lkey, uint32_t rkey, uint64_t hca_va, void * user_addr, void * bounce_addr);
struct bounce_entry get_bounce_metadata_for_rkey(PdBounceMetadata * metadata, uint32_t rkey);
struct bounce_entry get_bounce_metadata_for_lkey(PdBounceMetadata * metadata, uint32_t lkey);

#ifdef __cplusplus
}
#endif