#pragma once

#include <inttypes.h>



#ifdef __cplusplus
#include "ThreadsafeMap.hpp"

using PdBounceMetadata = ThreadsafeMap<uint32_t, struct cvmrdma_mr *>;
extern "C" {
#else
typedef struct PdBounceMetadata PdBounceMetadata;
#endif

/* Forward declare cvmrdma_mr to avoid including cvmrdma_objects.h, which itself needs
   to include bounce_metadata.h (for the definition of PD) */
struct cvmrdma_mr;


PdBounceMetadata * bounce_metadata_alloc(void);
void bounce_metadata_destroy(PdBounceMetadata * bm);


void register_bounce_mr(PdBounceMetadata * metadata, uint32_t mr_id, struct cvmrdma_mr * mr);
struct cvmrdma_mr * get_bounce_metadata_for_rkey(PdBounceMetadata * metadata, uint32_t rkey);
struct cvmrdma_mr * get_bounce_metadata_for_lkey(PdBounceMetadata * metadata, uint32_t lkey);

void print_bounce_metadata(PdBounceMetadata * metadata);

#ifdef __cplusplus
}
#endif