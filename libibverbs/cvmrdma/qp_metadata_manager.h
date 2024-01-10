#pragma once

#include <assert.h>
#include <stdbool.h>
#include <sys/types.h>
#include <inttypes.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct QPMetadataManager QPMetadataManager;

QPMetadataManager * qp_metadata_manager_alloc(void);
void qp_metadata_manager_destroy(QPMetadataManager * qp_metadata_manager);


/* Forward declare to avoid inclusion of cvmrdma_objects.h */
struct cvmrdma_qp;

int qp_metadata_manager_register_qp(QPMetadataManager * qp_metadata_manager, uint32_t qpn, struct cvmrdma_qp * qp);
struct cvmrdma_qp * qp_metadata_manager_get_qp(QPMetadataManager * qp_metadata_manager, uint32_t qpn);


#ifdef __cplusplus
}
#endif