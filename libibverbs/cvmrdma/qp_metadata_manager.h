#pragma once

#include <assert.h>
#include <stdbool.h>
#include <sys/types.h>
#include <inttypes.h>

#include "notification_queue.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct QPMetadataManager QPMetadataManager;

QPMetadataManager * qp_metadata_manager_alloc(void);
void qp_metadata_manager_destroy(QPMetadataManager * qp_metadata_manager);

NotificationQueue * qp_metadata_manager_create_queue(QPMetadataManager * qp_metadata_manager, uint32_t qpn);
NotificationQueue * qp_metadata_manager_get_queue(QPMetadataManager * qp_metadata_manager, uint32_t qpn);


#ifdef __cplusplus
}
#endif