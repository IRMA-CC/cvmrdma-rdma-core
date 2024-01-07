#pragma once

#ifdef __cplusplus
#include "cvmrdma.capnp.h"

/* C++ types */
using CVMRDMAClient = CVMRDMA::Client;
using QPNotificationClient = CVMRDMA::QpNotificationChannel::Client;

extern "C" {
#else
/* C types */
typedef struct CVMRDMAClient CVMRDMAClient;
typedef struct QPNotificationClient QPNotificationClient;
typedef struct ClientEvloopThread ClientEvloopThread;
#endif

/* C API */

#ifdef __cplusplus
}
#endif