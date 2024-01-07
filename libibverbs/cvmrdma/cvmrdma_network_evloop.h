#pragma once

#ifdef __cplusplus
#include <capnp/ez-rpc.h>

using ClientSession = capnp::EzRpcClient; 
extern "C" {
#else
typedef struct ClientSession ClientSession;
#endif

typedef struct CapnpEvloopThread CapnpEvloopThread;

#ifdef __cplusplus
}
#endif

