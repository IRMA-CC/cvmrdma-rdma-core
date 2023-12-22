#pragma once


#ifdef __cplusplus

#include <capnp/ez-rpc.h>
using ClientSession = capnp::EzRpcClient;
using taskset_t = kj::TaskSet;
using taskset_error_handler_t = kj::TaskSet::ErrorHandler;

extern "C" {
#else
typedef struct ClientSession ClientSession;
typedef struct taskset_t taskset_t;
typedef struct taskset_error_handler_t taskset_error_handler_t;
#endif

taskset_error_handler_t * taskset_error_handler_alloc(void);

taskset_t * taskset_alloc(taskset_error_handler_t * handler);
void taskset_free(taskset_t * tk);
void taskset_wait(ClientSession * session, taskset_t * tk);


#ifdef __cplusplus
}
#endif