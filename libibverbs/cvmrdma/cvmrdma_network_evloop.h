#pragma once

#ifdef __cplusplus

#include <capnp/ez-rpc.h>
#include "ThreadsafeMap.hpp"
#include <string>
#include <mutex>
#include <condition_variable>
#include <thread>

using RPCClientSession = capnp::EzRpcClient;
using ClientSessions = ThreadsafeMap<std::string, struct ClientSession *>;

struct ClientSession {
    kj::Executor const * m_executor = nullptr;
    kj::TaskSet * m_taskset = nullptr;

    std::condition_variable m_executor_set_cv;
    std::mutex m_executor_lock;

    RPCClientSession * m_session;
    std::jthread m_client_thread;
};

extern "C" {
#else
typedef struct RPCClientSession RPCClientSession;
typedef struct ClientSession ClientSession;
typedef struct ClientSessions ClientSessions;
#endif

typedef struct CapnpEvloopThread CapnpEvloopThread;

#ifdef __cplusplus
}
#endif

