#include "cvmrdma_network_client.h"

#include <mutex>
#include <condition_variable>
#include <thread>

#include "debug.h"

#include <sodium.h>


/* Option 1: an event loop with its own capnp client session and client cap runs in a deicated thread */

static void client_event_loop(std::stop_token stoken,
    char const * server_addr,
    kj::Executor const ** client_evloop_executor, std::condition_variable * executor_cv, std::mutex * executor_lock
    )
{

    /* Create a channel to ask the client to stop: on stop request,
       the promise in the pair will be fulfilled. */
    auto stop_paf = kj::newPromiseAndFulfiller<void>();
    auto stop_cb = std::stop_callback(stoken,
        [&](){ stop_paf.fulfiller->fulfill(); }
    );

    DEBUG("Starting capnp client thread, connecting to '%s'\n", server_addr);


    auto session = ClientSession{server_addr};
    auto cvmrda_client = CVMRDMAClient{session.getMain<CVMRDMA>()};

    /* Pass executor to caller */
    {
        auto locked = std::unique_lock{*executor_lock};
        *client_evloop_executor = &kj::getCurrentThreadExecutor();
    }
    executor_cv->notify_one();

    stop_paf.promise.wait(session.getWaitScope());
}

class ClientEvloopThread
{
public:
    ClientEvloopThread(char const * server_addr)
    {
        m_thread = std::jthread{client_event_loop, server_addr, &m_client_executor, &m_executor_set_cv, &m_executor_lock};
    }

    /* Blocks until the client thread has passed us a pointer to its executor, then returns this pointer */
    kj::Executor const & get_client_thread_executor(void)
    {
        auto locked = std::unique_lock{m_executor_lock};
        while(! m_client_executor)
        {
            m_executor_set_cv.wait(locked);
        }
        return *m_client_executor;
    }

private:
    std::jthread m_thread;

    kj::Executor const * m_client_executor = nullptr;
    std::condition_variable m_executor_set_cv;
    std::mutex m_executor_lock;
};


/* Option 2: client session and client cap are used on the main thread */

ClientSession * cvmrdma_start_session(char const * server_addr)
{
    return new ClientSession{server_addr};
}

void cvmrdma_stop_session(ClientSession * session)
{
    delete session;
}


CVMRDMAClient * cvmrdma_client_init(ClientSession * session)
{
    return new CVMRDMAClient{session->getMain<CVMRDMA>()};
}

void cvmrdma_client_destroy(CVMRDMAClient * client)
{
    delete client;
}

QPNotificationClient * get_qp_notification_client(ClientSession * session, CVMRDMAClient * client, uint32_t remote_qpn, uint64_t const * tx_key)
{
    auto req = client->getQpChannelForQpnRequest();

    req.setQpn(remote_qpn);

    req.setTxKey0(tx_key[0]);
    req.setTxKey1(tx_key[1]);
    req.setTxKey2(tx_key[2]);
    req.setTxKey3(tx_key[3]);

    auto rep = req.send().wait(session->getWaitScope());

    auto qnc = rep.getQnc();

    return new QPNotificationClient{qnc};
}



void remote_qp_notify_write_rpc_add_to_taskset(taskset_t * taskset, QPNotificationClient * qnc, uint64_t hca_addr, uint64_t length, uint32_t rkey, uint32_t imm, bool has_imm, uint64_t const * nonce, uint64_t const * mac)
{
    DEBUG("remote_qp_notify_write_rpc\n");

    auto req = qnc->notifyWriteRequest();
    req.setHcaAddr(hca_addr);
    req.setLength(length);
    req.setRkey(rkey);
    req.setImm(imm);
    req.setHasImm(has_imm);
    
    req.setNonce0(nonce[0]);
    req.setNonce1(nonce[1]);
    req.setNonce2(nonce[2]);
    req.setNonce3(nonce[3]);

    req.setMac0(mac[0]);
    req.setMac1(mac[1]);
    req.setMac2(mac[2]);
    req.setMac3(mac[3]);

    auto rep = req.send();
    
    taskset->add(rep.ignoreResult());
}