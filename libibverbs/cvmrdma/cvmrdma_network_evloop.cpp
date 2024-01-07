#include "cvmrdma_network_evloop_priv.h"

#include <memory>
#include <mutex>
#include <condition_variable>
#include <thread>

#include <capnp/ez-rpc.h>

#include "cvmrdma_network_client_priv.h"
#include "cvmrdma_network_server_priv.h"
#include "debug.h"


class TrivialHandler : public kj::TaskSet::ErrorHandler
{
public:
    void taskFailed(kj::Exception&& exception) override
    {
        DEBUG("Task failed: %s", exception.getDescription().cStr());
    }
};


/* The capnp thread starts the CVMRDMA server and returns its executor
   so that the main thread can call functions on this thread. In particular;
   this allows the main thread to create new client sessions on this thread. */
void capnp_event_loop(std::stop_token stoken,
    char const * local_bind_addr,
    kj::Executor const ** client_evloop_executor,
    kj::TaskSet ** taskset,
    std::condition_variable * executor_cv, std::mutex * executor_lock,
    CVMRDMAServer ** server_cap
    )
{

    /* Create a channel to ask the event loop to stop: on stop request,
       the promise in the pair will be fulfilled. Since a PAF is not
       threadsafe, we must fulfill it through the executor as the
       stop cb is executed in the requester thread! */
    auto stop_paf = kj::newPromiseAndFulfiller<void>();
    auto stop_cb = std::stop_callback(stoken,
        [&](){
            auto locked = std::unique_lock{*executor_lock};
            while(! client_evloop_executor)
            {
                executor_cv->wait(locked);
            }
            (*client_evloop_executor)->executeSync(
                [&] { stop_paf.fulfiller->fulfill(); }
            );
        }
    );

    DEBUG("Starting capnp server on %s\n", local_bind_addr);

    auto main_interface = kj::heap<CVMRDMAServer>(); 
    *server_cap = main_interface.get();

    capnp::EzRpcServer server(std::move(main_interface), local_bind_addr);

    auto tk_handler = TrivialHandler{};
    auto tk = kj::TaskSet{tk_handler};

    /* Pass executor to caller */
    {
        auto locked = std::unique_lock{*executor_lock};
        *client_evloop_executor = &kj::getCurrentThreadExecutor();
        *taskset = &tk;
    }
    executor_cv->notify_one();

    stop_paf.promise.wait(server.getWaitScope());
}

class CapnpEvloopThread
{
public:
    CapnpEvloopThread(char const * local_bind_addr, CVMRDMAServer ** server)
    {
        m_thread = std::jthread{capnp_event_loop, local_bind_addr, &m_executor, &m_taskset, &m_executor_set_cv, &m_executor_lock, server};

        /* Wait until thread sets executor */
        {
            auto locked = std::unique_lock{m_executor_lock};
            while(! m_executor)
            {
                m_executor_set_cv.wait(locked);
            }
        }
    }

    
    static void trivialHandler(kj::Exception&& exception)
    {
        DEBUG("Task failed: %s", exception.getDescription().cStr());
    }

    template<typename Func>
    void executeLongRunning(Func && fun)
    {
        /* Execute the RPC on the thread running the event loop, and let this long
           running task run in the background  */
        m_executor->executeSync(
            [this, fun = std::forward<Func>(fun)]() -> void {
                m_taskset->add(fun());
            }
        );
    }

    kj::Executor const & get_executor()
    {
        return *m_executor;
    }

    kj::TaskSet & get_taskset()
    {
        return *m_taskset;
    }


    /* Cannot make it a kj::Own bc destructor fails on different thread ...
       HACKY: using raw new/delete is not exception safe ! */
    std::unique_ptr<capnp::EzRpcClient> new_rpc_session(char const * server_addr)
    {
        /* Create a new client session on the capnp thread */
        return m_executor->executeSync(
            [server_addr]() {
                DEBUG("Starting capnp client session connected to '%s'\n", server_addr);
                return std::make_unique<capnp::EzRpcClient>(server_addr);
            }
        );
    }


private:
    std::jthread m_thread;

    kj::Executor const * m_executor = nullptr;
    kj::TaskSet * m_taskset = nullptr;

    std::condition_variable m_executor_set_cv;
    std::mutex m_executor_lock;
};


/* C API */




CapnpEvloopThread * create_capnp_event_loop(char const * local_bind_addr, CVMRDMAServer ** server)
{
    /* HACKY: it would be nicer to create the CVMRDAServer here, but kj makes it difficult to 
       pass kj::Own object to STL objects such as std::jthread[^1] ... So instead, create the
       server interface in the other thread and pass _back_ a raw pointer here. 
       
       [1]: std::jthread excepts to build a State object with a noexcept destructor, but
            kj::Own objects may throw in their destructors, so they cannot be directly passed
            to a thread object
    */
        
    return new CapnpEvloopThread{local_bind_addr, server};
}

void destroy_capnp_event_loop(CapnpEvloopThread * evloop)
{
    delete evloop;
}

ClientSession * new_client_rpc_session(CapnpEvloopThread * evloop, char const * server_addr)
{
    /* Create a new client session on the capnp thread */
    return evloop->get_executor().executeSync(
        [server_addr]() {
            DEBUG("Starting capnp client session connected to '%s'\n", server_addr);
            return new capnp::EzRpcClient{server_addr};
        }
    );
}

void destroy_client_rpc_session(CapnpEvloopThread * evloop, ClientSession * session)
{
    /* MUST be destroyed on the thread where it was created ! */
    evloop->get_executor().executeSync(
        [session]() { delete session; }
    );
}

CVMRDMAClient * get_cvmrdma_client(CapnpEvloopThread * evloop, ClientSession * session)
{
    /* Create a new client session on the capnp thread */
    auto client_cap = evloop->get_executor().executeSync(
        [session]() {
            return session->getMain<CVMRDMA>();
        }
    );

    return new CVMRDMAClient{std::move(client_cap)};
}

void destroy_cvmrdma_client(CapnpEvloopThread * evloop, CVMRDMAClient * client)
{
    /* MUST be destroyed on the thread where it was created ! */
    evloop->get_executor().executeSync(
        [client]() { delete client; }
    );
}


/*
   Capnp objects can only be used from the thread where they have been created (also they can be owned
   in another one).
   
   In order to run these RPCs in the background, they are scheduled off the main thread, on the thread
   running the capnp event loop.

*/
QPNotificationClient * get_qp_notification_client(CapnpEvloopThread * evloop, CVMRDMAClient * client, uint32_t remote_qpn, uint64_t const * tx_key)
{
    /* The capability is created in this thread, is this a problem ??
       Plus it's a _lazy_ capability ! Should we wait on it before ?
    */

    return new QPNotificationClient{
        evloop->get_executor().executeSync(
            [=]() {
                return get_qp_notification_client_rpc(client, remote_qpn, tx_key);
            }
        )
    };
}

void remote_qp_notify_send(CapnpEvloopThread *cev, QPNotificationClient *qnc,
			   uint64_t length, uint64_t const *nonce,
			   uint64_t const *mac)
{
    DEBUG("remote_qp_notify_send\n");

    /* Add to taskset on evloop thread */
    cev->get_executor().executeSync(
        [=]() {
            cev->get_taskset().add(
                remote_qp_notify_send_rpc(qnc, length, nonce, mac)
            );
        }
    );
}


void remote_qp_notify_send_with_imm(CapnpEvloopThread *cev, QPNotificationClient *qnc,
				    uint64_t length, uint32_t imm,
				    uint64_t const *nonce, uint64_t const *mac)
{
	DEBUG("remote_qp_notify_send_with_imm\n");

    cev->get_executor().executeSync(
        [=]() {
            cev->get_taskset().add(
                remote_qp_notify_send_with_imm_rpc(qnc, length, imm, nonce, mac)
            );
        }
    );

}

void remote_qp_notify_write(CapnpEvloopThread *cev, QPNotificationClient *qnc,
			    uint64_t hca_addr, uint64_t length, uint32_t rkey,
			    uint64_t const *nonce, uint64_t const *mac)
{
	DEBUG("remote_qp_notify_write\n");

    cev->get_executor().executeSync(
        [=]() {
            cev->get_taskset().add(
                remote_qp_notify_write_rpc(qnc, hca_addr, length, rkey, nonce, mac)
            );
        }
    );
}

void remote_qp_notify_write_with_imm(CapnpEvloopThread *cev, QPNotificationClient *qnc,
				     uint64_t hca_addr, uint64_t length,
				     uint32_t rkey, uint32_t imm,
				     uint64_t const *nonce, uint64_t const *mac)
{
	DEBUG("remote_qp_notify_write_with_imm\n");

    cev->get_executor().executeSync(
        [=]() {
            cev->get_taskset().add(
                remote_qp_notify_write_with_imm_rpc(qnc, hca_addr, length, rkey,
						    imm, nonce, mac)
            );
        }
    );
}
