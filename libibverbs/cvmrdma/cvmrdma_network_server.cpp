#include "cvmrdma_network_server.h"

#include <cinttypes>
#include <cassert>

#include <functional>
#include <unordered_map>
#include <mutex>
#include <stop_token>
#include <thread>

#include <capnp/ez-rpc.h>
#include <sodium.h>

#include "aegis.hpp"
#include "notification_queue.hpp"
#include "cvmrdma.capnp.h"
#include "debug.h"

static capnp::CapabilityServerSet<CVMRDMA::QpNotificationChannel> qnc_capset;


class QpNotificationChannel : public CVMRDMA::QpNotificationChannel::Server
{
public:

    QpNotificationChannel(PdBounceMetadata & bounce_metadata, NotificationQueue * nq, uint32_t qpn)
    : m_bounce_metadata{bounce_metadata},
      m_qpn{qpn},
      m_notification_queue{nq}
    { }

    kj::Promise<void> notifyWrite(NotifyWriteContext context) override
    {
        DEBUG("[QPN %" PRIx32 "] notifyWrite\n", m_qpn);

        auto p = context.getParams();
        uint64_t length = p.getLength();
        uint64_t hca_addr = p.getHcaAddr();
        uint32_t rkey = p.getRkey();
        uint32_t imm = p.getImm();
        bool has_imm = p.getHasImm();

        unsigned int nonce_counter = m_rx_counter++;


        /* Interpret the rkey here (ie per QP) before pushing the notification to the CQ poller:
           rkeys are per PD, but CQs do not belong to a particular PD. Therefore, only push per-context
           information through the notification. We can do the conversion here because the current QP
           indicates which PD to use. */
        struct bounce_entry bounce_info = get_bounce_metadata_for_rkey(&m_bounce_metadata, rkey);

        size_t offt = hca_addr - bounce_info.hca_va;
        void * bounce_addr = (unsigned char*)bounce_info.bounce_buffer + offt;
        void * user_addr = (unsigned char*)bounce_info.user_buffer + offt;

        /* Push nonce, bounce_addr, user_addr, length to QP specific queue
           Poll_cq will get the qpn, and can pop entries off the right queue
        */

        auto notification = std::make_unique<struct cq_server_notification>();

        notification->bounce_addr = bounce_addr;
        notification->user_addr = user_addr;
        notification->length = length;
        notification->imm = imm;
        notification->has_imm = has_imm;

        /* Pass nonce and MAC from their uint64 form to CQ server */
        uint64_t * nonce64 = notification->nonce;
        nonce64[0] = p.getNonce0();
        nonce64[1] = p.getNonce1();
        nonce64[2] = p.getNonce2();
        nonce64[3] = p.getNonce3();

        uint64_t * mac64 = notification->mac;
        mac64[0] = p.getMac0();
        mac64[1] = p.getMac1();
        mac64[2] = p.getMac2();
        mac64[3] = p.getMac3();
        notification_queue_push(m_notification_queue, std::move(notification));
        TIMING("notif_srv_done");

        return kj::READY_NOW;
    }

    uint32_t get_qpn()
    {
        return m_qpn;
    }

    void set_rx_key(aegis_key_t const & rx_key)
    {
        DEBUG("setting rx_key on QNC for QPN %" PRIx32 "\n", m_qpn);
        notification_queue_set_rx_key(m_notification_queue, rx_key);
    }

private:
    uint32_t m_qpn = 0;

    /* Per QP counter used as a nonce. */
    unsigned int m_rx_counter = 0;

    /* Metadata of the bounce buffers in this QP's PD */
    PdBounceMetadata & m_bounce_metadata;

    /* Queue to CQ server. Owned by the CQ server through the NotificationQueueManager */
    NotificationQueue * m_notification_queue;
};




/* Threadsafe hashmap */
template <typename K, typename V>
class ThreadsafeMap {
public:
    void insert_or_assign(K qpn, V qnc)
    {
        auto locked = std::scoped_lock{m_lock};
        m_map.insert_or_assign(qpn, std::move(qnc));
    }

    V & at(K qpn)
    {
        auto locked = std::scoped_lock{m_lock};
        return m_map.at(qpn);
    }

    size_t erase(K qpn)
    {
        auto locked = std::scoped_lock{m_lock};
        return m_map.erase(qpn);
    }

private:
    std::unordered_map<K, V> m_map;
    std::mutex m_lock;
};

using qnc_client_t = CVMRDMA::QpNotificationChannel::Client;
using QncMap = ThreadsafeMap<uint32_t, qnc_client_t>;

class CVMRDMAImpl : public CVMRDMA::Server
{
public:
    CVMRDMAImpl(QncMap & qpn_to_qnc)
    : m_qpn_to_qnc{qpn_to_qnc}
    { }


    kj::Promise<void> getQpChannelForQpn(GetQpChannelForQpnContext context) override
    {
        auto p = context.getParams();
        auto rb = context.initResults();

        uint32_t local_qpn = p.getQpn();
        DEBUG("getQpChannelForQpn 0x%" PRIx32 "\n", local_qpn);


        /* Let's associate the remote's Tx key with this local qpn.
           The remote's Tx key is an Rx key for us. */
        aegis_key_t rx_key_64;
        rx_key_64[0] = p.getTxKey0();
        rx_key_64[1] = p.getTxKey1();
        rx_key_64[2] = p.getTxKey2();
        rx_key_64[3] = p.getTxKey3();

        /* get a QP notification channel client. Create a new copy s.t. the server does
           not get destroyed on disconnects */
        auto qnc = m_qpn_to_qnc.at(local_qpn);

        /* first start finding the corresponding server */
        auto qnc_server = qnc_capset.getLocalServer(qnc);

        /* and pass to the user */
        rb.setQnc(std::move(qnc));

        /* Actually find the server to save the key. Make sure to move the array here so that it is not destroyed before the promise is resolved ! */
        return qnc_server.then(
            [local_qpn, rx_key_64 = std::move(rx_key_64)](kj::Maybe<CVMRDMA::QpNotificationChannel::Server &> qnc_server_maybe) -> kj::Promise<void> {
                KJ_IF_MAYBE(qnc_server, qnc_server_maybe)
                {
                    auto qnc_impl = dynamic_cast<QpNotificationChannel *>(qnc_server);
                    assert(qnc_impl);
                    qnc_impl->set_rx_key(rx_key_64);
                }
                else
                {
                    fprintf(stderr, "QpNotificationChannel for QPN %" PRIx32 " is NOT a local server\n", local_qpn);
                }

        return kj::READY_NOW;
            }
        );
    }

private:
    QncMap & m_qpn_to_qnc;
};


static int cvmrdma_server_loop(std::stop_token stoken, QncMap * qm, char const * server_bind_addr)
{
    /* Create a channel to ask the server to stop: on stop request,
       the promise in the pair will be fulfilled. */
    auto stop_paf = kj::newPromiseAndFulfiller<void>();
    auto stop_cb = std::stop_callback(stoken,
        [&](){ stop_paf.fulfiller->fulfill(); }
    );

    auto main_interface = kj::heap<CVMRDMAImpl>(*qm);


    DEBUG("Starting capnp server on '%s'\n", server_bind_addr);
    capnp::EzRpcServer server(std::move(main_interface), server_bind_addr);
    stop_paf.promise.wait(server.getWaitScope());

    return 0;
}


/* Represents the thread of execution. On destruction, stops the server, joins the thread,
   and destroys the memory ressources. */
class CVMRDMAServer
{
public:
    CVMRDMAServer(char const * server_bind_addr)
    {
        m_server_thread = std::jthread{cvmrdma_server_loop, &m_qp_for_qpn, server_bind_addr};
    }


    void qp_notification_channel_create(PdBounceMetadata * pd_bounce_metadata, NotificationQueue * nq, uint32_t local_qpn)
    {
        auto qnc_server = kj::heap<QpNotificationChannel>(*pd_bounce_metadata, nq, local_qpn);
        DEBUG("Register fresh notification channel for qpn 0x%" PRIx32 "\n", local_qpn);

        auto qnc_client = qnc_capset.add(std::move(qnc_server));

        m_qp_for_qpn.insert_or_assign(local_qpn, std::move(qnc_client));
    }

    void qp_notification_channel_destroy(QpNotificationChannel * pnc)
    {
        uint32_t qpn = pnc->get_qpn();

        m_qp_for_qpn.erase(qpn);

        delete pnc;
    }

private:
    QncMap m_qp_for_qpn;
    std::jthread m_server_thread;
};

CVMRDMAServer * cvmrdma_start_network_server(char const * server_bind_addr)
{
    return new CVMRDMAServer{server_bind_addr};
}

void cvmrdma_stop_network_server(CVMRDMAServer * cs)
{
    delete cs;
}

/* 

    pd_bounce_metadata: parameters of the MR/bounce in the PD that this QP belongs to
*/
void cvmrdma_server_register_qp(CVMRDMAServer * cs, PdBounceMetadata * pd_bounce_metadata, NotificationQueue * nq, uint32_t local_qpn)
{
    cs->qp_notification_channel_create(pd_bounce_metadata, nq, local_qpn);
}