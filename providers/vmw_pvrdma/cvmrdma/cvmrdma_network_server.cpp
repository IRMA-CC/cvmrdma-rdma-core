#include "cvmrdma_network_server.h"

#include <functional>
#include <unordered_map>
#include <mutex>
#include <stop_token>
#include <thread>

#include <capnp/ez-rpc.h>

#include "cvmrdma.capnp.h"


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
        auto p = context.getParams();
        uint32_t imm = p.getImm();
        uint64_t length = p.getLength();
        uint64_t hca_addr = p.getHcaAddr();
        uint32_t rkey = p.getRkey();

        unsigned int nonce_counter = m_rx_counter++;


        /* Interpret the rkey here (ie per QP) before pushing the notification to the CQ poller:
           rkeys are per PD, but CQs do not belong to a particular PD. Therefore, only push per-context
           information through the notification */
        struct bounce_entry bounce_info = get_bounce_metadata_for_rkey(&m_bounce_metadata, rkey);

        size_t offt = hca_addr - bounce_info.hca_va;
        void * bounce_addr = (unsigned char*)bounce_info.bounce_buffer + offt;
        void * user_addr = (unsigned char*)bounce_info.user_buffer + offt;

        /* TODO: push nonce, bounce_addr, user_addr, length to QP specific queue

           poll_cq will get the qpn, and can pop entries off the right queue
        */

        struct cq_server_notification notification = {
            .bounce_addr = bounce_addr,
            .user_addr = user_addr,
            .length = length,
            .imm = imm,
            /* nonce: 3 first bytes, little endian (24 bits) */
            .nonce_counter = nonce_counter
        };

        notification_queue_push(m_notification_queue, &notification);

        return kj::READY_NOW;
    }

    uint32_t get_qpn()
    {
        return m_qpn;
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
class QpnMap {
public:
    using qnc_client_t = CVMRDMA::QpNotificationChannel::Client;
    void insert_or_assign(uint32_t qpn, qnc_client_t qnc)
    {
        auto locked = std::scoped_lock{m_lock};
        m_map.insert_or_assign(qpn, std::move(qnc));
    }

    qnc_client_t & at(uint32_t qpn)
    {
        auto locked = std::scoped_lock{m_lock};
        return m_map.at(qpn);
    }

    size_t erase(uint32_t qpn)
    {
        auto locked = std::scoped_lock{m_lock};
        return m_map.erase(qpn);
    }

private:
    std::unordered_map<uint32_t, qnc_client_t> m_map;
    std::mutex m_lock;
};


class CVMRDMAImpl : public CVMRDMA::Server
{
public:
    CVMRDMAImpl(QpnMap & qpn_to_qp)
    : m_qpn_to_qp{qpn_to_qp}
    { }


    kj::Promise<void> getQpChannelForQpn(GetQpChannelForQpnContext context) override
    {
        auto p = context.getParams();
        auto rb = context.initResults();

        uint32_t qpn = p.getQpn();

        /* get a reference to the QP notification channel client */
        auto & qnc = m_qpn_to_qp.at(qpn);

        /* and create a new client for the user */
        rb.setQnc(qnc);

        return kj::READY_NOW;
    }

private:
    QpnMap & m_qpn_to_qp;
};


static int cvmrdma_server_loop(std::stop_token stoken, QpnMap * qm)
{

    /* Create a channel to ask the server to stop: on stop request,
       the promise in the pair will be fulfilled. */
    auto stop_paf = kj::newPromiseAndFulfiller<void>();
    auto stop_cb = std::stop_callback(stoken,
        [&](){ stop_paf.fulfiller->fulfill(); }
    );

    auto main_interface = kj::heap<CVMRDMAImpl>(*qm);

    capnp::EzRpcServer server(std::move(main_interface), "*:3456");
    stop_paf.promise.wait(server.getWaitScope());

    return 0;
}


/* Represents the thread of execution. On destruction, stops the server, joins the thread,
   and destroys the memory ressources. */
class CVMRDMAServer
{
public:
    CVMRDMAServer()
    {
        m_server_thread = std::jthread{cvmrdma_server_loop, &m_qp_for_qpn};
    }


    QpNotificationChannel * qp_notification_channel_create(PdBounceMetadata * pd_bounce_metadata, NotificationQueue * nq, uint32_t qpn)
    {
        auto qnc = kj::heap<QpNotificationChannel>(*pd_bounce_metadata, nq, qpn);
        auto qnc_client = qnc_capset.add(std::move(qnc));

        m_qp_for_qpn.insert_or_assign(qpn, std::move(qnc_client));
        return qnc;
    }

    void qp_notification_channel_destroy(QpNotificationChannel * pnc)
    {
        uint32_t qpn = pnc->get_qpn();

        m_qp_for_qpn.erase(qpn);

        delete pnc;
    }

private:
    QpnMap m_qp_for_qpn;
    std::jthread m_server_thread;
};

CVMRDMAServer * cvmrdma_start_network_server()
{
    return new CVMRDMAServer{};
}

void cvmrdma_stop_network_server(CVMRDMAServer * cs)
{
    delete cs;
}

/* 

    pd_bounce_metadata: parameters of the MR/bounce in the PD that this QP belongs to
*/
void cvmrdma_server_register_qp(CVMRDMAServer * cs, PdBounceMetadata * pd_bounce_metadata, NotificationQueue * nq, uint32_t qpn)
{
    cs->qp_notification_channel_create(pd_bounce_metadata, nq, qpn);
}