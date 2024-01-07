#include "cvmrdma_network_server_priv.h"

#include <cinttypes>
#include <cassert>

#include <functional>
#include <unordered_map>
#include <mutex>
#include <stop_token>
#include <thread>

#include <capnp/ez-rpc.h>
#include <sodium.h>

#include "recv_metadata.hpp"
#include "debug.h"

static capnp::CapabilityServerSet<CVMRDMA::QpNotificationChannel> qnc_capset;


QpNotificationChannel::QpNotificationChannel(PdBounceMetadata & bounce_metadata, QpRecvMetadata & recv_metadata, NotificationQueue * nq, uint32_t qpn)
: m_bounce_metadata{bounce_metadata},
    m_recv_metadata{recv_metadata},
    m_qpn{qpn},
    m_notification_queue{nq}
{ }

kj::Promise<void> QpNotificationChannel::notifySend(NotifySendContext context)
{
    DEBUG("[QPN %" PRIx32 "] notifySend\n", m_qpn);
    TIMING("notif_srv_t0");

    auto p = context.getParams();
    auto op = p.getOp();
    uint64_t length = p.getLength();
    uint32_t imm = p.getImm();
    uint32_t rkey = p.getRkey();

    unsigned int nonce_counter = m_rx_counter++;
    
    struct bounce_entry bounce_info = {0};
    uint64_t hca_addr = 0;
    switch (op) {
        case CVMRDMA::IBVOp::WRITE:
        case CVMRDMA::IBVOp::WRITE_WITH_IMM: {
            /* Interpret the rkey here (ie per QP) before pushing the notification to the CQ poller:
            rkeys are per PD, but CQs do not belong to a particular PD. Therefore, only push per-context
            information through the notification. We can do the conversion here because the current QP
            indicates which PD to use. */
            
            bounce_info = get_bounce_metadata_for_rkey(&m_bounce_metadata, rkey);

            /* For a WRITE, the HCA addr is defined by the remote */
            hca_addr = p.getHcaAddr();

            break;
        }
        case CVMRDMA::IBVOp::SEND:
        case CVMRDMA::IBVOp::SEND_WITH_IMM: {
            /* Get the sgl of the recv */
            recv_metadata_entry & recv_metadata = m_recv_metadata.queue.front();

            /* XXX: for now assume the sg-list is a single element */
            if (recv_metadata.sgl.size() > 1)
            {
                fprintf(stderr, "ERROR: more than 1 sge in an sg-list for recv is NYI!\n");
                exit(1);
            }

            if(recv_metadata.sgl.size() > 0)
            {
                bounce_info = get_bounce_metadata_for_lkey(&m_bounce_metadata, recv_metadata.sgl[0].lkey);

                /* For a SEND, the HCA addr was chosen locally at the corresponding recv */
                hca_addr = recv_metadata.sgl[0].addr;
            }
            else
            {
                fprintf(stderr, "ERROR: empty sge in an sg-list for recv is NYI!\n");
                exit(1);
            }
            break;
        }
    }

    size_t offt = hca_addr - bounce_info.hca_va;
    void * bounce_addr = (unsigned char*)bounce_info.bounce_buffer + offt;
    void * user_addr = (unsigned char*)bounce_info.user_buffer + offt;

    /* Translate CVMRDMA opcode into ibv opcode */
    enum ibv_wr_opcode ibv_op;
    switch (op)
    {
    case CVMRDMA::IBVOp::SEND: {
        ibv_op = IBV_WR_SEND;
        break;
    }
    case CVMRDMA::IBVOp::SEND_WITH_IMM: {
        ibv_op = IBV_WR_SEND_WITH_IMM;
        break;
    }

    case CVMRDMA::IBVOp::WRITE: {
        ibv_op = IBV_WR_RDMA_WRITE;
        break;
    }
    case CVMRDMA::IBVOp::WRITE_WITH_IMM: {
        ibv_op = IBV_WR_RDMA_WRITE_WITH_IMM;
        break;
    }
    }


    /* Push nonce, bounce_addr, user_addr, length to QP specific queue
        Poll_cq will get the qpn, and can pop entries off the right queue
    */

    auto notification = std::make_unique<struct cq_server_notification>();

    notification->bounce_addr = bounce_addr;
    notification->user_addr = user_addr;
    notification->length = length;
    notification->op = ibv_op;
    notification->imm = imm;

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

    TIMING("notif_srv_push");
    notification_queue_push(m_notification_queue, std::move(notification));
    TIMING("notif_srv_done");

    return kj::READY_NOW;
}



kj::Promise<void> CVMRDMAServer::getQpChannelForQpn(GetQpChannelForQpnContext context)
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

void CVMRDMAServer::qp_notification_channel_create(PdBounceMetadata * pd_bounce_metadata, QpRecvMetadata * recv_metadata, NotificationQueue * nq, uint32_t local_qpn)
{
    auto qnc_server = kj::heap<QpNotificationChannel>(*pd_bounce_metadata, *recv_metadata, nq, local_qpn);
    DEBUG("Register fresh notification channel for qpn 0x%" PRIx32 "\n", local_qpn);

    auto qnc_client = qnc_capset.add(std::move(qnc_server));

    m_qpn_to_qnc.insert_or_assign(local_qpn, std::move(qnc_client));
}

void CVMRDMAServer::qp_notification_channel_destroy(QpNotificationChannel * pnc)
{
    uint32_t qpn = pnc->get_qpn();

    m_qpn_to_qnc.erase(qpn);

    delete pnc;
}

/* C API */

/* 
    pd_bounce_metadata: parameters of the MR/bounce in the PD that this QP belongs to
*/


void cvmrdma_server_register_qp(CVMRDMAServer * cs, PdBounceMetadata * pd_bounce_metadata, QpRecvMetadata * recv_metadata, NotificationQueue * nq, uint32_t local_qpn)
{
    cs->qp_notification_channel_create(pd_bounce_metadata, recv_metadata, nq, local_qpn);
}