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

#include "recv_metadata.h"
#include "debug.h"

static capnp::CapabilityServerSet<CVMRDMA::QpNotificationChannel> qnc_capset;


static enum ibv_wr_opcode capnp_to_ibv_opcode(CVMRDMA::IBVOp op)
{
    switch (op)
    {
    case CVMRDMA::IBVOp::SEND: {
        return IBV_WR_SEND;
    }
    case CVMRDMA::IBVOp::SEND_WITH_IMM: {
        return IBV_WR_SEND_WITH_IMM;
    }

    case CVMRDMA::IBVOp::WRITE: {
        return IBV_WR_RDMA_WRITE;
    }
    case CVMRDMA::IBVOp::WRITE_WITH_IMM: {
        return IBV_WR_RDMA_WRITE_WITH_IMM;
    }
    }
}


QpNotificationChannel::QpNotificationChannel(
    NotificationQueue * nq,
    uint32_t qpn,
    int * send_lines_full_ptr,
    pthread_mutex_t * send_lines_lock_ptr,
    pthread_cond_t * send_lines_avail_ptr,
    uint64_t * rx_key_ptr,
    uint32_t * remote_ring_rkey_ptr,
    bool * remote_params_set_ptr,
    pthread_mutex_t *  remote_params_lock_ptr,
    pthread_cond_t * remote_params_set_cv_ptr
)
: m_qpn{qpn},
m_notification_queue{nq},
m_send_lines_full_ptr{send_lines_full_ptr},
m_send_lines_lock_ptr{send_lines_lock_ptr},
m_send_lines_avail_cv_ptr{send_lines_avail_ptr},
m_remote_ring_rkey_ptr{remote_ring_rkey_ptr},
m_rx_key_ptr{rx_key_ptr},
m_remote_params_set_ptr{remote_params_set_ptr},
m_remote_params_lock_ptr{remote_params_lock_ptr},
m_remote_params_set_cv_ptr{remote_params_set_cv_ptr}
{ }


kj::Promise<void> QpNotificationChannel::notifyChunk(NotifyChunkContext context)
{
    auto p = context.getParams();


    auto notification = std::make_unique<struct cq_server_notification>();

    notification->hca_addr = p.getHcaAddr();
    notification->op = capnp_to_ibv_opcode(p.getOp());
    notification->rkey = p.getRkey();
    notification->imm = p.getImm();
    notification->last_chunk = p.getLastChunk();

    TIMING("push_chunk_notify");
    notification_queue_push(m_notification_queue, std::move(notification));

    return kj::READY_NOW;
}

kj::Promise<void> QpNotificationChannel::notifyChunkRecv(NotifyChunkRecvContext context)
{
    TIMING("chunk_acked");
    pthread_mutex_lock(m_send_lines_lock_ptr);
    (*m_send_lines_full_ptr) = (*m_send_lines_full_ptr) - 1;
    pthread_mutex_unlock(m_send_lines_lock_ptr);

    pthread_cond_signal(m_send_lines_avail_cv_ptr);

    return kj::READY_NOW;
}


kj::Promise<void> QpNotificationChannel::setRemoteParams(SetRemoteParamsContext context)
{
    auto p = context.getParams();

    /* Let's associate the remote's Tx key with this local qpn.
    The remote's Tx key is an Rx key for us. */
    uint64_t rx_key[cvmrdma_aead_KEY_WORDS] = {
        p.getTxKey0(),
        p.getTxKey1(),
        p.getTxKey2(),
        p.getTxKey3()
    };
    uint32_t remote_ring_rkey = p.getLocalRingRkey();
    this->set_qp_remote_params(rx_key, remote_ring_rkey);

    return kj::READY_NOW;
}


kj::Promise<void> CVMRDMAServer::registerQpWithRemoteQp(RegisterQpWithRemoteQpContext context)
{
    auto p = context.getParams();
    auto rb = context.initResults();

    /* "remote" for the client is local on the server */
    uint32_t local_qpn = p.getRemoteQpn();
    DEBUG("server:getQpChannelForQpn 0x%" PRIx32 "\n", local_qpn);

    /* get a QP notification channel client. Create a new copy s.t. the server does
        not get destroyed on disconnects */
    auto qnc = m_qpn_to_qnc.at(local_qpn);

    /* and pass to the user */
    rb.setQnc(std::move(qnc));

    return kj::READY_NOW;
}

void CVMRDMAServer::qp_notification_channel_create(
    NotificationQueue * nq,
    uint32_t local_qpn,
    int * send_lines_full_ptr,
    pthread_mutex_t * send_lines_lock_ptr,
    pthread_cond_t * send_lines_avail_ptr,
    uint64_t * rx_key_ptr,
    uint32_t * remote_ring_rkey_ptr,
    bool * remote_params_set_ptr,
    pthread_mutex_t *  remote_params_lock_ptr,
    pthread_cond_t * remote_params_set_cv_ptr
)
{
    auto qnc_server = kj::heap<QpNotificationChannel>(nq, local_qpn, send_lines_full_ptr, send_lines_lock_ptr, send_lines_avail_ptr,
        rx_key_ptr, remote_ring_rkey_ptr, remote_params_set_ptr, remote_params_lock_ptr, remote_params_set_cv_ptr);
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


void cvmrdma_server_register_qp(CVMRDMAServer * cs,
    NotificationQueue * nq,
    uint32_t local_qpn,
    int * send_lines_full_ptr,
    pthread_mutex_t * send_lines_lock_ptr,
    pthread_cond_t * send_lines_avail_ptr,
    uint64_t * rx_key_ptr,
    uint32_t * remote_ring_rkey_ptr,
    bool * remote_params_set_ptr,
    pthread_mutex_t *  remote_params_lock_ptr,
    pthread_cond_t * remote_params_set_cv_ptr
)
{
    cs->qp_notification_channel_create(nq, local_qpn, send_lines_full_ptr, send_lines_lock_ptr, send_lines_avail_ptr,
        rx_key_ptr, remote_ring_rkey_ptr, remote_params_set_ptr, remote_params_lock_ptr, remote_params_set_cv_ptr);
}