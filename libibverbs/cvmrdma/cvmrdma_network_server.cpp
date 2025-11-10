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
#include "nonce_queue.h"
#include "debug.h"
#include "cvmrdma.h"

static capnp::CapabilityServerSet<CVMRDMA::QpNotificationChannel> qnc_capset;

QpNotificationChannel::QpNotificationChannel(
    uint32_t qpn,
    int * send_lines_full_ptr,
    pthread_mutex_t * send_lines_lock_ptr,
    pthread_cond_t * send_lines_avail_ptr,
    uint32_t * remote_ring_rkey_ptr,
    bool * remote_params_set_ptr,
    pthread_mutex_t *  remote_params_lock_ptr,
    pthread_cond_t * remote_params_set_cv_ptr,
    pthread_mutex_t * receive_nonce_lock,
    pthread_cond_t * receive_nonce_cv_ptr,

    QpNonce ** nonce_queue,
    bool * nonce_set_ptr,

    WRQueue ** wr_queue_ptr,
    struct cvmrdma_qp * qp_ptr
)
: m_qpn{qpn},
m_send_lines_full_ptr{send_lines_full_ptr},
m_send_lines_lock_ptr{send_lines_lock_ptr},
m_send_lines_avail_cv_ptr{send_lines_avail_ptr},
m_remote_ring_rkey_ptr{remote_ring_rkey_ptr},
m_remote_params_set_ptr{remote_params_set_ptr},
m_remote_params_lock_ptr{remote_params_lock_ptr},
m_remote_params_set_cv_ptr{remote_params_set_cv_ptr},
m_receive_nonce_lock{receive_nonce_lock},
m_receive_nonce_cv_ptr{receive_nonce_cv_ptr},
m_nonce_queue{nonce_queue},
m_nonce_set_ptr{nonce_set_ptr},
m_wr_queue_ptr{wr_queue_ptr},
m_qp_ptr{qp_ptr}
{ }

static int recv_counter = 0;

kj::Promise<void> QpNotificationChannel::notifyChunkRecv(NotifyChunkRecvContext context)
{
    recv_counter++;
    DEBUG("FFF [NS] notifyChunkRecv: %d\n", recv_counter);
    pthread_mutex_lock(m_send_lines_lock_ptr);
    (*m_send_lines_full_ptr) = (*m_send_lines_full_ptr) - (cvmrdma_ring_entries / 2);
    pthread_mutex_unlock(m_send_lines_lock_ptr);

    pthread_cond_signal(m_send_lines_avail_cv_ptr);

    return kj::READY_NOW;
}


kj::Promise<void> QpNotificationChannel::setRemoteParams(SetRemoteParamsContext context)
{
    auto p = context.getParams();

    /* Let's associate the remote's Tx key with this local qpn.
    The remote's Tx key is an Rx key for us. */
    uint32_t remote_ring_rkey = p.getLocalRingRkey();

    DEBUG("[NS] setRemoteParams: ring_rkey: 0x%" PRIx32 "\n",
        remote_ring_rkey);

    this->set_qp_remote_params(remote_ring_rkey);

    return kj::READY_NOW;
}

kj::Promise<void> QpNotificationChannel::notifyNonce(NotifyNonceContext context)
{
    auto p = context.getParams();

    KJ_REQUIRE(p.getNonces().size() == OPT_cvmrdma_nonce_cache_size * cvmrdma_aead_NPUB_WORDS, "Nonce list size mismatch");

    //fprintf(stderr, "Batch receive \n");
    for (int j = 0; j < OPT_cvmrdma_nonce_cache_size; j++){

        uint64_t nonce[cvmrdma_aead_NPUB_WORDS];
        for (size_t i = 0; i < cvmrdma_aead_NPUB_WORDS; i++) {
            nonce[i] = p.getNonces()[i + j * cvmrdma_aead_NPUB_WORDS];
        }

        //fprintf(stderr, "[NS] Received nonce: 0x%" PRIx64 " 0x%" PRIx64 " 0x%" PRIx64 "\n", nonce[0], nonce[1], nonce[2]);

        this->notify_nonce(nonce);
    }

    return kj::READY_NOW;
}

kj::Promise<void> QpNotificationChannel::notifyRead(NotifyReadContext context)
{
    auto p = context.getParams();
    uint64_t addr = p.getAddr();
    uint32_t size = p.getSize();
    uint32_t lkey = p.getLkey();

    DEBUG("[NS] Notify read at addr 0x%" PRIx64 " size 0x%" PRIx64 "\n", addr, size);

    this->notify_read(addr, size, lkey);

    return kj::READY_NOW;
}


kj::Promise<void> CVMRDMAServer::registerQpWithRemoteQp(RegisterQpWithRemoteQpContext context)
{
    auto p = context.getParams();
    auto rb = context.initResults();

    /* "remote" for the client is local on the server */
    uint32_t local_qpn = p.getRemoteQpn();
    DEBUG("[NS] server:getQpChannelForQpn 0x%" PRIx32 "\n", local_qpn);

    /* get a QP notification channel client. Create a new copy s.t. the server does
        not get destroyed on disconnects */
    auto qnc = m_qpn_to_qnc.at(local_qpn);

    /* and pass to the user */
    rb.setQnc(std::move(qnc));

    return kj::READY_NOW;
}

void CVMRDMAServer::qp_notification_channel_create(
    uint32_t local_qpn,
    int * send_lines_full_ptr,
    pthread_mutex_t * send_lines_lock_ptr,
    pthread_cond_t * send_lines_avail_ptr,
    uint32_t * remote_ring_rkey_ptr,
    bool * remote_params_set_ptr,
    pthread_mutex_t *  remote_params_lock_ptr,
    pthread_cond_t * remote_params_set_cv_ptr,
    pthread_mutex_t * receive_nonce_lock,
    pthread_cond_t * receive_nonce_cv_ptr,
    QpNonce ** nonce_queue,
    bool * nonce_set_ptr,
    WRQueue ** wr_queue_ptr,
    struct cvmrdma_qp * qp_ptr
)
{
    auto qnc_server = kj::heap<QpNotificationChannel>(local_qpn, send_lines_full_ptr, send_lines_lock_ptr, send_lines_avail_ptr,
        remote_ring_rkey_ptr, remote_params_set_ptr, remote_params_lock_ptr, remote_params_set_cv_ptr, receive_nonce_lock, receive_nonce_cv_ptr, nonce_queue, nonce_set_ptr, wr_queue_ptr, qp_ptr);
    DEBUG("[NS] Register fresh notification channel for qpn 0x%" PRIx32 "\n", local_qpn);

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
    uint32_t local_qpn,
    int * send_lines_full_ptr,
    pthread_mutex_t * send_lines_lock_ptr,
    pthread_cond_t * send_lines_avail_ptr,
    uint32_t * remote_ring_rkey_ptr,
    bool * remote_params_set_ptr,
    pthread_mutex_t *  remote_params_lock_ptr,
    pthread_cond_t * remote_params_set_cv_ptr,
    pthread_mutex_t * receive_nonce_lock,
    pthread_cond_t * receive_nonce_cv_ptr,
    QpNonce ** nonce_queue,
    bool * nonce_set_ptr,
    WRQueue ** wr_queue_ptr,
    struct cvmrdma_qp * qp_ptr
)
{
    assert(cs != nullptr);
    cs->qp_notification_channel_create(local_qpn, send_lines_full_ptr, send_lines_lock_ptr, send_lines_avail_ptr,
        remote_ring_rkey_ptr, remote_params_set_ptr, remote_params_lock_ptr, remote_params_set_cv_ptr, receive_nonce_lock, receive_nonce_cv_ptr, nonce_queue, nonce_set_ptr, wr_queue_ptr, qp_ptr);
}