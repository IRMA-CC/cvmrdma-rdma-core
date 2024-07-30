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

QpNotificationChannel::QpNotificationChannel(
    uint32_t qpn,
    int * send_lines_full_ptr,
    pthread_mutex_t * send_lines_lock_ptr,
    pthread_cond_t * send_lines_avail_ptr,
    uint64_t * rx_key_ptr,
    uint32_t * remote_ring_rkey_ptr,
    uint32_t * remote_control_rkey_ptr,
    uint32_t * control_qpn_ptr,
    uint32_t * control_remote_psn_ptr,
    bool * remote_params_set_ptr,
    pthread_mutex_t *  remote_params_lock_ptr,
    pthread_cond_t * remote_params_set_cv_ptr
)
: m_qpn{qpn},
m_send_lines_full_ptr{send_lines_full_ptr},
m_send_lines_lock_ptr{send_lines_lock_ptr},
m_send_lines_avail_cv_ptr{send_lines_avail_ptr},
m_rx_key_ptr{rx_key_ptr},
m_remote_ring_rkey_ptr{remote_ring_rkey_ptr},
m_remote_control_rkey_ptr{remote_control_rkey_ptr},
m_control_qpn_ptr{control_qpn_ptr},
m_control_remote_psn_ptr{control_remote_psn_ptr},
m_remote_params_set_ptr{remote_params_set_ptr},
m_remote_params_lock_ptr{remote_params_lock_ptr},
m_remote_params_set_cv_ptr{remote_params_set_cv_ptr}
{ }



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
    uint32_t remote_control_rkey = p.getLocalControlRkey();
    uint32_t control_qpn = p.getControlQPN();
    uint32_t control_remote_psn = p.getControlLocalPSN();

    DEBUG("[NS] setRemoteParams: rx_key: 0x%" PRIx64 " 0x%" PRIx64 " 0x%" PRIx64 " 0x%" PRIx64 ", ring_rkey: 0x%" PRIx32 ", control_rkey: 0x%" PRIx32 ", control_qpn: 0x%" PRIx32 ", control_remote_psn: 0x%" PRIx32 "\n",
        rx_key[0], rx_key[1], rx_key[2], rx_key[3], remote_ring_rkey, remote_control_rkey, control_qpn, control_remote_psn);

    this->set_qp_remote_params(rx_key, remote_ring_rkey, remote_control_rkey, control_qpn,  control_remote_psn);

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
    uint64_t * rx_key_ptr,
    uint32_t * remote_ring_rkey_ptr,
    uint32_t * remote_control_rkey_ptr,
    uint32_t * control_qpn_ptr,
    uint32_t * control_remote_psn_ptr,
    bool * remote_params_set_ptr,
    pthread_mutex_t *  remote_params_lock_ptr,
    pthread_cond_t * remote_params_set_cv_ptr
)
{
    auto qnc_server = kj::heap<QpNotificationChannel>(local_qpn, send_lines_full_ptr, send_lines_lock_ptr, send_lines_avail_ptr,
        rx_key_ptr, remote_ring_rkey_ptr, remote_control_rkey_ptr, control_qpn_ptr, control_remote_psn_ptr, remote_params_set_ptr, remote_params_lock_ptr, remote_params_set_cv_ptr);
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
    uint64_t * rx_key_ptr,
    uint32_t * remote_ring_rkey_ptr,
    uint32_t * remote_control_rkey_ptr,
    uint32_t * control_qpn_ptr,
    uint32_t * control_remote_psn_ptr,
    bool * remote_params_set_ptr,
    pthread_mutex_t *  remote_params_lock_ptr,
    pthread_cond_t * remote_params_set_cv_ptr
)
{
    assert(cs != nullptr);
    cs->qp_notification_channel_create(local_qpn, send_lines_full_ptr, send_lines_lock_ptr, send_lines_avail_ptr,
        rx_key_ptr, remote_ring_rkey_ptr, remote_control_rkey_ptr, control_qpn_ptr, control_remote_psn_ptr, remote_params_set_ptr, remote_params_lock_ptr, remote_params_set_cv_ptr);
}