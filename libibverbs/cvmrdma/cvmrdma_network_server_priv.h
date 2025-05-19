#pragma once

#include "cvmrdma_network_server.h"

#include <inttypes.h>

#include <pthread.h>

#include "bounce_metadata.h"
#include "recv_metadata.h"
#include "nonce_queue.h"

#ifdef __cplusplus
#include "cvmrdma.capnp.h"

#include "ThreadsafeMap.hpp"
#include "crypto.h"

#include "debug.h"

using qnc_client_t = CVMRDMA::QpNotificationChannel::Client;
using QncMap = ThreadsafeMap<uint32_t, qnc_client_t>;

/* C++ API */
class QpNotificationChannel : public CVMRDMA::QpNotificationChannel::Server
{
public:
    QpNotificationChannel(
        uint32_t qpn,
        int * send_lines_full_ptr,
        pthread_mutex_t * send_lines_lock_ptr,
        pthread_cond_t * send_lines_avail_ptr,
        uint32_t * remote_ring_rkey_ptr,
        uint32_t * remote_control_rkey_ptr,
        uint32_t * control_qpn_ptr,
        uint32_t * control_remote_psn_ptr,
        bool * remote_params_set_ptr,
        pthread_mutex_t *  remote_params_lock_ptr,
        pthread_cond_t * remote_params_set_cv_ptr,
        pthread_mutex_t * receive_nonce_lock,
        pthread_cond_t * receive_nonce_cv_ptr,
        NonceQueue ** nonce_queue,
        bool * nonce_set_ptr
    );
    
    kj::Promise<void> notifyChunkRecv(NotifyChunkRecvContext context) override;
    kj::Promise<void> setRemoteParams(SetRemoteParamsContext context) override;
    kj::Promise<void> notifyNonce(NotifyNonceContext context) override;
    
    uint32_t get_qpn()
    {
        return m_qpn;
    }

    void set_qp_remote_params(uint32_t ring_rkey, uint32_t control_rkey, uint32_t control_qpn, uint32_t control_remote_psn)
    {
        DEBUG("[NS] setting ring rkey %" PRIx32 " and control rkey %" PRIx32 " on QNC for local QPN 0x%" PRIx32 ", control QPN 0x%" PRIx32 ", control local PSN 0x%" PRIx32 ", control remote PSN 0x%" PRIx32 "\n",
            ring_rkey, control_rkey, m_qpn, control_qpn, control_remote_psn);

        pthread_mutex_lock(m_remote_params_lock_ptr);
        *m_remote_ring_rkey_ptr = ring_rkey;
        *m_remote_control_rkey_ptr = control_rkey;
        *m_control_qpn_ptr = control_qpn;
        *m_control_remote_psn_ptr = control_remote_psn;
        *m_remote_params_set_ptr = true;
        pthread_mutex_unlock(m_remote_params_lock_ptr);

        pthread_cond_broadcast(m_remote_params_set_cv_ptr);
    }

    void notify_nonce(uint64_t * nonce)
    {
        pthread_mutex_lock(m_receive_nonce_lock);
        qp_nonce_queue_push(*m_nonce_queue, nonce[0], nonce[1], nonce[2]);
        *m_nonce_set_ptr = true;
        pthread_mutex_unlock(m_receive_nonce_lock);
        DEBUG("[NS] Sending nonce signal %" PRIx64 " 0x%" PRIx64 " 0x%" PRIx64 "\n",
            nonce[0], nonce[1], nonce[2]);
        pthread_cond_signal(m_receive_nonce_cv_ptr);
    }

private:
    uint32_t m_qpn = 0;

    uint32_t * m_remote_ring_rkey_ptr = nullptr; 
    uint32_t * m_remote_control_rkey_ptr = nullptr; 
    uint32_t * m_control_qpn_ptr = nullptr;
    uint32_t * m_control_remote_psn_ptr = nullptr;
	bool * m_remote_params_set_ptr = nullptr;
	pthread_mutex_t * m_remote_params_lock_ptr = nullptr;
	pthread_cond_t * m_remote_params_set_cv_ptr = nullptr;
    pthread_mutex_t * m_receive_nonce_lock = nullptr;
    pthread_cond_t * m_receive_nonce_cv_ptr = nullptr;
    NonceQueue ** m_nonce_queue = nullptr;
    bool * m_nonce_set_ptr = nullptr;

    /* Counter in the original qp tracking the number of lines full in the remote recv ring / the locl send ring */
    int * m_send_lines_full_ptr = nullptr;
    pthread_mutex_t * m_send_lines_lock_ptr = nullptr;
    pthread_cond_t * m_send_lines_avail_cv_ptr = nullptr;
};


class CVMRDMAServer : public CVMRDMA::Server
{
public:
    kj::Promise<void> registerQpWithRemoteQp(RegisterQpWithRemoteQpContext context) override;

    void qp_notification_channel_create(
        uint32_t local_qpn,
        int * send_lines_full_ptr,
        pthread_mutex_t * send_lines_lock_ptr,
        pthread_cond_t * send_lines_avail_ptr,
        uint32_t * remote_ring_rkey_ptr,
        uint32_t * remote_control_rkey_ptr,
        uint32_t * control_qpn_ptr,
        uint32_t * control_remote_psn_ptr,
        bool * remote_params_set_ptr,
        pthread_mutex_t *  remote_params_lock_ptr,
        pthread_cond_t * remote_params_set_cv_ptr,
        pthread_mutex_t * receive_nonce_lock,
        pthread_cond_t * receive_nonce_cv_ptr,
        NonceQueue ** nonce_queue,
        bool * nonce_set_ptr
    );
    void qp_notification_channel_destroy(QpNotificationChannel * pnc);

private:
    QncMap m_qpn_to_qnc;
};


extern "C" {
#endif

/* C API */

void cvmrdma_server_register_qp(CVMRDMAServer * cs,
    uint32_t local_qpn,
    int * send_lines_full_ptr,
    pthread_mutex_t * send_lines_lock_ptr,
    pthread_cond_t * send_lines_avail_ptr,
    uint32_t * remote_ring_rkey_ptr,
    uint32_t * remote_control_rkey_ptr,
    uint32_t * control_qpn_ptr,
    uint32_t * control_remote_psn_ptr,
    bool * remote_params_set_ptr,
    pthread_mutex_t *  remote_params_lock_ptr,
    pthread_cond_t * remote_params_set_cv_ptr,
    pthread_mutex_t * receive_nonce_lock,
    pthread_cond_t * receive_nonce_cv_ptr,
    NonceQueue ** nonce_queue,
    bool * nonce_set_ptr
);

#ifdef __cplusplus
}
#endif