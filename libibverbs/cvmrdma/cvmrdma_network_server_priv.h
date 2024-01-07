#pragma once

#include "cvmrdma_network_server.h"

#include <inttypes.h>

#include "notification_queue.h"
#include "bounce_metadata.h"
#include "recv_metadata.h"

#ifdef __cplusplus
#include "cvmrdma.capnp.h"

#include "ThreadsafeMap.hpp"
#include "aegis.hpp"
#include "notification_queue.hpp"

#include "debug.h"

using qnc_client_t = CVMRDMA::QpNotificationChannel::Client;
using QncMap = ThreadsafeMap<uint32_t, qnc_client_t>;

/* C++ API */
class QpNotificationChannel : public CVMRDMA::QpNotificationChannel::Server
{
public:
    QpNotificationChannel(PdBounceMetadata & bounce_metadata, QpRecvMetadata & recv_metadata, NotificationQueue * nq, uint32_t qpn);
    
    kj::Promise<void> notifySend(NotifySendContext context) override;

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

    /* Location of the future recvs */
    QpRecvMetadata & m_recv_metadata;


    /* Queue to CQ server. Owned by the CQ server through the NotificationQueueManager */
    NotificationQueue * m_notification_queue;
};


class CVMRDMAServer : public CVMRDMA::Server
{
public:
    kj::Promise<void> getQpChannelForQpn(GetQpChannelForQpnContext context) override;

    void qp_notification_channel_create(PdBounceMetadata * pd_bounce_metadata, QpRecvMetadata * recv_metadata, NotificationQueue * nq, uint32_t local_qpn);
    void qp_notification_channel_destroy(QpNotificationChannel * pnc);

private:
    QncMap m_qpn_to_qnc;
};


extern "C" {
#endif

/* C API */

typedef struct CVMRDMAServer CVMRDMAServer;
void cvmrdma_server_register_qp(CVMRDMAServer * cs, PdBounceMetadata * pd_bounce_metadata, QpRecvMetadata * recv_metadata, NotificationQueue * nq, uint32_t local_qpn);

#ifdef __cplusplus
}
#endif