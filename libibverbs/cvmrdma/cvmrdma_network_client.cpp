#include "cvmrdma_network_client_priv.h"

#include <mutex>
#include <condition_variable>
#include <thread>

#include "debug.h"

static int send_counter = 0;

kj::Promise<void> remote_qp_notify_chunk_recv_rpc(QPNotificationClient * qnc)
{
    send_counter++;
    DEBUG("FFF [NC] Notify chunk recv: %d\n", send_counter);
    auto req = qnc->notifyChunkRecvRequest();

    return req.send().ignoreResult();
}


kj::Promise<void> set_remote_params_rpc(QPNotificationClient * qnc, uint32_t local_ring_rkey, 
                                        uint32_t local_control_rkey, uint32_t control_qpn, uint32_t control_local_psn)
{
    auto req = qnc->setRemoteParamsRequest();

    req.setLocalRingRkey(local_ring_rkey);
    req.setLocalControlRkey(local_control_rkey);

    req.setControlQPN(control_qpn);
    req.setControlLocalPSN(control_local_psn);

    DEBUG("[NC] Setting remote params for remote: ring rkey 0x%" PRIx32 ", control rkey 0x%" PRIx32 ", control qpn 0x%" PRIx32 ", control local psn 0x%" PRIx32 "\n",
        local_ring_rkey, local_control_rkey, control_qpn, control_local_psn);

    return req.send().ignoreResult();
}

kj::Promise<void> notify_nonce_rpc(QPNotificationClient * qnc, uint64_t * nonce)
{
    auto req = qnc->notifyNonceRequest();

    req.setNonce0(nonce[0]);
    req.setNonce1(nonce[1]);
    req.setNonce2(nonce[2]);

    DEBUG("[NC] Sending nonce to remote: 0x%" PRIx64 " 0x%" PRIx64 " 0x%" PRIx64 "\n",
        nonce[0], nonce[1], nonce[2]);

    return req.send().ignoreResult();
}

QPNotificationClient register_qp_with_remote_qp_rpc(CVMRDMAClient * client, uint32_t remote_qpn)
{
    auto req = client->registerQpWithRemoteQpRequest();

    req.setRemoteQpn(remote_qpn);

    /* WARNING: lazy, when returned we're not sure whether the remote really has the tx key !*/
    return req.send().getQnc();
}

kj::Promise<void> remote_qp_notify_read_rpc(QPNotificationClient * qnc, uint64_t addr, uint32_t size)
{
    auto req = qnc->notifyReadRequest();

    req.setAddr(addr);
    req.setSize(size);

    DEBUG("[NC] Notify read: addr 0x%" PRIx64 ", size 0x%" PRIx64 "\n", addr, size);

    return req.send().ignoreResult();
}

