#include "cvmrdma_network_client_priv.h"

#include <mutex>
#include <condition_variable>
#include <thread>

#include "debug.h"

#include <sodium.h>

kj::Promise<void> remote_qp_notify_send_rpc(
    QPNotificationClient * qnc,
    uint64_t length,
    uint64_t const * nonce, uint64_t const * mac)
{
    DEBUG("remote_qp_notify_write_rpc\n");

    auto req = qnc->notifySendRequest();

    req.setOp(CVMRDMA::IBVOp::SEND);

    req.setLength(length);
    
    req.setNonce0(nonce[0]);
    req.setNonce1(nonce[1]);
    req.setNonce2(nonce[2]);
    req.setNonce3(nonce[3]);

    req.setMac0(mac[0]);
    req.setMac1(mac[1]);
    req.setMac2(mac[2]);
    req.setMac3(mac[3]);

    free((void*)nonce);
    free((void*)mac);

    return req.send().ignoreResult();
}



kj::Promise<void> remote_qp_notify_send_with_imm_rpc(
    QPNotificationClient * qnc,
    uint64_t length, uint32_t imm,
    uint64_t const * nonce, uint64_t const * mac)
{
    DEBUG("remote_qp_notify_write_rpc\n");

    auto req = qnc->notifySendRequest();

    req.setOp(CVMRDMA::IBVOp::SEND_WITH_IMM);

    req.setLength(length);

    req.setImm(imm);
    
    req.setNonce0(nonce[0]);
    req.setNonce1(nonce[1]);
    req.setNonce2(nonce[2]);
    req.setNonce3(nonce[3]);

    req.setMac0(mac[0]);
    req.setMac1(mac[1]);
    req.setMac2(mac[2]);
    req.setMac3(mac[3]);

    free((void*)nonce);
    free((void*)mac);

    return req.send().ignoreResult();
}


kj::Promise<void> remote_qp_notify_write_rpc(
    QPNotificationClient * qnc,
    uint64_t hca_addr, uint64_t length, uint32_t rkey,
    uint64_t const * nonce, uint64_t const * mac)
{
    DEBUG("remote_qp_notify_write_rpc\n");

    auto req = qnc->notifySendRequest();

    req.setOp(CVMRDMA::IBVOp::WRITE);

    req.setLength(length);

    req.setHcaAddr(hca_addr);
    req.setRkey(rkey);

    req.setNonce0(nonce[0]);
    req.setNonce1(nonce[1]);
    req.setNonce2(nonce[2]);
    req.setNonce3(nonce[3]);

    req.setMac0(mac[0]);
    req.setMac1(mac[1]);
    req.setMac2(mac[2]);
    req.setMac3(mac[3]);

    free((void*)nonce);
    free((void*)mac);

    return req.send().ignoreResult();
}

kj::Promise<void> remote_qp_notify_write_with_imm_rpc(
    QPNotificationClient * qnc,
    uint64_t hca_addr, uint64_t length, uint32_t rkey, uint32_t imm,
    uint64_t const * nonce, uint64_t const * mac)
{
    DEBUG("remote_qp_notify_write_rpc\n");

    auto req = qnc->notifySendRequest();

    req.setOp(CVMRDMA::IBVOp::WRITE_WITH_IMM);

    req.setLength(length);

    req.setImm(imm);

    req.setHcaAddr(hca_addr);
    req.setRkey(rkey);
    
    req.setNonce0(nonce[0]);
    req.setNonce1(nonce[1]);
    req.setNonce2(nonce[2]);
    req.setNonce3(nonce[3]);

    req.setMac0(mac[0]);
    req.setMac1(mac[1]);
    req.setMac2(mac[2]);
    req.setMac3(mac[3]);

    free((void*)nonce);
    free((void*)mac);

    return req.send().ignoreResult();
}


QPNotificationClient get_qp_notification_client_rpc(CVMRDMAClient * client, uint32_t remote_qpn, uint64_t const * tx_key)
{
    auto req = client->getQpChannelForQpnRequest();

    req.setQpn(remote_qpn);

    req.setTxKey0(tx_key[0]);
    req.setTxKey1(tx_key[1]);
    req.setTxKey2(tx_key[2]);
    req.setTxKey3(tx_key[3]);

    /* Lazy RPC: QNC will actually be fetched on first send */
    return req.send().getQnc();
}
