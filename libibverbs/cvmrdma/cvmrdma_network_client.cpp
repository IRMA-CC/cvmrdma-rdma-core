#include "cvmrdma_network_client.h"

#include "debug.h"

ClientSession * cvmrdma_start_session(char const * server_addr)
{
    return new ClientSession{server_addr};
}

void cvmrdma_stop_session(ClientSession * session)
{
    delete session;
}


CVMRDMAClient * cvmrdma_client_init(ClientSession * session)
{
    return new CVMRDMAClient{session->getMain<CVMRDMA>()};
}

void cvmrdma_client_destroy(CVMRDMAClient * client)
{
    delete client;
}

QPNotificationClient * get_qp_notification_client(ClientSession * session, CVMRDMAClient * client, uint32_t remote_qpn, uint64_t const * tx_key)
{
    auto req = client->getQpChannelForQpnRequest();

    req.setQpn(remote_qpn);

    req.setTxKey0(tx_key[0]);
    req.setTxKey1(tx_key[1]);
    req.setTxKey2(tx_key[2]);
    req.setTxKey3(tx_key[3]);

    auto rep = req.send().wait(session->getWaitScope());

    auto qnc = rep.getQnc();

    return new QPNotificationClient{qnc};
}



void remote_qp_notify_write_rpc_add_to_taskset(taskset_t * taskset, QPNotificationClient * qnc, uint64_t hca_addr, uint64_t length, uint32_t rkey, uint32_t imm, bool has_imm, uint64_t const * nonce, uint64_t const * mac)
{
    DEBUG("remote_qp_notify_write_rpc\n");

    auto req = qnc->notifyWriteRequest();
    req.setHcaAddr(hca_addr);
    req.setLength(length);
    req.setRkey(rkey);
    req.setImm(imm);
    req.setHasImm(has_imm);
    
    req.setNonce0(nonce[0]);
    req.setNonce1(nonce[1]);
    req.setNonce2(nonce[2]);
    req.setNonce3(nonce[3]);

    req.setMac0(mac[0]);
    req.setMac1(mac[1]);
    req.setMac2(mac[2]);
    req.setMac3(mac[3]);
    
    auto rep = req.send();

    taskset->add(rep.ignoreResult());
}