#include "cvmrdma_network_client.h"



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

QPNotificationClient * get_qp_notification_client(ClientSession * session, CVMRDMAClient * client, uint32_t remote_qpn)
{
    auto req = client->getQpChannelForQpnRequest();

    req.setQpn(remote_qpn);

    auto rep = req.send().wait(session->getWaitScope());

    auto qnc = rep.getQnc();

    return new QPNotificationClient{qnc};
}



void remote_qp_notify_write_rpc(ClientSession * session, QPNotificationClient * qnc, uint32_t imm, uint64_t hca_addr, uint64_t length, uint32_t rkey)
{
    auto req = qnc->notifyWriteRequest();
    req.setImm(imm);
    req.setHcaAddr(hca_addr);
    req.setLength(length);
    req.setRkey(rkey);

    auto rep = req.send();

    /* Q: what happens if we never wait on the promise ?? */
}