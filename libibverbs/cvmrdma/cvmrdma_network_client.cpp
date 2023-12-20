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

QPNotificationClient * get_qp_notification_client(ClientSession * session, CVMRDMAClient * client, uint32_t remote_qpn)
{
    auto req = client->getQpChannelForQpnRequest();

    req.setQpn(remote_qpn);

    auto rep = req.send().wait(session->getWaitScope());

    auto qnc = rep.getQnc();

    return new QPNotificationClient{qnc};
}



void remote_qp_notify_write_rpc(ClientSession * session, QPNotificationClient * qnc, uint64_t hca_addr, uint64_t length, uint32_t rkey, uint32_t imm, bool has_imm)
{
    DEBUG("remote_qp_notify_write_rpc\n");

    auto req = qnc->notifyWriteRequest();
    req.setHcaAddr(hca_addr);
    req.setLength(length);
    req.setRkey(rkey);
    req.setImm(imm);
    req.setHasImm(has_imm);
    
    auto rep = req.send();

    /* TODO: run a client event loop in another thread and
       use kj::Executor https://github.com/capnproto/capnproto/blob/0050fd757683f140b4a07fdc3eab986db0d00c8d/c++/src/kj/async.h#L753 
    */
    DEBUG("WARN: waiting on the IP promise, designed to happen in parallel!\n");
    rep.wait(session->getWaitScope());
}