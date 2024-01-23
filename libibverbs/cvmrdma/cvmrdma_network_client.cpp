#include "cvmrdma_network_client_priv.h"

#include <mutex>
#include <condition_variable>
#include <thread>

#include "debug.h"


static CVMRDMA::IBVOp ibv_to_capnp_opcode(enum ibv_wr_opcode op)
{
    switch (op)
    {
    case IBV_WR_SEND: {
        return CVMRDMA::IBVOp::SEND;
    }
    case IBV_WR_SEND_WITH_IMM: {
        return CVMRDMA::IBVOp::SEND_WITH_IMM;
    }

    case IBV_WR_RDMA_WRITE: {
        return CVMRDMA::IBVOp::WRITE;
    }
    case IBV_WR_RDMA_WRITE_WITH_IMM: {
        return CVMRDMA::IBVOp::WRITE_WITH_IMM;
    }
    }
}



kj::Promise<void> remote_qp_notify_chunk_recv_rpc(QPNotificationClient * qnc)
{
    auto req = qnc->notifyChunkRecvRequest();

    return req.send().ignoreResult();
}

kj::Promise<void> set_remote_params_rpc(QPNotificationClient * qnc, uint64_t const * tx_key, uint32_t local_ring_rkey)
{
    auto req = qnc->setRemoteParamsRequest();

    req.setTxKey0(tx_key[0]);
    req.setTxKey1(tx_key[1]);
    req.setTxKey2(tx_key[2]);
    req.setTxKey3(tx_key[3]);

    req.setLocalRingRkey(local_ring_rkey);

    return req.send().ignoreResult();
}

QPNotificationClient register_qp_with_remote_qp_rpc(CVMRDMAClient * client, uint32_t remote_qpn)
{
    auto req = client->registerQpWithRemoteQpRequest();

    req.setRemoteQpn(remote_qpn);

    /* WARNING: lazy, when returned we're not sure whether the remote really has the tx key !*/
    return req.send().getQnc();
}

