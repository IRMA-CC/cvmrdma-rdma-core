#include "cvmrdma_network_client_priv.h"

#include <mutex>
#include <condition_variable>
#include <thread>

#include "debug.h"

kj::Promise<void> set_remote_params_rpc(QPNotificationClient * qnc, uint64_t const * tx_key, uint32_t local_ring_rkey, uint32_t local_control_rkey, uint32_t control_qpn)
{
    auto req = qnc->setRemoteParamsRequest();

    req.setTxKey0(tx_key[0]);
    req.setTxKey1(tx_key[1]);
    req.setTxKey2(tx_key[2]);
    req.setTxKey3(tx_key[3]);

    req.setLocalRingRkey(local_ring_rkey);
    req.setLocalControlRkey(local_control_rkey);

    req.setControlQPN(control_qpn);

    DEBUG("[NC] Setting remote params for remote: ring rkey 0x%" PRIx32 ", control rkey 0x%" PRIx32 ", control qpn 0x%" PRIx32 "\n", local_ring_rkey, local_control_rkey, control_qpn);

    return req.send().ignoreResult();
}

QPNotificationClient register_qp_with_remote_qp_rpc(CVMRDMAClient * client, uint32_t remote_qpn)
{
    auto req = client->registerQpWithRemoteQpRequest();

    req.setRemoteQpn(remote_qpn);

    /* WARNING: lazy, when returned we're not sure whether the remote really has the tx key !*/
    return req.send().getQnc();
}

