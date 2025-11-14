#include "cvmrdma_network_client_priv.h"

#include <mutex>
#include <condition_variable>
#include <thread>

#include "debug.h"
#include "cvmrdma.h"

kj::Promise<void> remote_qp_notify_chunk_recv_rpc(QPNotificationClient * qnc)
{
    auto req = qnc->notifyChunkRecvRequest();

    return req.send().ignoreResult();
}


kj::Promise<void> set_remote_params_rpc(QPNotificationClient * qnc, uint32_t local_ring_rkey)
{
    auto req = qnc->setRemoteParamsRequest();

    req.setLocalRingRkey(local_ring_rkey);

    DEBUG("[NC] Setting remote params for remote: ring rkey 0x%" PRIx32 "\n",
        local_ring_rkey);


    return req.send().ignoreResult();
}

kj::Promise<void> notify_nonce_rpc(QPNotificationClient * qnc, uint64_t * nonce)
{
    auto req = qnc->notifyNonceRequest();

    //fprintf(stderr, "Batch send \n");
    auto nonces = req.initNonces(OPT_cvmrdma_nonce_cache_size * cvmrdma_aead_NPUB_WORDS);
    for (int i = 0; i < OPT_cvmrdma_nonce_cache_size * cvmrdma_aead_NPUB_WORDS; i++){
        nonces.set(i, nonce[i]);

        //fprintf(stderr, "[NC] Sending nonce to remote: 0x%" PRIx64 "\n", nonce[i]);
    }

    return req.send().ignoreResult();
}

QPNotificationClient register_qp_with_remote_qp_rpc(CVMRDMAClient * client, uint32_t remote_qpn)
{
    auto req = client->registerQpWithRemoteQpRequest();

    req.setRemoteQpn(remote_qpn);

    /* WARNING: lazy, when returned we're not sure whether the remote really has the tx key !*/
    return req.send().getQnc();
}

kj::Promise<void> remote_qp_notify_read_rpc(QPNotificationClient * qnc, uint64_t addr, uint32_t size, uint32_t rkey)
{
    auto req = qnc->notifyReadRequest();

    req.setAddr(addr);
    req.setSize(size);
    req.setLkey(rkey);

    DEBUG("[NC] Notify read: addr 0x%" PRIx64 ", size 0x%" PRIx64 "\n", addr, size);

    return req.send().ignoreResult();
}

