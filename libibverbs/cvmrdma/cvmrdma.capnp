@0x945aa9c160087c97;

interface CVMRDMA {

    interface QpNotificationChannel {

        notifyNonce @2 (
            nonce: UInt64
        ) -> ();

        # called on WC for a WWI, freeing up a slot in the ring buffer
        notifyChunkRecv @0 () -> ();


        # tx key for client, rx on server
        setRemoteParams @1 (
            localRingRkey :UInt32,
            localControlRkey :UInt32,
            controlQPN :UInt32,
            controlLocalPSN :UInt32,
            txKey0 :UInt64, txKey1 :UInt64, txKey2 :UInt64, txKey3 :UInt64
        ) -> ();
    }

    # Register a QP with the remote CQ server: get a notfication channel to the server,
    # and set the transmission key for this QP.
    # Each QP uses a different key to prevent nonce reuse
    # Sending and receiving use different keys: the sender generates the key
    # and sends it to the receiver
    # Also pass the rkey to the ring buffer associated with the local qp

    registerQpWithRemoteQp @0 (
        remoteQpn :UInt32,
    ) -> (qnc :QpNotificationChannel);

}