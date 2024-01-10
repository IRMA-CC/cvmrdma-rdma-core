@0x945aa9c160087c97;

interface CVMRDMA {

    enum IBVOp {
      write @0;
      writeWithImm @1;
      send @2;
      sendWithImm @3;
    }

    interface QpNotificationChannel {

        # notify the arrival of a chunk with WWI on the ring buffer
        # the length will be present in the WWI_WC ; and decryption will
        # fail if the length is altered
        notifyChunk @0 (
            op :IBVOp,
            rkey :UInt32, hcaAddr :UInt64, # for WRITE_* variants
            nonce0 :UInt64, nonce1 :UInt64, nonce2 :UInt64,
            lastChunk :Bool, imm :UInt32
        ) -> ();

        # called on WC for a WWI, freeing up a slot in the ring buffer
        notifyChunkRecv @1 () -> ();

        # tx key for client, rx on server
        setRemoteParams @2 (
            localRingRkey :UInt32,
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