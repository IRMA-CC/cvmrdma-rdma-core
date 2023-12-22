@0x945aa9c160087c97;

interface CVMRDMA {

    interface QpNotificationChannel {
        # rkey is relative to the PD, so the notification is done on a PD-specific object

        # for RC: WRITEs on a QP happen in-order, so a counter can be used for the nonce
        # the counter has to be per-QP

        # the length in the WC is an uint32_t, what happens if we send more than 4GiB ?
        # maybe we can make the length here an uint32 too
        # nonce: 32B, mac: 32B
        notifyWrite @0 (
            hcaAddr :UInt64, length :UInt64, rkey :UInt32, imm :UInt32, hasImm :Bool,
            nonce0 :UInt64, nonce1 :UInt64, nonce2 :UInt64, nonce3 :UInt64,
            mac0 :UInt64, mac1 :UInt64, mac2 :UInt64, mac3 :UInt64
        ) -> ();

        # TODO: could add method to update txKey.
    }

    # Register a QP with the remote CQ server: get a notfication channel to the server,
    # and set the transmission key for this QP.
    # Send the local QPN, and the key that will be used to send on this QP.
    # Each QP uses a different key to prevent nonce reuse
    # Sending and receiving use different keys: the sender generates the key
    # and sends it to the receiver
    # AEGIS-256 keys are 256 bits long
    getQpChannelForQpn @0 (qpn :UInt32, txKey0 :UInt64, txKey1 :UInt64, txKey2 :UInt64, txKey3 :UInt64) -> (qnc :QpNotificationChannel);
}