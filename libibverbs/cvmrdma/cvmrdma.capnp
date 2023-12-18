@0x945aa9c160087c97;


interface CVMRDMA {

    interface QpNotificationChannel {
        # rkey is relative to the PD, so the notification is done on a PD-specific object

        # for RC: WRITEs on a QP happen in-order, so a counter can be used for the nonce
        # the counter has to be per-QP

        # the length in the WC is an uint32_t, what happens if we send more than 4GiB ?
        # maybe we can make the length here an uint32 too
        notifyWrite @0 (imm :UInt32, hcaAddr :UInt64, length :UInt64, rkey :UInt32) -> ();
    }


    getQpChannelForQpn @0 (qpn :UInt32) -> (qnc :QpNotificationChannel);
}