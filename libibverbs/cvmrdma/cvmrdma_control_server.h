#pragma once

#include "cvmrdma_objects.h"

struct thread_args {
    struct cvmrdma_context *ctxt;
    struct cvmrdma_qp *qp;
};


void *control_server_send_acks(void *args);
void *control_server_receive_acks(void *args);

