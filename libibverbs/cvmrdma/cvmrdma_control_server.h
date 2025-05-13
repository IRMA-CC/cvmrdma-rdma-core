/*#pragma once

#include "cvmrdma_objects.h"

#include <infiniband/verbs.h>

struct thread_args {
    struct cvmrdma_context *ctxt;
    struct cvmrdma_qp *qp;
};

struct start_args {
    struct cvmrdma_context *ctxt;
    struct cvmrdma_qp *qp;
    
    struct ibv_ah_attr *ah_attr;
};


void *start_control_server(void *args);
void *control_server_send_acks(void *args);
void *control_server_receive_acks(void *args);
*/
