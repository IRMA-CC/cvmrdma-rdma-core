#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include "cvmrdma_objects.h"
#include "cvmrdma.h"
#include "../ibverbs.h"

struct control_metadata {
    uint64_t ack_received_lines;
};

void send_ack(struct cvmrdma_context *ctxt, struct cvmrdma_qp *qp);
void poll_for_ack_once(struct cvmrdma_context *ctxt, struct cvmrdma_qp *qp, bool has_lock);