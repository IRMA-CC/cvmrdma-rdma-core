#pragma once

#include "cvmrdma_objects.h"

struct fault_handler_thread_args {
    int uffd;
    struct cvmrdma_mr *mr;
};

int invalidate_chunk(int uffd, struct cvmrdma_mr* user_mr);

void* fault_handler_thread(void* arg);