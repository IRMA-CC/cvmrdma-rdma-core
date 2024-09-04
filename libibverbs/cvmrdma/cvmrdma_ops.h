#pragma once

#include <infiniband/driver.h>

extern struct verbs_context_ops cvmrdma_ops;
extern struct verbs_device_ops cvmrdma_device_ops;

void *cvmrdma_do_bounce_loop(void *args);