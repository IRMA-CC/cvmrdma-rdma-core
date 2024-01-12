#pragma once

#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

int open_system_heap(void);


/* return a dma-buf fd, ready to be mmaped and passed to reg_dmabuf_mr */
int alloc_dma_buf(int dma_heap_fd, size_t size);


#ifdef __cplusplus
}
#endif