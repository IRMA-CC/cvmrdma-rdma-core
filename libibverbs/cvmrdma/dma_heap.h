#pragma once

#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

int open_system_heap(void);
int close_system_heap(int system_heap_fd);


/* return a dma-buf fd, ready to be mmaped and passed to reg_dmabuf_mr */
int alloc_dma_buf(int dma_heap_fd, size_t size);


#ifdef __cplusplus
}
#endif