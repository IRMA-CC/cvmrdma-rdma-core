#include "dma_heap.h"


#include <errno.h>
#include <stdio.h>

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <sys/ioctl.h>
#include <linux/dma-heap.h>


int open_system_heap(void)
{
    int ret;
    int fd = open("/dev/dma_heap/system", O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        ret = (int)errno;
        fprintf(stderr, "Failed to open system dma-buf heap %d\n", ret);
        return -1; 
    }
    return fd;
}

int close_system_heap(int system_heap_fd)
{
    return close(system_heap_fd);
}


int alloc_dma_buf(int system_heap_fd, size_t size)
{
    int ret = 0;

    struct dma_heap_allocation_data heap_data = {
        .len = size,
        .fd_flags = O_RDWR | O_CLOEXEC,
    };

    ret = ioctl(system_heap_fd, DMA_HEAP_IOCTL_ALLOC, &heap_data);
    if(ret)
    {
        ret = (int)errno;
        fprintf(stderr, "Failed alloc dma-buf with ioctl %d\n", ret);
        return -1;
    }

    return heap_data.fd;
}