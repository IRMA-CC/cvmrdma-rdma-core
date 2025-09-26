#include "cvmrdma_cache.h"


#include <pthread.h>
#include <linux/userfaultfd.h>
#include <poll.h>
#include <err.h>
#include <stdio.h>
#include <stdatomic.h>

#include "cvmrdma.h"
#include "debug.h"

int invalidate_chunk(int uffd, struct cvmrdma_mr* mr)
{
    struct uffd_msg  msg;
    struct ibv_mr* user_mr = &mr->user_mr.ibv_mr;

    int nread = read(uffd, &msg, sizeof(msg));
    if (nread == 0 || nread == -1) {
        return nread;
    }

    /* We expect only one kind of event; verify that assumption. */

    if (msg.event != UFFD_EVENT_PAGEFAULT) {
        fprintf(stderr, "[CACHE] Unexpected event on userfaultfd\n");
        exit(1);
    }

    DEBUG("[CACHE]    UFFD_EVENT_PAGEFAULT event: ");
    DEBUG("[CACHE] flags = %llx; ", msg.arg.pagefault.flags);
    DEBUG("[CACHE] address = %llx\n", msg.arg.pagefault.address);

    assert(msg.arg.pagefault.flags & UFFD_PAGEFAULT_FLAG_WP);
    assert(msg.arg.pagefault.flags & UFFD_PAGEFAULT_FLAG_WRITE);


    // Mark chunk where this address falls into as dirty
    uint64_t offt = (msg.arg.pagefault.address - mr->hca_va);
    uint64_t chunk_idx = offt / cvmrdma_chunk_bytes;

    sem_wait(&mr->chunk_tracking[chunk_idx].in_transit_sem);
    DEBUG("[CACHE] chunk_idx = %llu\n", chunk_idx);
    atomic_store(&mr->chunk_tracking[chunk_idx].cached, false);
    //fprintf(stderr, "[CACHE] Marked chunk %llu as dirty\n", chunk_idx);


    // make all pages of the chunk as writable
    uint64_t alinged_start = msg.arg.pagefault.address - (offt % cvmrdma_chunk_bytes);
    size_t wp_len = (alinged_start - (uint64_t)user_mr->addr) + cvmrdma_chunk_bytes > user_mr->length ? user_mr->length - (alinged_start - (uint64_t)user_mr->addr) : cvmrdma_chunk_bytes;


    struct uffdio_writeprotect uffdio_wp = {
        .range = {
            .start = alinged_start,
            .len = wp_len
        },
        .mode = 0,
    };
    //fprintf(stderr, "[CACHE] Making chunk %llu writable again. addr=%llx, len=%llx\n", chunk_idx, uffdio_wp.range.start, uffdio_wp.range.len);
    DEBUG("[CACHE] start = %llx; len = %llx\n", uffdio_wp.range.start, uffdio_wp.range.len);
    DEBUG("[CACHE] alinged_start = %llx; user_mr->length = %x user_mr->addr = %llx\n", alinged_start, user_mr->length, (uint64_t)user_mr->addr);
    
    if (ioctl(uffd, UFFDIO_WRITEPROTECT, &uffdio_wp) == -1) {
        fprintf(stderr, "[CACHE] ioctl-UFFDIO_WRITEPROTECT: %d\n", errno);
        exit(1);
    }
    
    sem_post(&mr->chunk_tracking[chunk_idx].in_transit_sem);


    return nread;
}



void* fault_handler_thread(void* arg) {
    struct fault_handler_thread_args* targs;                  /* userfaultfd file descriptor */
    targs = (struct fault_handler_thread_args*) arg;
    int uffd = targs->uffd;

    /* Loop, handling incoming events on the userfaultfd
       file descriptor. */

    for (;;) {
        /* See what poll() tells us about the userfaultfd. */

        struct pollfd pollfd;
        int nready;
        pollfd.fd = uffd;
        pollfd.events = POLLIN;
        nready = poll(&pollfd, 1, -1);
        if (nready == -1) {
            if (errno == EINTR) {
                continue;
            }
            fprintf(stderr, "[CACHE] poll: %d\n", nready);
            exit(1);
        }

        DEBUG("\n[CACHE] fault_handler_thread():\n");
        DEBUG(
            "[CACHE]    poll() returns: nready = %d; "
            "POLLIN = %d; POLLERR = %d\n",
            nready, (pollfd.revents & POLLIN) != 0,
            (pollfd.revents & POLLERR) != 0);

        int nread = invalidate_chunk(uffd, targs->mr);

        if (nread == 0) {
            fprintf(stderr, "[CACHE] EOF on userfaultfd!\n");
            exit(1);
        }

        if (nread == -1) {
            fprintf(stderr, "[CACHE] read: %d\n", nread);
            exit(1);
        }


    }
}