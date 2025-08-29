#include "wr_queue.h"
#include "debug.h"


#include <cassert>

WRQueue * alloc_wr_queue(void)
{
    return new WRQueue{};
}

void free_wr_queue(WRQueue * wrq)
{
    delete wrq;
}

struct ibv_send_wr* wrq_front(WRQueue * wrq)
{
    return &wrq->front();
}

void wrq_pop(WRQueue * wrq)
{
    struct ibv_send_wr * wr = &wrq->front();
    if (wr->sg_list != nullptr) {
        delete [] wr->sg_list;
    }
    wrq->pop();
}

void wrq_push(WRQueue * wrq, struct ibv_send_wr * wr)
{
    struct ibv_send_wr entry = {};
    memcpy(&entry, wr, sizeof(struct ibv_send_wr));

    if (wr->num_sge > 0) {
        struct ibv_sge * sgl_entry = new ibv_sge[wr->num_sge];
        memcpy(sgl_entry, wr->sg_list, sizeof(struct ibv_sge) * wr->num_sge);
        entry.sg_list = sgl_entry;
    } else {
        entry.sg_list = nullptr;
    }

    wrq->push(entry);
}