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
    delete [] wr->sg_list;
    wrq->pop();
}

void wrq_push(WRQueue * wrq, struct ibv_send_wr * wr)
{
    struct ibv_send_wr * wr_copy = new ibv_send_wr{};

    // The sgl is a _list_ not an array that simply can be copied. It contains a next pointer after all.
    assert(wr->num_sge == 1);

    struct ibv_sge * sgl_entry = new ibv_sge[1];

    memcpy(sgl_entry, wr->sg_list, sizeof(struct ibv_sge));

    struct ibv_send_wr entry = {};
    memcpy(&entry, wr, sizeof(struct ibv_send_wr));
    entry.sg_list = sgl_entry;

    wrq->push(entry);
}