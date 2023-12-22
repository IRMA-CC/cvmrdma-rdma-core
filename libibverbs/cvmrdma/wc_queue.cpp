#include "wc_queue.h"





WCQueue * alloc_wc_queue(void)
{
    return new WCQueue{};
}

void free_wc_queue(WCQueue * wcq)
{
    delete wcq;
}

int wcq_pop_into(WCQueue * wcq, ibv_wc * dest, int num_entries)
{
    return wcq->pop_n_into(dest, num_entries);
}

void wcq_pop(WCQueue * wcq)
{
    wcq->pop();
}

void wcq_push(WCQueue * wcq, struct ibv_wc * wc)
{
    wcq->push(*wc);
}