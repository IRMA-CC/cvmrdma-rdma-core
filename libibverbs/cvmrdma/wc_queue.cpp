#include "wc_queue.h"
#include "debug.h"




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
    int ret = wcq->pop_n_into(dest, num_entries);
    if(ret > 0) {
        DEBUG("[WC_QUEUE] popped %d entries, wcq[0].wr_id = %lu, wcq[0].status = %d, wcq[0].opcode = %d, \
        wcq[0].byte_len = %d, wcq[0].imm_data = %d, wcq[0].qp_num = %d, wcq[0].src_qp = %d, wcq[0].wc_flags = %d\n", 
        ret, dest[0].wr_id, dest[0].status, dest[0].opcode, dest[0].byte_len, dest[0].imm_data, dest[0].qp_num, 
        dest[0].src_qp, dest[0].wc_flags);
    }
    return ret;
}

void wcq_pop(WCQueue * wcq)
{
    wcq->pop();
}

void wcq_push(WCQueue * wcq, struct ibv_wc * wc)
{
    wcq->push(*wc);
}