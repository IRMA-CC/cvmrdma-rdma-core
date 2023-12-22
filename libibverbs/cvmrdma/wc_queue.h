#pragma once

#include <infiniband/verbs.h>


#ifdef __cplusplus

#include "ThreadsafeQueue.hpp"
using WCQueue = ThreadsafeQueue<ibv_wc>;

extern "C" {
#else
typedef struct WCQueue WCQueue;
#endif


WCQueue * alloc_wc_queue(void);
void free_wc_queue(WCQueue * wcq);


int wcq_pop_into(WCQueue * wcq, struct ibv_wc * dest, int num_entries);
void wcq_pop(WCQueue * wcq);
void wcq_push(WCQueue * wcq, struct ibv_wc * wc);

#ifdef __cplusplus
}
#endif