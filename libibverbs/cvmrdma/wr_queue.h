#pragma once

#include <infiniband/verbs.h>


#ifdef __cplusplus

#include "ThreadsafeQueue.hpp"
using WRQueue = ThreadsafeQueue<ibv_send_wr>;

extern "C" {
#else
typedef struct WRQueue WRQueue;
#endif


WRQueue * alloc_wr_queue(void);
void free_wr_queue(WRQueue * wrq);


void wrq_pop(WRQueue * wrq);
void wrq_push(WRQueue * wrq, struct ibv_send_wr * wr);
struct ibv_send_wr* wrq_front(WRQueue * wrq);

#ifdef __cplusplus
}
#endif