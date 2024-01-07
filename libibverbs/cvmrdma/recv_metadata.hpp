#pragma once

#include "recv_metadata.h"

#include <vector>

#include "ThreadsafeQueue.hpp"

/*
    After each recv, the recver must find where the recv happened
    in the bounce buffer. This information is not made available in
    the notification, it is therefore necessary to save it in a queue
    that can be read from the 
*/

using sge_vec = std::vector<struct ibv_sge>;
struct recv_metadata_entry
{
public:
    recv_metadata_entry(uint64_t wrid, sge_vec sgl)
    : wrid{wrid},
      sgl{std::move(sgl)}
    { }

    uint64_t wrid;
    sge_vec sgl;
};

struct QpRecvMetadata
{
    ThreadsafeQueue<recv_metadata_entry> queue;
};