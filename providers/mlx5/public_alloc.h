#pragma once

#include <infiniband/verbs.h>
#include <infiniband/cmd_ioctl.h>

int mlx5_cmd_alloc_public_mem(struct ibv_context *context, uint64_t size, uint32_t bounce_fd_flags, int * bounce_fd, struct ibv_command_buffer *link);
