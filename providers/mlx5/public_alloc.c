#include "public_alloc.h"

#include <infiniband/cmd_ioctl.h>
#include <rdma/ib_user_ioctl_cmds.h>
#include <rdma/mlx5_user_ioctl_cmds.h>

int mlx5_cmd_alloc_public_mem(struct ibv_context *context, uint64_t size, uint32_t bounce_fd_flags, int * bounce_fd, struct ibv_command_buffer *link)
{
	DECLARE_COMMAND_BUFFER_LINK(cmd, UVERBS_OBJECT_DEVICE,
		MLX5_IB_METHOD_ALLOC_PUBLIC_MEM,
		3,
		link);

	fill_attr_in_uint64(cmd, MLX5_IB_ATTR_ALLOC_PUBLIC_MEM_SIZE, size);
	fill_attr_in_uint32(cmd, MLX5_IB_ATTR_ALLOC_PUBLIC_MEM_FLAGS, bounce_fd_flags);
	fill_attr_out_ptr(cmd, MLX5_IB_ATTR_ALLOC_PUBLIC_MEM_FD, bounce_fd);

	return execute_ioctl(context, cmd);
}