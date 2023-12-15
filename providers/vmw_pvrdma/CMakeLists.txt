
capnp_generate_cpp(CAPNP_SRCS CAPNP_HDRS cvmrdma/cvmrdma.capnp)
add_custom_target(generate_capnp DEPENDS ${CAPNP_SRCS} ${CAPNP_HDRS})
get_filename_component(CAPNP_HDRS_DIR ${CAPNP_HDRS} DIRECTORY)



rdma_provider(vmw_pvrdma
  cq.c
  pvrdma_main.c
  qp.c
  verbs.c

  cvmrdma/bounce_metadata.cpp
  cvmrdma/cvmrdma_cq_server.c
  cvmrdma/cvmrdma_network_client.cpp
  cvmrdma/cvmrdma_network_server.cpp
  cvmrdma/notification_queue.cpp
  
  ${CAPNP_SRCS}
)

add_dependencies(vmw_pvrdma-rdmav${IBVERBS_PABI_VERSION} generate_capnp)

target_include_directories(vmw_pvrdma-rdmav${IBVERBS_PABI_VERSION}
PRIVATE
    ${CAPNP_HDRS_DIR}
)

# Use sources generated from capnp schema
target_link_libraries(vmw_pvrdma-rdmav${IBVERBS_PABI_VERSION}
LINK_PRIVATE
  CapnProto::capnp-rpc
)

target_compile_features(vmw_pvrdma-rdmav${IBVERBS_PABI_VERSION}
PUBLIC
  cxx_std_20
)