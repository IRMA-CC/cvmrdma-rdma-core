publish_headers(infiniband
  arch.h
  opcode.h
  sa-kern-abi.h
  sa.h
  verbs.h
  verbs_api.h
  tm_types.h
  )

publish_internal_headers(infiniband
  cmd_ioctl.h
  cmd_write.h
  driver.h
  kern-abi.h
  marshall.h
  )

publish_headers(infiniband/cvmrdma
  cvmrdma/cvmrdma_network_client.h
  cvmrdma/cvmrdma_network_evloop.h
  cvmrdma/cvmrdma_network_server.h
)

publish_internal_headers(infiniband/cvmrdma
  cvmrdma/crypto.h
  cvmrdma/cvmrdma_cq_server.h
  cvmrdma/cvmrdma_network_client_priv.h
  cvmrdma/cvmrdma_network_evloop_priv.h
  cvmrdma/cvmrdma_network_server_priv.h
  cvmrdma/bounce_metadata.h
  cvmrdma/notification_queue.h
  cvmrdma/notification_queue.hpp
  cvmrdma/cvmrdma_ops.h
  cvmrdma/qp_metadata_manager.h
  cvmrdma/recv_metadata.h
  cvmrdma/send_metadata.h
  cvmrdma/taskset_wrapper.h
  cvmrdma/wc_queue.h
  cvmrdma/dma_heap.h
)

configure_file("libibverbs.map.in"
  "${CMAKE_CURRENT_BINARY_DIR}/libibverbs.map" @ONLY)


capnp_generate_cpp(CAPNP_SRCS CAPNP_HDRS cvmrdma/cvmrdma.capnp)
add_custom_target(generate_capnp DEPENDS ${CAPNP_SRCS} ${CAPNP_HDRS})
get_filename_component(CAPNP_HDRS_DIR ${CAPNP_HDRS} DIRECTORY)


rdma_library(ibverbs "${CMAKE_CURRENT_BINARY_DIR}/libibverbs.map"
  # See Documentation/versioning.md
  1 1.14.${PACKAGE_VERSION}
  all_providers.c
  cmd.c
  cmd_ah.c
  cmd_counters.c
  cmd_cq.c
  cmd_device.c
  cmd_dm.c
  cmd_fallback.c
  cmd_flow.c
  cmd_flow_action.c
  cmd_ioctl.c
  cmd_mr.c
  cmd_mw.c
  cmd_pd.c
  cmd_qp.c
  cmd_rwq_ind.c
  cmd_srq.c
  cmd_wq.c
  cmd_xrcd.c
  compat-1_0.c
  device.c
  dummy_ops.c
  dynamic_driver.c
  enum_strs.c
  ibdev_nl.c
  init.c
  marshall.c
  memory.c
  neigh.c
  static_driver.c
  sysfs.c
  verbs.c


  cvmrdma/bounce_metadata.cpp
  cvmrdma/cvmrdma_cq_server.c
  cvmrdma/cvmrdma_network_client.cpp
  cvmrdma/cvmrdma_network_evloop.cpp
  cvmrdma/cvmrdma_network_server.cpp
  cvmrdma/cvmrdma_ops.c
  cvmrdma/notification_queue.cpp
  cvmrdma/qp_metadata_manager.cpp
  cvmrdma/recv_metadata.cpp
  cvmrdma/send_metadata.cpp
  cvmrdma/taskset_wrapper.cpp
  cvmrdma/wc_queue.cpp
  cvmrdma/dma_heap.c

  
  ${CAPNP_SRCS}
  )

add_dependencies(ibverbs generate_capnp)


target_include_directories(ibverbs
PRIVATE
  ${CAPNP_HDRS_DIR}
)


target_link_libraries(ibverbs PRIVATE
  ${NL_LIBRARIES}
  ${CMAKE_THREAD_LIBS_INIT}
  ${CMAKE_DL_LIBS}
  kern-abi

  CapnProto::capnp-rpc
  Sodium::sodium
  )


target_compile_features(ibverbs
PUBLIC
  cxx_std_20
)


function(ibverbs_finalize)
  if (ENABLE_STATIC)
    # In static mode the .pc file lists all of the providers for static
    # linking. The user should set RDMA_STATIC_PROVIDERS to select which ones
    # to include.
    list(LENGTH RDMA_PROVIDER_LIST LEN)
    math(EXPR LEN ${LEN}-1)
    foreach(I RANGE 0 ${LEN} 2)
      list(GET RDMA_PROVIDER_LIST ${I} PROVIDER_NAME)
      math(EXPR I ${I}+1)
      list(GET RDMA_PROVIDER_LIST ${I} LIB_NAME)
      math(EXPR I ${I}+1)

      set(PROVIDER_LIBS "${PROVIDER_LIBS} -l${LIB_NAME}")
      set(FOR_EACH_PROVIDER "${FOR_EACH_PROVIDER} FOR_PROVIDER(${PROVIDER_NAME})")
    endforeach()

    if (NOT NL_KIND EQUAL 0)
      set(REQUIRES "libnl-3.0, libnl-route-3.0")
    endif()
    rdma_pkg_config("ibverbs" "${REQUIRES}" "${PROVIDER_LIBS} -libverbs ${CMAKE_THREAD_LIBS_INIT}")

    file(WRITE ${BUILD_INCLUDE}/infiniband/all_providers.h "#define FOR_EACH_PROVIDER() ${FOR_EACH_PROVIDER}")
  else()
    rdma_pkg_config("ibverbs" "" "${CMAKE_THREAD_LIBS_INIT}")
  endif()
endfunction()
