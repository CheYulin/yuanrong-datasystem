set(COMMON_BUFFER_SRCS
        buffer.cpp
        lock.cpp
        shm_guard.cpp
        urma_fallback_tcp_limiter.cpp
        device_buffer.cpp
        buffer_composer.cpp)

set(COMMON_BUFFER_DEPEND_LIBS
        ${SECUREC_LIBRARY}
        ${CLIENT_LIB}
        common_log
        common_util
        )

set(COMMON_REF_SRCS object_ref_info.cpp)

set(COMMON_REF_DEPEND_LIBS
        ${TBB_LIBRARY}
        common_log
        common_util
        common_metrics
        common_shared_memory
        )

add_library(common_buffer STATIC ${COMMON_BUFFER_SRCS})
target_link_libraries(common_buffer PRIVATE ${COMMON_BUFFER_DEPEND_LIBS})
add_dependencies(common_buffer posix_protos)

add_library(common_ref STATIC ${COMMON_REF_SRCS})
target_link_libraries(common_ref PRIVATE ${COMMON_REF_DEPEND_LIBS})

set(COMMON_OC_READ_ACCESS_SRCS
        read_access/object_read_access_flow.cpp
        read_access/query_meta_merge_helper.cpp
        read_access/query_meta_redirect_helper.cpp)

set(COMMON_OC_READ_ACCESS_DEPEND_LIBS
        common_util
        common_rpc_zmq
        master_object_protos
        worker_object_protos
        posix_protos)

add_library(common_oc_read_access STATIC ${COMMON_OC_READ_ACCESS_SRCS})
target_link_libraries(common_oc_read_access PRIVATE ${COMMON_OC_READ_ACCESS_DEPEND_LIBS})
add_dependencies(common_oc_read_access master_object_protos worker_object_protos posix_protos)

set(COMMON_READ_ONLY_HASH_RING_VIEW_SRCS read_only_hash_ring_view.cpp)

set(COMMON_READ_ONLY_HASH_RING_VIEW_DEPEND_LIBS
        common_util
        worker_hash_ring
        hash_ring_protos)

add_library(common_read_only_hash_ring_view STATIC ${COMMON_READ_ONLY_HASH_RING_VIEW_SRCS})
target_link_libraries(common_read_only_hash_ring_view PRIVATE ${COMMON_READ_ONLY_HASH_RING_VIEW_DEPEND_LIBS})
add_dependencies(common_read_only_hash_ring_view hash_ring_protos worker_hash_ring)
