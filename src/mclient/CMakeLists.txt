# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

# nebula_add_subdirectory(tests)

add_executable(MetaClientDemo
    MetaClientDemo.cpp
)

target_link_libraries(MetaClientDemo
        ${NEBULA_MCLIENT_OBJS}
        $<TARGET_OBJECTS:graph_thrift_obj>
        $<TARGET_OBJECTS:nebula_common_obj>
        $<TARGET_OBJECTS:nebula_graph_client_obj>
        $<TARGET_OBJECTS:nebula_meta_client_obj>
        ${NEBULA_THIRD_PARTY_LIBRARIES}
        ${ROCKSDB_LIBRARIES}
)