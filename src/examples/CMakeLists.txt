# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
#

# Set the project name

add_executable(session_example
    SessionExample.cpp
)

target_link_libraries(session_example
        ${NEBULA_MCLIENT_OBJS}
        $<TARGET_OBJECTS:graph_thrift_obj>
        $<TARGET_OBJECTS:nebula_common_obj>
        $<TARGET_OBJECTS:nebula_graph_client_obj>
        $<TARGET_OBJECTS:nebula_meta_client_obj>
        ${NEBULA_THIRD_PARTY_LIBRARIES}
        ${ROCKSDB_LIBRARIES}
)
