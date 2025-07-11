/* Copyright (c) 2025. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "nebula/mclient/MetaClient.h"

#include <folly/executors/IOThreadPoolExecutor.h>

#include <functional>

#include "../thrift/ThriftClientManager.h"
#include "common/thrift/ThriftTypes.h"
#include "interface/gen-cpp2/MetaServiceAsyncClient.h"
#include "interface/gen-cpp2/meta_types.h"
#include "nebula/mclient/MetaClient.h"
#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/env.h"

#include "nebula/mclient/MetaClient.h"

#include <errno.h>
#include <folly/ssl/Init.h>
#include <folly/init/Init.h>
#include <signal.h>
#include <string.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include <glog/logging.h>

#include <functional>
#include <iostream>
#include <unordered_map>

DEFINE_string(meta_server_addrs,  "127.0.0.1:9559", "default meta server addr");
DEFINE_int32(spaceid, 1, "default meta server http port");

int32_t main(int argc, char* argv[]) {

    folly::init(&argc, &argv, true);

    std::vector<std::string> meta_server_addrs;
    folly::split(",", FLAGS_meta_server_addrs, meta_server_addrs, true);

    nebula::MetaClient client(meta_server_addrs);

    auto meta = client.getSpaceNameByIdFromCache(FLAGS_spaceid);

    std::cout << meta.first << ":" << meta.second << std::endl;

    // auto tags = client.listTagSchemas(meta.second);

    // for (auto item : tags.second) {
    //     std::cout << item.get_tag_name() << ":" << item.get_tag_id() << std::endl;
    // }

    /*
    // 初始化SstFileWriter
    rocksdb::Options options;  // 可以进行配置以优化SST文件
    rocksdb::SstFileWriter sst_file_writer(rocksdb::EnvOptions(), options);

    // 打开文件进行写入
    std::string file_path = "/root/sst_file.sst";
    sst_file_writer.Open(file_path);

    // 完成SST文件的写入
    sst_file_writer.Finish();
    */

    return 0;
}
