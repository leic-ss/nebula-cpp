/* Copyright (c) 2025. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "nebula/mclient/MetaClient.h"

#include <errno.h>
#include <folly/ssl/Init.h>
#include <folly/init/Init.h>
#include <signal.h>
#include <string.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include <glog/logging.h>

#include <folly/executors/IOThreadPoolExecutor.h>

#include <functional>
#include <iostream>
#include <unordered_map>
#include <set>
#include <algorithm>

#include "rocksdb/db.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/env.h"
#include "rocksdb/utilities/options_util.h"
#include "admin_server.h"

#include "Schedule.h"
#include "cpphttplib/httplib.h"

#include <atomic>
#include <chrono>
#include <thread>

#include <nebula/client/Config.h>
#include <nebula/client/ConnectionPool.h>
#include <common/Init.h>

static void printHelp(const char *prog) {
    fprintf(stderr, "%s --datafile <data_file>\n", prog);
}

DEFINE_string(meta_server_addrs,  "127.0.0.1:9559", "default meta server addr");
DEFINE_int32(http_port, 8108, "default meta server http port");
DEFINE_string(db_path, "data/sysdb", "system data path");
DEFINE_string(storage_http_addrs, "127.0.0.1:19779", "system data path");
DEFINE_string(hdfs_job_dir, "hdfs://", "hdfs job dir");

DEFINE_int32(thread_num, 9, "default process thread number");
DEFINE_int32(download_wait_seconds, 60, "download wait seconds");

static const std::string _sys_task_prefix_ = "_SYS_TASKS_";
static const std::string _sys_begin_tag_ = ":";
static const std::string _sys_end_tag_ = ";";
static const std::string _sys_meta_prefix_ = "_SYS_META_";

static const uint32_t JOB_STATE_INITIAL = 0;
static const uint32_t JOB_STATE_RUNNING = 1;
static const uint32_t JOB_STATE_DONE = 2;
static const uint32_t JOB_STATE_END = 3;

static const uint32_t META_EDGE_1 = 0;
static const uint32_t META_EDGE_2 = 1;
static const uint32_t META_TAG_1 = 2;
static const uint32_t META_TAG_2 = 3;


static uint64_t gGetCurrentUs()
{
    struct timeval tm;
    gettimeofday(&tm, nullptr);
    uint64_t cur = tm.tv_sec*((uint64_t)1000000) + tm.tv_usec;
    return cur;
}

static std::string gTimeConvert(time_t ts)
{
    struct tm * timeinfo = localtime ( &ts );

    int32_t Year = timeinfo->tm_year+1900;
    int32_t Mon = timeinfo->tm_mon+1;
    int32_t Day = timeinfo->tm_mday;
    int32_t Hour = timeinfo->tm_hour;
    int32_t Min = timeinfo->tm_min;
    int32_t Second = timeinfo->tm_sec;

    std::string str;
    str.append(std::to_string(Year)).append("-");
    str.append(std::to_string(Mon)).append("-");
    str.append(std::to_string(Day)).append(" ");

    str.append(std::to_string(Hour)).append(":");
    str.append(std::to_string(Min)).append(":");
    str.append(std::to_string(Second));

    return str;
}

static std::vector<std::string> tokenize(const std::string& src, const std::string& delim)
{
    std::vector<std::string> ret;
    size_t last = 0;
    size_t pos = src.find(delim, last);
    while (pos != std::string::npos) {
        ret.push_back( src.substr(last, pos - last) );
        last = pos + delim.size();
        pos = src.find(delim, last);
    }
    if (last < src.size()) {
        ret.push_back( src.substr(last) );
    }
    return std::move(ret);
}

// static std::string timeConvertDay(time_t ts)
// {
//     struct tm * timeinfo = localtime ( &ts );

//     int32_t Year = timeinfo->tm_year+1900;
//     int32_t Mon = timeinfo->tm_mon+1;
//     int32_t Day = timeinfo->tm_mday;

//     char str[16] = {0};
//     sprintf(str, "%04d%02d%02d", Year, Mon, Day);

//     return str;
// }

void Schedule::register_http_callbacks()
{
    adminserver.reg_handler("/api/v1/task/schedule", std::bind(&Schedule::taskschedule,
                            this,
                            std::placeholders::_1));
    adminserver.reg_handler("/api/v1/task/status", std::bind(&Schedule::taskstatus,
                            this,
                            std::placeholders::_1));
    adminserver.reg_handler("/api/v1/task/list", std::bind(&Schedule::tasklist,
                            this,
                            std::placeholders::_1));
    adminserver.reg_handler("/api/v1/task/delete", std::bind(&Schedule::taskdelete,
                            this,
                            std::placeholders::_1));
    adminserver.reg_handler("/api/v1/hdfs/list", std::bind(&Schedule::hdfslist,
                            this,
                            std::placeholders::_1));
    adminserver.reg_handler("/api/v1/relation/create", std::bind(&Schedule::relationcreate,
                            this,
                            std::placeholders::_1));
    adminserver.reg_handler("/api/v1/relation/list", std::bind(&Schedule::relationlist,
                            this,
                            std::placeholders::_1));
    adminserver.reg_handler("/api/v1/subgraph/clear", std::bind(&Schedule::subgraphclear,
                            this,
                            std::placeholders::_1));
    adminserver.reg_handler("/api/v1/subgraph/newmeta", std::bind(&Schedule::subgraphmeta,
                            this,
                            std::placeholders::_1));
    adminserver.reg_handler("/api/v1/subgraph/newdata", std::bind(&Schedule::subgraphdata,
                            this,
                            std::placeholders::_1));
}

bool Schedule::JsonParse(const std::string& json_str, nlohmann::json& json_obj)
{
    bool success = true;
    try {
        json_obj = nlohmann::json::parse(json_str);
    } catch (std::exception& e) {
        LOG(WARNING) << "catch exception:" << e.what();
        success = false;
    }

    return success;
}

Schedule::~Schedule()
{
    for (auto thd : threads) {
        delete thd;
    }

    threads.clear();
}

void Schedule::taskschedule(struct evhttp_request *req)
{
    std::string mime_type("application/json; charset=utf-8");
    evhttp_add_header(evhttp_request_get_output_headers(req), "Content-Type", mime_type.c_str());
    // evhttp_add_header(evhttp_request_get_output_headers(req), mime_type.c_str(), "1");

    if (req->type != evhttp_cmd_type::EVHTTP_REQ_POST) {
        AdminServer::http_error(req, 400, "not a post request!");
        return ;
    }

    std::string content = AdminServer::read_content(req);
    if (content.empty()) {
        AdminServer::http_error(req, 400, "empty content in post request!");
        return ;
    }

    nlohmann::json json_obj;
    if (!JsonParse(content, json_obj)) {
        AdminServer::http_error(req, 400, "invalid json format!");
        return ;
    }

    LOG(WARNING) << content;

    uint32_t taskid = json_obj["execRecord"].value("taskScheRecordId", 0);

    std::string task_status = json_obj.value("status", "SUCCESS");
    if (task_status == "FAILED") {
        LOG(ERROR) << "Failed Task " << taskid;
        AdminServer::http_error(req, 400, "Failed Task");
        return ;
    }

    std::string db_key = _sys_task_prefix_ + std::to_string(JOB_STATE_INITIAL) + std::to_string(taskid);

    nlohmann::json obj;
    obj["initial_time"] = gTimeConvert( gGetCurrentUs()/1000000 );
    obj["taskid"] = taskid;
    obj["state"] = "Initial";

    std::string db_value = obj.dump();

    rocksdb::WriteOptions writeopts;
    rocksdb::Status status = sysdb->Put(writeopts, db_key, db_value);

    if (!status.ok()) {
        LOG(ERROR) << "rocksdb put failed! db_key:" << db_key;
        AdminServer::http_error(req, 400, "schedule failed");
        return ;
    }

    AdminServer::http_ok(req, obj.dump());
}

// {"srctag": "tag1", "dsttag": "tag2", "edges": ["edge1", "edge2"]}
void Schedule::relationcreate(struct evhttp_request *req)
{
    std::string mime_type("application/json; charset=utf-8");
    evhttp_add_header(evhttp_request_get_output_headers(req), "Content-Type", mime_type.c_str());
    // evhttp_add_header(evhttp_request_get_output_headers(req), mime_type.c_str(), "1");

    if (req->type != evhttp_cmd_type::EVHTTP_REQ_POST) {
        AdminServer::http_error(req, 400, "not a post request!");
        return ;
    }

    std::string content = AdminServer::read_content(req);
    if (content.empty()) {
        AdminServer::http_error(req, 400, "empty content in post request!");
        return ;
    }

    nlohmann::json json_obj;
    if (!JsonParse(content, json_obj)) {
        AdminServer::http_error(req, 400, "invalid json format!");
        return ;
    }

    LOG(WARNING) << "relationcreate: " << content;

    std::string space = json_obj.value("space", "");
    std::string srcnode = json_obj.value("srcnode", "");
    std::string dstnode = json_obj.value("dstnode", "");
    int32_t rankval = json_obj.value("rankval", 0);

    std::vector<std::string> nodes;
    nodes.push_back(srcnode);
    nodes.push_back(dstnode);
    std::sort(nodes.begin(), nodes.end());

    std::string val = std::to_string(rankval) + _sys_begin_tag_ + srcnode + _sys_begin_tag_ + dstnode;

    nlohmann::json edges = json_obj["edges"];

    rocksdb::WriteBatch batch;
    for (std::string edge : edges) {
        // std::string edge = obj.value("edge", "");

        std::string key1 = _sys_meta_prefix_ + space + std::to_string(META_EDGE_1) + edge + _sys_begin_tag_ + nodes[0] + _sys_begin_tag_ + nodes[1];
        std::string key2 = _sys_meta_prefix_ + space + std::to_string(META_EDGE_2) + nodes[0] + _sys_begin_tag_ + nodes[1] + _sys_begin_tag_ + edge;

        std::string key3 = _sys_meta_prefix_ + space + std::to_string(META_TAG_1) + nodes[0] + _sys_begin_tag_ + edge + _sys_begin_tag_ + nodes[1];
        std::string key4 = _sys_meta_prefix_ + space + std::to_string(META_TAG_1) + nodes[1] + _sys_begin_tag_ + edge + _sys_begin_tag_ + nodes[0];

        LOG(WARNING) << key1;
        LOG(WARNING) << key2;
        LOG(WARNING) << key3;
        LOG(WARNING) << key4;

        batch.Put(key1, val);
        batch.Put(key2, val);
        batch.Put(key3, val);
        batch.Put(key4, val);
    }

    auto status = sysdb->Write(rocksdb::WriteOptions(), &batch);
    if (!status.ok()) {
        AdminServer::http_error(req, 400, "rocksdb write failed! err: " + status.ToString());
        return ;
    }

    AdminServer::http_error(req, 200, "relation create success!");
}

void Schedule::relationlist(struct evhttp_request *req)
{
    std::string mime_type("application/json; charset=utf-8");
    evhttp_add_header(evhttp_request_get_output_headers(req), "Content-Type", mime_type.c_str());
    // evhttp_add_header(evhttp_request_get_output_headers(req), mime_type.c_str(), "1");

    AdminServer::Http_Map params = AdminServer::parse_params(req);
    if (params.find("space") == params.end()) {
        AdminServer::http_error(req, 400, "missing space");
        return ;
    }

    std::string key1 = _sys_meta_prefix_ + params["space"] + std::to_string(META_EDGE_1);
    std::string key2 = _sys_meta_prefix_ + params["space"] + std::to_string(META_EDGE_2);

    rocksdb::Slice startkey( key1 );
    rocksdb::Slice endkey( key2 );

    LOG(WARNING) << key1;
    LOG(WARNING) << key2;

    rocksdb::ReadOptions scan_read_options;
    scan_read_options.fill_cache = false; // not fill cache
    scan_read_options.iterate_lower_bound = &startkey;
    scan_read_options.iterate_upper_bound = &endkey;

    rocksdb::Iterator* scan_it_ = sysdb->NewIterator(scan_read_options);
    if (!scan_it_) {
        LOG(ERROR) << "NewIterator failed!";
        AdminServer::http_error(req, 400, "NewIterator failed!");
        return ;
    }

    nlohmann::json edge_objs;
    nlohmann::json rank_objs;
    nlohmann::json tag_objs;
    nlohmann::json relation_objs;
    nlohmann::json node_objs;

    scan_it_->SeekToFirst();
    while(scan_it_->Valid()) {
        std::string key( scan_it_->key().data(), scan_it_->key().size() );
        std::string value( scan_it_->value().data(), scan_it_->value().size() );

        std::vector<std::string> vec1 = tokenize(key.substr(startkey.size()), _sys_begin_tag_);
        std::vector<std::string> vec2 = tokenize(value, _sys_begin_tag_);

        // {
        //     std::string tag1 = vec2[1];
        //     std::string tag2 = vec2[2];

        //     std::string edge = vec1[0];
        //     std::string rank = vec2[0];

        //     if (rank_objs[rank].is_null()) {
        //         nlohmann::json rank_obj;
        //         // rank_obj[] = vec2[1];
        //         nlohmann::json obj_arr = nlohmann::json::array();
        //         obj_arr.push_back(tag1);
        //         obj_arr.push_back(tag2);
        //         rank_obj["tags"] = obj_arr;
        //         rank_obj["desc"] = tag1 + " -> " + tag2;

        //         rank_obj["edges"] = nlohmann::json::array();
        //         rank_obj[tag1] = tag2;
        //         rank_obj[tag2] = tag1;
        //         rank_objs[rank] = rank_obj;
        //     }

        //     rank_objs[rank]["edges"].push_back( edge );
        // }

        {
            std::string tag1 = vec2[1];
            std::string tag2 = vec2[2];
            std::string rank = vec2[0];
            std::string edge = vec1[0];

            if (rank_objs[tag1].is_null()) {
                rank_objs[tag1] = nlohmann::json();
            }
            if (rank_objs[tag1][ rank ].is_null()) {
                nlohmann::json tag_obj;
                tag_obj["tag"] = tag2;
                tag_obj["edges"] = nlohmann::json::array();
                // tag_obj["direction"] = "->";

                rank_objs[tag1][ rank ] = tag_obj;
            }

            rank_objs[tag1][ rank ]["edges"].push_back( edge );

            if (tag1 != tag2) {
                if (rank_objs[tag2].is_null()) {
                    rank_objs[tag2] = nlohmann::json();
                }
                if (rank_objs[tag2][ rank ].is_null()) {
                    nlohmann::json tag_obj;
                    tag_obj["tag"] = tag1;
                    tag_obj["edges"] = nlohmann::json::array();
                    // tag_obj["direction"] = "->";

                    rank_objs[tag2][ rank ] = tag_obj;
                }

                rank_objs[tag2][ rank ]["edges"].push_back( edge );
            }
        }
/*
        if (vec2[1] != vec2[2]) {
            // if (tag_objs[vec2[2]].is_null()) {
            //     tag_objs[vec2[2]] = nlohmann::json::array();
            // }

            std::string tag = vec2[2];
            std::string rank = vec2[0];
            std::string edge = vec1[0];
            // int32_t rank = atoi( vec2[0].c_str() );
            if (tag_objs[tag].is_null()) {
                tag_objs[tag] = nlohmann::json();
            }
            if (tag_objs[tag][ rank ].is_null()) {
                nlohmann::json tag_obj;
                tag_obj["tag"] = vec2[1];
                tag_obj["edges"] = nlohmann::json::array();
                // tag_obj["edge"] = vec1[0];
                // tag_obj["rank"] = atoi( vec2[0].c_str() );
                tag_obj["direction"] = "<-";
                tag_objs[tag][ rank ] = tag_obj;
            }

            tag_objs[tag][ rank ]["edges"].push_back( edge );
            // tag_objs[tag][ rank ][ edge ] = tag_obj;
            // tag_objs[vec2[2]].push_back(tag_obj);
        }
*/
        {
            std::string tag1 = vec2[1];
            std::string tag2 = vec2[2];
            std::string rank = vec2[0];
            std::string edge = vec1[0];

            if (relation_objs[tag1].is_null()) {
                relation_objs[tag1] = nlohmann::json();
            }

            if (relation_objs[tag1][edge].is_null()) {
                relation_objs[tag1][edge] = nlohmann::json::array();
            }

            {
                nlohmann::json tag_obj;
                tag_obj["tag"] = tag2;
                tag_obj["rank"] = rank;

                relation_objs[tag1][edge].push_back( tag_obj );
            }

            if (tag1 != tag2) {
                if (relation_objs[tag2].is_null()) {
                    relation_objs[tag2] = nlohmann::json();
                }
                if (relation_objs[tag2][edge].is_null()) {
                    relation_objs[tag2][edge] = nlohmann::json::array();
                }

                nlohmann::json tag_obj;
                tag_obj["tag"] = tag1;
                tag_obj["rank"] = rank;

                relation_objs[tag2][ edge ].push_back( tag_obj );
            }
        }

        {
            std::string tag1 = vec2[1];
            std::string tag2 = vec2[2];
            std::string rank = vec2[0];
            std::string edge = vec1[0];

            if (tag_objs[tag1].is_null()) {
                tag_objs[tag1] = nlohmann::json();
            }

            if (tag_objs[tag1][tag2].is_null()) {
                tag_objs[tag1][tag2] = nlohmann::json::array();
            }

            {
                nlohmann::json tag_obj;
                tag_obj["rank"] = rank;
                tag_obj["edges"] = edge;
                tag_objs[tag1][tag2].push_back(tag_obj);
            }

            if (tag1 != tag2) {
                if (tag_objs[tag2].is_null()) {
                    tag_objs[tag2] = nlohmann::json();
                }

                if (tag_objs[tag2][tag1].is_null()) {
                    tag_objs[tag2][tag1] = nlohmann::json::array();
                }

                nlohmann::json tag_obj;
                tag_obj["rank"] = rank;
                tag_obj["edges"] = edge;
                tag_objs[tag2][tag1].push_back(tag_obj);
            }
        }

        scan_it_->Next();
    }
    delete scan_it_;

    nlohmann::json resp_obj;
    resp_obj["code"] = 0;
    resp_obj["edges"] = edge_objs;
    resp_obj["ranks"] = rank_objs;
    resp_obj["tags"] = tag_objs;
    resp_obj["relations"] = relation_objs;

    AdminServer::http_ok(req, resp_obj.dump());
}

void Schedule::taskstatus(struct evhttp_request *req)
{
    std::string mime_type("application/json; charset=utf-8");
    evhttp_add_header(evhttp_request_get_output_headers(req), "Content-Type", mime_type.c_str());
    // evhttp_add_header(evhttp_request_get_output_headers(req), mime_type.c_str(), "1");

    AdminServer::Http_Map params = AdminServer::parse_params(req);
    if (params.find("taskid") == params.end()) {
        AdminServer::http_error(req, 400, "missing taskid");
        return ;
    }

    nlohmann::json obj;
    std::string key;
    std::string value;

    do {
        key = _sys_task_prefix_ + std::to_string(JOB_STATE_INITIAL) + params["taskid"];
        rocksdb::Status status = sysdb->Get(rocksdb::ReadOptions(), key, &value);
        if (status.ok()) break;

        key = _sys_task_prefix_ + std::to_string(JOB_STATE_RUNNING) + params["taskid"];
        status = sysdb->Get(rocksdb::ReadOptions(), key, &value);
        if (status.ok()) break;

        key = _sys_task_prefix_ + std::to_string(JOB_STATE_DONE) + params["taskid"];
        status = sysdb->Get(rocksdb::ReadOptions(), key, &value);
        if (status.ok()) break;
    } while (false);

    if (!value.empty()) JsonParse(value, obj);
    obj["key"] = key;

    do {
        if (obj.contains("succ")) break;

        std::unique_lock<std::mutex> lk(mtx);

        auto iter1 = jobStatus.find(params["taskid"]);
        if (iter1 == jobStatus.end()) break;
        auto& jobstatus = iter1->second;

        uint32_t succ = 0;
        uint32_t fail = 0;
        uint32_t total = 0;
        for (auto& item : jobstatus) {
            succ += item.second.succ;
            fail += item.second.fail;
            total += item.second.total;
        }

        obj["succ"] = succ;
        obj["fail"] = fail;
        obj["total"] = total;
    } while(false);

    AdminServer::http_ok(req, obj.dump());
}

void Schedule::tasklist(struct evhttp_request *req)
{
    std::string mime_type("application/json; charset=utf-8");
    evhttp_add_header(evhttp_request_get_output_headers(req), "Content-Type", mime_type.c_str());
    // evhttp_add_header(evhttp_request_get_output_headers(req), mime_type.c_str(), "1");

    AdminServer::Http_Map params = AdminServer::parse_params(req);

    std::string start_tag = std::to_string(JOB_STATE_INITIAL);
    std::string end_tag = std::to_string(JOB_STATE_END);
    if (params.find("state") != params.end()) {
        start_tag = params["state"];
        end_tag = std::to_string( atoi( params["state"].c_str() ) + 1);
    }

    rocksdb::Slice startkey( _sys_task_prefix_ + start_tag );
    rocksdb::Slice endkey( _sys_task_prefix_ + end_tag );

    rocksdb::ReadOptions scan_read_options;
    scan_read_options.fill_cache = false; // not fill cache
    scan_read_options.iterate_lower_bound = &startkey;
    scan_read_options.iterate_upper_bound = &endkey;

    rocksdb::Iterator* scan_it_ = sysdb->NewIterator(scan_read_options);
    if (!scan_it_) {
        LOG(ERROR) << "NewIterator failed!";
        AdminServer::http_error(req, 400, "NewIterator failed!");
        return ;
    }

    nlohmann::json resp_obj;
    nlohmann::json json_objs = nlohmann::json::array();

    scan_it_->SeekToFirst();
    while(scan_it_->Valid()) {
        // std::string key(scan_it_->key().data(), scan_it_->key().size());
        std::string value( scan_it_->value().data(), scan_it_->value().size() );

        nlohmann::json obj;
        JsonParse(value, obj);
        std::string key = std::string(scan_it_->key().data(), scan_it_->key().size());

        obj["key"] = key;
        std::string jobid = key.substr(startkey.size());

        do {
            if (obj.contains("succ")) break;

            std::unique_lock<std::mutex> lk(mtx);

            auto iter1 = jobStatus.find(jobid);
            if (iter1 == jobStatus.end()) {
                LOG(ERROR) << "task " << jobid << " not exist";
                break;
            }
            auto& jobstatus = iter1->second;

            uint32_t succ = 0;
            uint32_t fail = 0;
            uint32_t total = 0;
            for (auto& item : jobstatus) {
                succ += item.second.succ;
                fail += item.second.fail;
                total += item.second.total;
            }

            obj["succ"] = succ;
            obj["fail"] = fail;
            obj["total"] = total;
        } while(false);

        json_objs.push_back( obj );

        scan_it_->Next();
    }
    delete scan_it_;

    resp_obj["code"] = 0;
    resp_obj["tasks"] = json_objs;
    AdminServer::http_ok(req, resp_obj.dump());
}

void Schedule::taskdelete(struct evhttp_request *req)
{
    std::string mime_type("application/json; charset=utf-8");
    evhttp_add_header(evhttp_request_get_output_headers(req), "Content-Type", mime_type.c_str());
    // evhttp_add_header(evhttp_request_get_output_headers(req), mime_type.c_str(), "1");

    if (req->type != evhttp_cmd_type::EVHTTP_REQ_POST) {
        AdminServer::http_error(req, 400, "not a post request!");
        return ;
    }

    std::string content = AdminServer::read_content(req);
    if (content.empty()) {
        AdminServer::http_error(req, 400, "empty content in post request!");
        return ;
    }

    LOG(WARNING) << content;

    nlohmann::json json_obj;
    if (!JsonParse(content, json_obj)) {
        AdminServer::http_error(req, 400, "invalid json format!");
        return ;
    }

    if (!json_obj.contains("taskid")) {
        AdminServer::http_error(req, 400, "miss taskid!");
        return ;
    }

    std::string taskid;
    if (json_obj["taskid"].is_number()) {
        taskid = std::to_string( json_obj.value("taskid", 0) );
    } else if (json_obj["taskid"].is_string()) {
        taskid = json_obj.value("taskid", "0");
    }

    rocksdb::Status status;
    std::string key;
    std::string value;
    do {
        key = _sys_task_prefix_ + std::to_string(JOB_STATE_INITIAL) + taskid;
        status = sysdb->Get(rocksdb::ReadOptions(), key, &value);
        if (status.ok()) break;

        key = _sys_task_prefix_ + std::to_string(JOB_STATE_RUNNING) + taskid;
        status = sysdb->Get(rocksdb::ReadOptions(), key, &value);
        if (status.ok()) break;

        key = _sys_task_prefix_ + std::to_string(JOB_STATE_DONE) + taskid;
        status = sysdb->Get(rocksdb::ReadOptions(), key, &value);
        if (status.ok()) break;
    } while (false);

    if (status.ok()) {
        status = sysdb->Delete(rocksdb::WriteOptions(), key);
    }

    nlohmann::json resp_obj;
    nlohmann::json obj;
    JsonParse(value, obj);
    if (status.ok()) {
        resp_obj["code"] = 0;
        resp_obj["resp"] = obj;
    } else {
        resp_obj["code"] = -1;
        resp_obj["err"] = "task " + taskid + " not exist!";
    }

    AdminServer::http_ok(req, resp_obj.dump());
}

void Schedule::hdfslist(struct evhttp_request *req)
{
    std::string mime_type("application/json; charset=utf-8");
    evhttp_add_header(evhttp_request_get_output_headers(req), "Content-Type", mime_type.c_str());
    // evhttp_add_header(evhttp_request_get_output_headers(req), mime_type.c_str(), "1");

    AdminServer::Http_Map params = AdminServer::parse_params(req);

    std::vector<std::string> storage_http_addrs;
    folly::split(",", FLAGS_storage_http_addrs, storage_http_addrs, true);
    if (storage_http_addrs.empty()) {
        AdminServer::http_error(req, 400, "storage_http_addrs is empty!");
        return ;
    }

    std::string path = "/list?" + std::string("hdfspath=");
    if (params.find("hdfspath") != params.end()) {
        path += params["hdfspath"];
    } else {
        path += FLAGS_hdfs_job_dir;
    }

    httplib::Client cli(storage_http_addrs[0]);
    auto resp = cli.Get(path);

    AdminServer::http_ok(req, resp->body);
}

void Schedule::subgraphclear(struct evhttp_request *req)
{
    std::string mime_type("application/json; charset=utf-8");
    evhttp_add_header(evhttp_request_get_output_headers(req), "Content-Type", mime_type.c_str());

    if (req->type != evhttp_cmd_type::EVHTTP_REQ_POST) {
        AdminServer::http_error(req, 400, "not a post request!");
        return ;
    }

    nebula::ConnectionPool pool;
    pool.init({"10.48.128.50:9669","10.48.40.231:9669","10.48.40.232:9669"}, nebula::Config{});

    auto session = pool.getSession("root", "nebula");
    if (!session.valid()) {
        AdminServer::http_error(req, 400, "graph database login failed!");
        return ;
    }

    std::string space = "subgraphdata";

    {
        auto result = session.execute("clear space " + space + ";");
        if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
            std::string msg = "graph database exec failed! error code: " + std::to_string((int32_t)result.errorCode) + *result.errorMsg;
            AdminServer::http_error(req, 400, msg);
            return ;
        }
    }

    session.release();

    nlohmann::json resp_obj;
    if (true) {
        resp_obj["code"] = 0;
    } else {
        resp_obj["code"] = -1;
    }

    AdminServer::http_ok(req, resp_obj.dump());
}

void Schedule::subgraphmeta(struct evhttp_request* req)
{
    std::string mime_type("application/json; charset=utf-8");
    evhttp_add_header(evhttp_request_get_output_headers(req), "Content-Type", mime_type.c_str());

    if (req->type != evhttp_cmd_type::EVHTTP_REQ_POST) {
        AdminServer::http_error(req, 400, "not a post request!");
        return ;
    }

    std::string content = AdminServer::read_content(req);
    if (content.empty()) {
        AdminServer::http_error(req, 400, "empty content in post request!");
        return ;
    }

    LOG(WARNING) << content;

    nlohmann::json json_obj;
    if (!JsonParse(content, json_obj)) {
        AdminServer::http_error(req, 400, "invalid json format!");
        return ;
    }

    nebula::ConnectionPool pool;
    pool.init({"10.48.128.50:9669","10.48.40.231:9669","10.48.40.232:9669"}, nebula::Config{});

    auto session = pool.getSession("root", "nebula");
    if (!session.valid()) {
        AdminServer::http_error(req, 400, "graph database login failed!");
        return ;
    }

    std::string space = json_obj.value("space", "subgraphdata");

    {
        auto result = session.execute("use " + space + ";");
        if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
            std::string msg = "graph database exec failed! error code: " + std::to_string((int32_t)result.errorCode) + *result.errorMsg;
            AdminServer::http_error(req, 400, msg);
            return ;
        }
    }

    {

        std::string cmd = "CREATE TAG IF NOT EXISTS entities(type string, name string, properties string);";

        LOG(WARNING) << cmd;

        auto result = session.execute(cmd);
        if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
            std::string msg = "graph database exec " + cmd + " failed! error code: " + std::to_string((int32_t)result.errorCode) + *result.errorMsg;
            AdminServer::http_error(req, 400, msg);
            return ;
        }
    }

    for (auto obj : json_obj["relations"]) {

        std::string cmd = "CREATE EDGE IF NOT EXISTS ";
        std::string id = obj.value("id", "default");
        std::string type = obj.value("type", "default");
        std::string source_id = obj.value("source_id", "default");
        std::string target_id = obj.value("target_id", "default");
        std::string properties = "{}";
        if (!obj["properties"].is_null()) {
            properties = obj["properties"].dump();
        }

        cmd.append(type).append(" (id string, properties string);");

        LOG(WARNING) << cmd;

        auto result = session.execute(cmd);
        if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
            std::string msg = "graph database exec " + cmd + " failed! error code: " + std::to_string((int32_t)result.errorCode) + *result.errorMsg;
            AdminServer::http_error(req, 400, msg);
            return ;
        }
    }

    session.release();

    nlohmann::json resp_obj;
    if (true) {
        resp_obj["code"] = 0;
    } else {
        resp_obj["code"] = -1;
    }

    AdminServer::http_ok(req, resp_obj.dump());
}

void Schedule::subgraphdata(struct evhttp_request *req)
{
    std::string mime_type("application/json; charset=utf-8");
    evhttp_add_header(evhttp_request_get_output_headers(req), "Content-Type", mime_type.c_str());

    if (req->type != evhttp_cmd_type::EVHTTP_REQ_POST) {
        AdminServer::http_error(req, 400, "not a post request!");
        return ;
    }

    std::string content = AdminServer::read_content(req);
    if (content.empty()) {
        AdminServer::http_error(req, 400, "empty content in post request!");
        return ;
    }

    LOG(WARNING) << content;

    nlohmann::json json_obj;
    if (!JsonParse(content, json_obj)) {
        AdminServer::http_error(req, 400, "invalid json format!");
        return ;
    }

    nebula::ConnectionPool pool;
    pool.init({"10.48.128.50:9669","10.48.40.231:9669","10.48.40.232:9669"}, nebula::Config{});

    auto session = pool.getSession("root", "nebula");
    if (!session.valid()) {
        AdminServer::http_error(req, 400, "graph database login failed!");
        return ;
    }

    std::string space = json_obj.value("space", "subgraphdata");

    {
        auto result = session.execute("use " + space + "; CLEAR SPACE " + space + ";");
        if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
            std::string msg = "graph database exec failed! error code: " + std::to_string((int32_t)result.errorCode) + *result.errorMsg;
            AdminServer::http_error(req, 400, msg);
            return ;
        }
    }

    for (auto obj : json_obj["entities"]) {

        std::string cmd = "INSERT VERTEX entities(type, name, properties) VALUES \"";
        std::string id = obj.value("id", "default");
        std::string type = obj.value("type", "default");
        std::string name = obj.value("name", "default");
        std::string properties = "{}";
        if (!obj["properties"].is_null()) {
            properties = obj["properties"].dump();
        }

        cmd.append(id).append("\"").append(" : (\"").append(type).append("\", \"").append(name).append("\", '")
            .append(properties).append("' );");

        LOG(WARNING) << cmd;

        auto result = session.execute(cmd);
        if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
            std::string msg = "graph database exec " + cmd + " failed! error code: " + std::to_string((int32_t)result.errorCode) + *result.errorMsg;
            AdminServer::http_error(req, 400, msg);
            return ;
        }
    }

    for (auto obj : json_obj["relations"]) {

        std::string id = obj.value("id", "default");
        std::string type = obj.value("type", "default");
        std::string source_id = obj.value("source_id", "default");
        std::string target_id = obj.value("target_id", "default");
        std::string properties = "{}";
        if (!obj["properties"].is_null()) {
            properties = obj["properties"].dump();
        }

        std::string cmd;
        cmd.append("INSERT EDGE ").append(type).append(" (id, properties) VALUES ");
        cmd.append("\"").append(source_id).append("\"->").append("\"").append(target_id).append("\"");
        cmd.append(":(").append("\"").append(id).append("\", ").append("'").append(properties).append("');");

        LOG(WARNING) << cmd;

        auto result = session.execute(cmd);
        if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
            std::string msg = "graph database exec " + cmd + " failed! error code: " + std::to_string((int32_t)result.errorCode) + *result.errorMsg;
            AdminServer::http_error(req, 400, msg);
            return ;
        }
    }

    session.release();

    nlohmann::json resp_obj;
    if (true) {
        resp_obj["code"] = 0;
    } else {
        resp_obj["code"] = -1;
    }

    AdminServer::http_ok(req, resp_obj.dump());
}

bool Schedule::Initialize()
{
    register_http_callbacks();

    std::vector<rocksdb::ColumnFamilyDescriptor> cfdescs;
    rocksdb::Status s = rocksdb::LoadLatestOptions(FLAGS_db_path, rocksdb::Env::Default(), &dbopts, &cfdescs, false);
    if (s.ok()) {
        LOG(WARNING) << "load latest option success! path: " << FLAGS_db_path;
    } else if (s.IsNotFound()) {
        LOG(WARNING) << "db option not found, create one! path: " << FLAGS_db_path;

        dbopts.IncreaseParallelism();
        dbopts.create_if_missing = true;

        cfopts.OptimizeLevelStyleCompaction();
        cfopts.optimize_filters_for_hits = true;

        cfdescs.emplace_back(rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, cfopts));
    } else {
        LOG(ERROR) << "load latest option failed! path: " << FLAGS_db_path << " err: " << s.ToString();

        return false;
    }

    std::vector<rocksdb::ColumnFamilyHandle*> cfhandles;
    s = rocksdb::DB::Open(dbopts, FLAGS_db_path, cfdescs, &cfhandles, &sysdb);
    if (!s.ok()) {
        LOG(ERROR) << "db open failed! path: " << FLAGS_db_path << " err: " << s.ToString();
        return false;
    }

    for (int32_t i = 0; i < FLAGS_thread_num; i++) {
        rpc::CThread* thd = new rpc::CThread("wk_" + std::to_string(i));
        threads.push_back(thd);
    }

    adminserver.initialize(FLAGS_http_port);
    return true;
}

std::string Schedule::pickInitialJob()
{
    std::string jobid;

    rocksdb::Slice startkey(_sys_task_prefix_ + std::to_string(JOB_STATE_INITIAL) );
    rocksdb::Slice endkey(_sys_task_prefix_ + std::to_string(JOB_STATE_RUNNING) );

    rocksdb::ReadOptions scan_read_options;
    scan_read_options.fill_cache = false; // not fill cache
    scan_read_options.iterate_lower_bound = &startkey;
    scan_read_options.iterate_upper_bound = &endkey;

    rocksdb::Iterator* scan_it_ = sysdb->NewIterator(scan_read_options);
    if (!scan_it_) {
        LOG(ERROR) << "scheduleInitialJob NewIterator failed!";
        return jobid;
    }

    scan_it_->SeekToFirst();
    if (!scan_it_->Valid()) {
        // LOG(WARNING) << "scheduleInitialJob no Initial job!";
        delete scan_it_;
        return jobid;
    }

    std::string jobkey(scan_it_->key().data(), scan_it_->key().size());

    LOG(WARNING) << "Schedule Initial job: " << jobkey.substr(startkey.size());
    delete scan_it_;

    return jobkey.substr(startkey.size());
}

std::string Schedule::pickRunningJob()
{
    std::string jobid;

    rocksdb::Slice startkey(_sys_task_prefix_ + std::to_string(JOB_STATE_RUNNING) );
    rocksdb::Slice endkey(_sys_task_prefix_ + std::to_string(JOB_STATE_DONE) );

    rocksdb::ReadOptions scan_read_options;
    scan_read_options.fill_cache = false; // not fill cache
    scan_read_options.iterate_lower_bound = &startkey;
    scan_read_options.iterate_upper_bound = &endkey;

    rocksdb::Iterator* scan_it_ = sysdb->NewIterator(scan_read_options);
    if (!scan_it_) {
        LOG(ERROR) << "scheduleInitialJob NewIterator failed!";
        return jobid;
    }

    scan_it_->SeekToFirst();
    if (!scan_it_->Valid()) {
        delete scan_it_;
        return jobid;
    }

    std::string jobkey(scan_it_->key().data(), scan_it_->key().size());

    LOG(WARNING) << "Still Running job: " << jobkey.substr(startkey.size());
    delete scan_it_;

    return jobkey.substr(startkey.size());
}

void Schedule::incrSucc(const std::string& jobid, std::string spacename, uint32_t count)
{
    std::unique_lock<std::mutex> lk(mtx);

    auto iter1 = jobStatus.find(jobid);
    if (iter1 == jobStatus.end()) return ;
    auto& jobstatus = iter1->second;

    auto iter2 = jobstatus.find(spacename);
    if (iter2 != jobstatus.end()) {
        iter2->second.succ += count;
    } else {
        JobStat stat;
        stat.succ = count;
        jobstatus.emplace(spacename, stat);
    }

    uint32_t succ_count = 0;
    uint32_t fail_count = 0;
    uint32_t total_count = 0;
    for (auto& ele : jobstatus) {
        succ_count += ele.second.succ;
        fail_count += ele.second.fail;
        total_count += ele.second.total;
    }

    if ( (succ_count + fail_count) < total_count) return ;

    std::string value;
    std::string oldkey = _sys_task_prefix_ + std::to_string(JOB_STATE_RUNNING) + jobid;
    rocksdb::Status status = sysdb->Get(rocksdb::ReadOptions(), oldkey, &value);
    if (status.ok()) {

        rocksdb::WriteBatch batch;
        batch.Delete(oldkey);

        std::string newkey = _sys_task_prefix_ + std::to_string(JOB_STATE_DONE) + jobid;

        nlohmann::json obj;
        JsonParse(value, obj);
        obj["done_time"] = gTimeConvert( gGetCurrentUs()/1000000 );
        obj["state"] = "Done";
        obj["succ"] = succ_count;
        obj["fail"] = fail_count;
        obj["total"] = total_count;

        batch.Put(newkey, obj.dump());

        status = sysdb->Write(rocksdb::WriteOptions(), &batch);

        jobStatus.erase(iter1);
    }
}

void Schedule::incrFail(const std::string& jobid, std::string spacename, uint32_t count)
{
    std::unique_lock<std::mutex> lk(mtx);

    auto iter1 = jobStatus.find(jobid);
    if (iter1 == jobStatus.end()) return ;
    auto& jobstatus = iter1->second;

    auto iter2 = jobstatus.find(spacename);
    if (iter2 != jobstatus.end()) {
        iter2->second.fail += count;
    } else {
        JobStat stat;
        stat.fail = count;
        jobstatus.emplace(spacename, stat);
    }

    uint32_t succ_count = 0;
    uint32_t fail_count = 0;
    uint32_t total_count = 0;
    for (auto& ele : jobstatus) {
        succ_count += ele.second.succ;
        fail_count += ele.second.fail;
        total_count += ele.second.total;
    }

    if ( (succ_count + fail_count) < total_count) return ;

    std::string value;
    std::string oldkey = _sys_task_prefix_ + std::to_string(JOB_STATE_RUNNING) + jobid;
    rocksdb::Status status = sysdb->Get(rocksdb::ReadOptions(), oldkey, &value);
    if (status.ok()) {

        rocksdb::WriteBatch batch;
        batch.Delete(oldkey);

        std::string newkey = _sys_task_prefix_ + std::to_string(JOB_STATE_DONE) + jobid;

        nlohmann::json obj;
        JsonParse(value, obj);
        obj["done_time"] = gTimeConvert( gGetCurrentUs()/1000000 );
        obj["state"] = "Done";
        obj["succ"] = succ_count;
        obj["fail"] = fail_count;
        obj["total"] = total_count;

        batch.Put(newkey, obj.dump());

        status = sysdb->Write(rocksdb::WriteOptions(), &batch);

        jobStatus.erase(iter1);
    }
}

void Schedule::incrTotal(const std::string& jobid, std::string spacename, uint32_t count)
{
    std::unique_lock<std::mutex> lk(mtx);

    auto iter1 = jobStatus.find(jobid);
    if (iter1 == jobStatus.end()) return ;
    auto& jobstatus = iter1->second;

    auto iter2 = jobstatus.find(spacename);
    if (iter2 != jobstatus.end()) {
        iter2->second.total += count;
    } else {
        JobStat stat;
        stat.total = count;
        jobstatus.emplace(spacename, stat);
    }
}

bool Schedule::scheduleJob(const std::string& jobid)
{
    std::vector<std::string> storage_http_addrs;
    folly::split(",", FLAGS_storage_http_addrs, storage_http_addrs, true);
    if (storage_http_addrs.empty()) {
        LOG(ERROR) << "storage_http_addrs is empty!";
        return false;
    }

    std::string hdfspath = "/list?" + std::string("hdfspath=") + FLAGS_hdfs_job_dir + "/" + jobid;

    httplib::Client cli(storage_http_addrs[0]);
    auto resp = cli.Get(hdfspath);

    if (!resp || resp->status != httplib::StatusCode::OK_200) {
        LOG(ERROR) << httplib::to_string(resp.error());
        return false;
    }

    nlohmann::json obj;
    JsonParse(resp->body, obj);

    LOG(WARNING) << hdfspath;
    LOG(WARNING) << obj.dump();

    if (obj["code"] != 0) {
        LOG(ERROR) << "list failed! " << obj["resp"];
        return false;
    }

    nlohmann::json resp_obj = obj["resp"];
    if (resp_obj["dirs"].is_null() || resp_obj["dirs"].empty()) {
        LOG(ERROR) << "dirs is empty!";
        return false;
    }

    std::vector<std::string> meta_server_addrs;
    folly::split(",", FLAGS_meta_server_addrs, meta_server_addrs, true);

    nebula::MetaClient mclient(meta_server_addrs);

    auto hostsRet = mclient.listHosts(nebula::meta::cpp2::ListHostType::ALLOC);
    if (!hostsRet.first) {
        LOG(ERROR) << "List hosts failed";
        return false;
    }

    // preWorker(jobid, resp_obj["dirs"]);

    auto& hostItems = hostsRet.second;
    for (auto& hostitem : hostItems) {
        LOG(WARNING) << hostitem.get_hostAddr().hostString() << " : " << (uint32_t)hostitem.get_role();
    }

    std::vector<JobTask> tasks;
    for (std::string spacedir : resp_obj["dirs"]) {

        std::string spacename = spacedir.substr(spacedir.find_last_of('/') + 1);
        auto ret = mclient.getSpaceIdByNameFromCache(spacename);
        LOG(WARNING) << "space: " << spacename << " " << ret.first << " : " << ret.second;

        uint32_t spaceid = ret.second;
        auto all_space_parts = mclient.getPartsFromCache(spaceid);
        for (auto partid : all_space_parts.second) {

            for (auto& hostitem : hostItems) {
                if ((uint32_t)hostitem.get_role() != 2) {
                    continue;
                }

                auto all_host_parts = hostitem.get_all_parts();

                auto iter = all_host_parts.find(spacename);
                if (iter == all_host_parts.end()) {
                    continue;
                }

                std::string storage_addrs = hostitem.get_hostAddr().host + ":" + std::to_string(hostitem.get_hostAddr().port + 10000);

                for (auto part_id : iter->second) {
                    if (partid != part_id) continue;

                    std::string hdfsdir = "/list?" + std::string("hdfspath=") + spacedir + "/" + std::to_string(partid);

                    httplib::Client client(storage_addrs);
                    auto resp2 = client.Get(hdfsdir);

                    if (!resp2 || resp2->status != httplib::StatusCode::OK_200) {
                        LOG(ERROR) << httplib::to_string(resp2.error());
                        continue ;
                    }

                    JsonParse(resp2->body, obj);

                    if (obj["code"] != 0) {
                        LOG(ERROR) << "list failed! " << obj["resp"];
                        continue ;
                    }

                    nlohmann::json resp_obj2 = obj["resp"];
                    if (resp_obj2["files"].is_null() || resp_obj2["files"].empty()) {
                        LOG(ERROR) << "files is empty!";
                        continue ;
                    }

                    incrTotal(jobid, spacename, resp_obj2["files"].size());

                    JobTask task;
                    task.jobid = jobid;
                    task.spaceid = spaceid;
                    task.spacename = spacename;
                    task.partid = partid;
                    task.hostitem = hostitem;
                    task.files = resp_obj2["files"];
                    tasks.emplace_back(task);

                    break;
                }
            }
        }        
    }

    uint32_t index = 0;
    uint32_t size = threads.size();

    for (auto task : tasks) {
        auto func = [task, this] (rpc::CThread *thr_) {
            (void)thr_;
            this->ingestFiles(task);
        };

        threads[index++%size]->dispatch(func);
    }
    

    return true;
}

void Schedule::ingestFiles(JobTask task)
{
    std::string storage_addrs = task.hostitem.get_hostAddr().host + ":" + std::to_string(task.hostitem.get_hostAddr().port + 10000);
    httplib::Client client(storage_addrs);

    for (auto file_obj : task.files) {

        std::string hdfsdir = "/download?spaceid=" + std::to_string(task.spaceid) + "&partid=" + std::to_string(task.partid) + "&hdfspath=" + file_obj.get<std::string>();
        httplib::Result response = client.Get(hdfsdir);

        if (!response || response->status != httplib::StatusCode::OK_200) {
            LOG(ERROR) << "download failed! hdfsdir:" << hdfsdir<< httplib::to_string(response.error());
            incrFail(task.jobid, task.spacename, 1);
            continue;
        }

        nlohmann::json obj;
        JsonParse(response->body, obj);

        if (obj["code"] != 0) {
            LOG(ERROR) << task.hostitem.get_hostAddr().host <<" download " << file_obj << " failed!" << obj["code"] << " " << obj["resp"];
            incrFail(task.jobid, task.spacename, 1);
            continue;
        }

        std::string filepath = obj["resp"];

        uint32_t wait_times = FLAGS_download_wait_seconds;
        bool exist = false;
        for (uint32_t i = 0; i < wait_times; i++) {
            usleep(500*1000);
            hdfsdir = "/check?filepath=" + filepath;
            response = client.Get(hdfsdir);
            if (!response || response->status != httplib::StatusCode::OK_200) {
                // LOG(ERROR) << "check failed! hdfsdir:" << hdfsdir << httplib::to_string(response.error());
                continue;
            }

            JsonParse(response->body, obj);
            if (obj["code"] != 0) {
                continue;
            }

            LOG(WARNING) << task.hostitem.get_hostAddr().host << " download " << file_obj << " success!";
            exist = true;
            break;
        }

        if (!exist) {
            LOG(ERROR) << task.hostitem.get_hostAddr().host <<" check " << file_obj << " failed!";
            incrFail(task.jobid, task.spacename, 1);
            continue;
        }

        hdfsdir = "/ingest?spaceid=" + std::to_string(task.spaceid) + "&partid=" + std::to_string(task.partid) + "&filepath=" + filepath;

        response = client.Get(hdfsdir);

        if (!response || response->status != httplib::StatusCode::OK_200) {
            LOG(ERROR) << task.hostitem.get_hostAddr().host << " ingest " << file_obj << " failed! " << httplib::to_string(response.error());
            incrFail(task.jobid, task.spacename, 1);
            continue;
        }

        JsonParse(response->body, obj);

        if (obj["code"] != 0) {
            LOG(ERROR) << task.hostitem.get_hostAddr().host << " ingest " << file_obj << " failed! " << obj["code"] << " " << obj["resp"];
            incrFail(task.jobid, task.spacename, 1);
            continue;
        }

        LOG(WARNING) << task.hostitem.get_hostAddr().host << " ingest " << file_obj << " success!";
        incrSucc(task.jobid, task.spacename, 1);
    }
}

void Schedule::Run()
{

    while (true) {

        sleep(5);

        std::string jobid = pickRunningJob();
        if (!jobid.empty()) continue;

        jobid = pickInitialJob();

        do {
            if (jobid.empty()) break;

            std::string value;
            std::string oldkey = _sys_task_prefix_ + std::to_string(JOB_STATE_INITIAL) + jobid;
            rocksdb::Status status = sysdb->Get(rocksdb::ReadOptions(), oldkey, &value);

            if (!status.ok()) break;

            rocksdb::WriteBatch batch1;
            batch1.Delete(oldkey);

            std::string newkey = _sys_task_prefix_ + std::to_string(JOB_STATE_RUNNING) + jobid;

            nlohmann::json obj;
            JsonParse(value, obj);
            obj["running_time"] = gTimeConvert( gGetCurrentUs()/1000000 );;
            obj["state"] = "Running";

            batch1.Put(newkey, obj.dump());

            status = sysdb->Write(rocksdb::WriteOptions(), &batch1);
            if (!status.ok()) break;

            {
                std::unique_lock<std::mutex> lk(mtx);
                std::unordered_map<std::string, JobStat> stats;
                jobStatus.emplace(jobid, stats);
            }

            bool ret = scheduleJob( jobid );

            rocksdb::WriteBatch batch2;

            obj["ingest_time"] = gTimeConvert( gGetCurrentUs()/1000000 );;

            if (!ret) {
                oldkey = newkey;
                batch2.Delete(oldkey);

                newkey = _sys_task_prefix_ + std::to_string(JOB_STATE_DONE) + jobid;

                obj["done_time"] = gTimeConvert( gGetCurrentUs()/1000000 );
                obj["succ"] = 0;
                obj["fail"] = 0;
                obj["total"] = 0;
                obj["state"] = "Done";
            }

            batch2.Put(newkey, obj.dump());

            status = sysdb->Write(rocksdb::WriteOptions(), &batch2);

            if (!status.ok()) break;
        } while (false);
    }

}

void Schedule::Start()
{
    workerthd = std::thread(&Schedule::Run, this);

    adminserver.start();
}

void Schedule::Wait()
{
    workerthd.join();

    adminserver.wait();
}

void Schedule::Stop()
{
    adminserver.stop();
}

int32_t main(int argc, char *argv[])
{
    // google::SetVersionString(nebula::versionString());
    google::SetUsageMessage("Usage: " + std::string(argv[0]) + " [options]");
    google::SetStderrLogging(google::INFO);

    if (argc == 1) {
        printHelp(argv[0]);
        return EXIT_FAILURE;
    }
    if (argc == 2) {
        if (::strcmp(argv[1], "-h") == 0) {
            printHelp(argv[0]);
            return EXIT_SUCCESS;
        }
    }

    folly::init(&argc, &argv, true);

    Schedule schedule;

    if (!schedule.Initialize()) {
        return -1;
    }

    schedule.Start();
    schedule.Wait();

    return 0;
}
