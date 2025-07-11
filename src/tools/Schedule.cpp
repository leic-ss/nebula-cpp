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

#include "rocksdb/db.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/env.h"
#include "rocksdb/utilities/options_util.h"
#include "admin_server.h"

#include "Schedule.h"
#include "cpphttplib/httplib.h"

static void printHelp(const char *prog) {
    fprintf(stderr, "%s --datafile <data_file>\n", prog);
}

DEFINE_string(meta_server_addrs,  "127.0.0.1:9559", "default meta server addr");
DEFINE_int32(http_port, 8108, "default meta server http port");
DEFINE_string(db_path, "data/sysdb", "system data path");
DEFINE_string(storage_http_addrs, "127.0.0.1:19779", "system data path");
DEFINE_string(hdfs_job_dir, "hdfs://", "hdfs job dir");

DEFINE_int32(thread_num, 6, "default process thread number");

static const std::string _sys_task_prefix_ = "_SYS_TASKS_";
static const std::string _sys_begin_tag_ = ":";
static const std::string _sys_end_tag_ = ";";

static const uint32_t JOB_STATE_INITIAL = 0;
static const uint32_t JOB_STATE_RUNNING = 1;
static const uint32_t JOB_STATE_DONE = 2;
static const uint32_t JOB_STATE_END = 3;

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
    evhttp_add_header(evhttp_request_get_output_headers(req), "Content-Type", "0");
    evhttp_add_header(evhttp_request_get_output_headers(req), mime_type.c_str(), "1");

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

void Schedule::taskstatus(struct evhttp_request *req)
{
    std::string mime_type("application/json; charset=utf-8");
    evhttp_add_header(evhttp_request_get_output_headers(req), "Content-Type", "0");
    evhttp_add_header(evhttp_request_get_output_headers(req), mime_type.c_str(), "1");

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

    JsonParse(value, obj);
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
    evhttp_add_header(evhttp_request_get_output_headers(req), "Content-Type", "0");
    evhttp_add_header(evhttp_request_get_output_headers(req), mime_type.c_str(), "1");

    AdminServer::Http_Map params = AdminServer::parse_params(req);

    std::string start_tag = std::to_string(JOB_STATE_INITIAL);
    std::string end_tag = std::to_string(JOB_STATE_END);
    if (params.find("state") != params.end()) {
        start_tag = params["state"];
        end_tag = std::to_string( atoi( params["state"].c_str() ) + 1);
        return ;
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
        obj["key"] = std::string(scan_it_->key().data(), scan_it_->key().size());

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
    evhttp_add_header(evhttp_request_get_output_headers(req), "Content-Type", "0");
    evhttp_add_header(evhttp_request_get_output_headers(req), mime_type.c_str(), "1");

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
    std::string value;
    do {
        std::string key = _sys_task_prefix_ + std::to_string(JOB_STATE_INITIAL) + taskid;
        status = sysdb->Get(rocksdb::ReadOptions(), key, &value);
        if (status.ok()) break;

        key = _sys_task_prefix_ + std::to_string(JOB_STATE_RUNNING) + taskid;
        status = sysdb->Get(rocksdb::ReadOptions(), key, &value);
        if (status.ok()) break;

        key = _sys_task_prefix_ + std::to_string(JOB_STATE_DONE) + taskid;
        status = sysdb->Get(rocksdb::ReadOptions(), key, &value);
        if (status.ok()) break;
    } while (false);

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
    evhttp_add_header(evhttp_request_get_output_headers(req), "Content-Type", "0");
    evhttp_add_header(evhttp_request_get_output_headers(req), mime_type.c_str(), "1");

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
        LOG(WARNING) << "scheduleInitialJob no Initial job!";
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
        obj["done_time"] = gTimeConvert( gGetCurrentUs()/1000000 );;
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
        obj["done_time"] = gGetCurrentUs()/1000000;
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

void Schedule::scheduleJob(const std::string& jobid)
{
    std::vector<std::string> storage_http_addrs;
    folly::split(",", FLAGS_storage_http_addrs, storage_http_addrs, true);
    if (storage_http_addrs.empty()) {
        LOG(ERROR) << "storage_http_addrs is empty!";
        return ;
    }

    std::string hdfspath = "/list?" + std::string("hdfspath=") + FLAGS_hdfs_job_dir + "/" + jobid;

    httplib::Client cli(storage_http_addrs[0]);
    auto resp = cli.Get(hdfspath);

    if (!resp || resp->status != httplib::StatusCode::OK_200) {
        LOG(ERROR) << httplib::to_string(resp.error());
        return ;
    }

    nlohmann::json obj;
    JsonParse(resp->body, obj);

    LOG(WARNING) << hdfspath;
    LOG(WARNING) << obj.dump();

    if (obj["code"] != 0) {
        LOG(ERROR) << "list failed! " << obj["resp"];
        return ;
    }

    nlohmann::json resp_obj = obj["resp"];
    if (resp_obj["dirs"].is_null() || resp_obj["dirs"].empty()) {
        LOG(ERROR) << "dirs is empty!";
        return ;
    }

    std::vector<std::string> meta_server_addrs;
    folly::split(",", FLAGS_meta_server_addrs, meta_server_addrs, true);

    nebula::MetaClient mclient(meta_server_addrs);

    auto hostsRet = mclient.listHosts(nebula::meta::cpp2::ListHostType::ALLOC);
    if (!hostsRet.first) {
        LOG(ERROR) << "List hosts failed";
        return ;
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
    

    return ;
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

        bool exist = false;
        for (uint32_t i = 0; i < 30; i++) {
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

        sleep(10);


        std::string jobid = pickRunningJob();
        if (!jobid.empty()) continue;

        jobid = pickInitialJob();
        if (!jobid.empty()) {

            {
                std::unique_lock<std::mutex> lk(mtx);
                std::unordered_map<std::string, JobStat> status;
                jobStatus.emplace(jobid, status);
            }

            scheduleJob( jobid );

            std::string value;
            std::string oldkey = _sys_task_prefix_ + std::to_string(JOB_STATE_INITIAL) + jobid;
            rocksdb::Status status = sysdb->Get(rocksdb::ReadOptions(), oldkey, &value);
            if (status.ok()) {

                rocksdb::WriteBatch batch;
                batch.Delete(oldkey);

                std::string newkey = _sys_task_prefix_ + std::to_string(JOB_STATE_RUNNING) + jobid;

                nlohmann::json obj;
                JsonParse(value, obj);
                obj["running_time"] = gTimeConvert( gGetCurrentUs()/1000000 );;
                obj["state"] = "Running";

                batch.Put(newkey, obj.dump());

                status = sysdb->Write(rocksdb::WriteOptions(), &batch);
            }
        }
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
