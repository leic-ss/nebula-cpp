#include "admin_server.h"

#include "../thrift/ThriftClientManager.h"
#include "common/thrift/ThriftTypes.h"
#include "interface/gen-cpp2/MetaServiceAsyncClient.h"
#include "interface/gen-cpp2/meta_types.h"
#include "nebula/mclient/MetaClient.h"

#include "nlohmann/json.hpp"
#include "cthread.h"

#include <mutex>

typedef struct JobStat
{
public:
    JobStat() : succ(0), fail(0), total(0) { }

    uint32_t succ{0};
    uint32_t fail{0};
    uint32_t total{0};
} JobStat;

typedef struct JobTask
{
public:
    std::string jobid;
    uint32_t spaceid;
    std::string spacename;
    uint32_t partid;
    nebula::meta::cpp2::HostItem hostitem;
    nlohmann::json files;
} JobTask;

class Schedule
{
public:
    Schedule() { }
    ~Schedule();

public:
    bool Initialize();
    void Start();
    void Wait();
    void Stop();
    void Run();

private:
    bool JsonParse(const std::string& json_str, nlohmann::json& json_obj);

    std::string pickInitialJob();
    std::string pickRunningJob();

    bool scheduleJob(const std::string& jobid);

    void ingestFiles(JobTask task);
    // void ingestFiles(std::string jobid, uint32_t spaceid, std::string spacename, uint32_t partid, nebula::meta::cpp2::HostItem hostitem, nlohmann::json files);

    void incrSucc(const std::string& jobid, std::string spacename, uint32_t count);
    void incrFail(const std::string& jobid, std::string spacename, uint32_t count);
    void incrTotal(const std::string& jobid, std::string spacename, uint32_t count);

private:
    void register_http_callbacks();
    void taskschedule(struct evhttp_request *req);
    void relationcreate(struct evhttp_request *req);
    void relationlist(struct evhttp_request *req);
    void taskstatus(struct evhttp_request *req);
    void taskdelete(struct evhttp_request *req);
    void tasklist(struct evhttp_request *req);
    void hdfslist(struct evhttp_request *req);
    void subgraphclear(struct evhttp_request *req);
    void subgraphmeta(struct evhttp_request *req);
    void subgraphdata(struct evhttp_request *req);

private:
    rocksdb::DB* sysdb;
    rocksdb::DBOptions dbopts;
    rocksdb::ColumnFamilyOptions cfopts;

    AdminServer adminserver;

    std::thread workerthd;

    std::vector<rpc::CThread*> threads;

    std::mutex mtx;
    std::unordered_map<std::string, std::unordered_map<std::string, JobStat> > jobStatus;
};