#pragma once

#include "event2/event.h"
#include "event2/http.h"
#include "event2/buffer.h"
#include "event2/util.h"
#include "event2/keyvalq_struct.h"
#include "event2/http_struct.h"

#include <memory>
#include <thread>
#include <functional>
#include <unordered_map>
#include <atomic>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <signal.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

class AdminServer {
public:
    enum State : int32_t { UNINITIALIZED = 0, INITIALIZED = 1, STARTED = 2, STOPING = 3};
    using Http_Handler = std::function<void(struct evhttp_request *)>;
    using Http_Map = std::unordered_map<std::string, std::string>;

public:
    AdminServer();
    ~AdminServer();

public:
    bool initialize(uint16_t port = 5050);
    bool reg_handler(std::string path, Http_Handler cb);

    bool start();
    bool stop();
    void wait();

private:
    void run();
    void break_loop() { event_base_loopbreak(evBase.get()); }
    void handle(struct evhttp_request *req);

public:
    static void http_ok(struct evhttp_request *req, const std::string& msg);
    static void http_error(struct evhttp_request *req, int32_t code, const std::string& msg);

    static std::string parse_uri_path(struct evhttp_request *req);
    static AdminServer::Http_Map parse_headers(struct evhttp_request *req);
    static AdminServer::Http_Map parse_params(struct evhttp_request *req);
    static std::string read_content(struct evhttp_request* req);

private:
    static void generic_handler(struct evhttp_request *, void *);

private:
    uint16_t http_port;
    std::thread loopThread;
    std::unordered_map<std::string, Http_Handler> cbs;
    std::atomic<State> state;
    struct evhttp_bound_socket* evhtpHandler;

private:
    struct EventBaseDeleter { void operator()(event_base* ptr) { event_base_free(ptr); } };
    std::unique_ptr<event_base, EventBaseDeleter> evBase;
    struct EventHtpDeleter { void operator()(evhttp* ptr) { evhttp_free(ptr); } };
    std::unique_ptr<evhttp, EventHtpDeleter> evHtp;
};
