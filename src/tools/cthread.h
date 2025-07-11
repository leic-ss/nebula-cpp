/************************************************************************
Copyright 2020 ~ 2021
Author: zhanglei
Email: shanshenshi@126.com

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
 
    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#pragma once

#include <thread>
#include <vector>
#include <list>
#include <map>
#include <mutex>
#include <iostream>
#include <functional>
#include <atomic>

#include <event2/event.h>

namespace rpc
{

template<class T>
class Attr
{
public:
    Attr() {
        pthread_key_create(&k, NULL);
    }
    ~Attr() {
        pthread_key_delete(k);
    }
    void set(const T * t) {
        pthread_setspecific(k, (const void *)t);
    }
    T * get() const {
        return (T *)pthread_getspecific(k);
    }
    T * operator -> () {
        return get();
    }
    T & operator * () {
        return *get();
    }
    T * operator = (T * t) {
        set(t);
        return t;
    }

private:
    pthread_key_t k;
};

class CThread
{
public:
    class SpinLock {
        public:
            SpinLock() : flag(false)
            {}

            void lock() {
                bool expect = false;
                while (!flag.compare_exchange_strong(expect, true)) {
                    expect = false;
                }
            }
            void unlock() { flag.store(false); }

        private:
            std::atomic<bool> flag;
    };

public:
    using callback = std::function<void (CThread *)>;
    enum Status {
        IDLE = ' ',
        WAKE = '1',
        EXIT = '0',
    };
    enum Schedule : uint8_t {
        STACK = 0,
        DEFER = 1,
    };

private:
    evutil_socket_t fd_r;
    evutil_socket_t fd_w;

private:
    event * e;

public:
    event_base * eb;

private:
    SpinLock spin;

private:
    volatile Status status;
    
private:
    std::thread* thr{nullptr};

public:
    std::string name;

public:
    uint64_t tid;

private:
    std::list<callback> callbacks;

public:
    CThread(const std::string&, event_base*);
    CThread(const std::string&, callback = nullptr);

public:
   ~CThread(void);

public:
    void sync    (const callback &);
    bool dispatch(const callback &, Schedule = Schedule::STACK);

private:
    void wake(void);
    void init(void);

public:
    void stop(void);

public:
    static CThread * self(void);

public:
    std::string format(void);

public:
    friend std::ostream & operator << (std::ostream &, CThread &);
};

} // end of naemspace rpc
