/************************************************************************
Modifications Copyright 2020 ~ 2021
Author: zhanglei
Email: shanshenshi@126.com

Original Copyright:
See URL: https://github.com/datatechnology/cornerstone
See URL: https://github.com/eBay/NuRaft

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

#include <atomic>
#include <condition_variable>
#include <mutex>

class EAwaiter {
private:
    enum class AwStatus {
        idle    = 0x0,
        ready   = 0x1,
        waiting = 0x2,
        done    = 0x3
    };

public:
    EAwaiter() : status(AwStatus::idle) { }

    void reset() { status.store(AwStatus::idle); }

    void wait() { wait_us(0); }

    void wait_ms(size_t time_ms) { wait_us(time_ms * 1000); }

    void wait_us(size_t time_us)
    {
        AwStatus expected = AwStatus::idle;
        if (status.compare_exchange_strong(expected, AwStatus::ready)) {
            // invoke() has not been invoked yet, wait for it.
            std::unique_lock<std::mutex> l(cvLock);
            expected = AwStatus::ready;
            if (status.compare_exchange_strong(expected, AwStatus::waiting)) {
                if (time_us) {
                    cv.wait_for(l, std::chrono::microseconds(time_us));
                } else {
                    cv.wait(l);
                }
                status.store(AwStatus::done);
            } else {
                // invoke() has grabbed `cvLock` earlier than this.
            }
        } else {
            // invoke() already has been called earlier than this.
        }
    }

    void invoke()
    {
        AwStatus expected = AwStatus::idle;
        if (status.compare_exchange_strong(expected, AwStatus::done)) {
            // wait() has not been invoked yet, do nothing.
            return;
        }

        std::unique_lock<std::mutex> l(cvLock);
        expected = AwStatus::ready;
        if (status.compare_exchange_strong(expected, AwStatus::done)) {
            // wait() has been called earlier than invoke(),
            // but invoke() has grabbed `cvLock` earlier than wait().
            // Do nothing.
        } else {
            // wait() is waiting for ack.
            cv.notify_all();
        }
    }

private:
    std::atomic<AwStatus> status;
    std::mutex cvLock;
    std::condition_variable cv;
};


