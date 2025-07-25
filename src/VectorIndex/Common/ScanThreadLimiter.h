/*
 * Copyright (2024) ORIGINHUB SINGAPORE PTE. LTD. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <shared_mutex>
#include <condition_variable>
#include <atomic>
#include <mutex>

#include <Common/logger_useful.h>
#include <Common/OpenTelemetryTraceContext.h>

namespace VectorIndex
{
class LimiterSharedContext {
public:
    std::shared_mutex mutex;
    std::condition_variable_any cv;
    std::atomic_int count{0};
    int max_threads;

    LimiterSharedContext(int max_threads_) : max_threads(max_threads_) { }
};

class ScanThreadLimiter
{
private:
    LimiterSharedContext& context;
    const LoggerPtr log;

public:
    ScanThreadLimiter(LimiterSharedContext& context_, const LoggerPtr log_)
    : context(context_), log(log_)
    {
        DB::OpenTelemetry::SpanHolder span_search("VectorExecutor::performSearch()::search::ScanThreadLimiter");
        std::shared_lock<std::shared_mutex> lock(context.mutex);
        context.cv.wait(lock, [&] { return context.count.load(std::memory_order_acquire) < context.max_threads; });
        context.count.fetch_add(1, std::memory_order_acquire);
        LOG_DEBUG(log, "Uses {}/{} threads", context.count.load(std::memory_order_relaxed), context.max_threads);
    }

    ~ScanThreadLimiter()
    {
        context.count.fetch_sub(1, std::memory_order_release);
        context.cv.notify_one();
    }
};
}
