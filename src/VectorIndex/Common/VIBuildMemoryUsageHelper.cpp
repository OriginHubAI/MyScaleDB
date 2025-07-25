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

#include <mutex>

#include <VectorIndex/Cache/VICacheManager.h>
#include <VectorIndex/Common/VIBuildMemoryUsageHelper.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int VECTOR_INDEX_BUILD_MEMORY_TOO_LARGE;
    extern const int VECTOR_INDEX_BUILD_MEMORY_INSUFFICIENT;
}
}

namespace VectorIndex
{

std::mutex VIBuildMemoryUsageHelper::build_memory_mutex;
size_t VIBuildMemoryUsageHelper::build_memory_size_limit = 0;
size_t VIBuildMemoryUsageHelper::current_build_memory_size = 0;

void VIBuildMemoryUsageHelper::setCacheManagerSizeInBytes(size_t size)
{
    VICacheManager::setCacheSize(size);
}

void VIBuildMemoryUsageHelper::setBuildMemorySizeInBytes(size_t size)
{
    std::lock_guard lock(build_memory_mutex);
    build_memory_size_limit = size;
}

BuildMemoryCheckResult VIBuildMemoryUsageHelper::checkBuildMemorySize(size_t size)
{
    std::lock_guard lock(build_memory_mutex);

    if (build_memory_size_limit == 0)
        return BuildMemoryCheckResult::OK;
    else if (size > build_memory_size_limit)
        return BuildMemoryCheckResult::NEVER;
    else if (current_build_memory_size + size > build_memory_size_limit)
        return BuildMemoryCheckResult::LATER;

    current_build_memory_size += size;
    LOG_DEBUG(
        getLogger("VIBuildMemoryUsageHelper"), "allow building: size = {}, current_total = {}", size, current_build_memory_size);
    return BuildMemoryCheckResult::OK;
}

void VIBuildMemoryUsageHelper::checkBuildMemory(size_t size)
{
    Stopwatch stopwatch;
    while (true)
    {
        auto res = checkBuildMemorySize(size);
        switch (res)
        {
            case BuildMemoryCheckResult::OK:
                /// record reserved build memory size. will be decreased in deconstructor
                {
                    std::lock_guard lock(build_memory_mutex);
                    build_memory_size_recorded += size;
                }
                return;

            case BuildMemoryCheckResult::NEVER:
                throw VIException(
                    DB::ErrorCodes::VECTOR_INDEX_BUILD_MEMORY_TOO_LARGE, "cannot build vector index, build memory required is too large");

            case BuildMemoryCheckResult::LATER:
                if (stopwatch.elapsedSeconds() > 5 * 60) /// 5 miniutes
                    throw VIException(
                        DB::ErrorCodes::VECTOR_INDEX_BUILD_MEMORY_INSUFFICIENT,
                        "cannot build vector index for now due to build memory limitation");
                else /// currently unable to build index, sleep and retry
                    std::this_thread::sleep_for(std::chrono::seconds(10));
        }
    }
}
}
