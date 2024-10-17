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

#include <boost/noncopyable.hpp>
#include <unordered_set>

#include <base/types.h>
#include <base/scope_guard.h>
#include <functional>

#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace VectorIndex
{

/// designed for file path lock, and singleton
class TempVIHolder : private boost::noncopyable
{
public:
    static TempVIHolder & instance()
    {
        static TempVIHolder instance;
        return instance;
    }

    scope_guard lockCachePath(const String & path)
    {
        std::lock_guard lock(mutex);
        if (!temp_vi_cache_files.insert(path).second)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "VICachePath {} is already locked", path);
        return [this, path]
        {
            removeCachePath(path);
        };
    }
private:
    TempVIHolder() = default;
    void removeCachePath(const String & path)
    {
        std::lock_guard lock(mutex);
        if (!temp_vi_cache_files.erase(path))
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "VICachePath {} does not exist", path);
    }

    std::mutex mutex;
    std::unordered_set<String> temp_vi_cache_files;

};



}
