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

#include <map>
#include <mutex>
#include <functional>
#include <optional>

#include <Columns/IColumn.h>
#include <Common/CacheBase.h>
#include <Storages/KeyDescription.h>

namespace DB
{


class ColumnsWeightFunc
{
public:
    size_t operator()(const Columns & cols) const
    {
        size_t total_size = 0;
        for (auto & column : cols)
        {
            total_size += column->byteSize();
        }
        return total_size;
    }
};


class PKCacheManager
{
public:
    void setPartPkCache(String cache_key, Columns columns);
    std::optional<Columns> getPartPkCache(String cache_key);
    void removeFromPKCache(const String & cache_key);

    /// tools
    static bool isSupportedPrimaryKey(const KeyDescription & kd);


private:
    CacheBase<String, Columns, std::hash<String>, ColumnsWeightFunc> cache_ex;
    LoggerPtr log;

    explicit PKCacheManager(size_t max_size);
    ~PKCacheManager() = default;

public:
    static PKCacheManager & getMgr();

/// no copy
public:
    PKCacheManager(const PKCacheManager &) = delete;
    PKCacheManager & operator=(const PKCacheManager &) = delete;
};

}
