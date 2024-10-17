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

#include <functional>
#include <list>
#include <memory>
#include <string>

#include <Common/LRUResourceCache.h>
#include <VectorIndex/Cache/CachedSegmentKey.h>
#include <VectorIndex/Cache/CachedSegment.h>
#include <VectorIndex/Storages/VIDescriptions.h>

namespace std
{
template <>
struct hash<VectorIndex::CachedSegmentKey>
{
    std::size_t operator()(VectorIndex::CachedSegmentKey const & key) const noexcept { return std::hash<std::string>{}(key.toString()); }
};
}

namespace VectorIndex
{

static bool m = false;
static size_t cache_size_in_bytes = 0;

class CachedSegmentWeightFunc
{
public:
    size_t operator()(const CachedSegment & cached_segment) const;
};

class CachedSegmentReleaseFunction
{
public:
    void operator()(std::shared_ptr<CachedSegment> cached_segment_ptr);
};

using VectorIndexCache
    = DB::LRUResourceCache<CachedSegmentKey, CachedSegment, CachedSegmentWeightFunc, CachedSegmentReleaseFunction, std::hash<CachedSegmentKey>>;
using CachedSegmentHolderPtr = VectorIndexCache::MappedHolderPtr;

class VICacheManager
{
    // cache manager manages a series of cache instance.
    // these caches could either be cache in memory or cache on GPU device.
    // it privides a getInstance() method which returns a consistent view
    // of all caches to all classes trying to access cache.

private:
    explicit VICacheManager(int);

    void forceExpire(const CachedSegmentKey & cache_key);

    size_t countItem() const;

    /// don't allow direct access to cache
    static std::unique_ptr<VectorIndexCache> cache;
    LoggerPtr log;

public:
    /// put a new item into cache
    void put(const CachedSegmentKey & cache_key, CachedSegmentPtr index);
    /// get an item from cache
    CachedSegmentHolderPtr get(const CachedSegmentKey & cache_key);
    /// load an item from cache
    CachedSegmentHolderPtr load(const CachedSegmentKey & cache_key,
                                std::function<CachedSegmentPtr()> load_func);

    static VICacheManager * getInstance();
    static std::list<std::pair<CachedSegmentKey, std::shared_ptr<DB::VIDescription>>> getAllItems(bool exclude_expired = false);
    static void setCacheSize(size_t size_in_bytes);

    static bool storedInCache(const CachedSegmentKey & cache_key);

    static void removeFromCache(const CachedSegmentKey & cache_key);
};

}
