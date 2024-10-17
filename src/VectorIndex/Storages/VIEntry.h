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

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <Common/logger_useful.h>

#include <VectorIndex/Interpreters/VIEventLog.h>

namespace VectorIndex
{
class BaseSegment;
using SegmentPtr = std::shared_ptr<BaseSegment>;
}

namespace DB
{

struct VIDescription;

struct VIContext
{
    StorageMetadataPtr metadata_snapshot;
    VIDescription vec_index_desc{VIDescription()};
    MergeTreeData::DataPartPtr source_part;
    VectorIndex::SegmentStatusPtr vi_status;
    VectorIndex::CachedSegmentPtr build_index;
    ActionBlocker * builds_blocker;
    String part_name;
    String vector_index_name;
    String vector_tmp_full_path;
    String vector_tmp_relative_path;
    std::shared_ptr<MergeTreeDataPartChecksums> vector_index_checksum;

    LoggerPtr log{nullptr};

    Stopwatch watch = Stopwatch();

    scope_guard temporary_directory_lock;

    VectorIndex::IndexBuildMemoryUsageHelperPtr index_build_memory_lock;

    std::function<void(const String &)> clean_tmp_folder = {};

    std::function<bool()> build_cancel_callback = {};

    std::function<void(VIEventLogElement::Type, int, const String &)> write_event_log = {};

    bool slow_mode{false};

    const int maxBuildRetryCount = 3;

    int failed_count{0};
};

struct VIEntry
{
    String part_name;
    String vector_index_name;
    MergeTreeData & data;
    scope_guard temporary_vi_part_holder;
    bool is_replicated; /// no use now
    LoggerPtr log = getLogger("VectorIndexEntry");

    VIEntry(const String part_name_, const String & index_name_, MergeTreeData & data_, scope_guard && temporary_vi_part_holder_ = {}, const bool is_replicated_ = false)
     : part_name(std::move(part_name_))
     , vector_index_name(index_name_)
     , data(data_)
     , temporary_vi_part_holder(std::move(temporary_vi_part_holder_))
     , is_replicated(is_replicated_)
    {
    }

    ~VIEntry()
    {
    }
};

using VIEntryPtr = std::shared_ptr<VIEntry>;

}
