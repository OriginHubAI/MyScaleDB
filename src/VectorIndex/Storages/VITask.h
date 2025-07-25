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

#include <Core/Names.h>

#include <VectorIndex/Common/VectorIndicesMgr.h>
#include <VectorIndex/Storages/VITaskBase.h>

#include <Common/logger_useful.h>

namespace DB
{

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class MergeTreeData;

class VITask : public VITaskBase
{
public:
    template <class Callback>
    VITask(
        MergeTreeData & storage_,
        StorageMetadataPtr /*metadata_snapshot_*/,
        VIEntryPtr vector_index_entry_,
        VectorIndicesMgr & builder_,
        Callback && task_result_callback_,
        bool slow_mode_)
        : VITaskBase(
            storage_, builder_, task_result_callback_, vector_index_entry_->part_name, vector_index_entry_->vector_index_name, slow_mode_)
        , vector_index_entry(std::move(vector_index_entry_))
    {
        log = getLogger("VITask");
        LOG_DEBUG(log, "Create VITask for {}, slow mode: {}", vector_index_entry->part_name, slow_mode);
    }

    ~VITask() override;

private:
    VIEntryPtr vector_index_entry;

    void recordBuildStatus();

    VectorIndex::SegmentBuiltStatus prepare() override;
};
}
