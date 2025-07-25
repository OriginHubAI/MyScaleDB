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

#include <Storages/MergeTree/ReplicatedMergeTreeQueue.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/logger_useful.h>

#include <VectorIndex/Common/VectorIndicesMgr.h>
#include <VectorIndex/Storages/VITaskBase.h>

namespace DB
{

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class StorageReplicatedMergeTree;

class ReplicatedVITask : public VITaskBase
{
public:
    template <class Callback>
    ReplicatedVITask(
        StorageReplicatedMergeTree & storage_,
        ReplicatedMergeTreeQueue::SelectedEntryPtr & selected_entry_,
        VectorIndicesMgr & builder_,
        Callback && task_result_callback_)
        : VITaskBase(
            storage_,
            builder_,
            task_result_callback_,
            selected_entry_->log_entry->source_parts.at(0),
            selected_entry_->log_entry->index_name,
            selected_entry_->log_entry->slow_mode)
        , selected_entry(selected_entry_)
        , entry(*selected_entry->log_entry)
    {
        log = getLogger("ReplicatedVITask");
    }

    ~ReplicatedVITask() override;

private:
    /// result, need_to_fetch
    VectorIndex::SegmentBuiltStatus prepare() override;

    void remove_processed_entry() override;

    ReplicatedMergeTreeQueue::SelectedEntryPtr selected_entry;
    ReplicatedMergeTreeLogEntry & entry;

    String replica_to_fetch;
};

}
