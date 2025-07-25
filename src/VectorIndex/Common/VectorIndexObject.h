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

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <VectorIndex/Common/VICommands.h>
#include <VectorIndex/Storages/VIDescriptions.h>


namespace DB
{
class VectorIndexObject
{
public:
    VectorIndexObject(MergeTreeData & storage_, const VIDescription & vec_desc_, const bool is_replica_)
    : storage(storage_)
    , vec_desc(vec_desc_)
    , is_replica(is_replica_)
    {}

    ~VectorIndexObject() = default;
    
    /// only call in alter table add index, will add segment for all parts,
    /// if table is replicated, will create zookeeper node for vector index build status
    void init() const;
    /// only call in alter table drop index, will remove segment for all parts,
    /// if table is replicated, will remove zookeeper node for vector index build status
    void drop();

    void waitBuildFinish();

    /// Similar as MergeTreeMutationStatus. For the system table vector_indices.
    struct MergeTreeVectorIndexStatus
    {
        String latest_failed_part;
        DB::MergeTreePartInfo latest_failed_part_info;
        String latest_fail_reason;
    };

    /// Get the status of the vector index build for the part.
    MergeTreeVectorIndexStatus getVectorIndexBuildStatus() const;
protected:
    MergeTreeData & storage;
    const VIDescription vec_desc;
    const bool is_replica;
    std::atomic<bool> is_dropped{false};
    mutable std::once_flag init_flag;
};

using VectorIndexObjectPtr
    = std::shared_ptr<VectorIndexObject>;
using VectorIndexObjectMap = std::unordered_map<String, VectorIndexObjectPtr>;


} // namespace VectorIndex
