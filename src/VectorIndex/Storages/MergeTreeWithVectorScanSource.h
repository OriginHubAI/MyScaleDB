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
#include <Processors/ISource.h>

namespace DB
{

class  MergeTreeSelectWithHybridSearchProcessor;
using MergeTreeSelectWithHybridSearchProcessorPtr = std::unique_ptr< MergeTreeSelectWithHybridSearchProcessor>;

struct ChunkAndProgress;

/// Reference from MergeTreeSource, without async read in linux
class MergeTreeWithVectorScanSource final : public ISource
{
public:
    explicit MergeTreeWithVectorScanSource(MergeTreeSelectWithHybridSearchProcessorPtr processor_, const std::string & log_name_);
    ~MergeTreeWithVectorScanSource() override;

    std::string getName() const override;

    Status prepare() override;

protected:
    std::optional<Chunk> tryGenerate() override;

    void onCancel() noexcept override;

private:
     MergeTreeSelectWithHybridSearchProcessorPtr processor;
    const std::string log_name;

    Chunk processReadResult(ChunkAndProgress chunk);
};

/// Create stream for reading parts from MergeTree.
Pipe createReadInOrderFromPartsSource(
    const RangesInDataParts & parts_with_ranges,
    Names columns_to_read,
    const StorageSnapshotPtr & storage_snapshot,
    String log_name,
    const PrewhereInfoPtr & prewhere_info,
    const ExpressionActionsSettings & actions_settings,
    const MergeTreeReaderSettings & reader_settings,
    const MergeTreeReadTask::BlockSizeParams & block_size_params,
    ContextPtr context,
    size_t max_streams,
    size_t min_marks_for_concurrent_read,
    bool use_uncompressed_cache);

/// Create stream for parallel reading single part from MergeTree.
Pipe createReadFromPoolWithFilterFromPartSource(
    RangesInDataParts parts_with_ranges,
    Names columns_to_read,
    VectorIndex::VIBitmapPtr filter,
    const StorageSnapshotPtr & storage_snapshot,
    String log_name,
    const PrewhereInfoPtr & prewhere_info,
    const ExpressionActionsSettings & actions_settings,
    const MergeTreeReaderSettings & reader_settings,
    const MergeTreeReadTask::BlockSizeParams & block_size_params,
    ContextPtr context,
    size_t max_streams,
    size_t min_marks_for_concurrent_read);
}
