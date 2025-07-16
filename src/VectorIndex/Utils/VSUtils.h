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
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <VectorIndex/Storages/MergeTreeBaseSearchManager.h>
#include <VectorIndex/Common/VICommon.h>

namespace DB
{

/// if we has precompute search result, use it to filter mark ranges
void filterMarkRangesByVectorScanResult(MergeTreeData::DataPartPtr part, MergeTreeBaseSearchManagerPtr base_search_mgr, MarkRanges & mark_ranges);

/// if we has precompute search result, use it to filter mark ranges
void filterMarkRangesBySearchResult(MergeTreeData::DataPartPtr part, const Settings & settings, CommonSearchResultPtr common_search_result, MarkRanges & mark_ranges);

/// if we has labels from precompute search result, use it to filter mark ranges
void filterMarkRangesByLabels(MergeTreeData::DataPartPtr part, const Settings & settings, const std::set<UInt64> labels, MarkRanges & mark_ranges);

/// get topk from limit clause
UInt64 getTopKFromLimit(const ASTSelectQuery * select_query, ContextPtr context, bool is_batch = false);

VectorIndex::VIMetric getVSMetric(MergeTreeData::DataPartPtr part, const VSDescription & desc);

}
