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

#include <base/types.h>
#include <vector>
#include "config.h"

#if USE_TANTIVY_SEARCH
#    include <tantivy_search.h>
#endif

namespace DB
{

#if USE_TANTIVY_SEARCH

using RustVecDocWithFreq = rust::cxxbridge1::Vec<TANTIVY::DocWithFreq>;

/// Support tantivy index on multiple text columns
using VecTextColumnTokenNums = rust::cxxbridge1::Vec<TANTIVY::FieldTokenNums>;

struct BM25InfoInDataPart
{
    UInt64 total_docs; /// Total number of documents in a data part   
    VecTextColumnTokenNums text_cols_total_num_tokens;  /// Total number of tokens from all documents on all text columns in a data part
    RustVecDocWithFreq term_with_doc_nums;  /// vector of terms with number of documents containing it

    BM25InfoInDataPart() = default;

    BM25InfoInDataPart(
        const UInt64 & total_docs_,
        const VecTextColumnTokenNums & text_cols_total_num_tokens_,
        const RustVecDocWithFreq & term_with_doc_nums_)
        : total_docs{total_docs_}
        , text_cols_total_num_tokens{text_cols_total_num_tokens_}
        , term_with_doc_nums{term_with_doc_nums_}
    {}

    UInt64 getTotalDocsCount() const;
    VecTextColumnTokenNums getTextColsTotalNumTokens() const;
    const RustVecDocWithFreq & getTermWithDocNums() const;
};

struct BM25InfoInDataParts: public std::vector<BM25InfoInDataPart>
{
    using std::vector<BM25InfoInDataPart>::vector;

    UInt64 getTotalDocsCountAllParts() const;
    VecTextColumnTokenNums getTextColsTotalNumTokensAllParts() const;
    RustVecDocWithFreq getTermWithDocNumsAllParts() const;
};

#endif
}
