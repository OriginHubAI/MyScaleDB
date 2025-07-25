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

#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>

namespace DB
{
class ASTFunction;

/** name BY expr TYPE typename(args) in create query
 * expr TYPE typename(args) in create vector index query
  */
class ASTVIDeclaration : public IAST
{
public:
    ASTVIDeclaration(ASTPtr type, const String & name_, const String & column_);

    String name;
    String column;
    bool part_of_create_index_query = false;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "VectorIndex"; }

    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    std::shared_ptr<ASTFunction> getType() const;

private:
  static constexpr size_t type_idx = 0;
};

}
