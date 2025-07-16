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

#include <IO/Operators.h>
#include <Parsers/ASTFunction.h>
#include <Common/quoteString.h>

#include <VectorIndex/Parsers/ASTVIDeclaration.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ASTVIDeclaration::ASTVIDeclaration(ASTPtr type, const String & name_, const String & column_)
: name(name_), column(column_)
{
    if (type)
    {
        if (!dynamic_cast<const ASTFunction *>(type.get()))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Vector index declaration type must be a function");
        children.push_back(type);
    }
}

ASTPtr ASTVIDeclaration::clone() const
{
    ASTPtr type = getType();
    if (type)
        type = type->clone();

    auto res = std::make_shared<ASTVIDeclaration>(type, name, column);
    res->part_of_create_index_query = part_of_create_index_query;

    return res;
}

std::shared_ptr<ASTFunction> ASTVIDeclaration::getType() const
{
    if (children.size() <= type_idx)
        return nullptr;
    auto func_ast = std::dynamic_pointer_cast<ASTFunction>(children[type_idx]);
    if (!func_ast)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Vector index declaration type must be a function");
    return func_ast;
}

void ASTVIDeclaration::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    if (!part_of_create_index_query)
        s.ostr << backQuoteIfNeed(name);
    s.ostr << " ";
    s.ostr << backQuoteIfNeed(column);

    if (auto type = getType())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << " TYPE " << (s.hilite ? hilite_none : "");
        type->formatImpl(s, state, frame);
    }
}

}
