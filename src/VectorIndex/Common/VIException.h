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

namespace VectorIndex
{
class VIException : public DB::Exception
{
public:
    VIException(int code, const std::string & message) : DB::Exception(code, "VectorIndex: {}", message) { }

    // Format message with fmt::format, like the logging functions.
    template <typename... Args>
    VIException(int code, const std::string & fmt, Args &&... args)
        : DB::Exception(code, fmt::runtime("VectorIndex: " + fmt), std::forward<Args>(args)...)
    {
    }
};
};
