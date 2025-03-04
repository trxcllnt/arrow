// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstdint>
#include <limits>
#include <memory>

#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/type_fwd.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "arrow/util/span.h"

namespace arrow::compute::internal {

using PivotWiderKeyIndex = uint8_t;

constexpr PivotWiderKeyIndex kNullPivotKey =
    std::numeric_limits<PivotWiderKeyIndex>::max();
constexpr PivotWiderKeyIndex kMaxPivotKey = kNullPivotKey - 1;

struct PivotWiderKeyMapper {
  virtual ~PivotWiderKeyMapper() = default;

  virtual Status Init(const PivotWiderOptions* options) = 0;
  virtual Result<::arrow::util::span<const PivotWiderKeyIndex>> MapKeys(
      const ArraySpan&) = 0;
  virtual Result<PivotWiderKeyIndex> MapKey(const Scalar&) = 0;

  static Result<std::unique_ptr<PivotWiderKeyMapper>> Make(
      const DataType& key_type, const PivotWiderOptions* options);
};

}  // namespace arrow::compute::internal
