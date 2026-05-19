// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "pb/plan.pb.h"

namespace milvus {

struct RowIdScanBatch {
    std::vector<int64_t> row_ids;
};

class RowIdScanCursor {
 public:
    virtual ~RowIdScanCursor() = default;

    virtual bool
    Next(RowIdScanBatch* out) = 0;
};

enum class ScanValueKind {
    Default,
    FixedWidth,
    StringView,
    JsonView,
    ArrayView,
};

enum class ValueEncoding {
    Empty,
    FixedWidth,
    StringView,
    JsonView,
    ArrayView,
};

enum class ValidityEncoding {
    AllValid,
    BoolArray,
    Bitmap,
};

struct ValueView {
    ValueEncoding encoding = ValueEncoding::Empty;
    ScanValueKind kind = ScanValueKind::Default;
    DataType physical_type = DataType::NONE;
    DataType logical_type = DataType::NONE;
    const void* data = nullptr;
    int64_t offset = 0;
    int64_t size = 0;
    int32_t byte_width = 0;

    bool
    empty() const {
        return encoding == ValueEncoding::Empty || data == nullptr;
    }

    template <typename T>
    const T*
    data_as() const {
        AssertInfo(encoding != ValueEncoding::Empty && data != nullptr,
                   "scan value view is empty");
        return static_cast<const T*>(data) + offset;
    }
};

struct ValidityView {
    ValidityEncoding encoding = ValidityEncoding::AllValid;
    const void* data = nullptr;
    int64_t offset = 0;
    int64_t size = 0;
    bool nullable = false;
    bool all_valid = true;

    bool
    IsValid(int64_t i) const {
        AssertInfo(i >= 0 && (size == 0 || i < size),
                   "validity offset {} out of range {}",
                   i,
                   size);
        if (encoding == ValidityEncoding::AllValid || all_valid ||
            data == nullptr) {
            return true;
        }
        const auto pos = offset + i;
        switch (encoding) {
            case ValidityEncoding::BoolArray:
                return static_cast<const bool*>(data)[pos];
            case ValidityEncoding::Bitmap: {
                const auto* bitmap = static_cast<const uint8_t*>(data);
                return (bitmap[pos >> 3] >> (pos & 0x07)) & 1;
            }
            case ValidityEncoding::AllValid:
                return true;
        }
        return true;
    }
};

struct DataScanBatch {
    ValueView values;
    ValidityView validity;
    std::shared_ptr<void> owner;
    int64_t row_id_start = 0;
    int64_t size = 0;
};

class DataScanCursor {
 public:
    virtual ~DataScanCursor() = default;

    virtual bool
    Next(DataScanBatch* out) = 0;
};

enum class ScanOutput {
    RowIds,
    Data,
};

enum class ScanProjection {
    Data,
    ValidityOnly,
};

enum class ScanPredicate {
    None,
    Unary,
    BinaryRange,
};

struct ScanOptions {
    ScanOutput output = ScanOutput::RowIds;
    ScanPredicate predicate = ScanPredicate::None;
    int64_t start_offset = 0;
    int64_t length = 0;
    int64_t max_batch_rows = 8192;
    ScanProjection projection = ScanProjection::Data;
    ScanValueKind value_kind = ScanValueKind::Default;
    proto::plan::OpType op_type = proto::plan::OpType::Invalid;
    proto::plan::GenericValue value;
    proto::plan::GenericValue lower_value;
    proto::plan::GenericValue upper_value;
    bool lower_inclusive = false;
    bool upper_inclusive = false;
};

struct ScanResult {
    std::unique_ptr<RowIdScanCursor> row_id_cursor;
    std::unique_ptr<DataScanCursor> data_cursor;
};

}  // namespace milvus
