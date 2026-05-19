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

#include <algorithm>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

#include "mmap/VortexColumn.h"

namespace milvus {

class VortexRowIdScanCursor final
    : public ChunkedColumnInterface::RowIdScanCursor {
 public:
    explicit VortexRowIdScanCursor(
        std::unique_ptr<VortexColumn::ScanCursor> cursor)
        : cursor_(std::move(cursor)) {
    }

    bool
    Next(ChunkedColumnInterface::RowIdScanBatch* out) override {
        AssertInfo(out != nullptr, "vortex row id scan output batch is null");
        out->row_ids.clear();

        VortexColumn::ScanBatch scan_batch;
        while (cursor_->Next(&scan_batch)) {
            AssertInfo(scan_batch.batch->num_columns() == 1,
                       "vortex scan row id batch expects one column, got {}",
                       scan_batch.batch->num_columns());
            auto row_ids = std::static_pointer_cast<arrow::UInt64Array>(
                scan_batch.batch->column(0));
            out->row_ids.reserve(static_cast<size_t>(row_ids->length()));
            for (int64_t i = 0; i < row_ids->length(); ++i) {
                if (row_ids->IsNull(i)) {
                    continue;
                }
                out->row_ids.emplace_back(
                    scan_batch.row_id_base +
                    static_cast<int64_t>(row_ids->Value(i)));
            }
            if (!out->row_ids.empty()) {
                return true;
            }
        }
        return false;
    }

 private:
    std::unique_ptr<VortexColumn::ScanCursor> cursor_;
};

class VortexDataScanCursor final
    : public ChunkedColumnInterface::DataScanCursor {
 public:
    VortexDataScanCursor(const VortexColumn* column,
                         milvus::OpContext* op_ctx,
                         int64_t start_offset,
                         int64_t length,
                         ChunkedColumnInterface::ScanProjection projection,
                         ChunkedColumnInterface::ScanValueKind value_kind,
                         int64_t max_batch_rows)
        : column_(column),
          op_ctx_(op_ctx),
          projection_(projection),
          value_kind_(value_kind),
          max_batch_rows_(max_batch_rows),
          scan_pos_(start_offset),
          scan_end_(start_offset + length) {
        AssertInfo(max_batch_rows_ > 0, "invalid vortex data scan batch size");
    }

    bool
    Next(ChunkedColumnInterface::DataScanBatch* out) override {
        AssertInfo(out != nullptr, "vortex data scan output batch is null");
        ResetOutput(out);
        if (scan_pos_ >= scan_end_) {
            return false;
        }

        while (scan_pos_ < scan_end_) {
            if (current_batch_ != nullptr &&
                current_batch_pos_ < current_batch_->num_rows()) {
                const auto rows_to_return = std::min<int64_t>(
                    {max_batch_rows_,
                     current_batch_->num_rows() - current_batch_pos_,
                     scan_end_ - scan_pos_});
                FillOutputAndRecord(rows_to_return, out);
                scan_pos_ += rows_to_return;
                current_batch_pos_ += rows_to_return;
                return true;
            }

            current_batch_.reset();
            current_batch_pos_ = 0;
            if (reader_.has_value()) {
                std::shared_ptr<arrow::RecordBatch> batch;
                auto status = reader_->get()->ReadNext(&batch);
                AssertInfo(status.ok(),
                           "failed to read vortex data scan batch: {}",
                           status.ToString());
                if (batch != nullptr) {
                    AssertInfo(batch->num_columns() == 1,
                               "vortex data scan expects one column, got {}",
                               batch->num_columns());
                    current_batch_ = std::move(batch);
                    current_batch_row_id_start_ = next_reader_row_id_;
                    next_reader_row_id_ += current_batch_->num_rows();
                    continue;
                }
                reader_.reset();
                continue;
            }

            OpenReaderForScanPos();
        }
        return false;
    }

 private:
    static void
    ResetOutput(ChunkedColumnInterface::DataScanBatch* out) {
        out->values = ChunkedColumnInterface::ValueView{};
        out->validity = ChunkedColumnInterface::ValidityView{};
        out->owner.reset();
        out->row_id_start = 0;
        out->size = 0;
    }

    void
    OpenReaderForScanPos() {
        auto [chunk_id, local_offset] = column_->GetChunkIDByOffset(scan_pos_);
        const auto chunk_start = column_->GetNumRowsUntilChunk(chunk_id);
        const auto chunk_end = chunk_start + column_->chunk_row_nums(chunk_id);
        const auto local_end =
            std::min<int64_t>(chunk_end, scan_end_) - chunk_start;
        const auto length = local_end - static_cast<int64_t>(local_offset);
        AssertInfo(length >= 0,
                   "invalid vortex data scan chunk range, chunk {}, offset {}, "
                   "end {}",
                   chunk_id,
                   local_offset,
                   local_end);
        if (length == 0) {
            scan_pos_ = chunk_end;
            return;
        }
        reader_ =
            column_->OpenDataScanForFile(op_ctx_,
                                         static_cast<int64_t>(chunk_id),
                                         static_cast<int64_t>(local_offset),
                                         length);
        next_reader_row_id_ = scan_pos_;
    }

    template <typename ArrowArrayT>
    const void*
    RawPrimitiveValues(const std::shared_ptr<arrow::Array>& array) const {
        auto typed = std::static_pointer_cast<ArrowArrayT>(array);
        return typed->raw_values();
    }

    struct BatchOwner {
        std::shared_ptr<arrow::Array> array;
        std::shared_ptr<FixedVector<bool>> bool_values;
        std::shared_ptr<FixedVector<bool>> validity;
        std::vector<std::string_view> string_views;
        std::vector<Json> json_values;
    };

    bool
    IsStringLikeScan() const {
        return value_kind_ ==
                   ChunkedColumnInterface::ScanValueKind::StringView ||
               value_kind_ == ChunkedColumnInterface::ScanValueKind::JsonView;
    }

    void
    FillDataPointer(const std::shared_ptr<arrow::Array>& array,
                    const std::shared_ptr<BatchOwner>& owner,
                    ChunkedColumnInterface::DataScanBatch* out) const {
        out->values.encoding =
            ChunkedColumnInterface::ValueEncoding::FixedWidth;
        out->values.kind =
            value_kind_ == ChunkedColumnInterface::ScanValueKind::Default
                ? ChunkedColumnInterface::ScanValueKind::FixedWidth
                : value_kind_;
        out->values.physical_type = column_->data_type_;
        out->values.logical_type = column_->data_type_;
        out->values.size = out->size;
        switch (column_->data_type_) {
            case DataType::INT8:
                out->values.data = RawPrimitiveValues<arrow::Int8Array>(array);
                out->values.byte_width = sizeof(int8_t);
                break;
            case DataType::INT16:
                out->values.data = RawPrimitiveValues<arrow::Int16Array>(array);
                out->values.byte_width = sizeof(int16_t);
                break;
            case DataType::INT32:
                out->values.data = RawPrimitiveValues<arrow::Int32Array>(array);
                out->values.byte_width = sizeof(int32_t);
                break;
            case DataType::INT64:
            case DataType::TIMESTAMPTZ:
                out->values.data = RawPrimitiveValues<arrow::Int64Array>(array);
                out->values.byte_width = sizeof(int64_t);
                break;
            case DataType::FLOAT:
                out->values.data = RawPrimitiveValues<arrow::FloatArray>(array);
                out->values.byte_width = sizeof(float);
                break;
            case DataType::DOUBLE:
                out->values.data =
                    RawPrimitiveValues<arrow::DoubleArray>(array);
                out->values.byte_width = sizeof(double);
                break;
            case DataType::BOOL: {
                auto typed =
                    std::static_pointer_cast<arrow::BooleanArray>(array);
                owner->bool_values = std::make_shared<FixedVector<bool>>();
                owner->bool_values->resize(array->length());
                for (int64_t i = 0; i < array->length(); ++i) {
                    (*owner->bool_values)[i] = typed->Value(i);
                }
                out->values.data = owner->bool_values->data();
                out->values.byte_width = sizeof(bool);
                break;
            }
            default:
                ThrowInfo(ErrorCode::Unsupported,
                          "unsupported vortex data scan type {}",
                          column_->data_type_);
        }
    }

    void
    FillValidityPointer(const std::shared_ptr<arrow::Array>& array,
                        const std::shared_ptr<BatchOwner>& owner,
                        ChunkedColumnInterface::DataScanBatch* out) const {
        out->validity.nullable = column_->IsNullable();
        out->validity.size = out->size;
        if (!column_->IsNullable() || array->null_count() == 0) {
            out->validity.encoding =
                ChunkedColumnInterface::ValidityEncoding::AllValid;
            out->validity.all_valid = true;
            return;
        }
        owner->validity = std::make_shared<FixedVector<bool>>();
        owner->validity->resize(array->length());
        for (int64_t i = 0; i < array->length(); ++i) {
            (*owner->validity)[i] = array->IsValid(i);
        }
        out->validity.encoding =
            ChunkedColumnInterface::ValidityEncoding::BoolArray;
        out->validity.data = owner->validity->data();
        out->validity.offset = current_batch_pos_;
        out->validity.all_valid = false;
    }

    void
    FillStringLikeOutput(const std::shared_ptr<arrow::Array>& array,
                         const std::shared_ptr<BatchOwner>& owner,
                         ChunkedColumnInterface::DataScanBatch* out) const {
        out->validity.nullable = column_->IsNullable();
        out->validity.size = out->size;

        if (projection_ ==
            ChunkedColumnInterface::ScanProjection::ValidityOnly) {
            FillValidityPointer(array, owner, out);
            return;
        }

        VortexColumn::ArrowStringLikeColumn string_column(array);
        const bool emit_valid =
            column_->field_meta_.is_nullable() && array->null_count() > 0;
        auto views = column_->BuildStringViewsFromArrow(
            string_column,
            std::make_pair(current_batch_pos_, out->size),
            emit_valid);

        out->values.physical_type = column_->data_type_;
        out->values.logical_type = column_->data_type_;
        out->values.offset = 0;
        out->values.size = out->size;

        if (value_kind_ == ChunkedColumnInterface::ScanValueKind::StringView) {
            owner->string_views = std::move(views.first);
            out->values.encoding =
                ChunkedColumnInterface::ValueEncoding::StringView;
            out->values.kind =
                ChunkedColumnInterface::ScanValueKind::StringView;
            out->values.data = owner->string_views.data();
            out->values.byte_width = sizeof(std::string_view);
        } else {
            owner->string_views = std::move(views.first);
            owner->json_values.reserve(owner->string_views.size());
            for (const auto& value : owner->string_views) {
                owner->json_values.emplace_back(Json(value));
            }
            out->values.encoding =
                ChunkedColumnInterface::ValueEncoding::JsonView;
            out->values.kind = ChunkedColumnInterface::ScanValueKind::JsonView;
            out->values.logical_type = DataType::JSON;
            out->values.data = owner->json_values.data();
            out->values.byte_width = sizeof(Json);
        }

        if (emit_valid) {
            owner->validity =
                std::make_shared<FixedVector<bool>>(std::move(views.second));
            out->validity.encoding =
                ChunkedColumnInterface::ValidityEncoding::BoolArray;
            out->validity.data = owner->validity->data();
            out->validity.offset = 0;
            out->validity.all_valid = false;
        } else {
            out->validity.encoding =
                ChunkedColumnInterface::ValidityEncoding::AllValid;
            out->validity.all_valid = true;
        }
    }

    void
    FillOutputFromCurrentBatch(
        int64_t rows_to_return,
        ChunkedColumnInterface::DataScanBatch* out) const {
        auto array = current_batch_->column(0);
        auto owner = std::make_shared<BatchOwner>();
        owner->array = array;
        out->row_id_start = current_batch_row_id_start_ + current_batch_pos_;
        out->size = rows_to_return;
        if (IsStringLikeScan()) {
            FillStringLikeOutput(array, owner, out);
        } else {
            if (projection_ !=
                ChunkedColumnInterface::ScanProjection::ValidityOnly) {
                FillDataPointer(array, owner, out);
                out->values.offset = current_batch_pos_;
            }
            FillValidityPointer(array, owner, out);
        }
        out->owner = std::move(owner);
    }

    void
    FillOutputAndRecord(int64_t rows_to_return,
                        ChunkedColumnInterface::DataScanBatch* out) {
        FillOutputFromCurrentBatch(rows_to_return, out);
    }

    const VortexColumn* column_;
    milvus::OpContext* op_ctx_;
    ChunkedColumnInterface::ScanProjection projection_;
    ChunkedColumnInterface::ScanValueKind value_kind_;
    int64_t max_batch_rows_;
    int64_t scan_pos_;
    int64_t scan_end_;
    std::optional<PinWrapper<std::shared_ptr<arrow::RecordBatchReader>>>
        reader_;
    int64_t next_reader_row_id_{0};
    std::shared_ptr<arrow::RecordBatch> current_batch_;
    int64_t current_batch_pos_{0};
    int64_t current_batch_row_id_start_{0};
};

}  // namespace milvus
