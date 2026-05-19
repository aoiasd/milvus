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
#include <cstdint>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/array_binary.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "cachinglayer/CacheSlot.h"
#include "cachinglayer/Manager.h"
#include "cachinglayer/Utils.h"
#include "common/ChunkWriter.h"
#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/Json.h"
#include "common/Span.h"
#include "common/Types.h"
#include "common/bson_view.h"
#include "mmap/ChunkedColumn.h"
#include "mmap/ChunkedColumnInterface.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/format/vortex/vortex_footer_reader.h"
#include "milvus-storage/format/vortex/vortex_format_reader.h"
#include "pb/plan.pb.h"
#include "storage/Util.h"

namespace milvus {

class VortexDataScanCursor;

class VortexColumn final : public ChunkedColumnInterface {
    friend class VortexDataScanCursor;

 public:
    struct FileInfo {
        std::string path;
        int64_t start_index = 0;
        int64_t end_index = 0;
        uint64_t file_size = 0;
        uint64_t footer_size = 0;
    };

    VortexColumn(FieldId field_id,
                 FieldMeta field_meta,
                 std::vector<FileInfo> files,
                 std::shared_ptr<arrow::Schema> arrow_schema,
                 std::shared_ptr<milvus_storage::api::Properties> properties,
                 CacheWarmupPolicy cache_warmup_policy,
                 milvus::OpContext* op_ctx)
        : field_id_(field_id),
          field_meta_(std::move(field_meta)),
          data_type_(field_meta_.get_data_type()),
          field_name_(field_meta_.is_external_field()
                          ? field_meta_.get_external_field()
                          : std::to_string(field_id_.get())) {
        AssertInfo(!IsVectorDataType(data_type_),
                   "vortex local_format does not support vector field {}",
                   field_id_.get());
        AssertInfo(!files.empty(),
                   "vortex column field {} has no files",
                   field_id_.get());
        AssertInfo(properties != nullptr, "vortex properties is null");
        auto local_format_properties = *properties;
        local_format_properties[PROPERTY_READER_VORTEX_SPLIT_ROW_INDICES] =
            true;
        files_.reserve(files.size());
        num_rows_until_chunk_.reserve(files.size() + 1);
        num_rows_until_chunk_.push_back(0);
        int64_t row_prefix = 0;

        for (auto& file : files) {
            auto fs_result = milvus_storage::FilesystemCache::getInstance().get(
                *properties, file.path);
            AssertInfo(fs_result.ok(),
                       "failed to get filesystem for vortex file {}: {}",
                       file.path,
                       fs_result.status().ToString());
            auto uri_result = milvus_storage::StorageUri::Parse(file.path);
            AssertInfo(uri_result.ok(),
                       "failed to parse vortex file uri {}: {}",
                       file.path,
                       uri_result.status().ToString());
            auto resolved_path = uri_result.ValueOrDie().key;

            auto footer_reader =
                std::make_shared<milvus_storage::vortex::VortexFooterReader>(
                    fs_result.ValueOrDie(),
                    arrow_schema,
                    resolved_path,
                    *properties,
                    file.file_size,
                    file.footer_size);
            auto open_status = footer_reader->Open();
            AssertInfo(open_status.ok(),
                       "failed to open vortex file {}: {}",
                       file.path,
                       open_status.ToString());
            auto projected_arrow_schema =
                MakeProjectedArrowSchema(footer_reader->file_schema(),
                                         field_name_);

            auto translater_result =
                footer_reader->MakeTranslater(field_name_, cache_warmup_policy);
            AssertInfo(translater_result.ok(),
                       "failed to create vortex translater for field {} file "
                       "{}: {}",
                       field_id_.get(),
                       file.path,
                       translater_result.status().ToString());
            std::unique_ptr<cachinglayer::Translator<
                milvus_storage::vortex::VortexCellGuard>>
                translater = std::move(translater_result).ValueOrDie();
            auto slot = cachinglayer::Manager::GetInstance().CreateCacheSlot(
                std::move(translater), op_ctx);

            auto data_reader =
                std::make_shared<milvus_storage::vortex::VortexFormatReader>(
                    footer_reader->local_filesystem(),
                    projected_arrow_schema,
                    footer_reader->local_path(),
                    local_format_properties,
                    std::vector<std::string>{field_name_},
                    footer_reader->file_size(),
                    footer_reader->footer_size());
            auto data_open_status = data_reader->open();
            AssertInfo(data_open_status.ok(),
                       "failed to open vortex data reader for file {}: {}",
                       file.path,
                       data_open_status.ToString());

            FileState state;
            state.path = std::move(file.path);
            state.footer_reader = std::move(footer_reader);
            state.reader = std::move(data_reader);
            state.slot = std::move(slot);
            state.rows = static_cast<int64_t>(state.footer_reader->rows());
            if (file.end_index > file.start_index) {
                AssertInfo(file.end_index - file.start_index == state.rows,
                           "vortex file {} row range [{}, {}) does not match "
                           "reader rows {}",
                           state.path,
                           file.start_index,
                           file.end_index,
                           state.rows);
            }
            state.memory_bytes = 0;
            auto cell_metas_result =
                state.footer_reader->GetFieldCellMetas(field_name_);
            AssertInfo(
                cell_metas_result.ok(),
                "failed to get vortex cell metas for field {} file {}: {}",
                field_id_.get(),
                state.path,
                cell_metas_result.status().ToString());
            for (const auto& meta : cell_metas_result.ValueOrDie()) {
                state.cell_ids.emplace_back(meta.cell_id);
                state.memory_bytes +=
                    std::max<uint64_t>(meta.memory_bytes, meta.storage_bytes);
            }

            row_prefix += state.rows;
            num_rows_until_chunk_.push_back(row_prefix);
            files_.emplace_back(std::move(state));
        }

        num_rows_ = row_prefix;
    }

    ~VortexColumn() override {
        CancelWarmup();
    }

    void
    ManualEvictCache() const override {
        for (const auto& file : files_) {
            file.slot->ManualEvictAll();
        }
    }

    void
    CancelWarmup() override {
        for (const auto& file : files_) {
            file.slot->CancelWarmup();
        }
    }

    bool
    IsNullable() const override {
        return field_meta_.is_nullable();
    }

    size_t
    NumRows() const override {
        return num_rows_;
    }

    int64_t
    num_chunks() const override {
        return static_cast<int64_t>(files_.size());
    }

    size_t
    DataByteSize() const override {
        size_t bytes = 0;
        for (const auto& file : files_) {
            bytes += file.memory_bytes;
        }
        return bytes;
    }

    int64_t
    chunk_row_nums(int64_t chunk_id) const override {
        CheckChunkId(chunk_id);
        return files_[chunk_id].rows;
    }

    PinWrapper<const char*>
    DataOfChunk(milvus::OpContext* op_ctx, int chunk_id) const override {
        auto chunk = MaterializeChunk(op_ctx, chunk_id);
        return PinWrapper<const char*>(chunk, chunk->Data());
    }

    bool
    IsValid(milvus::OpContext* op_ctx, size_t offset) const override {
        if (!field_meta_.is_nullable()) {
            return true;
        }
        auto [chunk_id, offset_in_chunk] =
            GetChunkIDByOffset(static_cast<int64_t>(offset));
        auto chunk = MaterializeChunk(op_ctx, chunk_id);
        return chunk->isValid(static_cast<int>(offset_in_chunk));
    }

    void
    BulkIsValid(milvus::OpContext* op_ctx,
                std::function<void(bool, size_t)> fn,
                const int64_t* offsets,
                int64_t count) const override {
        if (!field_meta_.is_nullable()) {
            if (offsets == nullptr) {
                for (int64_t i = 0; i < num_rows_; ++i) {
                    fn(true, i);
                }
            } else {
                for (int64_t i = 0; i < count; ++i) {
                    fn(true, i);
                }
            }
            return;
        }

        if (offsets == nullptr) {
            int64_t logical_offset = 0;
            for (int64_t chunk_id = 0; chunk_id < num_chunks(); ++chunk_id) {
                auto chunk = MaterializeChunk(op_ctx, chunk_id);
                for (int64_t i = 0; i < chunk->RowNums(); ++i) {
                    fn(chunk->isValid(static_cast<int>(i)), logical_offset + i);
                }
                logical_offset += chunk->RowNums();
            }
            return;
        }

        auto [chunk_ids, offsets_in_chunk] =
            GetChunkIDsByOffsets(offsets, count);
        std::unordered_map<int64_t, std::vector<int64_t>> indices_by_chunk;
        indices_by_chunk.reserve(chunk_ids.size());
        for (int64_t i = 0; i < count; ++i) {
            indices_by_chunk[chunk_ids[i]].emplace_back(i);
        }

        for (const auto& [chunk_id, indices] : indices_by_chunk) {
            auto chunk = MaterializeChunk(op_ctx, chunk_id);
            for (const auto index : indices) {
                fn(chunk->isValid(
                       static_cast<int>(offsets_in_chunk[index])),
                   index);
            }
        }
    }

    void
    PrefetchChunks(milvus::OpContext* op_ctx,
                   const std::vector<int64_t>& chunk_ids) const override {
        for (auto chunk_id : chunk_ids) {
            CheckChunkId(chunk_id);
            const auto& file = files_[chunk_id];
            std::vector<cachinglayer::cid_t> cell_ids;
            cell_ids.reserve(file.cell_ids.size());
            for (auto cell_id : file.cell_ids) {
                cell_ids.emplace_back(
                    static_cast<cachinglayer::cid_t>(cell_id));
            }
            cachinglayer::SemiInlineGet(file.slot->PinCells(op_ctx, cell_ids));
        }
    }

    PinWrapper<SpanBase>
    Span(milvus::OpContext* op_ctx, int64_t chunk_id) const override {
        if (!IsChunkedColumnDataType(data_type_)) {
            ThrowInfo(ErrorCode::Unsupported,
                      "VortexColumn::Span only supports fixed-width scalar "
                      "fields");
        }
        auto chunk = MaterializeChunk(op_ctx, chunk_id);
        auto span = static_cast<FixedWidthChunk*>(chunk.get())->Span();
        return PinWrapper<SpanBase>(chunk, span);
    }

    PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    StringViews(milvus::OpContext* op_ctx,
                int64_t chunk_id,
                std::optional<std::pair<int64_t, int64_t>> offset_len =
                    std::nullopt) const override {
        if (!IsChunkedVariableColumnDataType(data_type_)) {
            ThrowInfo(ErrorCode::Unsupported,
                      "VortexColumn::StringViews only supports variable "
                      "fields");
        }
        auto [holder, views] =
            ScanStringLikeViewsFromFile(op_ctx, chunk_id, offset_len);
        return PinWrapper<
            std::pair<std::vector<std::string_view>, FixedVector<bool>>>(
            std::move(holder), std::move(views));
    }

    PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
    ArrayViews(milvus::OpContext* op_ctx,
               int64_t chunk_id,
               std::optional<std::pair<int64_t, int64_t>> offset_len =
                   std::nullopt) const override {
        if (!IsChunkedArrayColumnDataType(data_type_)) {
            ThrowInfo(ErrorCode::Unsupported,
                      "VortexColumn::ArrayViews only supports array fields");
        }
        auto chunk = MaterializeChunk(op_ctx, chunk_id, offset_len);
        auto views = static_cast<ArrayChunk*>(chunk.get())->Views({});
        return PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>(
            chunk, std::move(views));
    }

    PinWrapper<std::pair<std::vector<VectorArrayView>, FixedVector<bool>>>
    VectorArrayViews(milvus::OpContext*,
                     int64_t,
                     std::optional<std::pair<int64_t, int64_t>> =
                         std::nullopt) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "VortexColumn does not support vector array fields");
    }

    PinWrapper<const size_t*>
    VectorArrayOffsets(milvus::OpContext*, int64_t) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "VortexColumn does not support vector array fields");
    }

    PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    StringViewsByOffsets(milvus::OpContext* op_ctx,
                         int64_t chunk_id,
                         const FixedVector<int32_t>& offsets) const override {
        if (!IsChunkedVariableColumnDataType(data_type_)) {
            ThrowInfo(ErrorCode::Unsupported,
                      "VortexColumn::StringViewsByOffsets only supports "
                      "variable fields");
        }
        CheckChunkId(chunk_id);
        std::vector<int64_t> segment_offsets;
        segment_offsets.reserve(offsets.size());
        auto chunk_start = GetNumRowsUntilChunk(chunk_id);
        for (auto offset : offsets) {
            AssertInfo(offset >= 0 && offset < files_[chunk_id].rows,
                       "vortex chunk-local offset {} out of chunk {} rows {}",
                       offset,
                       chunk_id,
                       files_[chunk_id].rows);
            segment_offsets.emplace_back(chunk_start + offset);
        }
        auto [holder, views] = TakeStringLikeViews(
            op_ctx, segment_offsets.data(), segment_offsets.size());
        return PinWrapper<
            std::pair<std::vector<std::string_view>, FixedVector<bool>>>(
            std::move(holder), std::move(views));
    }

    PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
    ArrayViewsByOffsets(milvus::OpContext* op_ctx,
                        int64_t chunk_id,
                        const FixedVector<int32_t>& offsets) const override {
        if (!IsChunkedArrayColumnDataType(data_type_)) {
            ThrowInfo(ErrorCode::Unsupported,
                      "VortexColumn::ArrayViewsByOffsets only supports array "
                      "fields");
        }
        CheckChunkId(chunk_id);
        std::vector<int64_t> segment_offsets;
        segment_offsets.reserve(offsets.size());
        auto chunk_start = GetNumRowsUntilChunk(chunk_id);
        for (auto offset : offsets) {
            AssertInfo(offset >= 0 && offset < files_[chunk_id].rows,
                       "vortex chunk-local offset {} out of chunk {} rows {}",
                       offset,
                       chunk_id,
                       files_[chunk_id].rows);
            segment_offsets.emplace_back(chunk_start + offset);
        }
        auto take =
            Take(op_ctx, segment_offsets.data(), segment_offsets.size());
        std::pair<std::vector<ArrayView>, FixedVector<bool>> views;
        views.first.reserve(offsets.size());
        views.second.reserve(offsets.size());
        for (size_t i = 0; i < offsets.size(); ++i) {
            auto array_chunk = static_cast<ArrayChunk*>(take->chunks[i].get());
            auto local_offset = static_cast<int>(take->local_offsets[i]);
            views.first.emplace_back(array_chunk->View(local_offset));
            views.second.emplace_back(array_chunk->isValid(local_offset));
        }
        return PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>(
            take, std::move(views));
    }

    std::pair<size_t, size_t>
    GetChunkIDByOffset(int64_t offset) const override {
        AssertInfo(offset >= 0 && offset < num_rows_,
                   "offset {} is out of range, num_rows: {}",
                   offset,
                   num_rows_);
        return ::milvus::GetChunkIDByOffset(offset, num_rows_until_chunk_);
    }

    std::pair<std::vector<milvus::cachinglayer::cid_t>, std::vector<int64_t>>
    GetChunkIDsByOffsets(const int64_t* offsets, int64_t count) const override {
        return ::milvus::GetChunkIDsByOffsets(
            offsets, count, num_rows_until_chunk_);
    }

    PinWrapper<Chunk*>
    GetChunk(milvus::OpContext*, int64_t) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "VortexColumn::GetChunk is disabled because it "
                  "materializes Vortex data; use column view/bulk APIs "
                  "instead");
    }

    std::vector<PinWrapper<Chunk*>>
    GetAllChunks(milvus::OpContext*) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "VortexColumn::GetAllChunks is disabled because it "
                  "materializes Vortex data; use column view/bulk APIs "
                  "instead");
    }

    int64_t
    GetNumRowsUntilChunk(int64_t chunk_id) const override {
        AssertInfo(
            chunk_id >= 0 &&
                chunk_id < static_cast<int64_t>(num_rows_until_chunk_.size()),
            "vortex chunk_id {} out of prefix range",
            chunk_id);
        return num_rows_until_chunk_[chunk_id];
    }

    const std::vector<int64_t>&
    GetNumRowsUntilChunk() const override {
        return num_rows_until_chunk_;
    }

    void
    BulkValueAt(milvus::OpContext* op_ctx,
                std::function<void(const char*, size_t)> fn,
                const int64_t* offsets,
                int64_t count) override {
        auto result = TakeOwn(op_ctx, offsets, count);
        for (int64_t i = 0; i < count; ++i) {
            fn(result.chunks[i]->ValueAt(result.local_offsets[i]), i);
        }
    }

    void
    BulkPrimitiveValueAt(milvus::OpContext* op_ctx,
                         void* dst,
                         const int64_t* offsets,
                         int64_t count,
                         bool small_int_raw_type = false) override {
        switch (data_type_) {
            case DataType::INT8: {
                BulkPrimitiveValueAtFromArrow<arrow::Int8Array,
                                              int8_t,
                                              int8_t,
                                              int32_t>(
                    op_ctx, dst, offsets, count, small_int_raw_type);
                break;
            }
            case DataType::INT16: {
                BulkPrimitiveValueAtFromArrow<arrow::Int16Array,
                                              int16_t,
                                              int16_t,
                                              int32_t>(
                    op_ctx, dst, offsets, count, small_int_raw_type);
                break;
            }
            case DataType::INT32: {
                BulkPrimitiveValueAtFromArrow<arrow::Int32Array,
                                              int32_t,
                                              int32_t,
                                              int32_t>(
                    op_ctx, dst, offsets, count, true);
                break;
            }
            case DataType::INT64:
            case DataType::TIMESTAMPTZ: {
                BulkPrimitiveValueAtFromArrow<arrow::Int64Array,
                                              int64_t,
                                              int64_t,
                                              int64_t>(
                    op_ctx, dst, offsets, count, true);
                break;
            }
            case DataType::FLOAT: {
                BulkPrimitiveValueAtFromArrow<arrow::FloatArray,
                                              float,
                                              float,
                                              float>(
                    op_ctx, dst, offsets, count, true);
                break;
            }
            case DataType::DOUBLE: {
                BulkPrimitiveValueAtFromArrow<arrow::DoubleArray,
                                              double,
                                              double,
                                              double>(
                    op_ctx, dst, offsets, count, true);
                break;
            }
            case DataType::BOOL: {
                BulkPrimitiveValueAtFromArrow<arrow::BooleanArray,
                                              bool,
                                              bool,
                                              bool>(
                    op_ctx, dst, offsets, count, true);
                break;
            }
            default:
                ThrowInfo(ErrorCode::Unsupported,
                          "VortexColumn::BulkPrimitiveValueAt unsupported "
                          "data type {}",
                          data_type_);
        }
    }

    void
    BulkVectorValueAt(
        milvus::OpContext*, void*, const int64_t*, int64_t, int64_t) override {
        ThrowInfo(ErrorCode::Unsupported,
                  "VortexColumn does not support vector fields");
    }

    void
    BulkRawStringAt(milvus::OpContext* op_ctx,
                    std::function<void(std::string_view, size_t, bool)> fn,
                    const int64_t* offsets = nullptr,
                    int64_t count = 0) const override {
        if (!IsChunkedVariableColumnDataType(data_type_) ||
            data_type_ == DataType::JSON) {
            ThrowInfo(ErrorCode::Unsupported,
                      "VortexColumn::BulkRawStringAt only supports string "
                      "fields");
        }
        BulkStringLikeAt(op_ctx, fn, offsets, count);
    }

    void
    BulkRawJsonAt(milvus::OpContext* op_ctx,
                  std::function<void(Json, size_t, bool)> fn,
                  const int64_t* offsets,
                  int64_t count) const override {
        if (data_type_ != DataType::JSON) {
            ThrowInfo(ErrorCode::Unsupported,
                      "VortexColumn::BulkRawJsonAt only supports JSON fields");
        }
        BulkStringLikeAt(
            op_ctx,
            [&](std::string_view value, size_t index, bool valid) {
                fn(Json(value.data(), value.size()), index, valid);
            },
            offsets,
            count);
    }

    void
    BulkRawBsonAt(milvus::OpContext* op_ctx,
                  std::function<void(BsonView, uint32_t, uint32_t)> fn,
                  const uint32_t* row_offsets,
                  const uint32_t* value_offsets,
                  int64_t count) const override {
        AssertInfo(row_offsets != nullptr && value_offsets != nullptr,
                   "row_offsets and value_offsets must be provided");
        std::vector<int64_t> offsets(count);
        for (int64_t i = 0; i < count; ++i) {
            offsets[i] = row_offsets[i];
        }
        BulkStringLikeAt(
            op_ctx,
            [&](std::string_view value, size_t index, bool) {
                fn(BsonView(reinterpret_cast<const uint8_t*>(value.data()),
                            value.size()),
                   row_offsets[index],
                   value_offsets[index]);
            },
            offsets.data(),
            count);
    }

    void
    BulkArrayAt(milvus::OpContext* op_ctx,
                std::function<void(const ArrayView&, size_t)> fn,
                const int64_t* offsets,
                int64_t count) const override {
        if (!IsChunkedArrayColumnDataType(data_type_)) {
            ThrowInfo(ErrorCode::Unsupported,
                      "VortexColumn::BulkArrayAt only supports array fields");
        }
        auto result = TakeOwn(op_ctx, offsets, count);
        for (int64_t i = 0; i < count; ++i) {
            auto view = static_cast<ArrayChunk*>(result.chunks[i].get())
                            ->View(result.local_offsets[i]);
            fn(view, i);
        }
    }

    void
    BulkVectorArrayAt(milvus::OpContext*,
                      std::function<void(VectorFieldProto&&, size_t)>,
                      const int64_t*,
                      int64_t) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "VortexColumn does not support vector array fields");
    }

    std::optional<milvus_storage::vortex::VortexUnaryRangeFilter>
    BuildUnaryRangeFilter(proto::plan::OpType op_type,
                          const proto::plan::GenericValue& value) const {
        auto scalar = BuildScalar(value);
        if (!scalar.has_value()) {
            return std::nullopt;
        }

        milvus_storage::vortex::VortexUnaryRangeFilter filter;
        filter.field_name = field_name_;
        filter.value = std::move(*scalar);
        switch (op_type) {
            case proto::plan::GreaterThan:
                filter.op =
                    milvus_storage::vortex::VortexCompareOp::GreaterThan;
                return filter;
            case proto::plan::GreaterEqual:
                filter.op =
                    milvus_storage::vortex::VortexCompareOp::GreaterEqual;
                return filter;
            case proto::plan::LessThan:
                filter.op = milvus_storage::vortex::VortexCompareOp::LessThan;
                return filter;
            case proto::plan::LessEqual:
                filter.op = milvus_storage::vortex::VortexCompareOp::LessEqual;
                return filter;
            case proto::plan::Equal:
                filter.op = milvus_storage::vortex::VortexCompareOp::Equal;
                return filter;
            case proto::plan::NotEqual:
                filter.op = milvus_storage::vortex::VortexCompareOp::NotEqual;
                return filter;
            default:
                return std::nullopt;
        }
    }

    struct ScanBatch {
        std::shared_ptr<arrow::RecordBatch> batch;
        int64_t row_id_base = 0;
    };

    class ScanCursor {
     public:
        struct FileScan {
            int64_t row_id_base = 0;
            std::shared_ptr<milvus_storage::vortex::VortexFormatReader> reader;
            milvus_storage::vortex::VortexScanPlan plan;
            std::shared_ptr<cachinglayer::CellAccessor<
                milvus_storage::vortex::VortexCellGuard>>
                pin;
        };

        explicit ScanCursor(std::vector<FileScan> file_scans)
            : file_scans_(std::move(file_scans)) {
        }

        bool
        Next(ScanBatch* out);

     private:
        std::vector<FileScan> file_scans_;
        size_t current_file_ = 0;
        std::shared_ptr<arrow::RecordBatchReader> current_reader_;
    };

    std::unique_ptr<ScanCursor>
    Scan(milvus::OpContext* op_ctx,
         int64_t start_offset,
         int64_t length,
         std::optional<milvus_storage::vortex::VortexUnaryRangeFilter> filter =
             std::nullopt) const;

    ScanResult
    Scan(milvus::OpContext* op_ctx, const ScanOptions& options) const override;

    struct TakeResult {
        std::vector<std::shared_ptr<Chunk>> chunks;
        std::vector<int64_t> local_offsets;
    };

    TakeResult
    TakeOwn(milvus::OpContext* op_ctx,
            const int64_t* segment_offsets,
            int64_t count) const;

    std::shared_ptr<TakeResult>
    Take(milvus::OpContext* op_ctx,
         const int64_t* segment_offsets,
         int64_t count) const;

 private:
    std::optional<DataType>
    GetDefaultScanDataType() const override {
        return data_type_;
    }

    static std::shared_ptr<arrow::Schema>
    MakeProjectedArrowSchema(const std::shared_ptr<arrow::Schema>& schema,
                             const std::string& field_name) {
        AssertInfo(schema != nullptr,
                   "vortex projected schema requires a non-null file schema");
        auto field = schema->GetFieldByName(field_name);
        AssertInfo(field != nullptr,
                   "vortex file schema does not contain field {}",
                   field_name);
        return arrow::schema({field});
    }

    struct FileState {
        std::string path;
        std::shared_ptr<milvus_storage::vortex::VortexFooterReader> footer_reader;
        std::shared_ptr<milvus_storage::vortex::VortexFormatReader> reader;
        std::shared_ptr<
            cachinglayer::CacheSlot<milvus_storage::vortex::VortexCellGuard>>
            slot;
        std::vector<uint64_t> cell_ids;
        int64_t rows = 0;
        size_t memory_bytes = 0;
    };

    struct ArrowTakeResult {
        std::shared_ptr<
            cachinglayer::CellAccessor<milvus_storage::vortex::VortexCellGuard>>
            pin;
        std::shared_ptr<arrow::Table> table;
    };

    struct ArrowStringViewHolder {
        std::vector<std::shared_ptr<
            cachinglayer::CellAccessor<milvus_storage::vortex::VortexCellGuard>>>
            pins;
        std::vector<std::shared_ptr<arrow::Table>> tables;
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    };

    class ArrowStringLikeColumn {
     public:
        explicit ArrowStringLikeColumn(
            const std::shared_ptr<arrow::Table>& table) {
            AssertInfo(table != nullptr, "vortex take table is null");
            AssertInfo(table->num_columns() == 1,
                       "vortex string-like take expects one column, got {}",
                       table->num_columns());
            Init(table->column(0)->chunks());
        }

        explicit ArrowStringLikeColumn(
            const std::shared_ptr<arrow::Array>& array) {
            AssertInfo(array != nullptr, "vortex string-like array is null");
            Init({array});
        }

        int64_t
        length() const {
            return prefix_.empty() ? 0 : prefix_.back();
        }

        bool
        IsValid(int64_t row) const {
            auto [array, local_offset] = ArrayAt(row);
            return array->IsValid(local_offset);
        }

        std::string_view
        ValueAt(int64_t row) const {
            auto [array, local_offset] = ArrayAt(row);
            if (!array->IsValid(local_offset)) {
                return {};
            }

            switch (array->type_id()) {
                case arrow::Type::BINARY: {
                    auto typed =
                        std::static_pointer_cast<arrow::BinaryArray>(array);
                    auto value = typed->GetView(local_offset);
                    return {value.data(), static_cast<size_t>(value.size())};
                }
                case arrow::Type::STRING: {
                    auto typed =
                        std::static_pointer_cast<arrow::StringArray>(array);
                    auto value = typed->GetView(local_offset);
                    return {value.data(), static_cast<size_t>(value.size())};
                }
                case arrow::Type::LARGE_BINARY: {
                    auto typed = std::static_pointer_cast<
                        arrow::LargeBinaryArray>(array);
                    auto value = typed->GetView(local_offset);
                    return {value.data(), static_cast<size_t>(value.size())};
                }
                case arrow::Type::LARGE_STRING: {
                    auto typed = std::static_pointer_cast<
                        arrow::LargeStringArray>(array);
                    auto value = typed->GetView(local_offset);
                    return {value.data(), static_cast<size_t>(value.size())};
                }
                case arrow::Type::BINARY_VIEW: {
                    auto typed = std::static_pointer_cast<
                        arrow::BinaryViewArray>(array);
                    auto value = typed->GetView(local_offset);
                    return {value.data(), static_cast<size_t>(value.size())};
                }
                case arrow::Type::STRING_VIEW: {
                    auto typed = std::static_pointer_cast<
                        arrow::StringViewArray>(array);
                    auto value = typed->GetView(local_offset);
                    return {value.data(), static_cast<size_t>(value.size())};
                }
                default:
                    ThrowInfo(ErrorCode::Unsupported,
                              "VortexColumn string-like take got unsupported "
                              "Arrow type {}",
                              array->type()->ToString());
                    return {};
            }
        }

     private:
        void
        Init(arrow::ArrayVector chunks) {
            chunks_ = std::move(chunks);
            prefix_.reserve(chunks_.size() + 1);
            prefix_.push_back(0);
            int64_t rows = 0;
            for (const auto& chunk : chunks_) {
                rows += chunk->length();
                prefix_.push_back(rows);
            }
        }

        std::pair<std::shared_ptr<arrow::Array>, int64_t>
        ArrayAt(int64_t row) const {
            AssertInfo(row >= 0 && row < length(),
                       "vortex string-like row {} out of range {}",
                       row,
                       length());
            auto it = std::upper_bound(prefix_.begin(), prefix_.end(), row);
            auto chunk_idx = static_cast<size_t>(
                std::distance(prefix_.begin(), it) - 1);
            return {chunks_[chunk_idx], row - prefix_[chunk_idx]};
        }

        arrow::ArrayVector chunks_;
        std::vector<int64_t> prefix_;
    };

    std::pair<std::vector<std::string_view>, FixedVector<bool>>
    BuildStringViewsFromArrow(const ArrowStringLikeColumn& column,
                              std::optional<std::pair<int64_t, int64_t>>
                                  offset_len,
                              bool emit_valid) const {
        int64_t start = 0;
        int64_t length = column.length();
        if (offset_len.has_value()) {
            start = offset_len->first;
            length = offset_len->second;
            AssertInfo(start >= 0 && length >= 0 &&
                           start + length <= column.length(),
                       "vortex string-like view range [{}, {}) out of rows {}",
                       start,
                       start + length,
                       column.length());
        }

        std::pair<std::vector<std::string_view>, FixedVector<bool>> views;
        views.first.reserve(length);
        if (emit_valid) {
            views.second.reserve(length);
        }
        for (int64_t i = 0; i < length; ++i) {
            const auto row = start + i;
            views.first.emplace_back(column.ValueAt(row));
            if (emit_valid) {
                views.second.emplace_back(column.IsValid(row));
            }
        }
        return views;
    }

    void
    CheckChunkId(int64_t chunk_id) const {
        AssertInfo(chunk_id >= 0 && chunk_id < num_chunks(),
                   "vortex chunk_id {} out of range {}",
                   chunk_id,
                   num_chunks());
    }

    std::shared_ptr<
        cachinglayer::CellAccessor<milvus_storage::vortex::VortexCellGuard>>
    PinPlanCells(milvus::OpContext* op_ctx,
                 const FileState& file,
                 const std::vector<uint64_t>& cell_ids) const {
        std::vector<cachinglayer::cid_t> cids;
        cids.reserve(cell_ids.size());
        for (auto cell_id : cell_ids) {
            cids.emplace_back(static_cast<cachinglayer::cid_t>(cell_id));
        }
        return cachinglayer::SemiInlineGet(file.slot->PinCells(op_ctx, cids));
    }

    std::optional<milvus_storage::vortex::VortexLiteral>
    BuildScalar(const proto::plan::GenericValue& value) const {
        using ValCase = proto::plan::GenericValue::ValCase;

        switch (data_type_) {
            case DataType::BOOL:
                if (value.val_case() == ValCase::kBoolVal) {
                    return value.bool_val();
                }
                return std::nullopt;
            case DataType::INT8:
                if (value.val_case() == ValCase::kInt64Val &&
                    value.int64_val() >= std::numeric_limits<int8_t>::min() &&
                    value.int64_val() <= std::numeric_limits<int8_t>::max()) {
                    return static_cast<int8_t>(value.int64_val());
                }
                return std::nullopt;
            case DataType::INT16:
                if (value.val_case() == ValCase::kInt64Val &&
                    value.int64_val() >= std::numeric_limits<int16_t>::min() &&
                    value.int64_val() <= std::numeric_limits<int16_t>::max()) {
                    return static_cast<int16_t>(value.int64_val());
                }
                return std::nullopt;
            case DataType::INT32:
                if (value.val_case() == ValCase::kInt64Val &&
                    value.int64_val() >= std::numeric_limits<int32_t>::min() &&
                    value.int64_val() <= std::numeric_limits<int32_t>::max()) {
                    return static_cast<int32_t>(value.int64_val());
                }
                return std::nullopt;
            case DataType::INT64:
            case DataType::TIMESTAMPTZ:
                if (value.val_case() == ValCase::kInt64Val) {
                    return value.int64_val();
                }
                return std::nullopt;
            case DataType::FLOAT:
                if (value.val_case() == ValCase::kFloatVal) {
                    return static_cast<float>(value.float_val());
                }
                if (value.val_case() == ValCase::kInt64Val) {
                    return static_cast<float>(value.int64_val());
                }
                return std::nullopt;
            case DataType::DOUBLE:
                if (value.val_case() == ValCase::kFloatVal) {
                    return value.float_val();
                }
                if (value.val_case() == ValCase::kInt64Val) {
                    return static_cast<double>(value.int64_val());
                }
                return std::nullopt;
            case DataType::VARCHAR:
            case DataType::STRING:
                if (value.val_case() == ValCase::kStringVal) {
                    return value.string_val();
                }
                return std::nullopt;
            default:
                return std::nullopt;
        }
    }

    std::shared_ptr<Chunk>
    ChunkFromTable(const std::shared_ptr<arrow::Table>& table) const {
        AssertInfo(table != nullptr, "vortex table is null");
        AssertInfo(table->num_columns() == 1,
                   "vortex materialization expects one column, got {}",
                   table->num_columns());
        auto arrays = table->column(0)->chunks();
        arrays = storage::NormalizeArrowForChunkWriter(arrays, field_meta_);
        return create_chunk(field_meta_, arrays);
    }

    std::shared_ptr<Chunk>
    MaterializeChunk(milvus::OpContext* op_ctx,
                     int64_t chunk_id,
                     std::optional<std::pair<int64_t, int64_t>> offset_len =
                         std::nullopt) const {
        CheckChunkId(chunk_id);
        const auto& file = files_[chunk_id];
        int64_t start = 0;
        int64_t length = file.rows;
        if (offset_len.has_value()) {
            start = offset_len->first;
            length = offset_len->second;
            AssertInfo(start >= 0 && length >= 0 && start + length <= file.rows,
                       "vortex materialize range [{}, {}) out of chunk rows {}",
                       start,
                       start + length,
                       file.rows);
        }

        auto scan = OpenDataScanForFile(op_ctx, chunk_id, start, length);
        auto arrays = read_single_column_batches(scan.get());
        arrays = storage::NormalizeArrowForChunkWriter(arrays, field_meta_);
        return create_chunk(field_meta_, arrays);
    }

    PinWrapper<std::shared_ptr<arrow::RecordBatchReader>>
    OpenDataScanForFile(milvus::OpContext* op_ctx,
                        int64_t chunk_id,
                        int64_t start_offset,
                        int64_t length) const {
        CheckChunkId(chunk_id);
        const auto& file = files_[chunk_id];
        AssertInfo(start_offset >= 0 && length >= 0 &&
                       start_offset + length <= file.rows,
                   "vortex data scan range [{}, {}) out of chunk rows {}",
                   start_offset,
                   start_offset + length,
                   file.rows);
        auto plan_result =
            file.footer_reader->PlanScan(field_name_,
                                         std::nullopt,
                                         static_cast<uint64_t>(start_offset),
                                         static_cast<uint64_t>(start_offset + length));
        AssertInfo(plan_result.ok(),
                   "failed to plan vortex data scan field {} chunk {}: {}",
                   field_id_.get(),
                   chunk_id,
                   plan_result.status().ToString());
        auto plan = std::move(plan_result).ValueOrDie();
        auto pin = PinPlanCells(op_ctx, file, plan.cell_ids);
        auto reader_result =
            file.reader->streaming_read_ranges(plan.row_ranges, plan.filter);
        AssertInfo(reader_result.ok(),
                   "failed to open vortex data scan field {} chunk {}: {}",
                   field_id_.get(),
                   chunk_id,
                   reader_result.status().ToString());
        return PinWrapper<std::shared_ptr<arrow::RecordBatchReader>>(
            pin, std::move(reader_result).ValueOrDie());
    }

    std::pair<std::shared_ptr<ArrowStringViewHolder>,
              std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    ScanStringLikeViewsFromFile(
        milvus::OpContext* op_ctx,
        int64_t chunk_id,
        std::optional<std::pair<int64_t, int64_t>> offset_len) const {
        CheckChunkId(chunk_id);
        const auto& file = files_[chunk_id];
        int64_t start = 0;
        int64_t length = file.rows;
        if (offset_len.has_value()) {
            start = offset_len->first;
            length = offset_len->second;
            AssertInfo(start >= 0 && length >= 0 && start + length <= file.rows,
                       "vortex string-like scan range [{}, {}) out of chunk "
                       "rows {}",
                       start,
                       start + length,
                       file.rows);
        }

        auto holder = std::make_shared<ArrowStringViewHolder>();
        std::pair<std::vector<std::string_view>, FixedVector<bool>> views;
        views.first.reserve(length);
        if (field_meta_.is_nullable()) {
            views.second.reserve(length);
        }
        if (length == 0) {
            return {std::move(holder), std::move(views)};
        }

        auto plan_result =
            file.footer_reader->PlanScan(field_name_,
                                         std::nullopt,
                                         static_cast<uint64_t>(start),
                                         static_cast<uint64_t>(start + length));
        AssertInfo(plan_result.ok(),
                   "failed to plan vortex string-like scan field {} chunk {}: "
                   "{}",
                   field_id_.get(),
                   chunk_id,
                   plan_result.status().ToString());
        auto plan = std::move(plan_result).ValueOrDie();
        auto pin = PinPlanCells(op_ctx, file, plan.cell_ids);
        auto reader_result =
            file.reader->streaming_read_ranges(plan.row_ranges, plan.filter);
        AssertInfo(reader_result.ok(),
                   "failed to open vortex string-like scan field {} chunk {}: "
                   "{}",
                   field_id_.get(),
                   chunk_id,
                   reader_result.status().ToString());

        auto reader = std::move(reader_result).ValueOrDie();
        while (true) {
            std::shared_ptr<arrow::RecordBatch> batch;
            auto status = reader->ReadNext(&batch);
            AssertInfo(status.ok(),
                       "failed to read vortex string-like scan batch: {}",
                       status.ToString());
            if (batch == nullptr) {
                break;
            }
            AssertInfo(batch->num_columns() == 1,
                       "vortex string-like scan expects one column, got {}",
                       batch->num_columns());
            ArrowStringLikeColumn column(batch->column(0));
            auto batch_views = BuildStringViewsFromArrow(
                column, std::nullopt, field_meta_.is_nullable());
            views.first.insert(views.first.end(),
                               batch_views.first.begin(),
                               batch_views.first.end());
            for (auto valid : batch_views.second) {
                views.second.emplace_back(valid);
            }
            holder->batches.emplace_back(std::move(batch));
        }
        holder->pins.emplace_back(std::move(pin));
        AssertInfo(static_cast<int64_t>(views.first.size()) == length,
                   "vortex string-like scan returned {} rows, expected {}",
                   views.first.size(),
                   length);
        return {std::move(holder), std::move(views)};
    }

    std::shared_ptr<Chunk>
    TakeFromFile(milvus::OpContext* op_ctx,
                 int64_t chunk_id,
                 const std::vector<int64_t>& local_offsets) const {
        auto take = TakeArrowFromFile(op_ctx, chunk_id, local_offsets);
        return ChunkFromTable(take.table);
    }

    ArrowTakeResult
    TakeArrowFromFile(milvus::OpContext* op_ctx,
                      int64_t chunk_id,
                      const std::vector<int64_t>& local_offsets) const {
        const auto& file = files_[chunk_id];
        auto plan_result =
            file.footer_reader->PlanTake(field_name_, local_offsets);
        AssertInfo(plan_result.ok(),
                   "failed to plan vortex take field {} chunk {}: {}",
                   field_id_.get(),
                   chunk_id,
                   plan_result.status().ToString());
        auto plan = std::move(plan_result).ValueOrDie();
        auto pin = PinPlanCells(op_ctx, file, plan.cell_ids);
        auto table_result = file.reader->take(plan.row_indices);
        AssertInfo(table_result.ok(),
                   "failed to take vortex field {} chunk {}: {}",
                   field_id_.get(),
                   chunk_id,
                   table_result.status().ToString());
        ArrowTakeResult result;
        result.pin = std::move(pin);
        result.table = table_result.ValueOrDie();
        return result;
    }

    std::pair<std::shared_ptr<ArrowStringViewHolder>,
              std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    TakeStringLikeViews(milvus::OpContext* op_ctx,
                        const int64_t* offsets,
                        int64_t count) const {
        auto holder = std::make_shared<ArrowStringViewHolder>();
        std::pair<std::vector<std::string_view>, FixedVector<bool>> views;
        views.first.resize(count);
        views.second.resize(count);
        if (count == 0) {
            return {std::move(holder), std::move(views)};
        }
        AssertInfo(offsets != nullptr,
                   "vortex string-like take requires explicit row offsets");

        std::unordered_map<int64_t, std::vector<std::pair<int64_t, int64_t>>>
            by_file;
        for (int64_t i = 0; i < count; ++i) {
            auto [chunk_id, offset_in_chunk] = GetChunkIDByOffset(offsets[i]);
            by_file[static_cast<int64_t>(chunk_id)].push_back(
                {static_cast<int64_t>(offset_in_chunk), i});
        }

        holder->pins.reserve(by_file.size());
        holder->tables.reserve(by_file.size());
        for (auto& [chunk_id, entries] : by_file) {
            std::sort(entries.begin(), entries.end());
            std::vector<int64_t> unique_offsets;
            unique_offsets.reserve(entries.size());
            for (const auto& [local_offset, _] : entries) {
                if (unique_offsets.empty() ||
                    unique_offsets.back() != local_offset) {
                    unique_offsets.emplace_back(local_offset);
                }
            }

            auto take = TakeArrowFromFile(op_ctx, chunk_id, unique_offsets);
            ArrowStringLikeColumn column(take.table);
            auto unique_views =
                BuildStringViewsFromArrow(column, std::nullopt, true);
            AssertInfo(static_cast<int64_t>(unique_views.first.size()) ==
                           static_cast<int64_t>(unique_offsets.size()),
                       "vortex string-like take returned {} rows, expected {}",
                       unique_views.first.size(),
                       unique_offsets.size());
            for (const auto& [local_offset, original_index] : entries) {
                auto it = std::lower_bound(unique_offsets.begin(),
                                           unique_offsets.end(),
                                           local_offset);
                auto table_offset =
                    std::distance(unique_offsets.begin(), it);
                views.first[original_index] = unique_views.first[table_offset];
                views.second[original_index] =
                    unique_views.second[table_offset];
            }
            holder->pins.emplace_back(std::move(take.pin));
            holder->tables.emplace_back(std::move(take.table));
        }

        return {std::move(holder), std::move(views)};
    }

    template <typename ArrowArrayT,
              typename SrcT,
              typename RawDstT,
              typename WidenDstT>
    void
    CopyArrowPrimitiveValues(void* dst,
                             const std::shared_ptr<arrow::Table>& table,
                             const std::vector<std::vector<int64_t>>&
                                 original_indices_by_unique,
                             bool small_int_raw_type) const {
        AssertInfo(table != nullptr, "vortex primitive take table is null");
        AssertInfo(table->num_columns() == 1,
                   "vortex primitive take expects one column, got {}",
                   table->num_columns());
        auto column = table->column(0);
        AssertInfo(column != nullptr, "vortex primitive take column is null");
        AssertInfo(static_cast<int64_t>(original_indices_by_unique.size()) ==
                       column->length(),
                   "vortex primitive take returned {} rows, expected {}",
                   column->length(),
                   original_indices_by_unique.size());

        auto raw_dst = static_cast<RawDstT*>(dst);
        auto widen_dst = static_cast<WidenDstT*>(dst);
        int64_t table_offset = 0;
        for (const auto& chunk : column->chunks()) {
            auto array = std::dynamic_pointer_cast<ArrowArrayT>(chunk);
            AssertInfo(array != nullptr,
                       "vortex primitive take field {} expected Arrow array "
                       "type for {}, got {}",
                       field_id_.get(),
                       data_type_,
                       chunk ? chunk->type()->ToString() : "<null>");
            for (int64_t i = 0; i < array->length(); ++i) {
                const auto& output_indices =
                    original_indices_by_unique[static_cast<size_t>(
                        table_offset + i)];
                auto value = static_cast<SrcT>(array->Value(i));
                for (auto output_index : output_indices) {
                    if (small_int_raw_type) {
                        raw_dst[output_index] = static_cast<RawDstT>(value);
                    } else {
                        widen_dst[output_index] =
                            static_cast<WidenDstT>(value);
                    }
                }
            }
            table_offset += array->length();
        }
    }

    template <typename ArrowArrayT,
              typename SrcT,
              typename RawDstT,
              typename WidenDstT>
    void
    BulkPrimitiveValueAtFromArrow(milvus::OpContext* op_ctx,
                                  void* dst,
                                  const int64_t* offsets,
                                  int64_t count,
                                  bool small_int_raw_type) const {
        if (count == 0) {
            return;
        }
        AssertInfo(offsets != nullptr,
                   "vortex primitive take requires explicit row offsets");

        std::unordered_map<int64_t, std::vector<std::pair<int64_t, int64_t>>>
            by_file;
        for (int64_t i = 0; i < count; ++i) {
            auto [chunk_id, offset_in_chunk] = GetChunkIDByOffset(offsets[i]);
            by_file[static_cast<int64_t>(chunk_id)].push_back(
                {static_cast<int64_t>(offset_in_chunk), i});
        }

        for (auto& [chunk_id, entries] : by_file) {
            std::sort(entries.begin(), entries.end());
            std::vector<int64_t> unique_offsets;
            std::vector<std::vector<int64_t>> original_indices_by_unique;
            unique_offsets.reserve(entries.size());
            original_indices_by_unique.reserve(entries.size());
            for (const auto& [local_offset, original_index] : entries) {
                if (unique_offsets.empty() ||
                    unique_offsets.back() != local_offset) {
                    unique_offsets.emplace_back(local_offset);
                    original_indices_by_unique.emplace_back();
                }
                original_indices_by_unique.back().emplace_back(original_index);
            }

            auto take = TakeArrowFromFile(op_ctx, chunk_id, unique_offsets);

            CopyArrowPrimitiveValues<ArrowArrayT, SrcT, RawDstT, WidenDstT>(
                dst, take.table, original_indices_by_unique, small_int_raw_type);
        }
    }

    void
    BulkStringLikeAt(
        milvus::OpContext* op_ctx,
        const std::function<void(std::string_view, size_t, bool)>& fn,
        const int64_t* offsets,
        int64_t count) const {
        if (offsets == nullptr) {
            int64_t global_offset = 0;
            for (int64_t chunk_id = 0; chunk_id < num_chunks(); ++chunk_id) {
                auto scan = OpenDataScanForFile(
                    op_ctx, chunk_id, 0, files_[chunk_id].rows);
                int64_t row_offset = 0;
                while (true) {
                    std::shared_ptr<arrow::RecordBatch> batch;
                    auto status = scan.get()->ReadNext(&batch);
                    AssertInfo(status.ok(),
                               "failed to read vortex string-like scan "
                               "batch: {}",
                               status.ToString());
                    if (batch == nullptr) {
                        break;
                    }
                    AssertInfo(batch->num_columns() == 1,
                               "vortex string-like scan expects one column, "
                               "got {}",
                               batch->num_columns());
                    ArrowStringLikeColumn column(batch->column(0));
                    for (int64_t i = 0; i < column.length(); ++i) {
                        fn(column.ValueAt(i),
                           global_offset + row_offset + i,
                           column.IsValid(i));
                    }
                    row_offset += column.length();
                }
                global_offset += files_[chunk_id].rows;
            }
            return;
        }

        auto [holder, views] = TakeStringLikeViews(op_ctx, offsets, count);
        (void)holder;
        for (int64_t i = 0; i < count; ++i) {
            fn(views.first[i], i, views.second[i]);
        }
    }

 private:
    FieldId field_id_;
    FieldMeta field_meta_;
    DataType data_type_;
    std::string field_name_;
    std::vector<FileState> files_;
    std::vector<int64_t> num_rows_until_chunk_;
    int64_t num_rows_ = 0;
};

inline VortexColumn::TakeResult
VortexColumn::TakeOwn(milvus::OpContext* op_ctx,
                      const int64_t* segment_offsets,
                      int64_t count) const {
    TakeResult result;
    result.chunks.resize(count);
    result.local_offsets.resize(count);
    if (count == 0) {
        return result;
    }
    AssertInfo(segment_offsets != nullptr,
               "vortex take requires explicit row offsets");

    std::unordered_map<int64_t, std::vector<std::pair<int64_t, int64_t>>>
        by_file;
    for (int64_t i = 0; i < count; ++i) {
        auto [chunk_id, offset_in_chunk] =
            GetChunkIDByOffset(segment_offsets[i]);
        by_file[static_cast<int64_t>(chunk_id)].push_back(
            {static_cast<int64_t>(offset_in_chunk), i});
    }

    for (auto& [chunk_id, entries] : by_file) {
        std::sort(entries.begin(), entries.end());
        std::vector<int64_t> unique_offsets;
        unique_offsets.reserve(entries.size());
        for (const auto& [local_offset, _] : entries) {
            if (unique_offsets.empty() ||
                unique_offsets.back() != local_offset) {
                unique_offsets.emplace_back(local_offset);
            }
        }

        auto chunk = TakeFromFile(op_ctx, chunk_id, unique_offsets);
        for (const auto& [local_offset, original_index] : entries) {
            auto it = std::lower_bound(
                unique_offsets.begin(), unique_offsets.end(), local_offset);
            result.chunks[original_index] = chunk;
            result.local_offsets[original_index] =
                std::distance(unique_offsets.begin(), it);
        }
    }

    return result;
}

inline std::shared_ptr<VortexColumn::TakeResult>
VortexColumn::Take(milvus::OpContext* op_ctx,
                   const int64_t* segment_offsets,
                   int64_t count) const {
    return std::make_shared<TakeResult>(
        TakeOwn(op_ctx, segment_offsets, count));
}

inline bool
VortexColumn::ScanCursor::Next(ScanBatch* out) {
    AssertInfo(out != nullptr, "vortex scan output batch is null");

    while (true) {
        if (current_reader_ != nullptr) {
            std::shared_ptr<arrow::RecordBatch> batch;
            auto status = current_reader_->ReadNext(&batch);
            AssertInfo(status.ok(),
                       "failed to read vortex scan batch: {}",
                       status.ToString());
            if (batch != nullptr) {
                out->batch = std::move(batch);
                out->row_id_base = file_scans_[current_file_].row_id_base;
                return true;
            }

            current_reader_.reset();
            ++current_file_;
            continue;
        }

        if (current_file_ >= file_scans_.size()) {
            out->batch.reset();
            out->row_id_base = 0;
            return false;
        }

        const auto& file_scan = file_scans_[current_file_];
        auto reader_result = file_scan.reader->row_id_read_ranges(
            file_scan.plan.row_ranges, file_scan.plan.filter);
        AssertInfo(reader_result.ok(),
                   "failed to open vortex row id scan: {}",
                   reader_result.status().ToString());
        current_reader_ = std::move(reader_result).ValueOrDie();
    }
}

}  // namespace milvus

#include "mmap/VortexColumnCursors.h"

namespace milvus {

inline ChunkedColumnInterface::ScanResult
VortexColumn::Scan(milvus::OpContext* op_ctx,
                   const ScanOptions& options) const {
    ScanResult result;
    if (options.output == ScanOutput::Data) {
        if (options.predicate != ScanPredicate::None) {
            return result;
        }
        if (options.projection == ScanProjection::ValidityOnly ||
            IsPrimitiveDataType(data_type_) ||
            IsChunkedVariableColumnDataType(data_type_)) {
            auto value_kind = options.value_kind;
            if (value_kind == ScanValueKind::Default &&
                IsChunkedVariableColumnDataType(data_type_)) {
                value_kind = data_type_ == DataType::JSON
                                 ? ScanValueKind::JsonView
                                 : ScanValueKind::StringView;
            }
            result.data_cursor = std::make_unique<VortexDataScanCursor>(
                this,
                op_ctx,
                options.start_offset,
                options.length,
                options.projection,
                value_kind,
                options.max_batch_rows);
        } else {
            result.data_cursor =
                detail::MakeDataScanCursor(this,
                                           op_ctx,
                                           options.start_offset,
                                           options.length,
                                           data_type_,
                                           options.projection,
                                           options.value_kind,
                                           options.max_batch_rows);
        }
        return result;
    }

    if (IsNullable()) {
        return result;
    }

    if (options.predicate == ScanPredicate::Unary) {
        auto filter = BuildUnaryRangeFilter(options.op_type, options.value);
        if (!filter.has_value()) {
            return result;
        }
        result.row_id_cursor = std::make_unique<VortexRowIdScanCursor>(
            Scan(op_ctx,
                 options.start_offset,
                 options.length,
                 std::move(filter)));
        return result;
    }

    if (options.predicate != ScanPredicate::BinaryRange) {
        return result;
    }

    auto lower_filter = BuildUnaryRangeFilter(
        options.lower_inclusive ? proto::plan::GreaterEqual
                                : proto::plan::GreaterThan,
        options.lower_value);
    auto upper_filter = BuildUnaryRangeFilter(
        options.upper_inclusive ? proto::plan::LessEqual
                                : proto::plan::LessThan,
        options.upper_value);
    if (!lower_filter.has_value() || !upper_filter.has_value()) {
        return result;
    }

    auto lower_cursor = std::make_unique<VortexRowIdScanCursor>(
        Scan(op_ctx,
             options.start_offset,
             options.length,
             std::move(lower_filter)));
    auto upper_cursor = std::make_unique<VortexRowIdScanCursor>(
        Scan(op_ctx,
             options.start_offset,
             options.length,
             std::move(upper_filter)));
    result.row_id_cursor = std::make_unique<detail::IntersectRowIdScanCursor>(
        std::move(lower_cursor), std::move(upper_cursor));
    return result;
}

inline std::unique_ptr<VortexColumn::ScanCursor>
VortexColumn::Scan(milvus::OpContext* op_ctx,
                   int64_t start_offset,
                   int64_t length,
                   std::optional<milvus_storage::vortex::VortexUnaryRangeFilter>
                       filter) const {
    AssertInfo(
        start_offset >= 0 && length >= 0 && start_offset + length <= num_rows_,
        "vortex scan range [{}, {}) out of rows {}",
        start_offset,
        start_offset + length,
        num_rows_);

    std::vector<ScanCursor::FileScan> file_scans;
    if (length == 0) {
        return std::make_unique<ScanCursor>(std::move(file_scans));
    }

    const int64_t scan_end = start_offset + length;
    for (int64_t chunk_id = 0; chunk_id < num_chunks(); ++chunk_id) {
        const int64_t chunk_start = num_rows_until_chunk_[chunk_id];
        const int64_t chunk_end = num_rows_until_chunk_[chunk_id + 1];
        if (chunk_start >= scan_end || chunk_end <= start_offset) {
            continue;
        }

        const auto& file = files_[chunk_id];
        const auto local_start = static_cast<uint64_t>(
            std::max<int64_t>(0, start_offset - chunk_start));
        const auto local_end = static_cast<uint64_t>(
            std::min<int64_t>(file.rows, scan_end - chunk_start));
        auto plan_result = file.footer_reader->PlanScan(
            field_name_, filter, local_start, local_end);
        AssertInfo(plan_result.ok(),
                   "failed to plan vortex scan field {} chunk {}: {}",
                   field_id_.get(),
                   chunk_id,
                   plan_result.status().ToString());
        auto plan = std::move(plan_result).ValueOrDie();
        if (plan.row_ranges.empty()) {
            continue;
        }

        ScanCursor::FileScan file_scan;
        file_scan.row_id_base = chunk_start;
        file_scan.reader = file.reader;
        file_scan.pin = PinPlanCells(op_ctx, file, plan.cell_ids);
        file_scan.plan = std::move(plan);
        file_scans.emplace_back(std::move(file_scan));
    }

    return std::make_unique<ScanCursor>(std::move(file_scans));
}

}  // namespace milvus
