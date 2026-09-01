// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <benchmark/benchmark.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <map>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <folly/init/Init.h>

#include "cachinglayer/Manager.h"
#include "common/Array.h"
#include "common/EasyAssert.h"
#include "common/Json.h"
#include "common/Schema.h"
#include "common/common_type_c.h"
#include "mmap/ChunkedColumnInterface.h"
#include "segcore/SegmentSealed.h"
#include "segcore/arrow_fs_c.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/MmapManager.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"

std::string TestLocalPath;
std::string TestRemotePath;
std::string TestMmapPath;

namespace milvus::benchmark_test {
namespace {

// Keep one naturally aligned boundary and make the later boundaries cross
// expression windows. Real sealed Cells come from binlogs and are not required
// to be multiples of the execution window.
constexpr std::array<int64_t, 4> kChunkRows{{8192, 7680, 8448, 7936}};
constexpr int64_t kChunkCount = kChunkRows.size();
constexpr int64_t kRowCount = 8192 + 7680 + 8448 + 7936;
constexpr int64_t kWindowRows = 1024;
constexpr int64_t kBenchmarkSegmentId = 710001;

enum class AccessPath {
    LegacyScan,
    InterfaceScan,
    LegacyTake,
    InterfaceTake,
};

enum class TakePattern {
    DenseOrdered,
    SparseOrdered25,
    SparseShuffled25,
};

enum class CompareTakePath {
    LegacyChunk,
    InterfaceTake,
};

constexpr std::array<TakePattern, 3> kTakePatterns{{
    TakePattern::DenseOrdered,
    TakePattern::SparseOrdered25,
    TakePattern::SparseShuffled25,
}};

class RawColumnBenchmarkFixture;

template <typename T>
uint64_t
RunTypedAccess(RawColumnBenchmarkFixture& fixture,
               AccessPath path,
               size_t field_index,
               TakePattern take_pattern);

struct FieldCase {
    const char* name;
    DataType data_type;
    DataType element_type;
    ChunkedColumnInterface::TargetType target_type;
    uint64_t (*run)(RawColumnBenchmarkFixture&,
                    AccessPath,
                    size_t,
                    TakePattern);
};

const std::array<FieldCase, 12> kFieldCases{{
    {"BOOL",
     DataType::BOOL,
     DataType::NONE,
     ChunkedColumnInterface::TargetType::Bool,
     &RunTypedAccess<bool>},
    {"INT8",
     DataType::INT8,
     DataType::NONE,
     ChunkedColumnInterface::TargetType::Int8,
     &RunTypedAccess<int8_t>},
    {"INT16",
     DataType::INT16,
     DataType::NONE,
     ChunkedColumnInterface::TargetType::Int16,
     &RunTypedAccess<int16_t>},
    {"INT32",
     DataType::INT32,
     DataType::NONE,
     ChunkedColumnInterface::TargetType::Int32,
     &RunTypedAccess<int32_t>},
    {"INT64",
     DataType::INT64,
     DataType::NONE,
     ChunkedColumnInterface::TargetType::Int64,
     &RunTypedAccess<int64_t>},
    {"FLOAT",
     DataType::FLOAT,
     DataType::NONE,
     ChunkedColumnInterface::TargetType::Float,
     &RunTypedAccess<float>},
    {"DOUBLE",
     DataType::DOUBLE,
     DataType::NONE,
     ChunkedColumnInterface::TargetType::Double,
     &RunTypedAccess<double>},
    {"TIMESTAMPTZ",
     DataType::TIMESTAMPTZ,
     DataType::NONE,
     ChunkedColumnInterface::TargetType::Int64,
     &RunTypedAccess<int64_t>},
    {"VARCHAR",
     DataType::VARCHAR,
     DataType::NONE,
     ChunkedColumnInterface::TargetType::StringView,
     &RunTypedAccess<std::string_view>},
    {"JSON",
     DataType::JSON,
     DataType::NONE,
     ChunkedColumnInterface::TargetType::Json,
     &RunTypedAccess<Json>},
    {"GEOMETRY",
     DataType::GEOMETRY,
     DataType::NONE,
     ChunkedColumnInterface::TargetType::StringView,
     &RunTypedAccess<std::string_view>},
    {"ARRAY_INT64",
     DataType::ARRAY,
     DataType::INT64,
     ChunkedColumnInterface::TargetType::ArrayView,
     &RunTypedAccess<ArrayView>},
}};

constexpr size_t kCompareLeftFieldIndex = 3;   // INT32
constexpr size_t kCompareRightFieldIndex = 4;  // INT64

template <typename T>
constexpr bool kUsesMaterializedViews =
    std::is_same_v<T, std::string_view> || std::is_same_v<T, Json> ||
    std::is_same_v<T, ArrayView>;

uint64_t
MixDigest(uint64_t digest, uint64_t value, int64_t row) {
    value ^= static_cast<uint64_t>(row) * 0x9e3779b97f4a7c15ULL;
    return digest ^
           (value + 0x9e3779b97f4a7c15ULL + (digest << 6) + (digest >> 2));
}

template <typename T>
std::enable_if_t<std::is_arithmetic_v<T>, uint64_t>
ValueDigest(T value) {
    uint64_t bits = 0;
    static_assert(sizeof(T) <= sizeof(bits));
    std::memcpy(&bits, &value, sizeof(T));
    return bits;
}

uint64_t
StringDigest(std::string_view value) {
    uint64_t digest = value.size();
    if (!value.empty()) {
        digest = digest * 131 + static_cast<uint8_t>(value.front());
        digest = digest * 131 + static_cast<uint8_t>(value.back());
    }
    return digest;
}

uint64_t
ValueDigest(std::string_view value) {
    return StringDigest(value);
}

uint64_t
ValueDigest(const Json& value) {
    return StringDigest(static_cast<std::string_view>(value));
}

uint64_t
ValueDigest(const ArrayView& value) {
    uint64_t digest = static_cast<uint64_t>(value.length()) << 32;
    digest ^= value.byte_size();
    if (value.byte_size() > 0) {
        digest ^= static_cast<const uint8_t*>(value.data())[0];
    }
    return digest;
}

template <typename T>
void
ConsumeValues(const T* values,
              int64_t row_start,
              int64_t count,
              uint64_t* digest) {
    AssertInfo(values != nullptr, "benchmark values are null");
    for (int64_t i = 0; i < count; ++i) {
        *digest = MixDigest(*digest, ValueDigest(values[i]), row_start + i);
    }
}

void
SetInt64Field(GeneratedData* dataset, FieldId field_id, int64_t start_value) {
    for (auto& field : *dataset->raw_->mutable_fields_data()) {
        if (field.field_id() != field_id.get()) {
            continue;
        }
        auto* values =
            field.mutable_scalars()->mutable_long_data()->mutable_data();
        values->Clear();
        values->Reserve(dataset->raw_->num_rows());
        for (int64_t i = 0; i < dataset->raw_->num_rows(); ++i) {
            values->Add(start_value + i);
        }
        return;
    }
    ThrowInfo(ErrorCode::FieldIDInvalid,
              "benchmark field {} not found",
              field_id.get());
}

class RawColumnBenchmarkFixture {
 public:
    RawColumnBenchmarkFixture() {
        schema_ = std::make_shared<Schema>();
        const auto pk_id = schema_->AddDebugField("pk", DataType::INT64);
        schema_->set_primary_field_id(pk_id);

        field_ids_.reserve(kFieldCases.size());
        for (const auto& field : kFieldCases) {
            const auto name = std::string("benchmark_") + field.name;
            if (field.data_type == DataType::ARRAY) {
                field_ids_.emplace_back(schema_->AddDebugField(
                    name, field.data_type, field.element_type));
            } else {
                field_ids_.emplace_back(
                    schema_->AddDebugField(name, field.data_type));
            }
        }
        compare_field_ids_.emplace_back(field_ids_[kCompareLeftFieldIndex],
                                        field_ids_[kCompareRightFieldIndex]);
        compare_field_ids_.emplace_back(
            schema_->AddDebugField(
                "benchmark_compare_nullable_INT32", DataType::INT32, true),
            schema_->AddDebugField(
                "benchmark_compare_nullable_INT64", DataType::INT64, true));

        std::unordered_map<int64_t, std::vector<FieldDataPtr>> field_chunks;
        int64_t row_start = 0;
        for (int64_t chunk_id = 0; chunk_id < kChunkCount; ++chunk_id) {
            const auto chunk_rows = kChunkRows[chunk_id];
            auto dataset = segcore::DataGen(schema_,
                                            chunk_rows,
                                            42 + chunk_id,
                                            row_start,
                                            1,
                                            4,
                                            1,
                                            false,
                                            false);
            SetInt64Field(&dataset, pk_id, row_start);
            for (int64_t i = 0; i < chunk_rows; ++i) {
                dataset.row_ids_[i] = row_start + i;
                dataset.timestamps_[i] = row_start + i;
            }
            AppendDataset(dataset, &field_chunks);
            row_start += chunk_rows;
        }
        AssertInfo(row_start == kRowCount,
                   "benchmark chunks contain {} rows, expected {}",
                   row_start,
                   kRowCount);

        LoadFieldDataInfo load_info;
        auto chunk_manager = storage::RemoteChunkManagerSingleton::GetInstance()
                                 .GetRemoteChunkManager();
        for (auto& [field_id, chunks] : field_chunks) {
            auto field_load_info =
                PrepareSingleFieldInsertBinlog(kCollectionID,
                                               kPartitionID,
                                               kBenchmarkSegmentId,
                                               field_id,
                                               std::move(chunks),
                                               chunk_manager);
            load_info.field_infos.merge(field_load_info.field_infos);
        }

        segment_ = segcore::CreateSealedSegment(
            schema_, empty_index_meta, kBenchmarkSegmentId);
        const auto status = ::LoadFieldData(segment_.get(), &load_info);
        AssertInfo(status.error_code == Success,
                   "failed to load Raw column benchmark segment: {}",
                   status.error_msg);

        auto& dense_offsets = offsets_[TakePattern::DenseOrdered];
        dense_offsets.resize(kRowCount);
        std::iota(dense_offsets.begin(), dense_offsets.end(), int64_t{0});

        auto& sparse_offsets = offsets_[TakePattern::SparseOrdered25];
        sparse_offsets.reserve((kRowCount + 3) / 4);
        for (int64_t offset = 0; offset < kRowCount; offset += 4) {
            sparse_offsets.push_back(offset);
        }
        offsets_[TakePattern::SparseShuffled25] = sparse_offsets;
        auto& shuffled_offsets = offsets_[TakePattern::SparseShuffled25];
        std::mt19937 random(42);
        std::shuffle(shuffled_offsets.begin(), shuffled_offsets.end(), random);

        for (const auto pattern : kTakePatterns) {
            auto& single_cell_offsets = single_cell_offsets_[pattern];
            for (const auto offset : offsets_[pattern]) {
                if (offset < kChunkRows.front()) {
                    single_cell_offsets.emplace_back(offset);
                }
            }
        }

        expected_scan_digests_.reserve(kFieldCases.size());
        expected_take_digests_.reserve(kFieldCases.size());
        for (size_t i = 0; i < kFieldCases.size(); ++i) {
            auto column = segment_->GetChunkedColumn(field_ids_[i]);
            AssertInfo(column != nullptr,
                       "benchmark field {} has no Raw column",
                       kFieldCases[i].name);
            auto pins = column->GetAllChunks(nullptr);
            benchmark::DoNotOptimize(pins.data());

            const auto expected_scan = kFieldCases[i].run(
                *this, AccessPath::LegacyScan, i, TakePattern::DenseOrdered);
            const auto actual_scan = kFieldCases[i].run(
                *this, AccessPath::InterfaceScan, i, TakePattern::DenseOrdered);
            AssertInfo(actual_scan == expected_scan,
                       "interface scan for {} produced digest {}, expected {}",
                       kFieldCases[i].name,
                       actual_scan,
                       expected_scan);
            expected_scan_digests_.emplace_back(expected_scan);

            std::map<TakePattern, uint64_t> take_digests;
            for (const auto pattern : kTakePatterns) {
                const auto expected_take = kFieldCases[i].run(
                    *this, AccessPath::LegacyTake, i, pattern);
                const auto actual_take = kFieldCases[i].run(
                    *this, AccessPath::InterfaceTake, i, pattern);
                AssertInfo(
                    actual_take == expected_take,
                    "interface take pattern {} for {} produced digest {}, "
                    "expected {}",
                    static_cast<int>(pattern),
                    kFieldCases[i].name,
                    actual_take,
                    expected_take);
                take_digests.emplace(pattern, expected_take);
            }
            expected_take_digests_.emplace_back(std::move(take_digests));
        }
    }

    segcore::SegmentInternalInterface*
    segment() const {
        return segment_.get();
    }

    FieldId
    field_id(size_t index) const {
        return field_ids_.at(index);
    }

    const std::vector<int64_t>&
    offsets(TakePattern pattern) const {
        return offsets_.at(pattern);
    }

    const std::vector<int64_t>&
    single_cell_offsets(TakePattern pattern) const {
        return single_cell_offsets_.at(pattern);
    }

    std::pair<FieldId, FieldId>
    compare_field_ids(bool nullable) const {
        return compare_field_ids_.at(nullable ? 1 : 0);
    }

    uint64_t
    expected_digest(size_t index, AccessPath path, TakePattern pattern) const {
        if (path == AccessPath::LegacyScan ||
            path == AccessPath::InterfaceScan) {
            return expected_scan_digests_.at(index);
        }
        return expected_take_digests_.at(index).at(pattern);
    }

 private:
    void
    AppendDataset(
        const GeneratedData& dataset,
        std::unordered_map<int64_t, std::vector<FieldDataPtr>>* field_chunks) {
        const auto row_count = dataset.row_ids_.size();

        auto row_ids =
            std::make_shared<FieldData<int64_t>>(DataType::INT64, false);
        row_ids->FillFieldData(dataset.row_ids_.data(), row_count);
        (*field_chunks)[RowFieldID.get()].emplace_back(std::move(row_ids));

        auto timestamps =
            std::make_shared<FieldData<int64_t>>(DataType::INT64, false);
        timestamps->FillFieldData(dataset.timestamps_.data(), row_count);
        (*field_chunks)[TimestampFieldID.get()].emplace_back(
            std::move(timestamps));

        const auto fields = schema_->get_fields();
        for (const auto& data : dataset.raw_->fields_data()) {
            const auto field_id = data.field_id();
            (*field_chunks)[field_id].emplace_back(
                segcore::CreateFieldDataFromDataArray(
                    row_count, &data, fields.at(FieldId(field_id))));
        }
    }

    SchemaPtr schema_;
    std::unique_ptr<segcore::SegmentSealed> segment_;
    std::vector<FieldId> field_ids_;
    std::vector<std::pair<FieldId, FieldId>> compare_field_ids_;
    std::map<TakePattern, std::vector<int64_t>> offsets_;
    std::map<TakePattern, std::vector<int64_t>> single_cell_offsets_;
    std::vector<uint64_t> expected_scan_digests_;
    std::vector<std::map<TakePattern, uint64_t>> expected_take_digests_;
};

template <typename T>
uint64_t
LegacyScan(RawColumnBenchmarkFixture& fixture, size_t field_index) {
    auto* segment = fixture.segment();
    const auto field_id = fixture.field_id(field_index);
    auto column = segment->GetChunkedColumn(field_id);
    AssertInfo(column != nullptr, "legacy scan column is null");

    uint64_t digest = 0;
    int64_t chunk_id = 0;
    int64_t chunk_offset = 0;
    for (int64_t window_start = 0; window_start < kRowCount;
         window_start += kWindowRows) {
        const auto window_end = std::min(window_start + kWindowRows, kRowCount);
        auto row = window_start;
        while (row < window_end) {
            const auto chunk_rows = column->chunk_row_nums(chunk_id);
            const auto count =
                std::min<int64_t>(window_end - row, chunk_rows - chunk_offset);
            if constexpr (kUsesMaterializedViews<T>) {
                auto pin = segment->get_batch_views<T>(
                    nullptr, field_id, chunk_id, chunk_offset, count);
                const auto& values = pin.get().first;
                AssertInfo(static_cast<int64_t>(values.size()) == count,
                           "legacy scan returned {} views, expected {}",
                           values.size(),
                           count);
                ConsumeValues(values.data(), row, count, &digest);
            } else {
                auto pin = segment->chunk_data<T>(nullptr, field_id, chunk_id);
                const auto span = pin.get();
                ConsumeValues(span.data() + chunk_offset, row, count, &digest);
            }
            row += count;
            chunk_offset += count;
            if (chunk_offset == chunk_rows) {
                ++chunk_id;
                chunk_offset = 0;
            }
        }
    }
    return digest;
}

template <typename T>
uint64_t
InterfaceScan(RawColumnBenchmarkFixture& fixture, size_t field_index) {
    auto* segment = fixture.segment();
    const auto field_id = fixture.field_id(field_index);
    auto column = segment->GetChunkedColumn(field_id);
    AssertInfo(column != nullptr, "interface scan column is null");

    uint64_t digest = 0;
    auto cursor = column->Scan(nullptr,
                               ChunkedColumnInterface::ScanOptions::ForData(
                                   0, kFieldCases[field_index].target_type));
    AssertInfo(cursor != nullptr, "Raw Scan returned null cursor");
    for (int64_t window_start = 0; window_start < kRowCount;
         window_start += kWindowRows) {
        const auto window_rows =
            std::min<int64_t>(kWindowRows, kRowCount - window_start);
        int64_t consumed = 0;
        while (consumed < window_rows) {
            ChunkedColumnInterface::ScanBatch batch;
            const auto expected_row = window_start + consumed;
            if (cursor->Position() != expected_row) {
                cursor->Seek(expected_row);
            }
            const auto returned = cursor->Next(
                window_rows - consumed,
                ChunkedColumnInterface::ScanReadMode::DataAndValidity,
                &batch);
            AssertInfo(returned && !batch.values.empty() && batch.size > 0 &&
                           batch.row_id_start == expected_row &&
                           batch.size <= window_rows - consumed,
                       "Raw scan returned empty data batch");
            ConsumeValues(batch.values.data_as<T>(),
                          batch.row_id_start,
                          batch.size,
                          &digest);
            consumed += batch.size;
        }
        AssertInfo(consumed == window_rows,
                   "Raw scan returned {} of {} rows",
                   consumed,
                   window_rows);
    }
    return digest;
}

template <typename T>
uint64_t
LegacyTake(RawColumnBenchmarkFixture& fixture,
           size_t field_index,
           TakePattern pattern) {
    auto* segment = fixture.segment();
    const auto field_id = fixture.field_id(field_index);
    const auto& offsets = fixture.offsets(pattern);
    uint64_t digest = 0;

    for (int64_t window_start = 0;
         window_start < static_cast<int64_t>(offsets.size());
         window_start += kWindowRows) {
        const auto window_end =
            std::min<int64_t>(window_start + kWindowRows, offsets.size());
        if constexpr (kUsesMaterializedViews<T>) {
            int64_t i = window_start;
            int64_t chunk_id = -1;
            int64_t chunk_offset = -1;
            if (i < window_end) {
                std::tie(chunk_id, chunk_offset) =
                    segment->get_chunk_by_offset(field_id, offsets[i]);
            }
            while (i < window_end) {
                const auto run_chunk_id = chunk_id;
                const auto run_start = i;
                FixedVector<int32_t> local_offsets;
                local_offsets.push_back(static_cast<int32_t>(chunk_offset));
                ++i;
                while (i < window_end) {
                    std::tie(chunk_id, chunk_offset) =
                        segment->get_chunk_by_offset(field_id, offsets[i]);
                    if (chunk_id != run_chunk_id) {
                        break;
                    }
                    local_offsets.push_back(static_cast<int32_t>(chunk_offset));
                    ++i;
                }
                auto pin = segment->get_views_by_offsets<T>(
                    nullptr, field_id, run_chunk_id, local_offsets);
                const auto& values = pin.get().first;
                AssertInfo(values.size() == local_offsets.size(),
                           "legacy take returned {} views, expected {}",
                           values.size(),
                           local_offsets.size());
                ConsumeValues(values.data(),
                              run_start,
                              static_cast<int64_t>(values.size()),
                              &digest);
            }
        } else {
            int64_t cached_chunk_id = -1;
            std::optional<PinWrapper<Span<T>>> pin;
            for (int64_t i = window_start; i < window_end; ++i) {
                const auto [chunk_id, chunk_offset] =
                    segment->get_chunk_by_offset(field_id, offsets[i]);
                if (chunk_id != cached_chunk_id) {
                    pin.emplace(
                        segment->chunk_data<T>(nullptr, field_id, chunk_id));
                    cached_chunk_id = chunk_id;
                }
                const auto span = pin->get();
                digest = MixDigest(
                    digest, ValueDigest(span.data()[chunk_offset]), i);
            }
        }
    }
    return digest;
}

template <typename T>
uint64_t
InterfaceTake(RawColumnBenchmarkFixture& fixture,
              size_t field_index,
              TakePattern pattern) {
    auto* segment = fixture.segment();
    const auto field_id = fixture.field_id(field_index);
    const auto& offsets = fixture.offsets(pattern);
    auto column = segment->GetChunkedColumn(field_id);
    AssertInfo(column != nullptr, "interface take column is null");

    uint64_t digest = 0;
    for (int64_t window_start = 0;
         window_start < static_cast<int64_t>(offsets.size());
         window_start += kWindowRows) {
        const auto count =
            std::min<int64_t>(kWindowRows, offsets.size() - window_start);
        auto take = column->Take(nullptr,
                                 ChunkedColumnInterface::TakeOptions{
                                     ChunkedColumnInterface::OffsetView::From(
                                         offsets.data() + window_start, count),
                                     kFieldCases[field_index].target_type});
        AssertInfo(take != nullptr && take->Size() == count,
                   "Raw Take returned {} rows, expected {}",
                   take == nullptr ? -1 : take->Size(),
                   count);
        const auto items = take->Access<T>();
        for (int64_t i = 0; i < count; ++i) {
            const auto item = items[i];
            AssertInfo(item.is_valid && !item.data_skipped && item.value,
                       "Raw Take returned an unavailable benchmark row {}",
                       window_start + i);
            digest =
                MixDigest(digest, ValueDigest(*item.value), window_start + i);
        }
    }
    return digest;
}

uint64_t
MixCompareDigest(uint64_t digest, bool is_valid, bool value, int64_t position) {
    const auto encoded = is_valid ? 2ULL + static_cast<uint64_t>(value) : 0ULL;
    return MixDigest(digest, encoded, position);
}

template <typename T, typename U>
uint64_t
LegacyCompareTake(RawColumnBenchmarkFixture& fixture,
                  bool nullable,
                  const std::vector<int64_t>& offsets) {
    auto* segment = fixture.segment();
    const auto [left_field, right_field] = fixture.compare_field_ids(nullable);
    uint64_t digest = 0;

    for (int64_t window_start = 0;
         window_start < static_cast<int64_t>(offsets.size());
         window_start += kWindowRows) {
        const auto window_end =
            std::min<int64_t>(window_start + kWindowRows, offsets.size());
        int64_t cached_left_chunk_id = -1;
        int64_t cached_right_chunk_id = -1;
        std::optional<PinWrapper<Span<T>>> left_pin;
        std::optional<PinWrapper<Span<U>>> right_pin;
        const T* left_data = nullptr;
        const U* right_data = nullptr;
        ValidityView left_validity;
        ValidityView right_validity;

        for (int64_t i = window_start; i < window_end; ++i) {
            const auto [left_chunk_id, left_chunk_offset] =
                segment->get_chunk_by_offset(left_field, offsets[i]);
            const auto [right_chunk_id, right_chunk_offset] =
                segment->get_chunk_by_offset(right_field, offsets[i]);
            if (left_chunk_id != cached_left_chunk_id) {
                left_pin.emplace(
                    segment->chunk_data<T>(nullptr, left_field, left_chunk_id));
                const auto span = left_pin->get();
                left_data = span.data();
                left_validity = span.validity();
                cached_left_chunk_id = left_chunk_id;
            }
            if (right_chunk_id != cached_right_chunk_id) {
                right_pin.emplace(segment->chunk_data<U>(
                    nullptr, right_field, right_chunk_id));
                const auto span = right_pin->get();
                right_data = span.data();
                right_validity = span.validity();
                cached_right_chunk_id = right_chunk_id;
            }

            const auto is_valid =
                (!left_validity || left_validity[left_chunk_offset]) &&
                (!right_validity || right_validity[right_chunk_offset]);
            digest =
                MixCompareDigest(digest,
                                 is_valid,
                                 is_valid && left_data[left_chunk_offset] <
                                                 right_data[right_chunk_offset],
                                 i);
        }
    }
    return digest;
}

template <typename T, typename U>
uint64_t
InterfaceCompareTake(RawColumnBenchmarkFixture& fixture,
                     bool nullable,
                     const std::vector<int64_t>& offsets) {
    auto* segment = fixture.segment();
    const auto [left_field, right_field] = fixture.compare_field_ids(nullable);
    uint64_t digest = 0;

    for (int64_t window_start = 0;
         window_start < static_cast<int64_t>(offsets.size());
         window_start += kWindowRows) {
        const auto left_column = segment->GetChunkedColumn(left_field);
        const auto right_column = segment->GetChunkedColumn(right_field);
        AssertInfo(left_column != nullptr && right_column != nullptr,
                   "interface compare Take column is null");
        const auto count =
            std::min<int64_t>(kWindowRows, offsets.size() - window_start);
        const auto offset_view = ChunkedColumnInterface::OffsetView::From(
            offsets.data() + window_start, count);
        const auto left_take = left_column->Take(
            nullptr,
            ChunkedColumnInterface::TakeOptions{
                offset_view, ChunkedColumnInterface::TargetType::Int32});
        const auto right_take = right_column->Take(
            nullptr,
            ChunkedColumnInterface::TakeOptions{
                offset_view, ChunkedColumnInterface::TargetType::Int64});
        AssertInfo(left_take != nullptr && right_take != nullptr &&
                       left_take->Size() == count &&
                       right_take->Size() == count,
                   "invalid interface compare Take result");

        const auto left_items = left_take->Access<T>();
        const auto right_items = right_take->Access<U>();
        for (int64_t i = 0; i < count; ++i) {
            const auto left = left_items[i];
            const auto right = right_items[i];
            const auto is_valid = left.is_valid && right.is_valid;
            digest = MixCompareDigest(digest,
                                      is_valid,
                                      is_valid && *left.value < *right.value,
                                      window_start + i);
        }
    }
    return digest;
}

template <typename T, typename U>
uint64_t
RunCompareTake(RawColumnBenchmarkFixture& fixture,
               CompareTakePath path,
               bool nullable,
               TakePattern pattern,
               bool single_cell) {
    const auto& offsets = single_cell ? fixture.single_cell_offsets(pattern)
                                      : fixture.offsets(pattern);
    switch (path) {
        case CompareTakePath::LegacyChunk:
            return LegacyCompareTake<T, U>(fixture, nullable, offsets);
        case CompareTakePath::InterfaceTake:
            return InterfaceCompareTake<T, U>(fixture, nullable, offsets);
    }
    ThrowInfo(ErrorCode::UnexpectedError,
              "unknown Raw compare Take benchmark path");
}

template <typename T>
uint64_t
RunTypedAccess(RawColumnBenchmarkFixture& fixture,
               AccessPath path,
               size_t field_index,
               TakePattern take_pattern) {
    switch (path) {
        case AccessPath::LegacyScan:
            return LegacyScan<T>(fixture, field_index);
        case AccessPath::InterfaceScan:
            return InterfaceScan<T>(fixture, field_index);
        case AccessPath::LegacyTake:
            return LegacyTake<T>(fixture, field_index, take_pattern);
        case AccessPath::InterfaceTake:
            return InterfaceTake<T>(fixture, field_index, take_pattern);
    }
    ThrowInfo(ErrorCode::UnexpectedError,
              "unknown Raw column benchmark access path");
}

RawColumnBenchmarkFixture&
GetFixture() {
    static RawColumnBenchmarkFixture fixture;
    return fixture;
}

const char*
AccessPathName(AccessPath path) {
    switch (path) {
        case AccessPath::LegacyScan:
            return "LegacyChunkScan";
        case AccessPath::InterfaceScan:
            return "InterfaceScan";
        case AccessPath::LegacyTake:
            return "LegacyChunkTake";
        case AccessPath::InterfaceTake:
            return "InterfaceTake";
    }
    return "Unknown";
}

const char*
TakePatternName(TakePattern pattern) {
    switch (pattern) {
        case TakePattern::DenseOrdered:
            return "DenseOrdered";
        case TakePattern::SparseOrdered25:
            return "SparseOrdered25";
        case TakePattern::SparseShuffled25:
            return "SparseShuffled25";
    }
    return "Unknown";
}

int64_t
ItemsPerTraversal(RawColumnBenchmarkFixture& fixture,
                  AccessPath path,
                  TakePattern pattern) {
    if (path == AccessPath::LegacyScan || path == AccessPath::InterfaceScan) {
        return kRowCount;
    }
    return fixture.offsets(pattern).size();
}

void
RunBenchmark(benchmark::State& state,
             AccessPath path,
             size_t field_index,
             TakePattern take_pattern) {
    auto& fixture = GetFixture();
    const auto expected =
        fixture.expected_digest(field_index, path, take_pattern);

    for (auto _ : state) {
        const auto digest = kFieldCases[field_index].run(
            fixture, path, field_index, take_pattern);
        benchmark::DoNotOptimize(digest);
        if (digest != expected) {
            state.SkipWithError("Raw column traversal digest changed");
            break;
        }
    }
    state.SetItemsProcessed(state.iterations() *
                            ItemsPerTraversal(fixture, path, take_pattern));
}

int64_t
ThreadCpuTimeNanos() {
    timespec timestamp{};
    const auto status = clock_gettime(CLOCK_THREAD_CPUTIME_ID, &timestamp);
    AssertInfo(status == 0, "failed to read benchmark thread CPU time");
    return timestamp.tv_sec * 1'000'000'000LL + timestamp.tv_nsec;
}

void
RunPairedBenchmark(benchmark::State& state,
                   AccessPath legacy_path,
                   AccessPath interface_path,
                   size_t field_index,
                   TakePattern take_pattern) {
    auto& fixture = GetFixture();
    const auto expected =
        fixture.expected_digest(field_index, legacy_path, take_pattern);
    int64_t legacy_cpu_nanos = 0;
    int64_t interface_cpu_nanos = 0;
    int64_t pair_index = 0;

    const auto measure = [&](AccessPath path, int64_t* elapsed_nanos) {
        const auto start = ThreadCpuTimeNanos();
        const auto digest = kFieldCases[field_index].run(
            fixture, path, field_index, take_pattern);
        const auto end = ThreadCpuTimeNanos();
        benchmark::DoNotOptimize(digest);
        *elapsed_nanos += end - start;
        return digest == expected;
    };

    for (auto _ : state) {
        const auto interface_first = (pair_index++ & 1) != 0;
        bool valid = false;
        if (interface_first) {
            valid = measure(interface_path, &interface_cpu_nanos) &&
                    measure(legacy_path, &legacy_cpu_nanos);
        } else {
            valid = measure(legacy_path, &legacy_cpu_nanos) &&
                    measure(interface_path, &interface_cpu_nanos);
        }
        if (!valid) {
            state.SkipWithError("Raw column paired traversal digest changed");
            break;
        }
    }

    const auto iterations = state.iterations();
    if (iterations > 0 && legacy_cpu_nanos > 0) {
        state.counters["legacy_cpu_us"] =
            benchmark::Counter(static_cast<double>(legacy_cpu_nanos) / 1'000.0,
                               benchmark::Counter::kAvgIterations);
        state.counters["interface_cpu_us"] = benchmark::Counter(
            static_cast<double>(interface_cpu_nanos) / 1'000.0,
            benchmark::Counter::kAvgIterations);
        state.counters["interface_over_legacy"] =
            static_cast<double>(interface_cpu_nanos) / legacy_cpu_nanos;
    }
    state.SetItemsProcessed(
        iterations * ItemsPerTraversal(fixture, legacy_path, take_pattern) * 2);
}

void
RunPairedCompareTakeBenchmark(benchmark::State& state,
                              bool nullable,
                              TakePattern pattern,
                              bool single_cell) {
    auto& fixture = GetFixture();
    constexpr auto legacy_path = CompareTakePath::LegacyChunk;
    constexpr auto interface_path = CompareTakePath::InterfaceTake;
    const auto expected = RunCompareTake<int32_t, int64_t>(
        fixture, legacy_path, nullable, pattern, single_cell);
    AssertInfo((RunCompareTake<int32_t, int64_t>(
                    fixture, interface_path, nullable, pattern, single_cell) ==
                expected),
               "Raw compare Take digest differs from legacy path");

    int64_t legacy_cpu_nanos = 0;
    int64_t interface_cpu_nanos = 0;
    int64_t pair_index = 0;
    const auto measure = [&](CompareTakePath path, int64_t* elapsed_nanos) {
        const auto start = ThreadCpuTimeNanos();
        const auto digest = RunCompareTake<int32_t, int64_t>(
            fixture, path, nullable, pattern, single_cell);
        const auto end = ThreadCpuTimeNanos();
        benchmark::DoNotOptimize(digest);
        *elapsed_nanos += end - start;
        return digest == expected;
    };

    for (auto _ : state) {
        const auto interface_first = (pair_index++ & 1) != 0;
        const auto valid =
            interface_first ? measure(interface_path, &interface_cpu_nanos) &&
                                  measure(legacy_path, &legacy_cpu_nanos)
                            : measure(legacy_path, &legacy_cpu_nanos) &&
                                  measure(interface_path, &interface_cpu_nanos);
        if (!valid) {
            state.SkipWithError("Raw compare Take digest changed");
            break;
        }
    }

    const auto iterations = state.iterations();
    if (iterations > 0 && legacy_cpu_nanos > 0) {
        state.counters["legacy_cpu_us"] =
            benchmark::Counter(static_cast<double>(legacy_cpu_nanos) / 1'000.0,
                               benchmark::Counter::kAvgIterations);
        state.counters["interface_cpu_us"] = benchmark::Counter(
            static_cast<double>(interface_cpu_nanos) / 1'000.0,
            benchmark::Counter::kAvgIterations);
        state.counters["interface_over_legacy"] =
            static_cast<double>(interface_cpu_nanos) / legacy_cpu_nanos;
    }
    const auto item_count = single_cell
                                ? fixture.single_cell_offsets(pattern).size()
                                : fixture.offsets(pattern).size();
    state.SetItemsProcessed(iterations * item_count * 2);
}

bool
RegisterBenchmarks() {
    for (size_t field_index = 0; field_index < kFieldCases.size();
         ++field_index) {
        for (const auto path :
             {AccessPath::LegacyScan, AccessPath::InterfaceScan}) {
            const auto name = std::string("RawColumn/") + AccessPathName(path) +
                              "/" + kFieldCases[field_index].name;
            benchmark::RegisterBenchmark(
                name.c_str(), [path, field_index](benchmark::State& state) {
                    RunBenchmark(
                        state, path, field_index, TakePattern::DenseOrdered);
                });
        }

        const auto scan_name = std::string("RawColumn/PairedScan/") +
                               kFieldCases[field_index].name;
        benchmark::RegisterBenchmark(
            scan_name.c_str(), [field_index](benchmark::State& state) {
                RunPairedBenchmark(state,
                                   AccessPath::LegacyScan,
                                   AccessPath::InterfaceScan,
                                   field_index,
                                   TakePattern::DenseOrdered);
            });

        for (const auto pattern : kTakePatterns) {
            for (const auto path :
                 {AccessPath::LegacyTake, AccessPath::InterfaceTake}) {
                const auto name = std::string("RawColumn/") +
                                  AccessPathName(path) + "/" +
                                  TakePatternName(pattern) + "/" +
                                  kFieldCases[field_index].name;
                benchmark::RegisterBenchmark(
                    name.c_str(),
                    [path, field_index, pattern](benchmark::State& state) {
                        RunBenchmark(state, path, field_index, pattern);
                    });
            }

            const auto paired_name = std::string("RawColumn/PairedTake/") +
                                     TakePatternName(pattern) + "/" +
                                     kFieldCases[field_index].name;
            benchmark::RegisterBenchmark(
                paired_name.c_str(),
                [field_index, pattern](benchmark::State& state) {
                    RunPairedBenchmark(state,
                                       AccessPath::LegacyTake,
                                       AccessPath::InterfaceTake,
                                       field_index,
                                       pattern);
                });
        }
    }
    for (const auto nullable : {false, true}) {
        for (const auto pattern : kTakePatterns) {
            const auto name = std::string("RawColumn/PairedCompareTake/") +
                              (nullable ? "Nullable/" : "NonNullable/") +
                              TakePatternName(pattern) + "/INT32_INT64";
            benchmark::RegisterBenchmark(
                name.c_str(), [nullable, pattern](benchmark::State& state) {
                    RunPairedCompareTakeBenchmark(
                        state, nullable, pattern, /*single_cell=*/false);
                });

            const auto single_cell_name =
                std::string("RawColumn/PairedCompareTakeFirstCell/") +
                (nullable ? "Nullable/" : "NonNullable/") +
                TakePatternName(pattern) + "/INT32_INT64";
            benchmark::RegisterBenchmark(
                single_cell_name.c_str(),
                [nullable, pattern](benchmark::State& state) {
                    RunPairedCompareTakeBenchmark(
                        state, nullable, pattern, /*single_cell=*/true);
                });
        }
    }
    return true;
}

[[maybe_unused]] const bool kBenchmarksRegistered = RegisterBenchmarks();

std::filesystem::path g_benchmark_root;

void
InitializeMilvusBenchmarkEnvironment() {
    const auto unique =
        std::chrono::steady_clock::now().time_since_epoch().count();
    g_benchmark_root =
        std::filesystem::temp_directory_path() /
        ("milvus_raw_column_benchmark_" + std::to_string(unique));
    TestLocalPath = (g_benchmark_root / "local").string() + "/";
    TestRemotePath = (g_benchmark_root / "remote").string() + "/";
    TestMmapPath = (g_benchmark_root / "mmap").string() + "/";
    std::filesystem::create_directories(TestLocalPath);
    std::filesystem::create_directories(TestRemotePath);
    std::filesystem::create_directories(TestMmapPath);

    storage::LocalChunkManagerSingleton::GetInstance().Init(TestLocalPath);
    storage::RemoteChunkManagerSingleton::GetInstance().Init(
        get_default_local_storage_config());
    storage::MmapManager::GetInstance().Init(get_default_mmap_config());

    CStorageConfig arrow_fs_config = {};
    arrow_fs_config.root_path = TestLocalPath.c_str();
    arrow_fs_config.storage_type = "local";
    const auto status = InitArrowFileSystem(arrow_fs_config);
    AssertInfo(status.error_code == 0,
               "failed to initialize benchmark Arrow filesystem: {}",
               status.error_msg);

    constexpr int64_t mb = 1024 * 1024;
    cachinglayer::Manager::ConfigureTieredStorage(
        {CacheWarmupPolicy::CacheWarmupPolicy_Disable,
         CacheWarmupPolicy::CacheWarmupPolicy_Disable,
         CacheWarmupPolicy::CacheWarmupPolicy_Disable,
         CacheWarmupPolicy::CacheWarmupPolicy_Disable},
        {1024 * mb, 1024 * mb, 1024 * mb, 1024 * mb, 1024 * mb, 1024 * mb},
        true,
        true,
        {10, true, 30},
        std::chrono::milliseconds(0),
        std::chrono::milliseconds(-1));
}

}  // namespace
}  // namespace milvus::benchmark_test

int
main(int argc, char** argv) {
    benchmark::Initialize(&argc, argv);
    folly::Init folly_init(&argc, &argv, false);
    milvus::benchmark_test::InitializeMilvusBenchmarkEnvironment();
    benchmark::DoNotOptimize(milvus::benchmark_test::GetFixture());
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    std::filesystem::remove_all(milvus::benchmark_test::g_benchmark_root);
    return 0;
}
