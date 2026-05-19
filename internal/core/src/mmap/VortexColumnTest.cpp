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

#include "mmap/VortexColumn.h"

#include <cmath>
#include <filesystem>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <unistd.h>

#include "arrow/api.h"
#include "arrow/filesystem/localfs.h"
#include "common/FieldMeta.h"
#include "gtest/gtest.h"
#include "milvus-storage/column_groups.h"
#include "milvus-storage/format/vortex/vortex_writer.h"
#include "milvus-storage/properties.h"

namespace milvus {
namespace {

constexpr int64_t kIntFieldId = 101;
constexpr int64_t kStringFieldId = 102;
constexpr int64_t kNullableFieldIdBase = 200;
constexpr int64_t kNullableRows = 16;

std::shared_ptr<arrow::Schema>
MakeSchema() {
    return arrow::schema({
        arrow::field(std::to_string(kIntFieldId), arrow::int32(), false),
        arrow::field(std::to_string(kStringFieldId), arrow::binary(), false),
    });
}

std::shared_ptr<arrow::RecordBatch>
MakeRecordBatch(int64_t begin, int64_t count) {
    arrow::Int32Builder int_builder;
    arrow::BinaryBuilder string_builder;
    EXPECT_TRUE(int_builder.Reserve(count).ok());
    EXPECT_TRUE(string_builder.Reserve(count).ok());
    for (int64_t i = begin; i < begin + count; ++i) {
        EXPECT_TRUE(int_builder.Append(static_cast<int32_t>(i * 10)).ok());
        auto value = "v" + std::to_string(i);
        EXPECT_TRUE(string_builder.Append(value).ok());
    }

    std::shared_ptr<arrow::Array> int_array;
    std::shared_ptr<arrow::Array> string_array;
    EXPECT_TRUE(int_builder.Finish(&int_array).ok());
    EXPECT_TRUE(string_builder.Finish(&string_array).ok());
    return arrow::RecordBatch::Make(
        MakeSchema(), count, {std::move(int_array), std::move(string_array)});
}

milvus_storage::api::Properties
MakeProperties() {
    milvus_storage::api::Properties properties;
    properties[PROPERTY_FS_STORAGE_TYPE] = std::string("local");
    properties[PROPERTY_FS_ROOT_PATH] = std::string("/");
    return properties;
}

VortexColumn::FileInfo
WriteVortexFile(const std::string& path,
                const std::shared_ptr<arrow::Schema>& schema,
                const milvus_storage::api::Properties& properties,
                int64_t begin = 0) {
    auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
    milvus_storage::vortex::VortexFileWriter writer(
        fs, schema, path, properties);
    EXPECT_TRUE(writer.Write(MakeRecordBatch(begin, 8)).ok());
    EXPECT_TRUE(writer.Write(MakeRecordBatch(begin + 8, 8)).ok());
    EXPECT_TRUE(writer.Flush().ok());
    auto close_result = writer.Close();
    EXPECT_TRUE(close_result.ok());
    auto cg_file = close_result.ValueOrDie();

    VortexColumn::FileInfo info;
    info.path = path;
    info.start_index = begin;
    info.end_index = begin + 16;
    info.file_size =
        cg_file.Get<uint64_t>(milvus_storage::api::kPropertyFileSize, 0);
    info.footer_size =
        cg_file.Get<uint64_t>(milvus_storage::api::kPropertyFooterSize, 0);
    return info;
}

std::vector<uint64_t>
CollectScanRows(std::unique_ptr<VortexColumn::ScanCursor> cursor) {
    std::vector<uint64_t> rows;
    VortexColumn::ScanBatch batch;
    while (cursor->Next(&batch)) {
        auto row_ids = std::static_pointer_cast<arrow::UInt64Array>(
            batch.batch->column(0));
        rows.reserve(rows.size() + row_ids->length());
        for (int64_t i = 0; i < row_ids->length(); ++i) {
            if (!row_ids->IsNull(i)) {
                rows.emplace_back(batch.row_id_base + row_ids->Value(i));
            }
        }
    }
    return rows;
}

bool
ExpectedValid(int64_t row) {
    return row % 4 != 1;
}

std::string
ExpectedString(DataType type, int64_t row) {
    switch (type) {
        case DataType::STRING:
            return "string_" + std::to_string(row);
        case DataType::VARCHAR:
            return "varchar_" + std::to_string(row);
        case DataType::TEXT:
            return "text_" + std::to_string(row);
        case DataType::JSON:
            return "{\"row\":" + std::to_string(row) + "}";
        case DataType::GEOMETRY:
            return "geometry_wkb_" + std::to_string(row);
        default:
            return {};
    }
}

FieldMeta
MakeNullableFieldMeta(FieldId field_id, DataType type) {
    auto name = FieldName("nullable_" + std::to_string(field_id.get()));
    switch (type) {
        case DataType::STRING:
        case DataType::VARCHAR:
        case DataType::TEXT:
            return FieldMeta(name, field_id, type, 256, true, std::nullopt);
        case DataType::ARRAY:
            return FieldMeta(name,
                             field_id,
                             type,
                             DataType::INT64,
                             true,
                             std::nullopt);
        default:
            return FieldMeta(name, field_id, type, true, std::nullopt);
    }
}

std::shared_ptr<arrow::DataType>
ArrowTypeForNullableField(DataType type) {
    switch (type) {
        case DataType::BOOL:
            return arrow::boolean();
        case DataType::INT8:
            return arrow::int8();
        case DataType::INT16:
            return arrow::int16();
        case DataType::INT32:
            return arrow::int32();
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            return arrow::int64();
        case DataType::FLOAT:
            return arrow::float32();
        case DataType::DOUBLE:
            return arrow::float64();
        case DataType::STRING:
        case DataType::VARCHAR:
        case DataType::TEXT:
        case DataType::JSON:
        case DataType::GEOMETRY:
            return arrow::binary();
        case DataType::ARRAY:
            return arrow::list(arrow::int64());
        default:
            return arrow::null();
    }
}

std::vector<DataType>
NullableLocalVortexTypes() {
    return {DataType::BOOL,
            DataType::INT8,
            DataType::INT16,
            DataType::INT32,
            DataType::INT64,
            DataType::FLOAT,
            DataType::DOUBLE,
            DataType::TIMESTAMPTZ,
            DataType::STRING,
            DataType::VARCHAR,
            DataType::TEXT,
            DataType::JSON,
            DataType::GEOMETRY,
            DataType::ARRAY};
}

std::shared_ptr<arrow::Schema>
MakeNullableSchema() {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    auto types = NullableLocalVortexTypes();
    fields.reserve(types.size());
    for (size_t i = 0; i < types.size(); ++i) {
        const auto field_id = kNullableFieldIdBase + static_cast<int64_t>(i);
        fields.emplace_back(arrow::field(std::to_string(field_id),
                                         ArrowTypeForNullableField(types[i]),
                                         true));
    }
    return arrow::schema(std::move(fields));
}

std::shared_ptr<arrow::Array>
BuildNullableArray(DataType type, int64_t begin, int64_t count) {
    switch (type) {
        case DataType::BOOL: {
            arrow::BooleanBuilder builder;
            for (int64_t row = begin; row < begin + count; ++row) {
                if (!ExpectedValid(row)) {
                    EXPECT_TRUE(builder.AppendNull().ok());
                } else {
                    EXPECT_TRUE(builder.Append(row % 2 == 0).ok());
                }
            }
            std::shared_ptr<arrow::Array> array;
            EXPECT_TRUE(builder.Finish(&array).ok());
            return array;
        }
        case DataType::INT8: {
            arrow::Int8Builder builder;
            for (int64_t row = begin; row < begin + count; ++row) {
                if (!ExpectedValid(row)) {
                    EXPECT_TRUE(builder.AppendNull().ok());
                } else {
                    EXPECT_TRUE(builder.Append(static_cast<int8_t>(row - 8)).ok());
                }
            }
            std::shared_ptr<arrow::Array> array;
            EXPECT_TRUE(builder.Finish(&array).ok());
            return array;
        }
        case DataType::INT16: {
            arrow::Int16Builder builder;
            for (int64_t row = begin; row < begin + count; ++row) {
                if (!ExpectedValid(row)) {
                    EXPECT_TRUE(builder.AppendNull().ok());
                } else {
                    EXPECT_TRUE(builder.Append(static_cast<int16_t>(row * 10)).ok());
                }
            }
            std::shared_ptr<arrow::Array> array;
            EXPECT_TRUE(builder.Finish(&array).ok());
            return array;
        }
        case DataType::INT32: {
            arrow::Int32Builder builder;
            for (int64_t row = begin; row < begin + count; ++row) {
                if (!ExpectedValid(row)) {
                    EXPECT_TRUE(builder.AppendNull().ok());
                } else {
                    EXPECT_TRUE(builder.Append(static_cast<int32_t>(row * 100)).ok());
                }
            }
            std::shared_ptr<arrow::Array> array;
            EXPECT_TRUE(builder.Finish(&array).ok());
            return array;
        }
        case DataType::INT64:
        case DataType::TIMESTAMPTZ: {
            arrow::Int64Builder builder;
            for (int64_t row = begin; row < begin + count; ++row) {
                if (!ExpectedValid(row)) {
                    EXPECT_TRUE(builder.AppendNull().ok());
                } else {
                    const auto value = type == DataType::TIMESTAMPTZ
                                           ? 1700000000000000LL + row
                                           : row * 1000;
                    EXPECT_TRUE(builder.Append(value).ok());
                }
            }
            std::shared_ptr<arrow::Array> array;
            EXPECT_TRUE(builder.Finish(&array).ok());
            return array;
        }
        case DataType::FLOAT: {
            arrow::FloatBuilder builder;
            for (int64_t row = begin; row < begin + count; ++row) {
                if (!ExpectedValid(row)) {
                    EXPECT_TRUE(builder.AppendNull().ok());
                } else {
                    EXPECT_TRUE(builder.Append(static_cast<float>(row) * 1.5f).ok());
                }
            }
            std::shared_ptr<arrow::Array> array;
            EXPECT_TRUE(builder.Finish(&array).ok());
            return array;
        }
        case DataType::DOUBLE: {
            arrow::DoubleBuilder builder;
            for (int64_t row = begin; row < begin + count; ++row) {
                if (!ExpectedValid(row)) {
                    EXPECT_TRUE(builder.AppendNull().ok());
                } else {
                    EXPECT_TRUE(builder.Append(static_cast<double>(row) * 2.25).ok());
                }
            }
            std::shared_ptr<arrow::Array> array;
            EXPECT_TRUE(builder.Finish(&array).ok());
            return array;
        }
        case DataType::STRING:
        case DataType::VARCHAR:
        case DataType::TEXT:
        case DataType::JSON:
        case DataType::GEOMETRY: {
            arrow::BinaryBuilder builder;
            for (int64_t row = begin; row < begin + count; ++row) {
                if (!ExpectedValid(row)) {
                    EXPECT_TRUE(builder.AppendNull().ok());
                } else {
                    EXPECT_TRUE(builder.Append(ExpectedString(type, row)).ok());
                }
            }
            std::shared_ptr<arrow::Array> array;
            EXPECT_TRUE(builder.Finish(&array).ok());
            return array;
        }
        case DataType::ARRAY: {
            auto value_builder = std::make_shared<arrow::Int64Builder>();
            arrow::ListBuilder builder(arrow::default_memory_pool(),
                                       value_builder);
            auto* values = static_cast<arrow::Int64Builder*>(
                builder.value_builder());
            for (int64_t row = begin; row < begin + count; ++row) {
                if (!ExpectedValid(row)) {
                    EXPECT_TRUE(builder.AppendNull().ok());
                } else {
                    EXPECT_TRUE(builder.Append().ok());
                    EXPECT_TRUE(values->Append(row).ok());
                    EXPECT_TRUE(values->Append(row + 1).ok());
                    EXPECT_TRUE(values->Append(row + 2).ok());
                }
            }
            std::shared_ptr<arrow::Array> array;
            EXPECT_TRUE(builder.Finish(&array).ok());
            return array;
        }
        default:
            return nullptr;
    }
}

std::shared_ptr<arrow::RecordBatch>
MakeNullableRecordBatch(int64_t begin, int64_t count) {
    auto types = NullableLocalVortexTypes();
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(types.size());
    for (auto type : types) {
        arrays.emplace_back(BuildNullableArray(type, begin, count));
    }
    return arrow::RecordBatch::Make(
        MakeNullableSchema(), count, std::move(arrays));
}

VortexColumn::FileInfo
WriteNullableVortexFile(
    const std::string& path,
    const std::shared_ptr<arrow::Schema>& schema,
    const milvus_storage::api::Properties& properties) {
    auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
    milvus_storage::vortex::VortexFileWriter writer(
        fs, schema, path, properties);
    EXPECT_TRUE(writer.Write(MakeNullableRecordBatch(0, 8)).ok());
    EXPECT_TRUE(writer.Write(MakeNullableRecordBatch(8, 8)).ok());
    EXPECT_TRUE(writer.Flush().ok());
    auto close_result = writer.Close();
    EXPECT_TRUE(close_result.ok());
    auto cg_file = close_result.ValueOrDie();

    VortexColumn::FileInfo info;
    info.path = path;
    info.start_index = 0;
    info.end_index = kNullableRows;
    info.file_size =
        cg_file.Get<uint64_t>(milvus_storage::api::kPropertyFileSize, 0);
    info.footer_size =
        cg_file.Get<uint64_t>(milvus_storage::api::kPropertyFooterSize, 0);
    return info;
}

ChunkedColumnInterface::ScanValueKind
ScanKindForType(DataType type) {
    switch (type) {
        case DataType::JSON:
            return ChunkedColumnInterface::ScanValueKind::JsonView;
        case DataType::STRING:
        case DataType::VARCHAR:
        case DataType::TEXT:
        case DataType::GEOMETRY:
            return ChunkedColumnInterface::ScanValueKind::StringView;
        case DataType::ARRAY:
            return ChunkedColumnInterface::ScanValueKind::ArrayView;
        default:
            return ChunkedColumnInterface::ScanValueKind::FixedWidth;
    }
}

VortexColumn
MakeNullableColumn(DataType type,
                   FieldId field_id,
                   const VortexColumn::FileInfo& file_info,
                   const std::shared_ptr<arrow::Schema>& schema,
                   const std::shared_ptr<milvus_storage::api::Properties>&
                       properties) {
    return VortexColumn(field_id,
                        MakeNullableFieldMeta(field_id, type),
                        {file_info},
                        schema,
                        properties,
                        CacheWarmupPolicy::CacheWarmupPolicy_Disable,
                        nullptr);
}

void
CheckValidityOnlyScan(VortexColumn& column) {
    ChunkedColumnInterface::ScanOptions options;
    options.output = ChunkedColumnInterface::ScanOutput::Data;
    options.projection = ChunkedColumnInterface::ScanProjection::ValidityOnly;
    options.start_offset = 0;
    options.length = kNullableRows;
    options.max_batch_rows = 3;

    auto result = column.Scan(nullptr, options);
    ASSERT_NE(result.data_cursor, nullptr);
    ChunkedColumnInterface::DataScanBatch batch;
    int64_t seen = 0;
    while (result.data_cursor->Next(&batch)) {
        EXPECT_TRUE(batch.values.empty());
        for (int64_t i = 0; i < batch.size; ++i) {
            const auto row = batch.row_id_start + i;
            EXPECT_EQ(batch.validity.IsValid(i), ExpectedValid(row)) << row;
        }
        seen += batch.size;
    }
    EXPECT_EQ(seen, kNullableRows);
}

template <typename T>
void
CheckFixedWidthBatch(DataType type,
                     const ChunkedColumnInterface::DataScanBatch& batch) {
    const auto* values = batch.values.data_as<T>();
    for (int64_t i = 0; i < batch.size; ++i) {
        const auto row = batch.row_id_start + i;
        EXPECT_EQ(batch.validity.IsValid(i), ExpectedValid(row)) << row;
        if (!ExpectedValid(row)) {
            continue;
        }
        if constexpr (std::is_same_v<T, bool>) {
            EXPECT_EQ(values[i], row % 2 == 0) << row;
        } else if constexpr (std::is_same_v<T, int8_t>) {
            EXPECT_EQ(values[i], static_cast<int8_t>(row - 8)) << row;
        } else if constexpr (std::is_same_v<T, int16_t>) {
            EXPECT_EQ(values[i], static_cast<int16_t>(row * 10)) << row;
        } else if constexpr (std::is_same_v<T, int32_t>) {
            EXPECT_EQ(values[i], static_cast<int32_t>(row * 100)) << row;
        } else if constexpr (std::is_same_v<T, int64_t>) {
            const auto expected = type == DataType::TIMESTAMPTZ
                                      ? 1700000000000000LL + row
                                      : row * 1000;
            EXPECT_EQ(values[i], expected) << row;
        } else if constexpr (std::is_same_v<T, float>) {
            EXPECT_NEAR(values[i], static_cast<float>(row) * 1.5f, 1e-5)
                << row;
        } else if constexpr (std::is_same_v<T, double>) {
            EXPECT_NEAR(values[i], static_cast<double>(row) * 2.25, 1e-9)
                << row;
        }
    }
}

void
CheckStringLikeBatch(DataType type,
                     const ChunkedColumnInterface::DataScanBatch& batch) {
    if (type == DataType::JSON) {
        const auto* values = batch.values.data_as<Json>();
        for (int64_t i = 0; i < batch.size; ++i) {
            const auto row = batch.row_id_start + i;
            EXPECT_EQ(batch.validity.IsValid(i), ExpectedValid(row)) << row;
            if (ExpectedValid(row)) {
                std::string_view view = values[i];
                EXPECT_EQ(view, ExpectedString(type, row)) << row;
            }
        }
        return;
    }

    const auto* values = batch.values.data_as<std::string_view>();
    for (int64_t i = 0; i < batch.size; ++i) {
        const auto row = batch.row_id_start + i;
        EXPECT_EQ(batch.validity.IsValid(i), ExpectedValid(row)) << row;
        if (ExpectedValid(row)) {
            EXPECT_EQ(values[i], ExpectedString(type, row)) << row;
        }
    }
}

void
CheckArrayBatch(const ChunkedColumnInterface::DataScanBatch& batch) {
    const auto* values = batch.values.data_as<ArrayView>();
    for (int64_t i = 0; i < batch.size; ++i) {
        const auto row = batch.row_id_start + i;
        EXPECT_EQ(batch.validity.IsValid(i), ExpectedValid(row)) << row;
        if (!ExpectedValid(row)) {
            continue;
        }
        EXPECT_EQ(values[i].length(), 3) << row;
        EXPECT_EQ(values[i].get_data<int64_t>(0), row) << row;
        EXPECT_EQ(values[i].get_data<int64_t>(1), row + 1) << row;
        EXPECT_EQ(values[i].get_data<int64_t>(2), row + 2) << row;
    }
}

void
CheckDataScan(VortexColumn& column, DataType type) {
    ChunkedColumnInterface::ScanOptions options;
    options.output = ChunkedColumnInterface::ScanOutput::Data;
    options.projection = ChunkedColumnInterface::ScanProjection::Data;
    options.start_offset = 0;
    options.length = kNullableRows;
    options.max_batch_rows = 5;
    options.value_kind = ScanKindForType(type);

    auto result = column.Scan(nullptr, options);
    ASSERT_NE(result.data_cursor, nullptr);

    ChunkedColumnInterface::DataScanBatch batch;
    int64_t seen = 0;
    while (result.data_cursor->Next(&batch)) {
        ASSERT_GT(batch.size, 0);
        EXPECT_TRUE(batch.validity.nullable);
        switch (type) {
            case DataType::BOOL:
                CheckFixedWidthBatch<bool>(type, batch);
                break;
            case DataType::INT8:
                CheckFixedWidthBatch<int8_t>(type, batch);
                break;
            case DataType::INT16:
                CheckFixedWidthBatch<int16_t>(type, batch);
                break;
            case DataType::INT32:
                CheckFixedWidthBatch<int32_t>(type, batch);
                break;
            case DataType::INT64:
            case DataType::TIMESTAMPTZ:
                CheckFixedWidthBatch<int64_t>(type, batch);
                break;
            case DataType::FLOAT:
                CheckFixedWidthBatch<float>(type, batch);
                break;
            case DataType::DOUBLE:
                CheckFixedWidthBatch<double>(type, batch);
                break;
            case DataType::STRING:
            case DataType::VARCHAR:
            case DataType::TEXT:
            case DataType::JSON:
            case DataType::GEOMETRY:
                CheckStringLikeBatch(type, batch);
                break;
            case DataType::ARRAY:
                CheckArrayBatch(batch);
                break;
            default:
                FAIL() << "unexpected data type";
        }
        seen += batch.size;
    }
    EXPECT_EQ(seen, kNullableRows);
}

}  // namespace

TEST(VortexColumnTest, ScanAndTake) {
    auto schema = MakeSchema();
    auto properties =
        std::make_shared<milvus_storage::api::Properties>(MakeProperties());

    auto dir =
        std::filesystem::temp_directory_path() /
        ("milvus_vortex_column_test_" + std::to_string(::getpid()) + "_" +
         std::to_string(reinterpret_cast<uintptr_t>(properties.get())));
    std::filesystem::create_directories(dir);
    auto path = (dir / "cg0.vx").string();

    auto file_info = WriteVortexFile(path, schema, *properties);

    FieldMeta int_meta(FieldName("int_field"),
                       FieldId(kIntFieldId),
                       DataType::INT32,
                       false,
                       std::nullopt);
    VortexColumn int_column(
        FieldId(kIntFieldId),
        int_meta,
        {file_info},
        schema,
        properties,
        CacheWarmupPolicy::CacheWarmupPolicy_Disable,
        nullptr);
    EXPECT_EQ(int_column.NumRows(), 16);
    EXPECT_EQ(int_column.num_chunks(), 1);

    std::vector<int64_t> offsets{7, 1, 7, 15};
    std::vector<int32_t> values(offsets.size());
    int_column.BulkPrimitiveValueAt(
        nullptr, values.data(), offsets.data(), offsets.size(), false);
    EXPECT_EQ(values, (std::vector<int32_t>{70, 10, 70, 150}));

    auto row_ids = CollectScanRows(int_column.Scan(nullptr, 3, 5));
    EXPECT_EQ(row_ids, (std::vector<uint64_t>{3, 4, 5, 6, 7}));

    proto::plan::GenericValue filter_value;
    filter_value.set_int64_val(80);
    auto filter = int_column.BuildUnaryRangeFilter(proto::plan::GreaterEqual,
                                                   filter_value);
    ASSERT_TRUE(filter.has_value());
    row_ids = CollectScanRows(
        int_column.Scan(nullptr, 0, int_column.NumRows(), filter));
    EXPECT_EQ(row_ids, (std::vector<uint64_t>{8, 9, 10, 11, 12, 13, 14, 15}));

    FieldMeta string_meta(FieldName("string_field"),
                          FieldId(kStringFieldId),
                          DataType::VARCHAR,
                          128,
                          false,
                          std::nullopt);
    VortexColumn string_column(
        FieldId(kStringFieldId),
        string_meta,
        {file_info},
        schema,
        properties,
        CacheWarmupPolicy::CacheWarmupPolicy_Disable,
        nullptr);

    std::vector<std::string> strings(offsets.size());
    string_column.BulkRawStringAt(
        nullptr,
        [&](std::string_view value, size_t index, bool valid) {
            EXPECT_TRUE(valid);
            strings[index] = std::string(value);
        },
        offsets.data(),
        offsets.size());
    EXPECT_EQ(strings, (std::vector<std::string>{"v7", "v1", "v7", "v15"}));

    std::filesystem::remove_all(dir);
}

TEST(VortexColumnTest, MultiFileTakeAndScan) {
    auto schema = MakeSchema();
    auto properties =
        std::make_shared<milvus_storage::api::Properties>(MakeProperties());

    auto dir =
        std::filesystem::temp_directory_path() /
        ("milvus_vortex_column_multifile_test_" + std::to_string(::getpid()) +
         "_" + std::to_string(reinterpret_cast<uintptr_t>(properties.get())));
    std::filesystem::create_directories(dir);

    auto file0 =
        WriteVortexFile((dir / "cg0.vx").string(), schema, *properties, 0);
    auto file1 =
        WriteVortexFile((dir / "cg1.vx").string(), schema, *properties, 16);

    FieldMeta int_meta(FieldName("int_field"),
                       FieldId(kIntFieldId),
                       DataType::INT32,
                       false,
                       std::nullopt);
    VortexColumn int_column(
        FieldId(kIntFieldId),
        int_meta,
        {file0, file1},
        schema,
        properties,
        CacheWarmupPolicy::CacheWarmupPolicy_Disable,
        nullptr);
    EXPECT_EQ(int_column.NumRows(), 32);
    EXPECT_EQ(int_column.num_chunks(), 2);
    EXPECT_EQ(int_column.chunk_row_nums(0), 16);
    EXPECT_EQ(int_column.chunk_row_nums(1), 16);

    std::vector<int64_t> offsets{0, 15, 16, 17, 31, 16};
    std::vector<int32_t> values(offsets.size());
    int_column.BulkPrimitiveValueAt(
        nullptr, values.data(), offsets.data(), offsets.size(), false);
    EXPECT_EQ(values, (std::vector<int32_t>{0, 150, 160, 170, 310, 160}));

    auto row_ids = CollectScanRows(int_column.Scan(nullptr, 18, 4));
    EXPECT_EQ(row_ids, (std::vector<uint64_t>{18, 19, 20, 21}));

    proto::plan::GenericValue filter_value;
    filter_value.set_int64_val(170);
    auto filter = int_column.BuildUnaryRangeFilter(proto::plan::GreaterEqual,
                                                   filter_value);
    ASSERT_TRUE(filter.has_value());
    row_ids = CollectScanRows(int_column.Scan(nullptr, 12, 10, filter));
    EXPECT_EQ(row_ids, (std::vector<uint64_t>{17, 18, 19, 20, 21}));

    std::filesystem::remove_all(dir);
}

TEST(VortexColumnTest, NullableAllScalarTypesScanCorrectness) {
    auto schema = MakeNullableSchema();
    auto properties =
        std::make_shared<milvus_storage::api::Properties>(MakeProperties());

    auto dir =
        std::filesystem::temp_directory_path() /
        ("milvus_vortex_column_nullable_test_" + std::to_string(::getpid()) +
         "_" + std::to_string(reinterpret_cast<uintptr_t>(properties.get())));
    std::filesystem::create_directories(dir);

    auto file_info =
        WriteNullableVortexFile((dir / "nullable.vx").string(),
                                schema,
                                *properties);

    auto types = NullableLocalVortexTypes();
    for (size_t i = 0; i < types.size(); ++i) {
        const auto type = types[i];
        FieldId field_id(kNullableFieldIdBase + static_cast<int64_t>(i));
        auto column = MakeNullableColumn(
            type, field_id, file_info, schema, properties);

        ASSERT_EQ(column.NumRows(), kNullableRows);
        ASSERT_TRUE(column.IsNullable());
        CheckValidityOnlyScan(column);
        CheckDataScan(column, type);
    }

    std::filesystem::remove_all(dir);
}

}  // namespace milvus
