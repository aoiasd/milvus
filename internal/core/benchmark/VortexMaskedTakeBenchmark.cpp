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
#include <chrono>
#include <ctime>
#include <filesystem>
#include <random>
#include <string>
#include <vector>

#include "RawJsonTakeBenchmark.h"
#include "arrow/api.h"
#include "arrow/filesystem/localfs.h"
#include "common/Json.h"
#include "mmap/VortexColumn.h"
#include "milvus-storage/format/vortex/vortex_writer.h"

extern std::string TestLocalPath;

namespace milvus::benchmark_test {
namespace {

std::shared_ptr<ChunkedColumnInterface>
GetVortexJsonColumn() {
    static auto column = [] {
        const auto raw = GetRawJsonTakeBenchmarkInput(false);
        auto source = raw.column->Take(
            nullptr,
            {OffsetView::From(raw.offsets.data(), raw.offsets.size()),
             TargetType::Json,
             nullptr,
             TakePinPolicy::Batch});
        AssertInfo(source != nullptr, "benchmark source Take unsupported");
        const auto items = source->Access<Json>();
        arrow::BinaryBuilder builder;
        for (int64_t i = 0; i < items.Size(); ++i) {
            const auto item = items[i];
            AssertInfo(item.is_valid, "benchmark source must be nonnullable");
            const auto value = item.value->data();
            const auto status = builder.Append(
                value.data(), static_cast<int32_t>(value.size()));
            AssertInfo(
                status.ok(), "benchmark Arrow append: {}", status.ToString());
        }
        auto array_result = builder.Finish();
        AssertInfo(array_result.ok(),
                   "benchmark Arrow finish: {}",
                   array_result.status().ToString());
        auto array = std::move(array_result).ValueOrDie();
        auto schema =
            arrow::schema({arrow::field("101", arrow::binary(), false)});
        auto properties = std::make_shared<milvus_storage::api::Properties>();
        (*properties)[PROPERTY_FS_STORAGE_TYPE] = std::string("local");
        (*properties)[PROPERTY_FS_ROOT_PATH] = std::string("/");
        const auto path =
            (std::filesystem::path(TestLocalPath) / "candidate-mask.vx")
                .string();
        auto writer_result = milvus_storage::vortex::VortexFileWriter::Open(
            std::make_shared<arrow::fs::LocalFileSystem>(),
            schema,
            path,
            *properties);
        AssertInfo(writer_result.ok(),
                   "benchmark Vortex open: {}",
                   writer_result.status().ToString());
        auto writer = std::move(writer_result).ValueOrDie();
        const auto status = writer->Write(
            arrow::RecordBatch::Make(schema, array->length(), {array}));
        AssertInfo(
            status.ok(), "benchmark Vortex write: {}", status.ToString());
        const auto flush = writer->Flush();
        AssertInfo(flush.ok(), "benchmark Vortex flush: {}", flush.ToString());
        auto close = writer->Close();
        AssertInfo(close.ok(),
                   "benchmark Vortex close: {}",
                   close.status().ToString());
        const auto file = std::move(close).ValueOrDie();
        VortexColumn::FileInfo info{
            path,
            0,
            array->length(),
            file.Get<uint64_t>(milvus_storage::api::kPropertyFileSize, 0),
            file.Get<uint64_t>(milvus_storage::api::kPropertyFooterSize, 0)};
        auto group = std::make_shared<VortexColumnGroup>(
            std::vector<VortexColumn::FileInfo>{info},
            properties,
            std::vector<std::string>{"101"},
            1,
            CacheWarmupPolicy::CacheWarmupPolicy_Disable,
            nullptr);
        return std::make_shared<VortexColumn>(FieldId(101),
                                              FieldMeta(FieldName("json"),
                                                        FieldId(101),
                                                        DataType::JSON,
                                                        false,
                                                        std::nullopt),
                                              "101",
                                              properties,
                                              std::move(group));
    }();
    return column;
}

int64_t
ThreadNanos() {
    timespec ts{};
    AssertInfo(clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) == 0,
               "benchmark thread clock failed");
    return ts.tv_sec * 1'000'000'000LL + ts.tv_nsec;
}

void
MaskedTake(benchmark::State& state,
           bool vortex,
           bool shuffled,
           int64_t rows,
           int stride) {
    const auto raw = GetRawJsonTakeBenchmarkInput(false);
    auto column = vortex ? GetVortexJsonColumn() : raw.column;
    FixedVector<int32_t> offsets;
    offsets.reserve(rows);
    for (int64_t i = 0; i < rows; ++i) {
        offsets.push_back(raw.offsets[i * raw.offsets.size() / rows]);
    }
    if (shuffled) {
        std::mt19937 rng(42);
        std::shuffle(offsets.begin(), offsets.end(), rng);
    }
    TargetBitmap mask(rows, false);
    for (int64_t i = 0; i < rows; i += stride) {
        mask.set(i);
    }
    const auto read = [&](bool internal_mask) {
        FixedVector<int32_t> selected;
        auto input = OffsetView::From(offsets.data(), offsets.size());
        if (!internal_mask) {
            // Match the pre-PR executor, including reserving N rather than k.
            selected.reserve(offsets.size());
            for (size_t i = 0; i < offsets.size(); ++i) {
                if (mask[i]) {
                    selected.push_back(offsets[i]);
                }
            }
            input = OffsetView::From(selected.data(), selected.size());
        }
        auto take = column->Take(nullptr,
                                 {input,
                                  TargetType::Json,
                                  nullptr,
                                  TakePinPolicy::CurrentCell,
                                  internal_mask ? &mask : nullptr});
        AssertInfo(take != nullptr, "benchmark masked Take unsupported");
        const auto items = take->Access<Json>();
        uint64_t digest = 0;
        int64_t selected_position = 0;
        for (int64_t i = 0; i < rows; ++i) {
            if (!mask[i]) {
                continue;
            }
            const auto item = items[internal_mask ? i : selected_position++];
            AssertInfo(item.is_valid && !item.data_skipped,
                       "benchmark selected row lost data");
            const auto value = item.value->data();
            digest = digest * 131 + value.size();
            if (!value.empty()) {
                digest = digest * 131 + static_cast<uint8_t>(value.front());
                digest = digest * 131 + static_cast<uint8_t>(value.back());
            }
            digest ^= static_cast<uint64_t>(offsets[i]);
        }
        return digest;
    };
    const auto expected = read(false);
    AssertInfo(read(true) == expected, "benchmark masked result mismatch");
    double wall[2]{};
    double cpu[2]{};
    int64_t pair = 0;
    for (auto _ : state) {
        for (int step = 0; step < 2; ++step) {
            const auto mode = (pair + step) & 1;
            const auto thread_start = ThreadNanos();
            const auto begin = std::chrono::steady_clock::now();
            const auto digest = read(mode);
            wall[mode] += std::chrono::duration<double, std::micro>(
                              std::chrono::steady_clock::now() - begin)
                              .count();
            cpu[mode] += (ThreadNanos() - thread_start) / 1000.0;
            benchmark::DoNotOptimize(digest);
            AssertInfo(digest == expected, "benchmark masked result mismatch");
        }
        ++pair;
    }
    for (int mode = 0; mode < 2; ++mode) {
        const auto prefix = mode ? "mask_" : "compact_";
        state.counters[std::string(prefix) + "wall_us"] =
            benchmark::Counter(wall[mode], benchmark::Counter::kAvgIterations);
        state.counters[std::string(prefix) + "thread_us"] =
            benchmark::Counter(cpu[mode], benchmark::Counter::kAvgIterations);
    }
    state.counters["wall_ratio"] = wall[1] / wall[0];
    state.counters["selected"] = mask.count();
    state.counters["json_slot_bytes"] = sizeof(Json);
}

void
EmptySlots(benchmark::State& state, bool pointers) {
    const auto rows = state.range(0);
    for (auto _ : state) {
        if (pointers) {
            std::vector<const Json*> slots(rows, nullptr);
            benchmark::DoNotOptimize(slots.data());
            benchmark::ClobberMemory();
        } else {
            std::vector<Json> slots(rows);
            benchmark::DoNotOptimize(slots.data());
            benchmark::ClobberMemory();
        }
    }
    // Slots only: pointer-backed Take would additionally need owned values
    // and different indexing; this is not an equivalent Take implementation.
    state.counters["slot_bytes"] = pointers ? sizeof(Json*) : sizeof(Json);
    state.counters["storage_bytes"] =
        rows * (pointers ? sizeof(Json*) : sizeof(Json));
}

[[maybe_unused]] const bool kRegistered = [] {
    for (const auto vortex : {false, true}) {
        for (const auto shuffled : {false, true}) {
            for (const auto rows : {1024, 8192}) {
                for (const auto stride : {1, 4, 64, 1024}) {
                    const auto name = std::string("CandidateTake/") +
                                      (vortex ? "Vortex/" : "Raw/") +
                                      (shuffled ? "Shuffled/" : "Ordered/") +
                                      std::to_string(rows) + "/" +
                                      std::to_string(stride);
                    benchmark::RegisterBenchmark(
                        name.c_str(), [=](benchmark::State& state) {
                            MaskedTake(state, vortex, shuffled, rows, stride);
                        });
                }
            }
        }
    }
    benchmark::RegisterBenchmark("CandidateSlots/EmptyJson",
                                 [](auto& state) { EmptySlots(state, false); })
        ->Arg(1024)
        ->Arg(8192);
    benchmark::RegisterBenchmark("CandidateSlots/NullPointer",
                                 [](auto& state) { EmptySlots(state, true); })
        ->Arg(1024)
        ->Arg(8192);
    return true;
}();

}  // namespace
}  // namespace milvus::benchmark_test
