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
#include <ctime>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "RawJsonTakeBenchmark.h"
#include "common/Chunk.h"
#include "common/EasyAssert.h"
#include "common/Json.h"
#include "segcore/Utils.h"

namespace milvus::benchmark_test {
namespace {

using Column = ChunkedColumnInterface;

enum class Pattern { Dense, Ordered25, Shuffled25 };

constexpr Pattern kPatterns[] = {
    Pattern::Dense, Pattern::Ordered25, Pattern::Shuffled25};

const char*
PatternName(Pattern pattern) {
    switch (pattern) {
        case Pattern::Dense:
            return "DenseOrdered";
        case Pattern::Ordered25:
            return "SparseOrdered25";
        case Pattern::Shuffled25:
            return "SparseShuffled25";
    }
    return "Unknown";
}

uint64_t
Consume(uint64_t digest, std::string_view value, int64_t row) {
    uint64_t bits = value.size();
    if (!value.empty()) {
        bits = bits * 131 + static_cast<uint8_t>(value.front());
        bits = bits * 131 + static_cast<uint8_t>(value.back());
    }
    bits ^= static_cast<uint64_t>(row) * 0x9e3779b97f4a7c15ULL;
    return digest ^
           (bits + 0x9e3779b97f4a7c15ULL + (digest << 6) + (digest >> 2));
}

int64_t
ThreadCpuNanos() {
    timespec timestamp{};
    const auto status = clock_gettime(CLOCK_THREAD_CPUTIME_ID, &timestamp);
    AssertInfo(status == 0, "failed to read benchmark thread CPU time");
    return timestamp.tv_sec * 1'000'000'000LL + timestamp.tv_nsec;
}

template <typename Legacy, typename Current>
void
RunPair(benchmark::State& state,
        Legacy legacy,
        Current current,
        int64_t count) {
    const auto expected = legacy();
    AssertInfo(current() == expected, "JSON stage output differs");
    int64_t legacy_nanos = 0;
    int64_t current_nanos = 0;
    int64_t pair = 0;
    const auto measure = [&](const auto& read, int64_t& elapsed) {
        const auto start = ThreadCpuNanos();
        const auto digest = read();
        elapsed += ThreadCpuNanos() - start;
        benchmark::DoNotOptimize(digest);
        return digest == expected;
    };
    for (auto _ : state) {
        const bool current_first = (pair++ & 1) != 0;
        const auto valid = current_first ? measure(current, current_nanos) &&
                                               measure(legacy, legacy_nanos)
                                         : measure(legacy, legacy_nanos) &&
                                               measure(current, current_nanos);
        if (!valid) {
            state.SkipWithError("JSON stage output changed");
            break;
        }
    }
    if (state.iterations() != 0 && legacy_nanos != 0) {
        state.counters["legacy_cpu_us"] = benchmark::Counter(
            legacy_nanos / 1'000.0, benchmark::Counter::kAvgIterations);
        state.counters["interface_cpu_us"] = benchmark::Counter(
            current_nanos / 1'000.0, benchmark::Counter::kAvgIterations);
        state.counters["interface_over_legacy"] =
            static_cast<double>(current_nanos) / legacy_nanos;
    }
    state.SetItemsProcessed(state.iterations() * count * 2);
}

Column::TakeResultPtr
TakeBatch(Column& column, std::span<const int64_t> offsets) {
    Column::TakeOptions options{
        Column::OffsetView::From(offsets.data(), offsets.size()),
        Column::TargetType::Json};
    options.pin_policy = Column::TakePinPolicy::Batch;
    auto take = column.Take(nullptr, options);
    AssertInfo(take != nullptr, "Raw JSON batch Take is unavailable");
    return take;
}

template <bool LegacyTemporary>
void
CopyJson(std::string& output, std::string_view value) {
    if constexpr (LegacyTemporary) {
        output = std::string(value);
    } else {
        output.assign(value);
    }
}

template <bool LegacyTemporary, typename GetValue>
uint64_t
BuildOutput(int64_t count, int64_t window_rows, GetValue get_value) {
    uint64_t digest = 0;
    for (int64_t start = 0; start < count; start += window_rows) {
        const auto rows = std::min(window_rows, count - start);
        DataArray output;
        segcore::CreateScalarDataArray(
            output, rows, DataType::JSON, DataType::NONE, false);
        auto* values =
            output.mutable_scalars()->mutable_json_data()->mutable_data();
        for (int64_t i = 0; i < rows; ++i) {
            CopyJson<LegacyTemporary>(values->at(i), get_value(start + i));
        }
        for (int64_t i = 0; i < rows; ++i) {
            digest = Consume(digest, values->Get(i), start + i);
        }
    }
    return digest;
}

// Isolate only the final protobuf string construction; source pins and views
// are prepared outside timing, and both arms still copy the same payload once.
void
RunCopyPair(benchmark::State& state,
            const RawJsonTakeBenchmarkInput& input,
            std::span<const int64_t> offsets) {
    auto take = TakeBatch(*input.column, offsets);
    const auto items = take->Access<Json>();
    std::vector<std::string_view> views;
    views.reserve(offsets.size());
    for (int64_t i = 0; i < items.Size(); ++i) {
        const auto item = items[i];
        AssertInfo(item.is_valid && item.value, "JSON source value is absent");
        views.push_back(item.value->data());
    }
    const auto get_value = [&](int64_t i) { return views[i]; };
    RunPair(
        state,
        [&] {
            return BuildOutput<true>(
                offsets.size(), input.window_rows, get_value);
        },
        [&] {
            return BuildOutput<false>(
                offsets.size(), input.window_rows, get_value);
        },
        offsets.size());
}

// Both arms use exactly the same Take/Batch path. Only the output string
// assignment is changed back to the pre-migration expression.
template <bool LegacyTemporary>
uint64_t
TakeOutput(const RawJsonTakeBenchmarkInput& input,
           std::span<const int64_t> offsets) {
    uint64_t digest = 0;
    for (size_t start = 0; start < offsets.size(); start += input.window_rows) {
        const auto rows =
            std::min<size_t>(input.window_rows, offsets.size() - start);
        DataArray output;
        segcore::CreateScalarDataArray(
            output, rows, DataType::JSON, DataType::NONE, false);
        auto* values =
            output.mutable_scalars()->mutable_json_data()->mutable_data();
        auto take = TakeBatch(*input.column, offsets.subspan(start, rows));
        const auto items = take->Access<Json>();
        for (int64_t i = 0; i < rows; ++i) {
            const auto item = items[i];
            if (item.is_valid) {
                CopyJson<LegacyTemporary>(values->at(i), item.value->data());
            }
        }
        for (int64_t i = 0; i < rows; ++i) {
            digest = Consume(digest, values->Get(i), start + i);
        }
    }
    return digest;
}

// A lower-bound dispatch diagnostic, not the complete legacy Bulk path:
// exclude planning/pinning and the old std::function callback from both loops.
void
RunBoundAccessPair(benchmark::State& state,
                   const RawJsonTakeBenchmarkInput& input,
                   std::span<const int64_t> offsets) {
    auto pins = input.column->GetAllChunks(nullptr);
    const auto [cell_ids, cell_offsets] =
        input.column->GetChunkIDsByOffsets(offsets.data(), offsets.size());
    std::vector<Column::TakeResultPtr> results;
    for (size_t start = 0; start < offsets.size(); start += input.window_rows) {
        results.push_back(TakeBatch(
            *input.column,
            offsets.subspan(
                start,
                std::min<size_t>(input.window_rows, offsets.size() - start))));
    }
    const auto legacy = [&] {
        uint64_t digest = 0;
        for (size_t i = 0; i < offsets.size(); ++i) {
            const auto* chunk =
                static_cast<StringChunk*>(pins[cell_ids[i]].get());
            digest = Consume(digest, (*chunk)[cell_offsets[i]], i);
        }
        return digest;
    };
    const auto current = [&] {
        uint64_t digest = 0;
        int64_t row = 0;
        for (const auto& result : results) {
            const auto items = result->Access<Json>();
            for (int64_t i = 0; i < items.Size(); ++i) {
                const auto item = items[i];
                digest = Consume(digest, item.value->data(), row++);
            }
        }
        return digest;
    };
    RunPair(state, legacy, current, offsets.size());
}

bool
RegisterStages() {
    for (const auto pattern : kPatterns) {
        for (const auto stage :
             {"CopyOnly", "TakeOutputCopy", "BoundAccessor"}) {
            const auto name = std::string("RawJsonStages/") + stage + "/" +
                              PatternName(pattern);
            benchmark::RegisterBenchmark(
                name.c_str(),
                [pattern, stage](benchmark::State& state) {
                    auto input = GetRawJsonTakeBenchmarkInput(
                        pattern == Pattern::Shuffled25);
                    std::vector<int64_t> ordered;
                    if (pattern == Pattern::Ordered25) {
                        for (size_t i = 0; i < input.offsets.size(); i += 4) {
                            ordered.push_back(input.offsets[i]);
                        }
                        input.offsets = ordered;
                    }
                    if (std::string_view(stage) == "CopyOnly") {
                        RunCopyPair(state, input, input.offsets);
                    } else if (std::string_view(stage) == "BoundAccessor") {
                        RunBoundAccessPair(state, input, input.offsets);
                    } else {
                        RunPair(
                            state,
                            [&] {
                                return TakeOutput<true>(input, input.offsets);
                            },
                            [&] {
                                return TakeOutput<false>(input, input.offsets);
                            },
                            input.offsets.size());
                    }
                })
                ->Unit(benchmark::kMicrosecond);
        }
    }
    return true;
}

[[maybe_unused]] const bool registered = RegisterStages();

}  // namespace
}  // namespace milvus::benchmark_test
