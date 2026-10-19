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
#include <string>
#include <string_view>
#include <vector>

#include "RawJsonTakeBenchmark.h"
#include "common/EasyAssert.h"
#include "common/Json.h"
#include "common/Types.h"

namespace milvus::benchmark_test {
namespace {

enum class TakePattern { DenseOrdered, SparseShuffled25 };

const char*
TakePatternName(TakePattern pattern) {
    return pattern == TakePattern::DenseOrdered ? "DenseOrdered"
                                                : "SparseShuffled25";
}

int64_t
ThreadCpuTimeNanos() {
    timespec timestamp{};
    const auto status = clock_gettime(CLOCK_THREAD_CPUTIME_ID, &timestamp);
    AssertInfo(status == 0, "failed to read benchmark thread CPU time");
    return timestamp.tv_sec * 1'000'000'000LL + timestamp.tv_nsec;
}

void
ConsumeValues(const Json* values,
              int64_t row_start,
              int64_t count,
              uint64_t* digest) {
    AssertInfo(values != nullptr, "benchmark values are null");
    for (int64_t i = 0; i < count; ++i) {
        const auto view = static_cast<std::string_view>(values[i]);
        uint64_t value = view.size();
        if (!view.empty()) {
            value = value * 131 + static_cast<uint8_t>(view.front());
            value = value * 131 + static_cast<uint8_t>(view.back());
        }
        value ^= static_cast<uint64_t>(row_start + i) * 0x9e3779b97f4a7c15ULL;
        *digest ^=
            value + 0x9e3779b97f4a7c15ULL + (*digest << 6) + (*digest >> 2);
    }
}

void
RunMaskedJsonTakeBenchmark(benchmark::State& state,
                           TakePattern pattern,
                           int candidate_stride) {
    const auto input =
        GetRawJsonTakeBenchmarkInput(pattern == TakePattern::SparseShuffled25);
    const auto& source_offsets = input.offsets;
    const auto kWindowRows = input.window_rows;
    FixedVector<int32_t> offsets(source_offsets.size());
    std::copy(source_offsets.begin(), source_offsets.end(), offsets.begin());
    auto column = input.column;
    std::vector<TargetBitmap> candidates;
    for (size_t start = 0; start < offsets.size(); start += kWindowRows) {
        const auto rows = std::min<size_t>(kWindowRows, offsets.size() - start);
        auto& mask = candidates.emplace_back(rows, candidate_stride == 1);
        if (candidate_stride > 1) {
            for (size_t i = 0; i < rows; i += candidate_stride) {
                mask.set(i);
            }
        }
    }
    const auto read = [&](bool internal_selection) {
        uint64_t digest = 0;
        for (size_t window = 0; window < candidates.size(); ++window) {
            const auto start = window * kWindowRows;
            const auto& mask = candidates[window];
            FixedVector<int32_t> selected;
            OffsetView input =
                OffsetView::From(offsets.data() + start, mask.size());
            if (internal_selection) {
                if (mask.none()) {
                    continue;
                }
            } else {
                // Model the old execution helper, including its allocation
                // even when all candidates pass or none do.
                selected.reserve(mask.size());
                for (size_t i = 0; i < mask.size(); ++i) {
                    if (mask[i]) {
                        selected.push_back(offsets[start + i]);
                    }
                }
                if (selected.empty()) {
                    continue;
                }
                input = OffsetView::From(selected.data(), selected.size());
            }
            auto take = column->Take(nullptr,
                                     {input,
                                      TargetType::Json,
                                      nullptr,
                                      TakePinPolicy::CurrentCell,
                                      internal_selection ? &mask : nullptr});
            AssertInfo(take != nullptr, "masked JSON Take is unsupported");
            const auto items = take->Access<Json>();
            int64_t take_pos = 0;
            for (size_t i = 0; i < mask.size(); ++i) {
                if (!mask[i]) {
                    continue;
                }
                const auto item = items[internal_selection ? i : take_pos++];
                if (item.is_valid) {
                    ConsumeValues(&*item.value, offsets[start + i], 1, &digest);
                }
            }
        }
        return digest;
    };
    const auto expected = read(false);
    AssertInfo(read(true) == expected, "masked JSON Take digest changed");
    int64_t legacy_cpu_nanos = 0;
    int64_t interface_cpu_nanos = 0;
    int64_t pair_index = 0;
    const auto measure = [&](bool internal_selection, int64_t* nanos) {
        const auto begin = ThreadCpuTimeNanos();
        const auto digest = read(internal_selection);
        *nanos += ThreadCpuTimeNanos() - begin;
        benchmark::DoNotOptimize(digest);
        return digest == expected;
    };
    for (auto _ : state) {
        const auto internal_first = (pair_index++ & 1) != 0;
        const auto valid = internal_first
                               ? measure(true, &interface_cpu_nanos) &&
                                     measure(false, &legacy_cpu_nanos)
                               : measure(false, &legacy_cpu_nanos) &&
                                     measure(true, &interface_cpu_nanos);
        if (!valid) {
            state.SkipWithError("masked JSON Take digest changed");
            break;
        }
    }
    state.counters["legacy_cpu_us"] = benchmark::Counter(
        legacy_cpu_nanos / 1000.0, benchmark::Counter::kAvgIterations);
    state.counters["interface_cpu_us"] = benchmark::Counter(
        interface_cpu_nanos / 1000.0, benchmark::Counter::kAvgIterations);
    if (legacy_cpu_nanos > 0) {
        state.counters["interface_over_legacy"] =
            static_cast<double>(interface_cpu_nanos) / legacy_cpu_nanos;
    }
}

[[maybe_unused]] const bool kBenchmarksRegistered = [] {
    for (const auto pattern :
         {TakePattern::DenseOrdered, TakePattern::SparseShuffled25}) {
        for (const auto stride : {1, 4, 64, 0}) {
            const auto name = std::string("RawJsonMaskedTake/") +
                              TakePatternName(pattern) + "/" +
                              std::to_string(stride);
            benchmark::RegisterBenchmark(
                name.c_str(), [pattern, stride](benchmark::State& state) {
                    RunMaskedJsonTakeBenchmark(state, pattern, stride);
                });
        }
    }
    return true;
}();

}  // namespace
}  // namespace milvus::benchmark_test
