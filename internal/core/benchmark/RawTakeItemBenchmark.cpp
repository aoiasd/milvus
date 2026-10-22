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
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "RawJsonTakeBenchmark.h"

namespace milvus::benchmark_test {

enum class ItemVariant { Current, WithoutSkip, IgnoreSkip, NoSeparatePrepare };

template <typename T>
struct ItemWithoutSkip {
    std::optional<T> value;
    bool is_valid;
};

// These ablations are valid only for the all-valid, unfiltered fixture below.
// Keep the real Raw result, source pins, getters and Json view construction.
struct RawTakeItemBenchmarkAccess {
    template <ItemVariant Variant, typename Result>
    static auto
    Read(const Result& result, int64_t index) {
        if constexpr (Variant == ItemVariant::Current) {
            return result.template GetUnchecked<Json>(index);
        } else {
            using Item = std::conditional_t<Variant == ItemVariant::WithoutSkip,
                                            ItemWithoutSkip<Json>,
                                            TakeItem<Json>>;
            TakeItemState state;
            Item item{std::nullopt, true};
            // Also build against a combined getter candidate without changing
            // the benchmark's result representation or consumer loop.
            if constexpr (requires { result.JsonViewAt(index, state); }) {
                const auto value = result.JsonViewAt(index, state);
                item.is_valid = state.is_valid;
                if (state.is_valid) {
                    item.value.emplace(value);
                }
            } else {
                if constexpr (Variant != ItemVariant::NoSeparatePrepare) {
                    state = result.PrepareItem(index, true);
                }
                item.is_valid = state.is_valid;
                if (state.is_valid) {
                    item.value.emplace(result.JsonViewAt(index));
                }
            }
            return item;
        }
    }
};

namespace {

using Column = ChunkedColumnInterface;

int64_t
CpuNanos() {
    timespec now{};
    AssertInfo(clock_gettime(CLOCK_THREAD_CPUTIME_ID, &now) == 0,
               "failed to read benchmark CPU time");
    return now.tv_sec * 1'000'000'000LL + now.tv_nsec;
}

uint64_t
Consume(uint64_t digest, std::string_view value, int64_t position) {
    uint64_t bits = value.size();
    if (!value.empty()) {
        bits = bits * 131 + static_cast<uint8_t>(value.front());
        bits = bits * 131 + static_cast<uint8_t>(value.back());
    }
    bits ^= static_cast<uint64_t>(position) * 0x9e3779b97f4a7c15ULL;
    return digest ^
           (bits + 0x9e3779b97f4a7c15ULL + (digest << 6) + (digest >> 2));
}

Column::TakeResultPtr
Take(const RawJsonTakeBenchmarkInput& input,
     std::span<const int64_t> offsets,
     TakePinPolicy policy) {
    return input.column->Take(
        nullptr,
        Column::TakeOptions{
            Column::OffsetView::From(offsets.data(), offsets.size()),
            TargetType::Json,
            nullptr,
            policy});
}

template <ItemVariant Variant>
uint64_t
Read(const Column::TakeResult& result, int64_t start, uint64_t digest) {
    const auto count = result.Size();
    for (int64_t i = 0; i < count; ++i) {
        const auto item = RawTakeItemBenchmarkAccess::Read<Variant>(result, i);
        if (item.is_valid) {
            digest = Consume(digest, item.value->data(), start + i);
        }
    }
    return digest;
}

template <ItemVariant Variant>
void
Run(benchmark::State& state,
    const RawJsonTakeBenchmarkInput& input,
    TakePinPolicy policy) {
    AssertInfo(!input.column->IsNullable(),
               "TakeItem ablations require a non-nullable column");
    uint64_t expected = 0;
    for (size_t start = 0; start < input.offsets.size();
         start += input.window_rows) {
        const auto offsets = input.offsets.subspan(
            start,
            std::min<size_t>(input.window_rows, input.offsets.size() - start));
        auto result = Take(input, offsets, policy);
        AssertInfo(result != nullptr, "TakeItem benchmark Take is unavailable");
        for (int64_t i = 0; i < result->Size(); ++i) {
            const auto current =
                RawTakeItemBenchmarkAccess::Read<ItemVariant::Current>(*result,
                                                                       i);
            const auto candidate =
                RawTakeItemBenchmarkAccess::Read<Variant>(*result, i);
            AssertInfo(current.is_valid && !current.data_skipped &&
                           current.value && candidate.is_valid &&
                           candidate.value &&
                           current.value->data() == candidate.value->data(),
                       "TakeItem ablation changes JSON at {}",
                       start + i);
            expected = Consume(expected, current.value->data(), start + i);
        }
    }

    int64_t current_ns = 0, variant_ns = 0, query = 0;
    for (auto _ : state) {
        uint64_t current_digest = 0, variant_digest = 0;
        for (size_t start = 0; start < input.offsets.size();
             start += input.window_rows) {
            const auto offsets = input.offsets.subspan(
                start,
                std::min<size_t>(input.window_rows,
                                 input.offsets.size() - start));
            auto current = Take(input, offsets, policy);
            auto candidate = Take(input, offsets, policy);
            const auto measure = [&]<ItemVariant Arm>(const auto& result,
                                                      uint64_t& digest,
                                                      int64_t& elapsed) {
                const auto begin = CpuNanos();
                digest = Read<Arm>(result, start, digest);
                elapsed += CpuNanos() - begin;
                benchmark::DoNotOptimize(digest);
            };
            if ((query & 1) == 0) {
                measure.template operator()<ItemVariant::Current>(
                    *current, current_digest, current_ns);
                measure.template operator()<Variant>(
                    *candidate, variant_digest, variant_ns);
            } else {
                measure.template operator()<Variant>(
                    *candidate, variant_digest, variant_ns);
                measure.template operator()<ItemVariant::Current>(
                    *current, current_digest, current_ns);
            }
        }
        ++query;
        if (current_digest != expected || variant_digest != expected) {
            state.SkipWithError("TakeItem ablation checksum differs");
            break;
        }
    }
    if (state.iterations() != 0 && current_ns != 0) {
        state.counters["current_cpu_us"] = benchmark::Counter(
            current_ns / 1000.0, benchmark::Counter::kAvgIterations);
        state.counters["variant_cpu_us"] = benchmark::Counter(
            variant_ns / 1000.0, benchmark::Counter::kAvgIterations);
        state.counters["variant_over_current"] =
            double(variant_ns) / current_ns;
    }
    state.counters["json_item_bytes"] = sizeof(TakeItem<Json>);
    state.counters["json_without_skip_bytes"] = sizeof(ItemWithoutSkip<Json>);
    state.counters["int64_item_bytes"] = sizeof(TakeItem<int64_t>);
    state.counters["int64_without_skip_bytes"] =
        sizeof(ItemWithoutSkip<int64_t>);
    state.counters["view_item_bytes"] = sizeof(TakeItem<std::string_view>);
    state.counters["view_without_skip_bytes"] =
        sizeof(ItemWithoutSkip<std::string_view>);
    state.SetItemsProcessed(state.iterations() * input.offsets.size() * 2);
}

template <ItemVariant Variant>
bool
Register(const char* variant) {
    for (const auto policy :
         {TakePinPolicy::Batch, TakePinPolicy::CurrentCell}) {
        for (const auto pattern :
             {"DenseOrdered", "SparseOrdered25", "SparseShuffled25"}) {
            const auto name =
                std::string("RawTakeItem/") + variant + "/" +
                (policy == TakePinPolicy::Batch ? "Batch/" : "CurrentCell/") +
                pattern;
            benchmark::RegisterBenchmark(
                name.c_str(),
                [policy, pattern](benchmark::State& state) {
                    auto input = GetRawJsonTakeBenchmarkInput(
                        std::string_view(pattern) == "SparseShuffled25");
                    std::vector<int64_t> ordered;
                    if (std::string_view(pattern) == "SparseOrdered25") {
                        for (size_t i = 0; i < input.offsets.size(); i += 4) {
                            ordered.push_back(input.offsets[i]);
                        }
                        input.offsets = ordered;
                    }
                    Run<Variant>(state, input, policy);
                })
                ->Unit(benchmark::kMicrosecond);
        }
    }
    return true;
}

[[maybe_unused]] const bool registered =
    Register<ItemVariant::WithoutSkip>("WithoutSkip") &&
    Register<ItemVariant::IgnoreSkip>("IgnoreSkip") &&
    Register<ItemVariant::NoSeparatePrepare>("NoSeparatePrepare");

}  // namespace
}  // namespace milvus::benchmark_test
