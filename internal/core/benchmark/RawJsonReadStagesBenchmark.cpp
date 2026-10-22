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
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "RawJsonTakeBenchmark.h"
#include "RawJsonBenchmarkAccess.h"
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

// Compare output expressions on the same Take/Batch path; production uses
// the legacy temporary-string construction.
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

enum class PipelineStage { Locate, Pin, Prepare, Cells, Read, ReadCopy, Full };

struct PipelineWindow {
    std::span<const int64_t> offsets;
    RawJsonLocations locations;
    std::vector<int64_t> unique_cells;
    std::shared_ptr<cachinglayer::CellAccessor<Chunk>> pin;
    Column::TakeResultPtr take;
    DataArray output;
};

void
InitOutput(PipelineWindow& window) {
    // A fresh protobuf must not reuse string capacity across iterations.
    window.output = DataArray{};
    segcore::CreateScalarDataArray(window.output,
                                   window.offsets.size(),
                                   DataType::JSON,
                                   DataType::NONE,
                                   false);
}

template <bool Copy>
uint64_t
ReadLegacyWindow(const RawJsonBenchmarkSource& source,
                 PipelineWindow& window,
                 uint64_t digest,
                 int64_t start) {
    auto* values = Copy ? window.output.mutable_scalars()
                              ->mutable_json_data()
                              ->mutable_data()
                        : nullptr;
    if constexpr (Copy) {
        VisitLegacyRawJson(*source.raw,
                           *window.pin,
                           window.locations,
                           [values](Json value, size_t i, bool valid) {
                               values->at(i) = std::string(value.data());
                           });
    } else {
        VisitLegacyRawJson(*source.raw,
                           *window.pin,
                           window.locations,
                           [&digest, start](Json value, size_t i, bool valid) {
                               digest =
                                   Consume(digest, value.data(), start + i);
                           });
    }
    if constexpr (Copy) {
        for (size_t i = 0; i < window.offsets.size(); ++i) {
            digest = Consume(digest, values->Get(i), start + i);
        }
    }
    return digest;
}

template <bool Copy>
uint64_t
ReadTakeWindow(PipelineWindow& window, uint64_t digest, int64_t start) {
    auto* values = Copy ? window.output.mutable_scalars()
                              ->mutable_json_data()
                              ->mutable_data()
                        : nullptr;
    const auto items = window.take->Access<Json>();
    for (int64_t i = 0; i < items.Size(); ++i) {
        const auto item = items[i];
        if (item.is_valid) {
            if constexpr (Copy) {
                values->at(i) = std::string(item.value->data());
            } else {
                digest = Consume(digest, item.value->data(), start + i);
            }
        }
    }
    if constexpr (Copy) {
        for (size_t i = 0; i < window.offsets.size(); ++i) {
            digest = Consume(digest, values->Get(i), start + i);
        }
    }
    return digest;
}

template <PipelineStage Stage>
void
RunWindowReadPair(benchmark::State& state,
                  const RawJsonTakeBenchmarkInput& input,
                  const RawJsonBenchmarkSource& source,
                  uint64_t expected) {
    int64_t legacy_ns = 0, current_ns = 0, query = 0;
    for (auto _ : state) {
        uint64_t old_digest = 0, new_digest = 0;
        for (size_t start = 0; start < input.offsets.size();
             start += input.window_rows) {
            PipelineWindow old, now;
            old.offsets = now.offsets = input.offsets.subspan(
                start,
                std::min<size_t>(input.window_rows,
                                 input.offsets.size() - start));
            // Keep this window's preparation outside timing but immediately
            // before its read, preserving production metadata locality.
            old.locations = LocateLegacyRawJson(source, old.offsets);
            old.pin = cachinglayer::SemiInlineGet(
                source.slot->PinCells(nullptr, old.locations.first));
            now.take = TakeBatch(*input.column, now.offsets);
            if constexpr (Stage == PipelineStage::ReadCopy) {
                InitOutput(old);
                InitOutput(now);
            }
            const auto read_old = [&] {
                const auto before = ThreadCpuNanos();
                if constexpr (Stage == PipelineStage::Cells) {
                    for (size_t i = 0; i < old.offsets.size(); ++i) {
                        const auto* chunk =
                            old.pin->get_cell_of(old.locations.first[i]);
                        old_digest =
                            (old_digest * 131) ^
                            (reinterpret_cast<uintptr_t>(chunk) >> 3) ^
                            (old.locations.second[i] * 131 + start + i);
                    }
                } else {
                    old_digest =
                        ReadLegacyWindow<Stage == PipelineStage::ReadCopy>(
                            source, old, old_digest, start);
                }
                legacy_ns += ThreadCpuNanos() - before;
                benchmark::DoNotOptimize(old_digest);
            };
            const auto read_new = [&] {
                const auto before = ThreadCpuNanos();
                if constexpr (Stage == PipelineStage::Cells) {
                    new_digest = detail::BenchmarkRawTakeCells(
                        *now.take, new_digest, start);
                } else {
                    new_digest =
                        ReadTakeWindow<Stage == PipelineStage::ReadCopy>(
                            now, new_digest, start);
                }
                current_ns += ThreadCpuNanos() - before;
                benchmark::DoNotOptimize(new_digest);
            };
            if ((query & 1) != 0) {
                read_new();
                read_old();
            } else {
                read_old();
                read_new();
            }
        }
        ++query;
        if (old_digest != expected || new_digest != expected) {
            state.SkipWithError("fresh window stage digest differs");
            break;
        }
    }
    if (state.iterations() != 0 && legacy_ns != 0) {
        state.counters["legacy_cpu_us"] = benchmark::Counter(
            legacy_ns / 1000.0, benchmark::Counter::kAvgIterations);
        state.counters["interface_cpu_us"] = benchmark::Counter(
            current_ns / 1000.0, benchmark::Counter::kAvgIterations);
        state.counters["interface_over_legacy"] =
            double(current_ns) / legacy_ns;
    }
    state.SetItemsProcessed(state.iterations() * input.offsets.size() * 2);
}

template <PipelineStage Stage>
void
RunPipelinePair(benchmark::State& state,
                const RawJsonTakeBenchmarkInput& input) {
    if constexpr (Stage == PipelineStage::Cells) {
        if (detail::BenchmarkRawTakeCells == nullptr) {
            state.SkipWithError("Cell probe is absent from this core");
            return;
        }
    }
    const auto source = GetRawJsonBenchmarkSource(*input.column);
    std::vector<PipelineWindow> legacy, current;
    uint64_t switches = 0;
    uint64_t distinct = 0;
    uint64_t expected = 0;
    for (size_t start = 0; start < input.offsets.size();
         start += input.window_rows) {
        const auto offsets = input.offsets.subspan(
            start,
            std::min<size_t>(input.window_rows, input.offsets.size() - start));
        PipelineWindow window;
        window.offsets = offsets;
        window.locations = LocateLegacyRawJson(source, offsets);
        const auto located = source.planner->LocateBatch(
            Column::OffsetView::From(offsets.data(), offsets.size()));
        AssertInfo(window.locations == located, "pipeline locations differ");
        {
            auto pin = cachinglayer::SemiInlineGet(
                source.slot->PinCells(nullptr, window.locations.first));
            auto take = TakeBatch(*input.column, offsets);
            const auto items = take->Access<Json>();
            VisitLegacyRawJson(
                *source.raw,
                *pin,
                window.locations,
                [&](Json value, size_t i, bool valid) {
                    const auto item = items[i];
                    AssertInfo(valid && item.is_valid && item.value &&
                                   value.data() == item.value->data(),
                               "pipeline values differ at {}",
                               start + i);
                    if constexpr (Stage == PipelineStage::Cells) {
                        const auto* chunk =
                            pin->get_cell_of(window.locations.first[i]);
                        expected =
                            (expected * 131) ^
                            (reinterpret_cast<uintptr_t>(chunk) >> 3) ^
                            (window.locations.second[i] * 131 + start + i);
                    } else {
                        expected = Consume(expected, value.data(), start + i);
                    }
                });
        }
        int64_t last = -1;
        for (const auto cell : window.locations.first) {
            switches += cell != last;
            last = cell;
            if (std::find(window.unique_cells.begin(),
                          window.unique_cells.end(),
                          cell) == window.unique_cells.end()) {
                window.unique_cells.push_back(cell);
            }
        }
        distinct += window.unique_cells.size();
        current.emplace_back();
        current.back().offsets = offsets;
        current.back().unique_cells = window.unique_cells;
        legacy.push_back(std::move(window));
    }
    state.counters["cell_switches"] = switches;
    state.counters["pin_cells"] = distinct;
    state.counters["windows"] = legacy.size();
    if constexpr (Stage == PipelineStage::Cells ||
                  Stage == PipelineStage::Read ||
                  Stage == PipelineStage::ReadCopy) {
        int64_t clock_ns = 0;
        for (int i = 0; i < 1000; ++i) {
            const auto begin = ThreadCpuNanos();
            clock_ns += ThreadCpuNanos() - begin;
        }
        state.counters["clock_baseline_us"] =
            clock_ns / 1'000'000.0 * legacy.size();
        RunWindowReadPair<Stage>(state, input, source, expected);
        return;
    }
    const auto read = [&]<bool Legacy>() {
        uint64_t digest = 0;
        int64_t start = 0;
        auto& windows = Legacy ? legacy : current;
        for (auto& window : windows) {
            if constexpr (Stage == PipelineStage::Locate) {
                auto locations =
                    Legacy
                        ? LocateLegacyRawJson(source, window.offsets)
                        : source.planner->LocateBatch(Column::OffsetView::From(
                              window.offsets.data(), window.offsets.size()));
                benchmark::DoNotOptimize(locations.first.data());
                benchmark::DoNotOptimize(locations.second.data());
                digest += locations.first.size();
            } else if constexpr (Stage == PipelineStage::Pin) {
                const auto& ids =
                    Legacy ? window.locations.first : window.unique_cells;
                auto pin = cachinglayer::SemiInlineGet(
                    source.slot->PinCells(nullptr, ids));
                benchmark::DoNotOptimize(pin);
                digest += window.offsets.size();
            } else if constexpr (Stage == PipelineStage::Prepare) {
                if constexpr (Legacy) {
                    auto locations =
                        LocateLegacyRawJson(source, window.offsets);
                    auto pin = cachinglayer::SemiInlineGet(
                        source.slot->PinCells(nullptr, locations.first));
                    benchmark::DoNotOptimize(pin);
                } else {
                    auto result = TakeBatch(*input.column, window.offsets);
                    benchmark::DoNotOptimize(result);
                }
                digest += window.offsets.size();
            } else {
                if constexpr (Stage == PipelineStage::Full) {
                    InitOutput(window);
                    if constexpr (Legacy) {
                        window.locations =
                            LocateLegacyRawJson(source, window.offsets);
                        window.pin =
                            cachinglayer::SemiInlineGet(source.slot->PinCells(
                                nullptr, window.locations.first));
                    } else {
                        window.take = TakeBatch(*input.column, window.offsets);
                    }
                }
                if constexpr (Legacy) {
                    digest = ReadLegacyWindow<Stage != PipelineStage::Read>(
                        source, window, digest, start);
                } else {
                    digest = ReadTakeWindow<Stage != PipelineStage::Read>(
                        window, digest, start);
                }
                if constexpr (Stage == PipelineStage::Full) {
                    window.pin.reset();
                    window.take.reset();
                    window.output = DataArray{};
                    window.locations = {};
                }
            }
            start += window.offsets.size();
        }
        return digest;
    };
    RunPair(
        state,
        [&] { return read.template operator()<true>(); },
        [&] { return read.template operator()<false>(); },
        input.offsets.size());
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

template <PipelineStage Stage>
bool
RegisterPipelineStage(const char* name) {
    for (const auto pattern : kPatterns) {
        const auto label =
            std::string("RawJsonPipeline/") + name + "/" + PatternName(pattern);
        benchmark::RegisterBenchmark(
            label.c_str(),
            [pattern](benchmark::State& state) {
                auto input = GetRawJsonTakeBenchmarkInput(pattern ==
                                                          Pattern::Shuffled25);
                std::vector<int64_t> ordered;
                if (pattern == Pattern::Ordered25) {
                    for (size_t i = 0; i < input.offsets.size(); i += 4) {
                        ordered.push_back(input.offsets[i]);
                    }
                    input.offsets = ordered;
                }
                RunPipelinePair<Stage>(state, input);
            })
            ->Unit(benchmark::kMicrosecond);
    }
    return true;
}

[[maybe_unused]] const bool pipeline_registered =
    RegisterPipelineStage<PipelineStage::Locate>("Locate") &&
    RegisterPipelineStage<PipelineStage::Pin>("PinUnpin") &&
    RegisterPipelineStage<PipelineStage::Prepare>("Prepare") &&
    RegisterPipelineStage<PipelineStage::Cells>("Cells") &&
    RegisterPipelineStage<PipelineStage::Read>("Read") &&
    RegisterPipelineStage<PipelineStage::ReadCopy>("ReadCopy") &&
    RegisterPipelineStage<PipelineStage::Full>("FullColumn");

}  // namespace
}  // namespace milvus::benchmark_test
