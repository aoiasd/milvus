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
#include <cstdint>
#include <numeric>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "mmap/ChunkedColumn.h"

namespace milvus::benchmark_test {
namespace {

constexpr int64_t kLookupWindow = 1024;
using Locations =
    std::pair<std::vector<cachinglayer::cid_t>, std::vector<int64_t>>;

enum class Lookup {
    Planner,
    Bucket,
    BatchBinary,
    ConfiguredBucket,
    ConfiguredBatchBinary
};

Locations
LocateOffsets(Lookup strategy,
              const ColumnPlanner& planner,
              const std::vector<int64_t>& boundaries,
              int64_t bucket_order,
              const std::vector<int64_t>& buckets,
              const int64_t* offsets,
              int64_t count) {
    if (strategy == Lookup::ConfiguredBucket ||
        strategy == Lookup::ConfiguredBatchBinary) {
        return planner.LocateBatch(OffsetView::From(offsets, count));
    }
    if (strategy == Lookup::Planner) {
        Locations result{std::vector<cachinglayer::cid_t>(count),
                         std::vector<int64_t>(count)};
        for (int64_t i = 0; i < count; ++i) {
            const auto location = planner.Locate(offsets[i]);
            result.first[i] = location.cell_id;
            result.second[i] = location.cell_offset;
        }
        return result;
    }
    // Legacy ToChunkIdAndOffset validated inputs before its bulk lookup too.
    for (int64_t i = 0; i < count; ++i) {
        AssertInfo(offsets[i] >= 0 && offsets[i] < boundaries.back(),
                   "benchmark offset out of range");
    }
    return strategy == Lookup::Bucket
               ? GetChunkIDsByOffsets(
                     offsets, count, boundaries, bucket_order, buckets)
               : GetChunkIDsByOffsets(offsets, count, boundaries);
}

void
VerifyLocations(const std::vector<int64_t>& boundaries,
                const int64_t* offsets,
                int64_t count,
                const Locations& locations) {
    AssertInfo(
        locations.first.size() == count && locations.second.size() == count,
        "locator output size changed");
    for (int64_t i = 0; i < count; ++i) {
        const auto cell = locations.first[i];
        AssertInfo(cell >= 0 && cell + 1 < boundaries.size(),
                   "locator returned invalid Cell");
        AssertInfo(boundaries[cell] <= offsets[i] &&
                       offsets[i] < boundaries[cell + 1] &&
                       locations.second[i] == offsets[i] - boundaries[cell],
                   "locator returned wrong Cell/offset");
    }
}

void
RunLookupBenchmark(benchmark::State& state,
                   Lookup strategy,
                   std::vector<int64_t> cell_rows,
                   int pattern) {
    std::vector<int64_t> boundaries(cell_rows.size() + 1, 0);
    std::partial_sum(
        cell_rows.begin(), cell_rows.end(), boundaries.begin() + 1);
    int64_t bucket_order = 0;
    std::vector<int64_t> buckets;
    segcore::storagev1translator::virtual_chunk_config(
        boundaries.back(), cell_rows.size(), boundaries, bucket_order, buckets);
    OffsetLookupOptions lookup_options;
    if (strategy == Lookup::ConfiguredBucket) {
        lookup_options = {OffsetLookupStrategy::Bucket, bucket_order, &buckets};
    } else if (strategy == Lookup::ConfiguredBatchBinary) {
        lookup_options.strategy = OffsetLookupStrategy::BatchBinary;
    }
    const ColumnPlanner planner(boundaries, lookup_options);

    std::vector<int64_t> offsets;
    if (pattern == 3) {
        // The bucket's forward correction can cross many tiny Cells.
        offsets.resize(1023);
        std::iota(offsets.begin(), offsets.end(), 0);
    } else {
        for (int64_t offset = 0; offset < boundaries.back();
             offset += pattern == 0 ? 1 : 4) {
            offsets.push_back(offset);
        }
        if (pattern == 2) {
            std::mt19937 random(42);
            std::shuffle(offsets.begin(), offsets.end(), random);
        }
    }
    const auto locate = [&](const int64_t* input, int64_t count) {
        return LocateOffsets(
            strategy, planner, boundaries, bucket_order, buckets, input, count);
    };
    for (int64_t start = 0; start < offsets.size(); start += kLookupWindow) {
        const auto count = std::min<int64_t>(
            kLookupWindow, static_cast<int64_t>(offsets.size()) - start);
        VerifyLocations(boundaries,
                        offsets.data() + start,
                        count,
                        locate(offsets.data() + start, count));
    }
    // Include empty requests, duplicates, exact boundaries and empty Cells.
    std::vector<int64_t> edge_offsets{boundaries.back() - 1, 0, 0};
    for (const auto boundary : boundaries) {
        if (boundary < boundaries.back()) {
            edge_offsets.push_back(boundary);
        }
    }
    VerifyLocations(boundaries,
                    edge_offsets.data(),
                    edge_offsets.size(),
                    locate(edge_offsets.data(), edge_offsets.size()));
    VerifyLocations(boundaries, offsets.data(), 0, locate(offsets.data(), 0));

    for (auto _ : state) {
        for (int64_t start = 0; start < offsets.size();
             start += kLookupWindow) {
            const auto count = std::min<int64_t>(
                kLookupWindow, static_cast<int64_t>(offsets.size()) - start);
            const auto locations = locate(offsets.data() + start, count);
            benchmark::DoNotOptimize(locations.first.data());
            benchmark::DoNotOptimize(locations.second.data());
            benchmark::ClobberMemory();
        }
    }
    state.SetItemsProcessed(state.iterations() * offsets.size());
    state.counters["bucket_bytes"] = buckets.size() * sizeof(int64_t);
    state.counters["cells"] = cell_rows.size();
    state.counters["rows"] = boundaries.back();
}

bool
RegisterLookupBenchmarks() {
    const std::vector<std::pair<std::string, std::vector<int64_t>>> layouts{
        {"Single", {32256}},
        {"Irregular4", {8192, 7680, 8448, 7936}},
        {"Uniform64", std::vector<int64_t>(64, 512)},
        {"Uniform1024", std::vector<int64_t>(1024, 32)},
        {"Skewed1024",
         [] {
             std::vector<int64_t> rows(1024, 1);
             rows.back() = 1'048'576 - 1023;
             return rows;
         }()},
        {"EmptyCells", {0, 8192, 0, 7680, 8448, 0, 7936, 0}},
    };
    const char* patterns[]{"DenseOrdered",
                           "SparseOrdered25",
                           "SparseShuffled25",
                           "SmallCellCluster"};
    for (const auto& [layout, rows] : layouts) {
        for (int pattern = 0; pattern < (layout == "Skewed1024" ? 4 : 3);
             ++pattern) {
            const std::pair<Lookup, const char*> strategies[]{
                {Lookup::Planner, "PlannerUpperBound"},
                {Lookup::Bucket, "V1Bucket"},
                {Lookup::BatchBinary, "BatchBinary"},
                {Lookup::ConfiguredBucket, "ConfiguredBucket"},
                {Lookup::ConfiguredBatchBinary, "ConfiguredBatchBinary"}};
            for (const auto& [strategy, strategy_name] : strategies) {
                const auto name = "RawOffsetLookup/" + layout + "/" +
                                  patterns[pattern] + "/" + strategy_name;
                benchmark::RegisterBenchmark(
                    name.c_str(),
                    [strategy, rows, pattern](benchmark::State& state) {
                        RunLookupBenchmark(state, strategy, rows, pattern);
                    })
                    ->Unit(benchmark::kMicrosecond);
            }
        }
    }
    return true;
}

[[maybe_unused]] const bool kLookupBenchmarksRegistered =
    RegisterLookupBenchmarks();

}  // namespace
}  // namespace milvus::benchmark_test
