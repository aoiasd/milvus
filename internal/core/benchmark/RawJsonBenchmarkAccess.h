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
#pragma once

#include <functional>
#include <memory>
#include <span>
#include <utility>
#include <vector>

#include "RawJsonTakeBenchmark.h"

namespace milvus {
class ChunkedColumnBase;

namespace detail {
// All other stages can run against the pristine core without this probe.
uint64_t
BenchmarkRawTakeCells(const ChunkedColumnInterface::TakeResult& result,
                      uint64_t digest,
                      int64_t row_start) __attribute__((weak));
}  // namespace detail

namespace benchmark_test {

using RawJsonLocations = std::pair<std::vector<int64_t>, std::vector<int64_t>>;

// Test-only access to actual source pins, not a pre-resolved Chunk* substitute.
struct RawJsonBenchmarkSource {
    const ChunkedColumnBase* raw;
    std::shared_ptr<cachinglayer::CacheSlot<Chunk>> slot;
    std::shared_ptr<const ColumnPlanner> planner;
};

RawJsonBenchmarkSource
GetRawJsonBenchmarkSource(const ChunkedColumnInterface& column);

RawJsonLocations
LocateLegacyRawJson(const RawJsonBenchmarkSource& source,
                    std::span<const int64_t> offsets);

// Preserve the old per-row callback across a translation-unit boundary.
void
VisitLegacyRawJson(const ChunkedColumnBase& column,
                   cachinglayer::CellAccessor<Chunk>& pinned,
                   const RawJsonLocations& locations,
                   const std::function<void(Json, size_t, bool)>& consume);

}  // namespace benchmark_test
}  // namespace milvus
