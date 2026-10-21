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

#include "RawJsonBenchmarkAccess.h"

#include "mmap/ChunkedColumn.h"

namespace milvus::benchmark_test {

struct RawJsonBenchmarkAccess {
    static bool
    Valid(const ChunkedColumnBase& column, Chunk* chunk, int64_t offset) {
        return !column.nullable_ || chunk->isValid(offset);
    }

    static RawJsonBenchmarkSource
    Source(const ChunkedColumnInterface& column) {
        const auto* raw = dynamic_cast<const ChunkedColumnBase*>(&column);
        AssertInfo(raw != nullptr && !raw->IsNullable(),
                   "pipeline benchmark requires non-nullable Raw V1");
        return {raw, raw->slot_, raw->PlannerHandle()};
    }

    static RawJsonLocations
    Locate(const ChunkedColumnBase& column, std::span<const int64_t> offsets) {
        return column.ToChunkIdAndOffset(offsets.data(), offsets.size());
    }
};

RawJsonBenchmarkSource
GetRawJsonBenchmarkSource(const ChunkedColumnInterface& column) {
    return RawJsonBenchmarkAccess::Source(column);
}

RawJsonLocations
LocateLegacyRawJson(const RawJsonBenchmarkSource& source,
                    std::span<const int64_t> offsets) {
    return RawJsonBenchmarkAccess::Locate(*source.raw, offsets);
}

void
VisitLegacyRawJson(const ChunkedColumnBase& column,
                   cachinglayer::CellAccessor<Chunk>& pinned,
                   const RawJsonLocations& locations,
                   const std::function<void(Json, size_t, bool)>& consume) {
    for (size_t i = 0; i < locations.first.size(); ++i) {
        auto* chunk = pinned.get_cell_of(locations.first[i]);
        const auto value =
            static_cast<StringChunk*>(chunk)->operator[](locations.second[i]);
        consume(
            Json(value),
            i,
            RawJsonBenchmarkAccess::Valid(column, chunk, locations.second[i]));
    }
}

}  // namespace milvus::benchmark_test
