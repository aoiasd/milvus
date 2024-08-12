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

package writebuffer

import (
	"github.com/milvus-io/milvus/internal/storage"
)

type statsBuffer struct {
	meta map[int64]*storage.BM25Stats
}

func (b *statsBuffer) Buffer(metaMap map[int64]*storage.BM25Stats) {
	for fieldID, meta := range metaMap {
		if fieldMeta, ok := b.meta[fieldID]; ok {
			fieldMeta.Merge(meta)
		} else {
			b.meta[fieldID] = meta
		}
	}

	return
}

func (b *statsBuffer) yieldBuffer() map[int64]*storage.BM25Stats {
	result := b.meta
	b.meta = make(map[int64]*storage.BM25Stats)
	return result
}

func newStatsBuffer() *statsBuffer {
	return &statsBuffer{
		meta: make(map[int64]*storage.BM25Stats),
	}
}
