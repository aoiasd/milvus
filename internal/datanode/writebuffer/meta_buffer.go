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
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/storage"
)

type metaBuffer struct {
	meta map[int64]storage.EmbeddingMeta

	startPos *msgpb.MsgPosition
	endPos   *msgpb.MsgPosition
}

func (b *metaBuffer) Buffer(metaMap map[int64]storage.EmbeddingMeta, starPos, endPos *msgpb.MsgPosition) error {
	for fieldID, meta := range metaMap {
		if fieldMeta, ok := b.meta[fieldID]; ok {
			err := fieldMeta.Merge(meta)
			if err != nil {
				return err
			}
		} else {
			b.meta[fieldID] = meta
		}
	}
	return nil
}

func (b *metaBuffer) yieldBuffer() (map[int64]storage.EmbeddingMeta, *msgpb.MsgPosition, *msgpb.MsgPosition) {
	result := b.meta
	start, end := b.startPos, b.endPos
	b.meta = make(map[int64]storage.EmbeddingMeta)
	b.startPos, b.endPos = nil, nil
	return result, start, end
}
