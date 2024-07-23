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
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type statsBuffer struct {
	meta map[int64]storage.ChannelStats

	startPos *msgpb.MsgPosition
	endPos   *msgpb.MsgPosition
	numRow   int64
}

func getNumRow(metaMap map[int64]storage.ChannelStats) (int64, error) {
	var numRow = int64(-1)
	for _, meta := range metaMap {
		if numRow == -1 {
			numRow = meta.NumRow()
		} else if meta.NumRow() != numRow {
			return 0, merr.WrapErrParameterInvalid(numRow, meta.NumRow(), "check embedding stats num row failed")
		}
	}
	return numRow, nil
}

func (b *statsBuffer) Buffer(metaMap map[int64]storage.ChannelStats, startPos, endPos *msgpb.MsgPosition) error {
	numRow, err := getNumRow(metaMap)
	if err != nil {
		return err
	}

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

	b.numRow += numRow
	if b.startPos == nil || startPos.GetTimestamp() < b.startPos.GetTimestamp() {
		b.startPos = startPos
	}

	if b.endPos == nil || endPos.GetTimestamp() > b.endPos.GetTimestamp() {
		b.endPos = endPos
	}
	return nil
}

func (b *statsBuffer) yieldBuffer() (map[int64]storage.ChannelStats, *msgpb.MsgPosition, *msgpb.MsgPosition) {
	result := b.meta
	start, end := b.startPos, b.endPos
	b.meta = make(map[int64]storage.ChannelStats)
	b.startPos, b.endPos = nil, nil
	b.numRow = 0
	return result, start, end
}

func (b *statsBuffer) EarliestPosition() *msgpb.MsgPosition {
	return b.startPos
}

func newStatsBuffer() *statsBuffer {
	return &statsBuffer{
		meta: make(map[int64]storage.ChannelStats),
	}
}
