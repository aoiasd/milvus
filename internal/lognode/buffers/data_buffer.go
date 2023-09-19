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

package buffers

import (
	"math"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/samber/lo"
)

type DataBuffer struct {
	// buffer data
	insertData *storage.InsertData
	deleteData *storage.DeleteData

	size  uint64
	limit uint64

	// messageg position range
	startPos *msgpb.MsgPosition
	endPos   *msgpb.MsgPosition

	// timestamp range
	tsFrom uint64
	tsTo   uint64
}

func (b *DataBuffer) updateSize(size uint64) {
	b.size += size
}

// updateTimeRange update BufferData tsFrom, tsTo range according to input time range
func (b *DataBuffer) updateTimeRange(datas []uint64) {
	for _, data := range datas {
		if data < b.tsFrom {
			b.tsFrom = data
		}
		if data > b.tsTo {
			b.tsTo = data
		}
	}
}

func (b *DataBuffer) IsFull() bool {
	return b.size >= b.limit
}

func (b *DataBuffer) BufferDelete(data *storage.DeleteData) error {
	storage.MergeDeleteData(b.deleteData, data)
	b.updateSize(uint64(data.RowCount))
	b.updateTimeRange(data.Tss)

	// TODO updatePosition
	return nil
}

func (b *DataBuffer) BufferInsert(data *storage.InsertData) error {
	tsData, err := storage.GetTimestampFromInsertData(data)
	if err != nil {
		return err
	}

	// Maybe there are large write zoom if frequent insert requests are met.
	storage.MergeInsertData(b.insertData, data)
	b.updateSize(uint64(len(tsData.Data)))
	b.updateTimeRange(lo.Map(tsData.Data, func(data int64, _ int) uint64 {
		return uint64(data)
	}))

	// TODO updatePosition
	return nil
}

func NewDataBuffer(limit uint64) *DataBuffer {
	return &DataBuffer{
		insertData: &storage.InsertData{Data: make(map[int64]storage.FieldData)},
		size:       0,
		limit:      limit,
		tsFrom:     math.MaxUint64,
		tsTo:       0,
	}
}
