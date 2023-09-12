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
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
)

type DataBuffer struct {
	// buffer data
	data  *storage.InsertData
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
func (b *DataBuffer) updateTimeRange(tsData *storage.Int64FieldData) {
	for _, data := range tsData.Data {
		udata := uint64(data)
		if udata < b.tsFrom {
			b.tsFrom = udata
		}
		if udata > b.tsTo {
			b.tsTo = udata
		}
	}
}

func (b *DataBuffer) IsFull() bool {
	return b.size >= b.limit
}

func (b *DataBuffer) Insert(msg *msgstream.InsertMsg, schema *schemapb.CollectionSchema) error {
	addedData, err := storage.InsertMsgToInsertData(msg, schema)
	if err != nil {
		return err
	}

	tsData, err := storage.GetTimestampFromInsertData(addedData)
	if err != nil {
		return err
	}

	// Maybe there are large write zoom if frequent insert requests are met.
	b.data = storage.MergeInsertData(b.data, addedData)
	b.updateSize(msg.NRows())
	b.updateTimeRange(tsData)

	//TODO updatePosition
	return nil
}

func NewDataBuffer(limit uint64) *DataBuffer {
	return &DataBuffer{
		data:   &storage.InsertData{Data: make(map[int64]storage.FieldData)},
		size:   0,
		limit:  limit,
		tsFrom: math.MaxUint64,
		tsTo:   0,
	}
}
