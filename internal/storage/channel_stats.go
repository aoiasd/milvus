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

package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/milvus-io/milvus/pkg/common"
)

type ChannelStats interface {
	Append(text string, data map[uint32]int32)
	Merge(meta ChannelStats) error
	NumRow() int64
	Serialize() ([]byte, error)
	// LoadBytes([]byte)
}

type BM25Stats struct {
	statistics map[uint32]int32
	numRow     int64
	tokenNum   int64
}

func NewBM25Stats() *BM25Stats {
	return &BM25Stats{
		statistics: map[uint32]int32{},
	}
}

func (m *BM25Stats) Append(text string, data map[uint32]int32) {
	for key, value := range data {
		m.statistics[key] += value
	}
	m.tokenNum += int64(len(text))
	m.numRow += 1
}

func (m *BM25Stats) NumRow() int64 {
	return m.numRow
}

func (m *BM25Stats) Merge(meta ChannelStats) error {
	bm25meta, ok := meta.(*BM25Stats)
	if !ok {
		return fmt.Errorf("Can't merge BM25 meta from other type meta")
	}

	for key, value := range bm25meta.statistics {
		m.statistics[key] += value
	}
	return nil
}

func (m *BM25Stats) Serialize() ([]byte, error) {
	buffer := bytes.NewBuffer(make([]byte, len(m.statistics)*8+18))

	if err := binary.Write(buffer, common.Endian, m.numRow); err != nil {
		return nil, err
	}

	if err := binary.Write(buffer, common.Endian, m.tokenNum); err != nil {
		return nil, err
	}

	for key, value := range m.statistics {
		if err := binary.Write(buffer, common.Endian, key); err != nil {
			return nil, err
		}

		if err := binary.Write(buffer, common.Endian, value); err != nil {
			return nil, err
		}
	}
	return buffer.Bytes(), nil
}
