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
	"math"

	"github.com/milvus-io/milvus/pkg/common"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type ChannelStats interface {
	Append(datas ...map[uint32]float32)
	Merge(meta ChannelStats) error
	NumRow() int64
	Serialize() ([]byte, error)
	Deserialize([]byte) error
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

func NewBM25StatsWithBytes(bytes []byte) (*BM25Stats, error) {
	stats := NewBM25Stats()
	err := stats.Deserialize(bytes)
	if err != nil {
		return nil, err
	}
	return stats, nil
}

func (m *BM25Stats) Append(datas ...map[uint32]float32) {
	for _, data := range datas {
		for key, value := range data {
			m.statistics[key] += 1
			m.tokenNum += int64(value)
		}

		m.numRow += 1
	}
}

func (m *BM25Stats) NumRow() int64 {
	return m.numRow
}

func (m *BM25Stats) Merge(meta ChannelStats) error {
	bm25meta, ok := meta.(*BM25Stats)
	if !ok {
		return fmt.Errorf("can't merge BM25 meta from other type meta")
	}

	for key, value := range bm25meta.statistics {
		m.statistics[key] += value
	}
	m.numRow += bm25meta.NumRow()
	m.tokenNum += bm25meta.tokenNum
	return nil
}

func (m *BM25Stats) Serialize() ([]byte, error) {
	buffer := bytes.NewBuffer(make([]byte, 0, len(m.statistics)*8+16))

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

	log.Info("test-- serialize", zap.Int64("numrow", m.numRow), zap.Int64("tokenNum", m.tokenNum), zap.Int("dim", len(m.statistics)), zap.Int("len", buffer.Len()))
	return buffer.Bytes(), nil
}

func (m *BM25Stats) Deserialize(bs []byte) error {
	buffer := bytes.NewBuffer(bs)
	var dim = (len(bs) - 16) / 8
	var numRow, tokenNum int64
	if err := binary.Read(buffer, common.Endian, &numRow); err != nil {
		return err
	}

	if err := binary.Read(buffer, common.Endian, &tokenNum); err != nil {
		return err
	}

	var keys []uint32 = make([]uint32, dim)
	var values []int32 = make([]int32, dim)
	for i := 0; i < dim; i++ {
		if err := binary.Read(buffer, common.Endian, &keys[i]); err != nil {
			return err
		}

		if err := binary.Read(buffer, common.Endian, &values[i]); err != nil {
			return err
		}
	}

	m.numRow += numRow
	m.tokenNum += tokenNum
	for i := 0; i < dim; i++ {
		m.statistics[keys[i]] += values[i]
	}

	log.Info("test-- deserialize", zap.Int64("numrow", m.numRow), zap.Int64("tokenNum", m.tokenNum))
	return nil
}

func (m *BM25Stats) BuildIDF(tf map[uint32]float32) map[uint32]float32 {
	vector := make(map[uint32]float32)
	for key, value := range tf {
		nq := m.statistics[key]
		vector[key] = value * float32(math.Log(1+(float64(m.numRow)-float64(nq)+0.5)/(float64(nq)+0.5)))
	}
	return vector
}

func (m *BM25Stats) GetAvgdl() float64 {
	return float64(m.tokenNum) / float64(m.numRow)
}
