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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
)

func TestSegmentTextTermBufferDeduplicatesAndAdvancesCoverage(t *testing.T) {
	buffer := newSegmentTextTermBuffer()
	buffer.Buffer([]*msgpb.TextTermBatch{
		{InputFieldId: 101, Terms: [][]byte{[]byte("world"), []byte("hello")}},
		{InputFieldId: 101, Terms: [][]byte{[]byte("fuzzy"), []byte("fuzzy")}},
	}, 100)
	require.EqualValues(t, len("world")+len("hello")+len("fuzzy"), buffer.MemorySize())
	buffer.Buffer([]*msgpb.TextTermBatch{
		{InputFieldId: 101, Terms: [][]byte{[]byte("hello"), []byte("again")}},
		{InputFieldId: 102},
	}, 120)
	buffer.Buffer(nil, 200)
	require.EqualValues(t, len("world")+len("hello")+len("fuzzy")+len("again"), buffer.MemorySize())

	result := buffer.Yield()
	require.NotNil(t, result)
	require.EqualValues(t, 120, result.CoverageTimestamp)
	require.Equal(t, [][]byte{[]byte("again"), []byte("fuzzy"), []byte("hello"), []byte("world")}, result.Fields[101])
	require.Empty(t, result.Fields[102])
}

func TestWriteBufferTextTermGenerationFreeze(t *testing.T) {
	wb := &writeBufferBase{textTermBuffers: make(map[int64]*segmentTextTermBuffer)}

	wb.bufferTextTerms(10, []*msgpb.TextTermBatch{
		{InputFieldId: 101, Terms: [][]byte{[]byte("first")}},
	}, 100)
	require.EqualValues(t, len("first"), wb.MemorySize())
	first := wb.yieldTextTerms(10)
	require.NotNil(t, first)
	require.EqualValues(t, 100, first.CoverageTimestamp)
	require.Equal(t, [][]byte{[]byte("first")}, first.Fields[101])
	require.Nil(t, wb.yieldTextTerms(10))
	require.Zero(t, wb.MemorySize())

	wb.bufferTextTerms(10, []*msgpb.TextTermBatch{
		{InputFieldId: 101, Terms: [][]byte{[]byte("second")}},
	}, 200)
	require.EqualValues(t, len("second"), wb.MemorySize())
	second := wb.yieldTextTerms(10)
	require.NotNil(t, second)
	require.EqualValues(t, 200, second.CoverageTimestamp)
	require.Equal(t, [][]byte{[]byte("second")}, second.Fields[101])
	require.Equal(t, [][]byte{[]byte("first")}, first.Fields[101])
}
