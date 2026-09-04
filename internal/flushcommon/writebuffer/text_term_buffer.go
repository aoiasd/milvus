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
	"sort"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
)

type segmentTextTermBuffer struct {
	fields            map[int64]map[string]struct{}
	coverageTimestamp uint64
	memorySize        int64
}

func newSegmentTextTermBuffer() *segmentTextTermBuffer {
	return &segmentTextTermBuffer{fields: make(map[int64]map[string]struct{})}
}

func (b *segmentTextTermBuffer) Buffer(batches []*msgpb.TextTermBatch, coverageTimestamp uint64) {
	buffered := false
	for _, batch := range batches {
		if batch == nil {
			continue
		}
		buffered = true
		terms := b.fields[batch.GetInputFieldId()]
		if terms == nil {
			terms = make(map[string]struct{})
			b.fields[batch.GetInputFieldId()] = terms
		}
		for _, term := range batch.GetTerms() {
			value := string(term)
			if _, exists := terms[value]; exists {
				continue
			}
			terms[value] = struct{}{}
			b.memorySize += int64(len(value))
		}
	}
	if buffered && coverageTimestamp > b.coverageTimestamp {
		b.coverageTimestamp = coverageTimestamp
	}
}

// MemorySize returns the retained key payload in bytes. As with the existing
// insert-buffer accounting, Go map/object overhead is intentionally excluded.
func (b *segmentTextTermBuffer) MemorySize() int64 {
	if b == nil {
		return 0
	}
	return b.memorySize
}

func (b *segmentTextTermBuffer) Yield() *syncmgr.TextTermData {
	if b == nil || len(b.fields) == 0 {
		return nil
	}

	result := &syncmgr.TextTermData{
		CoverageTimestamp: b.coverageTimestamp,
		Fields:            make(map[int64][][]byte, len(b.fields)),
	}
	for fieldID, termSet := range b.fields {
		terms := make([]string, 0, len(termSet))
		for term := range termSet {
			terms = append(terms, term)
		}
		sort.Strings(terms)
		encoded := make([][]byte, 0, len(terms))
		for _, term := range terms {
			encoded = append(encoded, []byte(term))
		}
		result.Fields[fieldID] = encoded
	}
	return result
}
