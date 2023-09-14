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

package meta

import (
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
)

type Segment struct {
	collectionID uint64
	partitionID  uint64
	segmentID    uint64
	sType        atomic.Value // datapb.SegmentType

	numRows    int64
	memorySize int64

	statLock     sync.RWMutex
	currentStat  *storage.PkStatistics
	historyStats []*storage.PkStatistics
}

func (s *Segment) SetType(sType datapb.SegmentType) {
	s.sType.Store(sType)
}

func (s *Segment) GetType() datapb.SegmentType {
	return s.sType.Load().(datapb.SegmentType)
}

func NewSegment(collectionID, partitionID, segmentID uint64, sType datapb.SegmentType) *Segment {
	segment := &Segment{
		collectionID: collectionID,
		partitionID:  partitionID,
		segmentID:    segmentID,
	}
	segment.SetType(sType)
	return segment
}
