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

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
)

type VChannel interface {
	GetCollectionSchema() *schemapb.CollectionSchema
	// GetChannelCheckpoint() *msgpb.MsgPosition

	AddSegment(segmentID, partationID uint64, sType datapb.SegmentType)
	RemoveSegment(segmentID, partationID uint64, sType datapb.SegmentType)
	GetSegment(segmentID uint64) *Segment
}

type VChannelMeta struct {
	collectionID uint64
	channelName  string
	schema       *schemapb.CollectionSchema

	segments  map[uint64]*Segment
	segmentMu sync.RWMutex
}

func (c *VChannelMeta) GetCollectionSchema() *schemapb.CollectionSchema {
	return c.schema
}

func (c *VChannelMeta) GetSegment(segmentID uint64) *Segment {
	c.segmentMu.RLock()
	defer c.segmentMu.RUnlock()

	segment, ok := c.segments[segmentID]
	if !ok {
		return nil
	}
	return segment
}

func (c *VChannelMeta) AddSegment(segmentID, partationID uint64, sType datapb.SegmentType) {
	c.segmentMu.Lock()
	defer c.segmentMu.Unlock()

	_, ok := c.segments[segmentID]
	if !ok {
		c.segments[segmentID] = NewSegment(c.collectionID, partationID, segmentID, sType)
	}
}

func (c *VChannelMeta) RemoveSegment(segmentID, partationID uint64, sType datapb.SegmentType) {
	c.segmentMu.Lock()
	defer c.segmentMu.Unlock()

	_, ok := c.segments[segmentID]
	if ok {
		delete(c.segments, segmentID)
	}
}

func NewVChannelMeta(channelName string, collectionID uint64, schema *schemapb.CollectionSchema) *VChannelMeta {
	return &VChannelMeta{
		channelName:  channelName,
		collectionID: collectionID,
		schema:       schema,
		segments:     make(map[uint64]*Segment),
	}
}
