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
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type PChannel interface {
	GetCollectionSchema(vchannelName string) (*schemapb.CollectionSchema, error)

	AddSegment(vChannelName string, partitionID, segmentID uint64, sType datapb.SegmentType) error
	GetSegment(segmentID uint64) *Segment

	AddVChannel(vChannelName string, collectionID uint64, schema *schemapb.CollectionSchema) error
	RemoveVChannel(vChannelName string, schema *schemapb.CollectionSchema) error
}

type PChannelMeta struct {
	channelName string

	vChannels  map[string]VChannel
	vChannelMu sync.RWMutex
}

func (c *PChannelMeta) GetCollectionSchema(vChannelName string) (*schemapb.CollectionSchema, error) {
	c.vChannelMu.RLock()
	defer c.vChannelMu.RUnlock()

	vChannel, ok := c.vChannels[vChannelName]
	if !ok {
		return nil, merr.WrapErrChannelNotFound(vChannelName, "get schema failed")
	}

	return vChannel.GetCollectionSchema(), nil
}

func (c *PChannelMeta) AddVChannel(vChannelName string, collectionID uint64, schema *schemapb.CollectionSchema) error {
	c.vChannelMu.Lock()
	defer c.vChannelMu.Unlock()

	_, ok := c.vChannels[vChannelName]
	if !ok {
		c.vChannels[vChannelName] = NewVChannelMeta(vChannelName, collectionID, schema)
	}
	return nil
}

func (c *PChannelMeta) RemoveVChannel(vChannelName string, schema *schemapb.CollectionSchema) error {
	c.vChannelMu.Lock()
	defer c.vChannelMu.Unlock()

	_, ok := c.vChannels[vChannelName]
	if ok {
		delete(c.vChannels, vChannelName)
	}
	return nil
}

func (c *PChannelMeta) GetVChannel(vChannelName string) VChannel {
	c.vChannelMu.RLock()
	defer c.vChannelMu.RUnlock()

	channel, ok := c.vChannels[vChannelName]
	if !ok {
		return nil
	}
	return channel
}
