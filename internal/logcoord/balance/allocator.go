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

package balance

import (
	"sync"

	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

// NodeAllocator alloc log node for pchannel
type NodeAllocator interface {
	Alloc(pChannel string) int64
	//return a realloc plan for balance
	//empty string means don't need balance
	//return pchannel, node, target_nodes
	Realloc() (string, int64, int64)

	AddNode(nodeID int64)
	RemoveNode(nodeID int64) []string
}

type UniformNodeAllocator struct {
	// nodeID -> pChannel
	channelList map[int64][]string
	// pChannel -> nodeID
	channelMapping map[string]int64

	mu sync.Mutex
}

func (allocator *UniformNodeAllocator) AddNode(nodeID int64) {
	allocator.mu.Lock()
	defer allocator.mu.Unlock()

	_, ok := allocator.channelList[nodeID]
	if !ok {
		allocator.channelList[nodeID] = []string{}
	}
}

func (allocator *UniformNodeAllocator) RemoveNode(nodeID int64) []string {
	allocator.mu.Lock()
	defer allocator.mu.Unlock()
	channels, ok := allocator.channelList[nodeID]
	if !ok {
		return []string{}
	}

	if len(channels) != 0 {
		log.Info("Remove log node but not reassign all pchannel", zap.Int64("nodeID", nodeID))
	}
	delete(allocator.channelList, nodeID)
	return channels
}

func (allocator *UniformNodeAllocator) selectMinNode() (int64, int64) {
	minNode, minCount := int64(-1), int64(-1)
	for nodeID, channels := range allocator.channelList {
		count := int64(len(channels))
		if minNode == -1 || count < minCount {
			minNode = nodeID
			minCount = count
		}
	}
	return minNode, minCount
}

func (allocator *UniformNodeAllocator) selectMaxNode() (int64, int64) {
	maxNode, maxCount := int64(-1), int64(-1)
	for nodeID, channels := range allocator.channelList {
		count := int64(len(channels))
		if maxNode == -1 || count > maxCount {
			maxNode = nodeID
			maxCount = count
		}
	}
	return maxNode, maxCount
}

func (allocator *UniformNodeAllocator) Alloc(pChannel string) int64 {
	allocator.mu.Lock()
	defer allocator.mu.Unlock()

	minNode, _ := allocator.selectMinNode()
	allocator.channelList[minNode] = append(allocator.channelList[minNode], pChannel)
	allocator.channelMapping[pChannel] = minNode
	return minNode
}

func (allocator *UniformNodeAllocator) Unalloc(pChannel string) {
	allocator.mu.Lock()
	defer allocator.mu.Unlock()

	node, ok := allocator.channelMapping[pChannel]
	if ok {
		remove(allocator.channelList[node], pChannel)
		delete(allocator.channelMapping, pChannel)
	}
}

func (allocator *UniformNodeAllocator) Realloc() (string, int64, int64) {
	allocator.mu.Lock()
	defer allocator.mu.Unlock()

	maxNode, maxCount := allocator.selectMaxNode()
	minNode, minCount := allocator.selectMinNode()
	if maxCount <= minCount+1 || maxCount == 0 {
		return "", -1, -1
	}

	target_channel := allocator.channelList[maxNode][0]
	allocator.channelList[maxNode] = allocator.channelList[maxNode][1:]
	allocator.channelMapping[target_channel] = minNode
	return target_channel, maxNode, minNode
}

func NewUniformNodeAllocator() *UniformNodeAllocator {
	return &UniformNodeAllocator{
		channelList:    make(map[int64][]string),
		channelMapping: make(map[string]int64),
	}
}

func remove(list []string, target string) {
	targetID := -1
	for id, value := range list {
		if value == target {
			targetID = id
			break
		}
	}
	if targetID != -1 {
		list = append(list[:targetID], list[targetID+1:]...)
	}
}
