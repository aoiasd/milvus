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

package allocators

import (
	"fmt"
	"sync"
)

// TODO ADD TO PARAMTABLE
const vChannelFormat = "%s_%dv%d"

type ChannelAllocator interface {
	Alloc(collectionID uint64, num int) []string
	Release(vchannel string)
}

// channel allocator deside pchannel and vchannel name
// alloc pchannel for vchannel.
type UniformChannelAllocator struct {
	// pChannel -> vChannelNum
	pChannelInfo map[string]int

	mu sync.Mutex
}

func (a *UniformChannelAllocator) selectPChannel() string {
	minChannel, minCount := "", -1
	for pChannel, count := range a.pChannelInfo {
		if minChannel == "" || count < minCount {
			minChannel = pChannel
			minCount = count
		}
	}
	return minChannel
}

func (a *UniformChannelAllocator) Alloc(collectionID uint64, num int) []string {
	a.mu.Lock()
	defer a.mu.Unlock()

	vChannels := make([]string, num)
	for id := 0; id < num; id++ {
		pChannel := a.selectPChannel()
		vChannel := fmt.Sprintf(vChannelFormat, pChannel, collectionID, id)

		vChannels[id] = vChannel
		//TODO ADD AND VERFY VCHANNEL IN META
		a.pChannelInfo[pChannel]++
	}

	return vChannels
}

func (a *UniformChannelAllocator) Release(vChannels ...string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, vChannel := range vChannels {
		//TODO DELETE AND VERFY VCHANNEL IN META
		a.pChannelInfo[vChannel]--
	}
}
