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

package channel

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/logcoord/balance"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/samber/lo"
)

type Manager interface {
	AddVChannels(collectionID uint64, num int) []string
}

type ChannelManager struct {
	ctx     context.Context
	factory msgstream.Factory

	allocator balance.ChannelAllocator

	vChannelInfo map[string]*VirtualChannel
	pChannelInfo map[string]*PhysicalChannel
	channelMu    sync.RWMutex

	initOnce sync.Once
}

func NewChannelManager(ctx context.Context, factory msgstream.Factory) *ChannelManager {
	params := &paramtable.Get().CommonCfg

	var names []string
	if params.PreCreatedTopicEnabled.GetAsBool() {
		names = params.TopicNames.GetAsStrings()
		checkTopicExist(ctx, factory, names...)
	} else {
		// TODO ADD TO PARAMTABLE
		prefix := "rootcoord-dml"
		num := int64(16)
		names = genPChannelNames(prefix, num)
	}

	channelManager := &ChannelManager{
		ctx:     ctx,
		factory: factory,
	}

	channelManager.Init(names...)
	return channelManager
}

func (m *ChannelManager) Init(channels ...string) {
	m.initOnce.Do(func() {
		m.channelMu.Lock()
		defer m.channelMu.Unlock()

		// init physical channel
		for _, channel := range channels {
			m.pChannelInfo[channel] = NewPhysicalChannel(channel)
		}
		// TODO LOAD FROM KV
	})
}

func (m *ChannelManager) GetPChannelNames() []string {
	m.channelMu.RLock()
	defer m.channelMu.RUnlock()

	return lo.Keys(m.pChannelInfo)
}

func (m *ChannelManager) GetVChannelNames() []string {
	m.channelMu.RLock()
	defer m.channelMu.RUnlock()

	return lo.Keys(m.vChannelInfo)
}

func (m *ChannelManager) AddVChannels(collectionID uint64, num int) []string {
	names := m.allocator.Alloc(collectionID, num)

	return names
}
