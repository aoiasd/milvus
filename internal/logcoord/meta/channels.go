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
	"context"
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"go.uber.org/zap"
)

type PhysicalChannel struct {
	name   string
	nodeID int64
}

type VirtualChannel struct {
	name     string
	pChannel string
	//watchStatus
}

type ChannelsMeta struct {
	ctx     context.Context
	factory msgstream.Factory

	pChannelList []string
	pChannelInfo map[string]*PhysicalChannel

	initOnce sync.Once
}

func genPChannelNames(prefix string, num int64) []string {
	var results []string
	for idx := int64(0); idx < num; idx++ {
		result := fmt.Sprintf("%s_%d", prefix, idx)
		results = append(results, result)
	}
	return results
}

// if topic was precreated
// check topic exist and check the existed topic whether empty or not
// kafka and rmq will err if the topic does not yet exist, pulsar will not
// if one of the topics is not empty, panic
func checkTopicExist(ctx context.Context, factory msgstream.Factory, names ...string) {
	ms, err := factory.NewMsgStream(ctx)

	if err != nil {
		log.Error("Failed to create msgstream",
			zap.Error(err))
		panic("failed to create msgstream")
	}
	defer ms.Close()
	subName := "pre-created-topic-check"
	ms.AsConsumer(ctx, names, subName, mqwrapper.SubscriptionPositionUnknown)

	for _, name := range names {
		err := ms.CheckTopicValid(name)
		if err != nil {
			log.Error("topic not exist", zap.String("name", name), zap.Error(err))
			panic("created topic not exist")
		}
	}
}

func NewChannelsMeta(ctx context.Context, factory msgstream.Factory) *ChannelsMeta {
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

	channelsMeta := &ChannelsMeta{
		ctx:          ctx,
		factory:      factory,
		pChannelList: names,
	}

	channelsMeta.Init()
	return channelsMeta
}

func (meta *ChannelsMeta) Init() {
	meta.initOnce.Do(func() {
		// Init PChannel
		for _, pChannel := range meta.pChannelList {
			meta.pChannelInfo[pChannel] = &PhysicalChannel{
				name:   pChannel,
				nodeID: -1,
			}
		}
		// TODO LOAD FROM KV
	})
}

func (meta *ChannelsMeta) GetPChannels() []string {
	return meta.pChannelList
}
