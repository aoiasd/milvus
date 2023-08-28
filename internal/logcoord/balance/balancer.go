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
	"context"
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/logcoord/session"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"go.uber.org/zap"
)

type NodeBalancer struct {
	nodeAllocator  NodeAllocator
	sessionManager *session.SessionManager

	waittingChannels chan string
	balanceNotify    chan struct{}

	wg        sync.WaitGroup
	startOnce sync.Once
	stopCh    chan struct{}
}

func (ba *NodeBalancer) alloc(ctx context.Context, channel string, nodeID int64) error {
	session := ba.sessionManager.GetSessions(nodeID)
	if session == nil {
		log.Warn("Session relased, but not remove from log node balancer", zap.Int64("nodeID", nodeID))
		return fmt.Errorf("alloc a relased node")
	}

	client := session.GetClient()
	resp, err := client.WatchChannel(ctx, logpb.WatchChannelRequest{})
	if err != nil {
		return err
	}

	err = merr.Error(resp)
	if err != nil {
		return err
	}
	return nil
}

func (ba *NodeBalancer) allocAll(ctx context.Context) {
	for {
		select {
		case channel := <-ba.waittingChannels:
			nodeID := ba.nodeAllocator.Alloc(channel)
			err := retry.Do(ctx, func() error {
				err := ba.alloc(ctx, channel, nodeID)
				return err
			}, retry.Attempts(3))

			if err != nil {
				ba.nodeAllocator.Unalloc(channel)
				ba.nodeAllocator.FreezeNode(nodeID)
			}
		default:
			return
		}
	}
}

func (ba *NodeBalancer) balance() {
	defer ba.wg.Done()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for {
		select {
		case <-ba.balanceNotify:
			ba.allocAll(ctx)
			//TODO REASSIGN CHANNEL TO NOT BALANCE NODE
		case <-ba.stopCh:
			log.Info("close log node balancer")
			return
		}
	}
}

func (ba *NodeBalancer) Start() {
	ba.startOnce.Do(func() {
		ba.wg.Add(1)
		go ba.balance()
	})
}

func (ba *NodeBalancer) AddNode(nodeID int64) {
	ba.nodeAllocator.AddNode(nodeID)
	ba.Notify()
}

func (ba *NodeBalancer) RemoveNode(nodeID int64) {
	offlineChannels := ba.nodeAllocator.RemoveNode(nodeID)
	ba.AddChannel(offlineChannels...)
}

func (ba *NodeBalancer) AddChannel(channels ...string) {
	for _, channel := range channels {
		ba.waittingChannels <- channel
	}
	ba.Notify()
}

func (ba *NodeBalancer) Notify() {
	select {
	case ba.balanceNotify <- struct{}{}:
		log.Info("notify log node balancer to do balance")
	default:
	}
}

func NewNodeBalancer(sessionManager *session.SessionManager) *NodeBalancer {
	nodeAllocator := NewUniformNodeAllocator()
	return &NodeBalancer{
		nodeAllocator:  nodeAllocator,
		sessionManager: sessionManager,
	}
}
