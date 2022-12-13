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

package datanode

import (
	"fmt"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

const activateInterval = 15 * time.Second

type event struct {
	eventType int
	vChanName string
	version   int64
	info      *datapb.ChannelWatchInfo
}

type channelEventManager struct {
	sync.Once
	eventChan         chan event
	closeChan         chan struct{}
	handlePutEvent    func(watchInfo *datapb.ChannelWatchInfo, version int64) error // node.handlePutEvent
	handleDeleteEvent func(vChanName string)                                        // node.handleDeleteEvent
}

const (
	putEventType    = 1
	deleteEventType = 2
)

func newChannelEventManager(handlePut func(*datapb.ChannelWatchInfo, int64) error,
	handleDel func(string)) *channelEventManager {
	return &channelEventManager{
		eventChan:         make(chan event, 10),
		closeChan:         make(chan struct{}),
		handlePutEvent:    handlePut,
		handleDeleteEvent: handleDel,
	}
}

func (e *channelEventManager) Run() {
	go func() {
		for {
			select {
			case event := <-e.eventChan:
				switch event.eventType {
				case putEventType:
					e.handlePutEvent(event.info, event.version)
				case deleteEventType:
					e.handleDeleteEvent(event.vChanName)
				}
			case <-e.closeChan:
				return
			}
		}
	}()
}

func (e *channelEventManager) handleEvent(event event) {
	e.eventChan <- event
}

func (e *channelEventManager) Close() {
	e.Do(func() {
		close(e.closeChan)
	})
}

func isEndWatchState(state datapb.ChannelWatchState) bool {
	return state != datapb.ChannelWatchState_ToWatch && // start watch
		state != datapb.ChannelWatchState_ToRelease && // start release
		state != datapb.ChannelWatchState_Uncomplete // legacy state, equal to ToWatch
}

type activateFunc func(channels ...string) error

//activePutEventManager like a tickle used to keep channel with state ToWatch or ToRelease active at datacoord
type activePutEventManager struct {
	channels     *typeutil.ConcurrentSet[string]
	intervalTime time.Duration
	ticker       *time.Ticker

	closeCh   chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup

	activate activateFunc
}

func (m *activePutEventManager) Add(channel string) {
	m.channels.Insert(channel)
}

func (m *activePutEventManager) Remove(channel string) {
	m.channels.Remove(channel)
}

func (m *activePutEventManager) Run() {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.ticker = time.NewTicker(m.intervalTime)
		for {
			select {
			case <-m.ticker.C:
				err := m.activate(m.channels.Collect()...)
				if err != nil {
					log.Warn(fmt.Sprintf("activate put event failed: %s", err.Error()), zap.Strings("channels", m.channels.Collect()))
				}
			case <-m.closeCh:
				log.Info("close active channel manager")
				return
			}
		}
	}()
}

func (m *activePutEventManager) Close() {
	m.closeOnce.Do(func() {
		close(m.closeCh)
		m.wg.Wait()
	})
}

func newActivePutEventManager(activate activateFunc) *activePutEventManager {
	return &activePutEventManager{
		activate:     activate,
		channels:     typeutil.NewConcurrentSet[string](),
		intervalTime: activateInterval,
		closeCh:      make(chan struct{}),
	}
}
