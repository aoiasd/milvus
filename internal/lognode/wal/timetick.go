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

package wal

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

type TimeTicker struct {
	interval time.Duration
	ticker   *time.Ticker

	// time stamp physical time of last time tick
	lastTime time.Time

	tsAllocator  TimestampAllocator
	loggerManger *LoggerManager

	wg        sync.WaitGroup
	closeCh   chan struct{}
	closeOnce sync.Once
}

func NewTimeTicker(interval time.Duration, logger *WriteAheadLogger) *TimeTicker {
	return &TimeTicker{
		interval: interval,
	}
}

func (t *TimeTicker) Start() {
	t.wg.Add(1)
	go t.work()
}

func (t *TimeTicker) Close() {
	t.closeOnce.Do(func() {
		close(t.closeCh)
		t.wg.Wait()
	})
}

func (t *TimeTicker) work() {
	ti := time.NewTicker(t.interval)
	defer t.wg.Done()
	for {
		select {
		case <-ti.C:
			ctx := context.Background()
			msg := t.newTTMsg()

			// broadcast time tick message for channel have no message after last time tick
			ts, err := t.loggerManger.Broadcast(ctx, msg, WithTsPhysicalTime(t.lastTime))
			if err != nil {
				log.Warn("produce time tick msg to channel failed")
				continue
			}
			t.lastTime = tsoutil.PhysicalTime(ts)
		case <-t.closeCh:
			log.Info("close time ticker of channels")
			return
		}
	}
}

func (t *TimeTicker) newTTMsg() *msgstream.TimeTickMsg {
	return &msgstream.TimeTickMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{0},
		},
		TimeTickMsg: msgpb.TimeTickMsg{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_TimeTick),
				commonpbutil.WithMsgID(0),
			),
		},
	}
}
