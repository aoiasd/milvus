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

package datacoord

import (
	"fmt"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type channelStateTimer struct {
	watchkv        kv.MetaKv
	timerStops     sync.Map // channel name to timer stop channels
	timers         sync.Map
	etcdWatcher    clientv3.WatchChan
	timeoutWatcher chan *ackEvent
}

func newChannelStateTimer(kv kv.MetaKv) *channelStateTimer {
	return &channelStateTimer{
		watchkv:        kv,
		timeoutWatcher: make(chan *ackEvent, 20),
	}
}

func (c *channelStateTimer) getWatchers(prefix string) (clientv3.WatchChan, chan *ackEvent) {
	if c.etcdWatcher == nil {
		c.etcdWatcher = c.watchkv.WatchWithPrefix(prefix)
	}
	return c.etcdWatcher, c.timeoutWatcher
}

func (c *channelStateTimer) loadAllChannels(nodeID UniqueID) ([]*datapb.ChannelWatchInfo, error) {
	prefix := path.Join(Params.CommonCfg.DataCoordWatchSubPath.GetValue(), strconv.FormatInt(nodeID, 10))

	// TODO: change to LoadWithPrefixBytes
	keys, values, err := c.watchkv.LoadWithPrefix(prefix)
	if err != nil {
		return nil, err
	}

	var ret []*datapb.ChannelWatchInfo

	for i, k := range keys {
		watchInfo, err := parseWatchInfo(k, []byte(values[i]))
		if err != nil {
			// TODO: delete this kv later
			log.Warn("invalid watchInfo loaded", zap.Error(err))
			continue
		}

		ret = append(ret, watchInfo)
	}

	return ret, nil
}

// startOne can write ToWatch or ToRelease states.
func (c *channelStateTimer) startOne(watchState datapb.ChannelWatchState, channelName string, nodeID UniqueID, timeoutTs int64) {
	if timeoutTs == 0 {
		log.Info("zero timeoutTs, skip starting timer",
			zap.String("watch state", watchState.String()),
			zap.Int64("nodeID", nodeID),
			zap.String("channel name", channelName),
		)
		return
	}

	c.removeTimers(channelName)
	stop := make(chan struct{})
	timeoutT := time.Unix(0, timeoutTs)
	timer := time.NewTimer(time.Until(timeoutT))
	c.timerStops.Store(channelName, stop)
	c.timers.Store(channelName, timer)

	go func() {
		log.Info("timer started",
			zap.String("watch state", watchState.String()),
			zap.Int64("nodeID", nodeID),
			zap.String("channel name", channelName))

		select {
		case <-timer.C:
			log.Info("timeout and stop timer: wait for channel ACK timeout",
				zap.String("watch state", watchState.String()),
				zap.Int64("nodeID", nodeID),
				zap.String("channel name", channelName))
			ackType := getAckType(watchState)
			c.notifyTimeoutWatcher(&ackEvent{ackType, channelName, nodeID})
		case <-stop:
			log.Info("stop timer before timeout",
				zap.String("watch state", watchState.String()),
				zap.Int64("nodeID", nodeID),
				zap.String("channel name", channelName))
		}
	}()
}

func (c *channelStateTimer) notifyTimeoutWatcher(e *ackEvent) {
	c.timeoutWatcher <- e
}

func (c *channelStateTimer) removeTimers(channels ...string) {
	for _, channel := range channels {
		if stop, ok := c.timerStops.LoadAndDelete(channel); ok {
			close(stop.(chan struct{}))
			c.timers.Delete(channel)
		}
	}
}

//reset set timers
func (c *channelStateTimer) resetTimers(timeoutTs int64, channels ...string) {
	for _, channel := range channels {
		if timer, ok := c.timers.Load(channel); ok {
			timer.(*time.Timer).Reset(time.Second * time.Duration(timeoutTs))
		}
	}
}

func parseWatchInfo(key string, data []byte) (*datapb.ChannelWatchInfo, error) {
	watchInfo := datapb.ChannelWatchInfo{}
	if err := proto.Unmarshal(data, &watchInfo); err != nil {
		return nil, fmt.Errorf("invalid event data: fail to parse ChannelWatchInfo, key: %s, err: %v", key, err)
	}

	if watchInfo.Vchan == nil {
		return nil, fmt.Errorf("invalid event: ChannelWatchInfo with nil VChannelInfo, key: %s", key)
	}
	reviseVChannelInfo(watchInfo.GetVchan())

	return &watchInfo, nil
}

// parseAckEvent transfers key-values from etcd into ackEvent
func parseAckEvent(nodeID UniqueID, info *datapb.ChannelWatchInfo) *ackEvent {
	ret := &ackEvent{
		ackType:     getAckType(info.GetState()),
		channelName: info.GetVchan().GetChannelName(),
		nodeID:      nodeID,
	}
	return ret
}

func getAckType(state datapb.ChannelWatchState) ackType {
	switch state {
	case datapb.ChannelWatchState_WatchSuccess, datapb.ChannelWatchState_Complete:
		return watchSuccessAck
	case datapb.ChannelWatchState_WatchFailure:
		return watchFailAck
	case datapb.ChannelWatchState_ReleaseSuccess:
		return releaseSuccessAck
	case datapb.ChannelWatchState_ReleaseFailure:
		return releaseFailAck
	case datapb.ChannelWatchState_ToWatch, datapb.ChannelWatchState_Uncomplete: // unchange watch states generates timeout acks
		return watchTimeoutAck
	case datapb.ChannelWatchState_ToRelease: // unchange watch states generates timeout acks
		return releaseTimeoutAck
	default:
		return invalidAck
	}
}
