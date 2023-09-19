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
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

type TimestampAllocator interface {
	AllocOne(ctx context.Context) (uint64, error)
	Refresh(ctx context.Context) (uint64, error)
}

type RemoteTimestampAllocator struct {
	rc     types.RootCoord
	nodeID int64

	retStart uint64
	retCnt   uint32
	retSize  uint32

	mu sync.Mutex
}

// newTimestampAllocator creates a new timestampAllocator
func NewTimestampAllocator(retSize uint32, rc types.RootCoord) (*RemoteTimestampAllocator, error) {
	a := &RemoteTimestampAllocator{
		nodeID:  paramtable.GetNodeID(),
		rc:      rc,
		retSize: retSize,
		mu:      sync.Mutex{},
	}
	return a, nil
}

func (ta *RemoteTimestampAllocator) refresh(ctx context.Context) error {
	tr := timerecord.NewTimeRecorder("refreshTimestamp")
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req := &rootcoordpb.AllocTimestampRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_RequestTSO),
			commonpbutil.WithMsgID(0),
			commonpbutil.WithSourceID(ta.nodeID),
		),
		Count: ta.retSize,
	}

	resp, err := ta.rc.AllocTimestamp(ctx, req)
	defer func() {
		metrics.ProxyApplyTimestampLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10)).Observe(float64(tr.ElapseSpan().Milliseconds()))
	}()

	if err != nil {
		return fmt.Errorf("syncTimestamp Failed:%w", err)
	}
	if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
		return fmt.Errorf("syncTimestamp Failed:%s", resp.Status.Reason)
	}

	ta.retStart = resp.Timestamp
	ta.retCnt = 0

	return nil
}

func (ta *RemoteTimestampAllocator) Refresh(ctx context.Context) (uint64, error) {
	ta.mu.Lock()
	defer ta.mu.Unlock()

	err := ta.refresh(ctx)
	if err != nil {
		return 0, err
	}

	return ta.retStart, nil
}

// AllocOne allocates a timestamp.
func (ta *RemoteTimestampAllocator) AllocOne(ctx context.Context) (uint64, error) {
	ta.mu.Lock()
	defer ta.mu.Unlock()

	if ta.retCnt >= ta.retSize {
		err := ta.refresh(ctx)
		if err != nil {
			return 0, err
		}
	}
	ret := ta.retStart + uint64(ta.retCnt)
	ta.retCnt++
	return ret, nil
}
