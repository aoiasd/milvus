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

package lognode

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"go.uber.org/zap"
)

func (node *LogNode) WatchChannel(ctx context.Context, req *logpb.WatchChannelRequest) (*commonpb.Status, error) {
	log.Debug("received WatchChannel Request",
		zap.Int64("msgID", req.GetBase().GetMsgID()),
		zap.String("pChannel", req.GetPChannel()))

	if !node.lifetime.Add(commonpbutil.IsHealthy) {
		msg := fmt.Sprintf("log node %d is not ready", paramtable.GetNodeID())
		err := merr.WrapErrServiceNotReady(msg)
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	if !CheckTargetID(req) {
		targetID := req.GetBase().GetTargetID()
		log.Warn("target ID not match",
			zap.Int64("targetID", targetID),
			zap.Int64("nodeID", paramtable.GetNodeID()),
		)
		return util.WrapStatus(commonpb.ErrorCode_NodeIDNotMatch, common.WrapNodeIDNotMatchMsg(targetID, paramtable.GetNodeID())), nil
	}

	err := node.loggerManager.AddLogger(ctx, req.GetPChannel())
	return merr.Status(err), nil
}

func (node *LogNode) Insert(ctx context.Context, req *logpb.InsertRequest) (*commonpb.Status, error) {
	log.Debug("received WatchChannel Request",
		zap.Int64("msgID", req.GetBase().GetMsgID()),
		zap.Strings("vChannels", req.GetVChannels()))

	if !node.lifetime.Add(commonpbutil.IsHealthy) {
		msg := fmt.Sprintf("log node %d is not ready", paramtable.GetNodeID())
		err := merr.WrapErrServiceNotReady(msg)
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	if !CheckTargetID(req) {
		targetID := req.GetBase().GetTargetID()
		log.Warn("target ID not match",
			zap.Int64("targetID", targetID),
			zap.Int64("nodeID", paramtable.GetNodeID()),
		)
		return util.WrapStatus(commonpb.ErrorCode_NodeIDNotMatch, common.WrapNodeIDNotMatchMsg(targetID, paramtable.GetNodeID())), nil
	}

	err := node.loggerManager.Produce()
	return merr.Status(nil), nil
}
