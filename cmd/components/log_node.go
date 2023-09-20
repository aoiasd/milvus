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

package components

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	grpclognode "github.com/milvus-io/milvus/internal/distributed/lognode"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"go.uber.org/zap"
)

type LogNode struct {
	ctx context.Context
	svr *grpclognode.Server
}

// NewLogNode creates a new LogNode
func NewLogNode(ctx context.Context, factory dependency.Factory) (*LogNode, error) {
	svr, err := grpclognode.NewServer(ctx, factory)
	if err != nil {
		return nil, err
	}

	return &LogNode{
		ctx: ctx,
		svr: svr,
	}, nil
}

// Run starts service
func (n *LogNode) Run() error {
	if err := n.svr.Run(); err != nil {
		log.Error("LogNode starts error", zap.Error(err))
		return err
	}
	log.Debug("LogNode successfully started")
	return nil
}

// Stop terminates service
func (n *LogNode) Stop() error {
	if err := n.svr.Stop(); err != nil {
		return err
	}
	return nil
}

// GetComponentStates returns LogNode's states
func (n *LogNode) Health(ctx context.Context) commonpb.StateCode {
	resp, err := n.svr.GetComponentStates(ctx)
	if err != nil {
		return commonpb.StateCode_Abnormal
	}
	return resp.State.GetStateCode()
}

func (q *LogNode) GetName() string {
	return typeutil.LogNodeRole
}
