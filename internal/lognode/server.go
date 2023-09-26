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
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/lognode/wal"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	etcd "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type LogNode struct {
	// channelMananger
	// flushManager
	// dispatcher

	ctx    context.Context
	cancel context.CancelFunc

	lifetime lifetime.Lifetime[commonpb.StateCode]

	initOnce  sync.Once
	startOnce sync.Once
	stopOnce  sync.Once

	session *sessionutil.Session
	etcdCli *etcd.Client
	address string

	factory       dependency.Factory
	loggerManager *wal.LoggerManager
	idAllocator   *allocator.IDAllocator

	rootCoord types.RootCoordClient
}

func NewLogNode(ctx context.Context, factory dependency.Factory) *LogNode {
	ctx, cancel := context.WithCancel(ctx)

	return &LogNode{
		ctx:      ctx,
		cancel:   cancel,
		factory:  factory,
		lifetime: lifetime.NewLifetime(commonpb.StateCode_Abnormal),
	}
}

func (node *LogNode) initSession() error {
	node.session = sessionutil.NewSession(node.ctx, paramtable.Get().EtcdCfg.MetaRootPath.GetValue(), node.etcdCli)
	if node.session == nil {
		return fmt.Errorf("session is nil, the etcd client connection may have failed")
	}
	node.session.Init(typeutil.QueryNodeRole, node.address, false, true)
	paramtable.SetNodeID(node.session.ServerID)
	log.Info("LogNode init session", zap.Int64("nodeID", paramtable.GetNodeID()), zap.String("node address", node.session.Address))
	return nil
}

// Register register log node at etcd
func (node *LogNode) Register() error {
	node.session.Register()
	// start liveness check
	node.session.LivenessCheck(node.ctx, func() {
		log.Error("LogNode disconnected from etcd, process will exit", zap.Int64("Server Id", paramtable.GetNodeID()))
		if err := node.Stop(); err != nil {
			log.Fatal("failed to stop server", zap.Error(err))
		}

		// manually send signal to starter goroutine
		if node.session.TriggerKill {
			if p, err := os.FindProcess(os.Getpid()); err == nil {
				p.Signal(syscall.SIGINT)
			}
		}
	})
	return nil
}

func (node *LogNode) Init() error {
	var err error
	node.initOnce.Do(func() {
		err = node.initSession()
		if err != nil {
			log.Error("LogNode init session failed",
				zap.Int64("nodeID", paramtable.GetNodeID()),
				zap.String("node address", node.session.Address))
			return
		}

		node.factory.Init(paramtable.Get())

		// init wal logger manager
		node.loggerManager = wal.NewLoggerManger(node.factory)
		err = node.loggerManager.Init(node.rootCoord)
		if err != nil {
			return
		}

		node.idAllocator, err = allocator.NewIDAllocator(node.ctx, node.rootCoord, paramtable.GetNodeID())
		if err != nil {
			log.Warn("failed to create id allocator",
				zap.String("role", typeutil.LogNodeRole),
				zap.Int64("nodeID", paramtable.GetNodeID()),
				zap.Error(err))
			return
		}
	})

	return err
}

func (node *LogNode) Start() error {
	var err error

	node.startOnce.Do(func() {
		if err = node.idAllocator.Start(); err != nil {
			log.Warn("failed to start id allocator",
				zap.String("role", typeutil.LogNodeRole),
				zap.Int64("nodeID", paramtable.GetNodeID()),
				zap.Error(err))
			return
		}
		log.Debug("start id allocator done", zap.String("role", typeutil.LogNodeRole), zap.Int64("nodeID", paramtable.GetNodeID()))

		node.UpdateStateCode(commonpb.StateCode_Healthy)
		log.Info("lognode node start successfully",
			zap.Int64("queryNodeID", paramtable.GetNodeID()),
			zap.String("Address", node.address),
		)
	})

	return err
}

func (node *LogNode) Stop() error {
	return nil
}

func (node *LogNode) SetAddress(address string) {
	node.address = address
}

func (node *LogNode) SetEtcdClient(etcdClient *etcd.Client) {
	node.etcdCli = etcdClient
}

func (node *LogNode) SetRootCoord(rc types.RootCoordClient) error {
	if rc == nil {
		return errors.New("null RootCoord interface")
	}

	node.rootCoord = rc
	return nil
}

func (node *LogNode) UpdateStateCode(stateCode commonpb.StateCode) {
	node.lifetime.SetState(stateCode)
}

func (node *LogNode) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	code := node.lifetime.GetState()
	nodeID := common.NotRegisteredID
	if node.session != nil && node.session.Registered() {
		nodeID = paramtable.GetNodeID()
	}

	resp := &milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			NodeID:    nodeID,
			Role:      typeutil.LogNodeRole,
			StateCode: code,
		},
		Status: merr.Status(nil),
	}
	return resp, nil
}

// GetStatisticsChannel returns the statistics channel
// Statistics channel contains statistics infos of query nodes, such as segment infos, memory infos
func (node *LogNode) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: merr.Status(nil),
	}, nil
}
