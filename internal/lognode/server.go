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
	"os"
	"sync"
	"syscall"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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

func (node *LogNode) UpdateStateCode(stateCode commonpb.StateCode) {
	node.lifetime.SetState(stateCode)
}
