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

package logcoord

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/logcoord/channel"
	"github.com/milvus-io/milvus/internal/logcoord/session"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
)

type Server struct {
	channelManager channel.Manager
	sessionManager *session.SessionManager
	streamFactory  msgstream.Factory

	initOnce  sync.Once
	startOnce sync.Once
	stopOnce  sync.Once
}

func NewLogCoord(ctx context.Context, factory msgstream.Factory, etcdSession *sessionutil.Session) *Server {
	return &Server{
		channelManager: channel.NewChannelManager(ctx, factory),
		streamFactory:  factory,
		sessionManager: session.NewSessionManager(session.DefaultLogNodeConnector, etcdSession),
	}
}

func (m *Server) Init() error {
	var err error = nil
	m.initOnce.Do(func() {
		err = m.sessionManager.Init()
	})
	return err
}

func (m *Server) Start() {
	m.startOnce.Do(func() {
		m.sessionManager.Start()
	})
}

func (m *Server) Stop() {
	m.stopOnce.Do(func() {
		m.sessionManager.Stop()
	})
}

func (m *Server) AllocVChannels(collectionID uint64, num int) []string {
	return m.channelManager.AllocVChannels(collectionID, num)
}
