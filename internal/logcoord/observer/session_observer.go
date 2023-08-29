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

package observer

import (
	"sync"

	"github.com/blang/semver/v4"
	"github.com/milvus-io/milvus/internal/logcoord/balance"
	"github.com/milvus-io/milvus/internal/logcoord/session"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"go.uber.org/zap"
)

type SessionObserver struct {
	nodeBalancer   *balance.NodeBalancer
	sessionManager *session.SessionManager
	session        *sessionutil.Session

	eventCh <-chan *sessionutil.SessionEvent

	startOnce sync.Once
	stopOnce  sync.Once
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

func (ob *SessionObserver) Init() error {
	r := semver.MustParseRange(">=2.2.3")
	sessions, rev, err := ob.session.GetSessionsWithVersionRange(typeutil.DataNodeRole, r)
	if err != nil {
		log.Warn("LogCoord failed to init service discovery", zap.Error(err))
		return err
	}

	for _, session := range sessions {
		ob.sessionManager.AddSession(session.ServerID, session.Address)
		ob.nodeBalancer.AddNode(session.ServerID)
	}

	ob.eventCh = ob.session.WatchServicesWithVersionRange(typeutil.DataNodeRole, r, rev+1, nil)
	return nil
}

func (ob *SessionObserver) Start() {
	ob.startOnce.Do(func() {
		ob.wg.Add(1)
		go ob.observe()
	})
}

func (ob *SessionObserver) Stop() {
	ob.stopOnce.Do(func() {
		close(ob.stopCh)
		ob.wg.Wait()
	})
}

func (ob *SessionObserver) observe() {
	defer ob.wg.Done()
	for {
		select {
		case event := <-ob.eventCh:
			ob.handlerEvent(event)
		case <-ob.stopCh:
			log.Info("stop log coord session observer")
			return
		}
	}
}

func (ob *SessionObserver) handlerEvent(event *sessionutil.SessionEvent) {
	switch event.EventType {
	case sessionutil.SessionAddEvent:
		ob.sessionManager.AddSession(event.Session.ServerID, event.Session.Address)
		ob.nodeBalancer.AddNode(event.Session.ServerID)
	case sessionutil.SessionDelEvent:
		ob.sessionManager.RemoveSession(event.Session.ServerID)
		ob.nodeBalancer.RemoveNode(event.Session.ServerID)
	default:
		log.Warn("session observer recieve unknown event", zap.String("type", event.EventType.String()))
	}
}
