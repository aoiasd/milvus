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

package session

import (
	"sync"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
)

// SessionManager manger all lognode session
type Manager interface {
	Init() error
	Start()
	Stop()

	AddSession(nodeID int64, address string)
	RemoveSession(nodeID int64)
	GetSessions(nodeID int64)
}

// SessionManager manage lognode sessions
// And trigger balance when log node changes
type SessionManager struct {
	sessions struct {
		sync.RWMutex
		data map[int64]*Session
	}

	connector SessionConnector
	balancer  *SessionBalancer
	observer  *SessionObserver
}

func NewSessionManager(connector SessionConnector, etcdSession *sessionutil.Session) *SessionManager {
	manager := &SessionManager{
		sessions: struct {
			sync.RWMutex
			data map[int64]*Session
		}{data: make(map[int64]*Session)},
		connector: connector,
	}
	manager.observer = NewSessionObserver(manager, etcdSession)
	manager.balancer = NewSessionBalancer(manager)

	return manager
}

func (m *SessionManager) Init() error {
	err := m.observer.Init()
	if err != nil {
		return err
	}

	return nil
}

func (m *SessionManager) Start() {
	m.balancer.Start()
	m.observer.Start()
}

func (s *SessionManager) Stop() {
	s.observer.Stop()
	s.balancer.Stop()
}

func (m *SessionManager) AddSession(nodeID int64, address string) {
	m.sessions.Lock()
	defer m.sessions.Unlock()

	session := NewSession(nodeID, address, m.connector)
	m.sessions.data[nodeID] = session
	m.balancer.AddNode(session.nodeID)
}

func (m *SessionManager) RemoveSession(nodeID int64) {
	m.sessions.Lock()
	defer m.sessions.Unlock()

	delete(m.sessions.data, nodeID)
	m.balancer.RemoveNode(nodeID)
}

func (m *SessionManager) GetSessions(nodeID int64) *Session {
	m.sessions.RLock()
	defer m.sessions.RUnlock()

	session, ok := m.sessions.data[nodeID]
	if !ok {
		return nil
	}
	return session
}
