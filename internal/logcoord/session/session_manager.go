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

import "sync"

type SessionManager struct {
	sessions struct {
		sync.RWMutex
		data map[int64]*Session
	}
	creator  SessionCreator
	notifyCh chan int64
}

func (s *SessionManager) Notify() chan<- int64 {
	return s.notifyCh
}

func (s *SessionManager) AddSession(nodeID int64, address string) {
	s.sessions.Lock()
	defer s.sessions.Unlock()

	session := NewSession(nodeID, address, s.creator)
	s.sessions.data[nodeID] = session
	s.notifyCh <- nodeID
}

func (s *SessionManager) GetSessions(nodeID int64) *Session {
	s.sessions.RLock()
	defer s.sessions.RUnlock()

	session, ok := s.sessions.data[nodeID]
	if !ok {
		return nil
	}
	return session
}

func NewSessionManager(creator SessionCreator) *SessionManager {
	return &SessionManager{
		sessions: struct {
			sync.RWMutex
			data map[int64]*Session
		}{data: make(map[int64]*Session)},
		creator:  creator,
		notifyCh: make(chan int64, 128),
	}
}
