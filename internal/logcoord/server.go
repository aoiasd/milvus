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
	"github.com/milvus-io/milvus/internal/logcoord/balance"
	"github.com/milvus-io/milvus/internal/logcoord/meta"
	"github.com/milvus-io/milvus/internal/logcoord/session"
)

type Server struct {
	meta             *meta.ChannelsMeta
	channelAllocator balance.ChannelAllocator
	nodeBalancer     balance.NodeBalancer
	sessionManager   session.SessionManager
}

func (m *Server) Init() {
	m.nodeBalancer.Start()
}

func (m *Server) AllocChannel(collectionID uint64, num int) []string {
	return m.channelAllocator.Alloc(collectionID, num)
}
