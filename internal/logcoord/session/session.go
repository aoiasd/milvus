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
	"context"
	"fmt"

	grpclognodeclient "github.com/milvus-io/milvus/internal/distributed/lognode/client"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

type SessionConnector func(ctx context.Context, addr string, nodeID int64) (types.LogNodeClient, error)
type SessionType int32
type Session struct {
	nodeID    int64
	address   string
	client    types.LogNodeClient
	connector SessionConnector
}

func (s *Session) connect(ctx context.Context) error {
	if s.connector == nil {
		return fmt.Errorf("unable to create client for %s because of a nil client creator", s.address)
	}

	client, err := s.connector(ctx, s.address, s.nodeID)
	if err != nil {
		return err
	}
	s.client = client
	return nil
}

func (s *Session) GetClient(ctx context.Context) types.LogNodeClient {
	if s.client == nil {
		err := s.connect(ctx)
		if err != nil {
			log.Info("session connect failed", zap.Int64("nodeID", s.nodeID), zap.String("address", s.address))
		}
	}
	return s.client
}

func NewSession(nodeID int64, address string, connector SessionConnector) *Session {
	return &Session{
		nodeID:    nodeID,
		address:   address,
		connector: connector,
	}
}

func DefaultLogNodeConnector(ctx context.Context, addr string, nodeID int64) (types.LogNodeClient, error) {
	return grpclognodeclient.NewClient(ctx, addr, nodeID)
}
