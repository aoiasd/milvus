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

	"github.com/milvus-io/milvus/internal/types"
)

type SessionCreator func(ctx context.Context, addr string) (types.LogNode, error)
type Session struct {
	nodeID  int64
	address string
	client  types.LogNode
	creator SessionCreator
}

func (s *Session) init(ctx context.Context) error {
	if s.creator == nil {
		return fmt.Errorf("unable to create client for %s because of a nil client creator", s.address)
	}

	client, err := s.creator(ctx, s.address)
	if err != nil {
		return err
	}
	s.client = client
	return nil
}

func (s *Session) GetClient(ctx context.Context) (types.LogNode, error) {
	if s.client == nil {
		err := s.init(ctx)
		if err != nil {
			return nil, err
		}
	}
	return s.client, nil
}

func NewSession(nodeID int64, address string, creator SessionCreator) *Session {
	return &Session{
		nodeID:  nodeID,
		address: address,
		creator: creator,
	}
}
