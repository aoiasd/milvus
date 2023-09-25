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

package channel

import (
	"sync/atomic"
)

type PhysicalChannel struct {
	name    string
	nodeID  atomic.Uint64
	leaseID atomic.Uint64
}

func NewPhysicalChannel(name string) *PhysicalChannel {
	return &PhysicalChannel{
		name: name,
	}
}

func (c *PhysicalChannel) Alloc(nodeID uint64) uint64 {
	c.nodeID.Store(nodeID)
	leaseID := c.leaseID.Add(1)
	return leaseID
}

func (c *PhysicalChannel) Revert(nodeID, leaseID uint64) {
	c.nodeID.Store(nodeID)
	c.leaseID.Store(leaseID)
}

func (c *PhysicalChannel) GetNodeID() uint64 {
	return c.nodeID.Load()
}
