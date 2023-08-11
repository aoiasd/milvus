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

package balance

import (
	"sync"

	"github.com/milvus-io/milvus/internal/logcoord/allocators"
)

type NodeBalancer struct {
	nodeAllocator *allocators.NodeAllocator

	startOnce sync.Once
	stopOnce  sync.Once
}

func (b *NodeBalancer) Start() {
	b.startOnce.Do(func() {
		go b.work()
	})
}

func (b *NodeBalancer) work() {
	for {
		select 
	}
}
