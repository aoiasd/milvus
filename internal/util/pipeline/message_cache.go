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

package pipeline

import (
	"sync"
)

// message cache of pipeline message
// used to limit message size and task sum in pipeline
// consumers use cache like a circle queue
type messageCache struct {
	cache       []Msg
	length      int
	memoryUse   int
	left, right *int

	storeMu sync.Mutex
}

func (c *messageCache) update(position int, msg Msg) {
	if c.cache[position] != nil {
		panic("use err cache position")
	}
	c.cache[position] = msg
}

func (c *messageCache) get(position int) Msg {
	msg := c.cache[position]
	c.cache[position] = nil
	return msg
}

func (c *messageCache) flush() {
	if (*c.left+1)%c.length == *c.right {
		c.storeMu.Lock()
		c.storeMu.Unlock()
		return
	}

}

//a consumer of message cache used in pipeline
type pipelineConsumer struct {
	cache       *messageCache
	left, right int

	storeMu sync.Mutex
}

func (c *pipelineConsumer) pop() Msg {
	c.storeMu.Lock()
	defer c.storeMu.Unlock()

	msg := c.cache.get(c.left)
	c.left = (c.left + 1) % c.cache.length
	return msg
}

func (c *pipelineConsumer) push(msg Msg) {
	c.storeMu.Lock()
	defer c.storeMu.Unlock()

	c.cache.update(c.right, msg)
	c.right = (c.right + 1) % c.cache.length
}
