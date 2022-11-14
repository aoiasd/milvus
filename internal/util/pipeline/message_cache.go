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

// a circle queue of message in pipeline
//
type messageCache struct {
	cache       []Msg
	left, right int
	length      int

	memoryUse int
}

func (c *messageCache) Push(msg Msg) {
	c.cache[c.right] = msg
	c.right++
}

func (c *messageCache) Pop() {
	c.cache[c.left] = nil
	c.left++
}

func (c *messageCache) Update(position int, msg Msg) {
	c.cache[position] = msg
}
