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
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"go.uber.org/zap"
)

type Node interface {
	Name() string
	Operate(in Msg) Msg
	Start()
	Close()
}

type nodeCtx struct {
	node Node

	inputChannel chan Msg

	next    *nodeCtx
	checker *timerecord.GroupChecker

	closeCh *chan struct{} // notify work to exit
	closeWg *sync.WaitGroup
}

func (c *nodeCtx) Start() {
	c.closeWg.Add(1)
	c.node.Start()
	go c.work()
}

func (c *nodeCtx) work() {
	name := fmt.Sprintf("nodeCtxTtChecker-%s", c.node.Name())
	if c.checker != nil {
		c.checker.Check(name)
		defer c.checker.Remove(name)
	}

	for {
		select {
		case <-*c.closeCh:
			c.close()
			log.Debug("flow graph node closed", zap.String("nodeName", c.node.Name()))
			return
		default:
			var input, output Msg
			input = <-c.inputChannel
			output = c.node.Operate(input)

			if c.checker != nil {
				c.checker.Check(name)
			}
			if c.next != nil {
				c.next.inputChannel <- &output
			}
		}
	}
}

func (c *nodeCtx) close() {
	c.node.Close()
	if c.next != nil && c.next.inputChannel != nil {
		close(c.next.inputChannel)
	}
	c.closeWg.Done()
}

func (c *nodeCtx) Close() {
	close(*c.closeCh)
}

type BaseNode struct {
	name string
}

// Return name of Node
func (node *BaseNode) Name() string {
	return node.name
}

// Start implementing Node, base node does nothing when starts
func (node *BaseNode) Start() {}

// Close implementing Node, base node does nothing when stops
func (node *BaseNode) Close() {}
