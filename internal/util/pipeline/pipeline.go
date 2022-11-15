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
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"go.uber.org/zap"
)

type Pipeline struct {
	nodes []*nodeCtx

	inputChannel chan Msg

	closeCh chan struct{} // notify work to exit
	closeWg sync.WaitGroup

	nodeTtInterval  time.Duration
	enableTtChecker bool
}

func (p *Pipeline) AddNode(node Node, queueLen int) {
	nodeCtx := nodeCtx{
		node:         node,
		inputChannel: make(chan Msg, queueLen),
		closeCh:      p.closeCh,
		closeWg:      &p.closeWg,
	}

	nodeCtx.checker = timerecord.GetGroupChecker("fgNode", p.nodeTtInterval, func(list []string) {
		log.Warn("some node(s) haven't received input", zap.Strings("list", list), zap.Duration("duration ", p.nodeTtInterval))
	})

	if len(p.nodes) != 0 {
		p.nodes[len(p.nodes)-1].next = &nodeCtx
	} else {
		p.inputChannel = nodeCtx.inputChannel
	}

	p.nodes = append(p.nodes, &nodeCtx)
}

func (p *Pipeline) Start() {
	if len(p.nodes) == 0 {
		log.Error("start an empty pipeline")
	}
	for _, node := range p.nodes {
		node.Start()
	}
}

func (p *Pipeline) Close() {
	close(p.closeCh)
	p.closeWg.Wait()
}

type StreamPipeline struct {
	*Pipeline
	instream  msgstream.MsgStream
	startOnce sync.Once
}

func (p *StreamPipeline) work() {
	p.closeWg.Add(1)
	defer p.closeWg.Done()
	for {
		select {
		case <-p.closeCh:
			p.Close()
			return
		case msg := <-p.instream.Chan():
			p.nodes[0].inputChannel <- &msg
		}
	}
}

func (p *StreamPipeline) Start() {
	p.instream.Start()
	p.Pipeline.Start()
	go p.work()
}

func NewPipelineWithStream(instream msgstream.MsgStream, nodeTtInterval time.Duration, enableTtChecker bool) *StreamPipeline {
	flowGraph := StreamPipeline{
		Pipeline: &Pipeline{
			nodes:           []*nodeCtx{},
			nodeTtInterval:  nodeTtInterval,
			enableTtChecker: enableTtChecker,
			closeCh:         make(chan struct{}),
			closeWg:         sync.WaitGroup{},
		},
		instream: instream,
	}

	return &flowGraph
}
