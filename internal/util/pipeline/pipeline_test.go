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
	"github.com/milvus-io/milvus/internal/mq/msgstream"
)

type testNode struct {
	BaseNode
	sum int
}

func (t *testNode) Operate(in Msg) Msg {
	_, ok := in.(msgstream.MsgPack)
	if ok {
		t.sum += 1
	}
	return nil
}

// func TestPipeline(t *testing.T) {
// 	stream := msgstream.NewMockMsgStream()
// 	pipeline := NewPipelineWithStream(stream, 2*time.Minute, true)

// 	nodeA := testNode{
// 		BaseNode: BaseNode{
// 			name: "Test-Node-A",
// 		},
// 	}

// 	pipeline.AddNode(&nodeA, 8)

// 	pipeline.Start()
// 	pipeline.nodes[0].inputChannel <- msgstream.MsgPack{}
// }
