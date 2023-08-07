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

package logservice

import (
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
)

type lsConsumer struct{}

func NewLogServiceConsumer(logcoord types.LogCoord, options mqwrapper.ConsumerOptions) (mqwrapper.Consumer, error) {
	return &lsConsumer{}, nil
}

// returns the subscription for the consumer
func (lc *lsConsumer) Subscription() string {
	panic("not implemented") // TODO: Implement
}

// Get Message channel, once you chan you can not seek again
func (lc *lsConsumer) Chan() <-chan mqwrapper.Message {
	panic("not implemented") // TODO: Implement
}

// Seek to the uniqueID position
func (lc *lsConsumer) Seek(_ mqwrapper.MessageID, _ bool) error {
	panic("not implemented") // TODO: Implement
}

// Ack make sure that msg is received
func (lc *lsConsumer) Ack(_ mqwrapper.Message) {
	panic("not implemented") // TODO: Implement
}

// Close consumer
func (lc *lsConsumer) Close() {
	panic("not implemented") // TODO: Implement
}

// GetLatestMsgID return the latest message ID
func (lc *lsConsumer) GetLatestMsgID() (mqwrapper.MessageID, error) {
	panic("not implemented") // TODO: Implement
}

// check created topic whether vaild or not
func (lc *lsConsumer) CheckTopicValid(channel string) error {
	panic("not implemented") // TODO: Implement
}
