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
	"context"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
)

func LogServiceDefaultConstructor(logcoord types.LogCoord) func(context.Context) (mqwrapper.Client, error) {
	return func(_ context.Context) (mqwrapper.Client, error) {
		return &LogServiceClient{
			logcoord: logcoord,
		}, nil
	}
}

type LogServiceClient struct {
	logcoord types.LogCoord
}

// CreateProducer creates a producer instance
func (lc *LogServiceClient) CreateProducer(options mqwrapper.ProducerOptions) (mqwrapper.Producer, error) {
	return NewLogServiceProducer(lc.logcoord, options)
}

// Subscribe creates a consumer instance and subscribe a topic
func (lc *LogServiceClient) Subscribe(options mqwrapper.ConsumerOptions) (mqwrapper.Consumer, error) {
	panic("not implemented") // TODO: Implement
}

// Get the earliest MessageID
func (lc *LogServiceClient) EarliestMessageID() mqwrapper.MessageID {
	// TODO use internal mq string to msgID function
	panic("not implemented") // TODO: Implement
}

// String to msg ID
func (lc *LogServiceClient) StringToMsgID(_ string) (mqwrapper.MessageID, error) {
	// TODO use internal mq string to msgID function
	panic("not implemented") // TODO: Implement
}

// Deserialize MessageId from a byte array
func (lc *LogServiceClient) BytesToMsgID(_ []byte) (mqwrapper.MessageID, error) {
	// TODO use internal mq bytes to msgID function
	panic("not implemented") // TODO: Implement
}

// Close the client and free associated resources
func (lc *LogServiceClient) Close() {}
