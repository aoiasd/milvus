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

// lsProducer wraps LogService cluster as `mqwrapper.Producer`.
type lsProducer struct {
	// producer options
	options mqwrapper.ProducerOptions

	// logcoord is the reference to LogCoord
	// handling topic dispatch and reassign when node offline
	logcoord types.LogCoord
}

func NewLogServiceProducer(logcoord types.LogCoord, options mqwrapper.ProducerOptions) (mqwrapper.Producer, error) {
	return &lsProducer{
		logcoord: logcoord,
		options:  options,
	}, nil
}

// Send produce message into logservice.
// internally, it finds the log node related to the topic and invoke produce method.
func (lp lsProducer) Send(ctx context.Context, message *mqwrapper.ProducerMessage) (mqwrapper.MessageID, error) {
	panic("not implemented") // TODO: Implement
}

func (lp lsProducer) Close() {
	panic("not implemented") // TODO: Implement
}
