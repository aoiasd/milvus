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

package wal

import (
	"context"

	"github.com/milvus-io/milvus/pkg/mq/msgstream"
)

type WriteAheadLogger struct {
	channel     string
	stream      msgstream.MsgStream
	tsAllocator TimestampAllocator
}

func (logger *WriteAheadLogger) Produce(ctx context.Context, msg *msgstream.MsgPack) error {
	ts, err := logger.tsAllocator.AllocOne(ctx)
	if err != nil {
		return err
	}

	msg.BeginTs = ts
	msg.EndTs = ts

	err = logger.stream.Produce(msg)
	if err != nil {
		return err
	}
	return nil
}

// func (logger *WriteAheadLogger)
