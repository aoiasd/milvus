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

package querynodev2

import (
	"errors"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
)

type workNodeMsg struct {
	insertMsgs []*InsertMsg
	deleteMsgs []*DeleteMsg
	timeRange  TimeRange
}

func (msg *workNodeMsg) append(taskMsg msgstream.TsMsg) error {
	switch taskMsg.Type() {
	case commonpb.MsgType_Insert:
		msg.insertMsgs = append(msg.insertMsgs, taskMsg.(*InsertMsg))
	case commonpb.MsgType_Delete:
		msg.deleteMsgs = append(msg.deleteMsgs, taskMsg.(*DeleteMsg))
	default:
		return errors.New("add message of invalid type")
	}
	return nil
}
