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
	"fmt"
	"reflect"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/util/pipeline"
	"go.uber.org/zap"
)

type filterNode struct {
	pipeline.BaseNode
	collectionID     UniqueID
	manager          Manager
	channel          Channel
	InsertMsgPolicys []InsertMsgFilter
	DeleteMsgPolicys []DeleteMsgFilter
}

func (fNode *filterNode) Operate(in pipeline.Msg) pipeline.Msg {
	if in == nil {
		log.Debug("type assertion failed for MsgStreamMsg because it's nil", zap.String("name", fNode.Name()))
		return nil
	}

	streamMsgPack, ok := in.(*msgstream.MsgPack)
	if !ok {
		log.Warn("type assertion failed for MsgPack", zap.String("msgType", reflect.TypeOf(in).Name()), zap.String("name", fNode.Name()))
		return nil
	}

	collection := fNode.manager.GetCollection(fNode.collectionID)
	out := workNodeMsg{
		insertMsgs: []*InsertMsg{},
		deleteMsgs: []*DeleteMsg{},
		timeRange: TimeRange{
			timestampMin: streamMsgPack.BeginTs,
			timestampMax: streamMsgPack.EndTs,
		},
	}

	//add msg to out if msg pass check of filter
	for _, msg := range streamMsgPack.Msgs {
		err := fNode.filtrate(collection, msg)
		if err != nil {
			log.Debug(fmt.Sprintf("filter invalid message: %s", err.Error()),
				zap.String("message type", msg.Type().String()),
				zap.String("channel", fNode.channel),
				zap.Int64("collectionID", fNode.collectionID))
		} else {
			out.append(msg)
		}
	}
	return out
}

func (fNode *filterNode) filtrate(c *Collection, msg msgstream.TsMsg) error {
	switch msg.Type() {
	case commonpb.MsgType_Insert:
		for _, policy := range fNode.InsertMsgPolicys {
			err := policy(c, msg.(*msgstream.InsertMsg))
			if err != nil {
				return err
			}
		}

	case commonpb.MsgType_Delete:
		for _, policy := range fNode.DeleteMsgPolicys {
			err := policy(c, msg.(*msgstream.DeleteMsg))
			if err != nil {
				return err
			}
		}
	default:
		return errors.New("invalid message type")
	}
	return nil
}

func NewfilterNode(collectionID UniqueID, manager Manager, channel Channel) *filterNode {
	return &filterNode{
		collectionID: collectionID,
		manager:      manager,
		channel:      channel,
		InsertMsgPolicys: []InsertMsgFilter{
			NotAlignedInsert,
			EmptyInsert,
			StrayedInsert,
			//TODO filter excluded segments
		},
		DeleteMsgPolicys: []DeleteMsgFilter{
			NotAlignedDelete,
			EmptyDelete,
			StrayedDelete,
		},
	}
}
