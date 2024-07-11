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

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/writebuffer"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/vectorizer"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/samber/lo"
	"go.uber.org/zap"
)

// TODO Support String and move type to proto
type EmbeddingType int32

const (
	Hash = 0
)

type embeddingNode struct {
	BaseNode

	schema        *schemapb.CollectionSchema
	pkField       *schemapb.FieldSchema
	channelName   string
	embeddingType EmbeddingType

	vectorizer vectorizer.Vectorizer
}

func (eNode *embeddingNode) Name() string {
	return fmt.Sprintf("embeddingNode-%s-%s", eNode.embeddingType, eNode.channelName)
}

func (eNode *embeddingNode) prepareInsert(insertMsgs []*msgstream.InsertMsg) ([]*writebuffer.InsertData, error) {
	groups := lo.GroupBy(insertMsgs, func(msg *msgstream.InsertMsg) int64 { return msg.SegmentID })
	segmentPartition := lo.SliceToMap(insertMsgs, func(msg *msgstream.InsertMsg) (int64, int64) { return msg.GetSegmentID(), msg.GetPartitionID() })

	result := make([]*writebuffer.InsertData, 0, len(groups))
	for segment, msgs := range groups {
		inData := writebuffer.NewInsertData(segment, segmentPartition[segment], len(msgs), eNode.pkField.GetDataType())

		for _, msg := range msgs {
			data, err := storage.InsertMsgToInsertData(msg, eNode.schema)
			if err != nil {
				log.Warn("failed to transfer insert msg to insert data", zap.Error(err))
				return nil, err
			}

			pkFieldData, err := storage.GetPkFromInsertData(eNode.schema, data)
			if err != nil {
				return nil, err
			}
			if pkFieldData.RowNum() != data.GetRowNum() {
				return nil, merr.WrapErrServiceInternal("pk column row num not match")
			}

			tsFieldData, err := storage.GetTimestampFromInsertData(data)
			if err != nil {
				return nil, err
			}
			if tsFieldData.RowNum() != data.GetRowNum() {
				return nil, merr.WrapErrServiceInternal("timestamp column row num not match")
			}

			emData, err := eNode.vectorizer.Vectorize(data)
			if err != nil {
				log.Warn("failed to embedding insert data", zap.Error(err))
				return nil, err
			}
			inData.Append(data, emData, pkFieldData, tsFieldData)
		}
		result = append(result, inData)
	}
	return result, nil
}

func (eNode *embeddingNode) Opearte(in []Msg) []Msg {
	fgMsg := in[0].(*FlowGraphMsg)
	return []Msg{fgMsg}
}
