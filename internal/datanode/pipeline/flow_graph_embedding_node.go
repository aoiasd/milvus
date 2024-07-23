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
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/samber/lo"
	"go.uber.org/zap"
)

// TODO support set EmbddingType
// type EmbeddingType int32

type embeddingNode struct {
	BaseNode

	schema      *schemapb.CollectionSchema
	pkField     *schemapb.FieldSchema
	channelName string

	// embeddingType EmbeddingType
	vectorizers map[int64]vectorizer.Vectorizer
}

func newEmbeddingNode(channelName string, schema *schemapb.CollectionSchema) (*embeddingNode, error) {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(paramtable.Get().DataNodeCfg.FlowGraphMaxQueueLength.GetAsInt32())
	baseNode.SetMaxParallelism(paramtable.Get().DataNodeCfg.FlowGraphMaxParallelism.GetAsInt32())

	node := &embeddingNode{
		BaseNode:    baseNode,
		channelName: channelName,
		schema:      schema,
		vectorizers: make(map[int64]vectorizer.Vectorizer),
	}

	for _, field := range schema.GetFields() {
		if field.IsPrimaryKey == true {
			node.pkField = field
		}

		// TODO SCHEMA
		// tokenizer, err := ctokenizer.NewTokenizer(make(map[string]string))
		// if err != nil {
		// 	return nil, err
		// }
		// node.vectorizers[field.GetFieldID()] = vectorizer.NewHashVectorizer(field, tokenizer)

	}
	return node, nil
}

func (eNode *embeddingNode) Name() string {
	return fmt.Sprintf("embeddingNode-%s-%s", "BM25test", eNode.channelName)
}

func (eNode *embeddingNode) vectorize(data *storage.InsertData, meta map[int64]storage.ChannelStats) error {
	for _, field := range eNode.schema.Fields {
		vectorizer, ok := eNode.vectorizers[field.GetFieldID()]
		if !ok {
			continue
		}

		//TODO Get Relate Field ID
		embeddingFieldID := int64(0)

		if _, ok := meta[field.GetFieldID()]; !ok {
			meta[field.GetFieldID()] = storage.NewBM25Stats()
		}

		embeddingData, ok := data.Data[embeddingFieldID].GetRows().([]string)
		if !ok {
			// TODO
			return fmt.Errorf("")
		}

		dim, sparseVector, err := vectorizer.Vectorize(meta[field.GetFieldID()], embeddingData...)
		if err != nil {
			return err
		}
		data.Data[field.GetFieldID()] = BuildSparseFieldData(dim, sparseVector)
	}
	return nil
}

func (eNode *embeddingNode) prepareInsert(insertMsgs []*msgstream.InsertMsg, meta map[int64]storage.ChannelStats) ([]*writebuffer.InsertData, error) {
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

			err = eNode.vectorize(data, meta)
			if err != nil {
				log.Warn("failed to embedding insert data", zap.Error(err))
				return nil, err
			}
			inData.Append(data, pkFieldData, tsFieldData)
		}
		result = append(result, inData)
	}
	return result, nil
}

func (eNode *embeddingNode) Operate(in []Msg) []Msg {
	fgMsg := in[0].(*FlowGraphMsg)

	if fgMsg.IsCloseMsg() {
		return []Msg{fgMsg}
	}

	meta := make(map[int64]storage.ChannelStats)
	insertData, err := eNode.prepareInsert(fgMsg.InsertMessages, meta)
	if err != nil {
		log.Error("failed to prepare insert data", zap.Error(err))
		panic(err)
	}

	fgMsg.ChannelStats = meta
	fgMsg.InsertData = insertData
	fgMsg.InsertMessages = nil
	return []Msg{fgMsg}
}

func BuildSparseFieldData(dim int64, data [][]byte) storage.FieldData {
	return &storage.SparseFloatVectorFieldData{
		SparseFloatArray: schemapb.SparseFloatArray{
			Contents: data,
			Dim:      dim,
		},
	}
}
