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

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/storage"
	base "github.com/milvus-io/milvus/internal/util/pipeline"
	"github.com/milvus-io/milvus/internal/util/vectorizer"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// TODO support set EmbddingType
// type EmbeddingType int32

type embeddingNode struct {
	*BaseNode

	schema      *schemapb.CollectionSchema
	pkField     *schemapb.FieldSchema
	channelName string

	// embeddingType EmbeddingType
	vectorizers map[int64]vectorizer.Vectorizer

	delegator     delegator.ShardDelegator
	StartPosition *msgpb.MsgPosition
}

func newEmbeddingNode(channelName string, schema *schemapb.CollectionSchema, delegator delegator.ShardDelegator, maxQueueLength int32) (*embeddingNode, error) {
	node := &embeddingNode{
		BaseNode:      base.NewBaseNode(fmt.Sprintf("EmbeddingNode-%s", channelName), maxQueueLength),
		channelName:   channelName,
		schema:        schema,
		delegator:     delegator,
		vectorizers:   make(map[int64]vectorizer.Vectorizer),
		StartPosition: delegator.GetChannelStatsStartCheckpoint(),
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

func (eNode *embeddingNode) vectorize(msgs []*msgstream.InsertMsg, stats map[int64]storage.ChannelStats) error {
	for _, msg := range msgs {
		var skipStats bool
		if msg.EndTimestamp < eNode.StartPosition.GetTimestamp() {
			skipStats = true
		}

		for fieldID, vectorizer := range eNode.vectorizers {
			field := vectorizer.GetField()
			// TODO REMOVE CODE FOR TEST
			// REMOVE invalid vector field data used as placeholder
			msg.FieldsData = RemoveFieldData(msg.GetFieldsData(), fieldID)

			//TODO Get Embedding From
			embeddingFieldID := int64(0)

			// if msg checkpoint early than stats checkpoint skip update stats
			var fieldStats storage.ChannelStats
			if skipStats {
				fieldStats = nil
			} else {
				if _, ok := stats[fieldID]; !ok {
					stats[fieldID] = storage.NewBM25Stats()
				}
				fieldStats = stats[fieldID]
			}

			data, err := GetEmbeddingFieldData(msg.GetFieldsData(), embeddingFieldID)
			if data == nil || err != nil {
				return merr.WrapErrFieldNotFound(fmt.Sprint(embeddingFieldID))
			}

			dim, sparseVector, err := vectorizer.Vectorize(fieldStats, data...)
			if err != nil {
				return err
			}

			msg.FieldsData = append(msg.FieldsData, BuildSparseFieldData(field, dim, sparseVector))
		}
	}

	return nil
}

func (eNode *embeddingNode) Operate(in []Msg) []Msg {
	nodeMsg := in[0].(*insertNodeMsg)
	nodeMsg.channelStats = make(map[int64]storage.ChannelStats)
	err := eNode.vectorize(nodeMsg.insertMsgs, nodeMsg.channelStats)
	if err != nil {
		panic(err)
	}

	return []Msg{nodeMsg}
}

func GetEmbeddingFieldData(datas []*schemapb.FieldData, fieldID int64) ([]string, error) {
	for _, data := range datas {
		if data.GetFieldId() == fieldID {
			return data.GetScalars().GetStringData().GetData(), nil
		}
	}
	return nil, fmt.Errorf("Field%d Not Found", fieldID)
}

func GetSparseVectorDim(data [][]byte) int64 {
	result := int64(0)
	for _, vector := range data {
		dim := typeutil.SparseFloatRowDim(vector)
		if dim > result {
			result = dim
		}
	}
	return result
}

func BuildSparseFieldData(field *schemapb.FieldSchema, dim int64, data [][]byte) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      field.GetDataType(),
		FieldName: field.GetName(),
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: dim,
				Data: &schemapb.VectorField_SparseFloatVector{
					SparseFloatVector: &schemapb.SparseFloatArray{
						Dim:      dim,
						Contents: data,
					}}}},
		FieldId: field.GetFieldID(),
	}
}

func RemoveFieldData(datas []*schemapb.FieldData, fieldID int64) []*schemapb.FieldData {
	for id, data := range datas {
		if data.GetFieldId() == fieldID {
			return append(datas[:id], datas[id+1:]...)
		}
	}
	return datas
}
