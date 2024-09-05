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

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/function"
	base "github.com/milvus-io/milvus/internal/util/pipeline"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// TODO support set EmbddingType
// type EmbeddingType int32

type embeddingNode struct {
	*BaseNode

	schema  *schemapb.CollectionSchema
	pkField *schemapb.FieldSchema

	collectionID int64
	channel      string

	manager *DataManager

	// embeddingType EmbeddingType
	functionRunners []function.FunctionRunner
}

func newEmbeddingNode(collectionID int64, channelName string, manager *DataManager, maxQueueLength int32) (*embeddingNode, error) {
	node := &embeddingNode{
		BaseNode:        base.NewBaseNode(fmt.Sprintf("EmbeddingNode-%s", channelName), maxQueueLength),
		collectionID:    collectionID,
		channel:         channelName,
		manager:         manager,
		functionRunners: make([]function.FunctionRunner, 0),
	}

	collection := manager.Collection.Get(collectionID)
	if collection == nil {
		log.Error("embeddingNode init failed with collection not exist", zap.Int64("collection", collectionID))
		return nil, merr.WrapErrCollectionNotFound(collectionID)
	}

	for _, field := range collection.Schema().GetFields() {
		if field.IsPrimaryKey {
			node.pkField = field
			break
		}
	}

	for _, tf := range collection.Schema().GetFunctions() {
		functionRunner, err := function.NewFunctionRunner(collection.Schema(), tf)
		if err != nil {
			return nil, err
		}
		node.functionRunners = append(node.functionRunners, functionRunner)
	}
	return node, nil
}

func (eNode *embeddingNode) Name() string {
	return fmt.Sprintf("embeddingNode-%s-%s", "BM25test", eNode.channel)
}

func (eNode *embeddingNode) addInsertData(insertDatas map[UniqueID]*delegator.InsertData, msg *InsertMsg, collection *Collection) {
	iData, ok := insertDatas[msg.SegmentID]
	if !ok {
		iData = &delegator.InsertData{
			PartitionID: msg.PartitionID,
			BM25Stats:   make(map[int64]*storage.BM25Stats),
			StartPosition: &msgpb.MsgPosition{
				Timestamp:   msg.BeginTs(),
				ChannelName: msg.GetShardName(),
			},
		}
		insertDatas[msg.SegmentID] = iData
	}

	err := eNode.embedding(msg, iData.BM25Stats)
	if err != nil {
		log.Error("failed to function data", zap.Error(err))
		panic(err)
	}

	insertRecord, err := storage.TransferInsertMsgToInsertRecord(collection.Schema(), msg)
	if err != nil {
		err = fmt.Errorf("failed to get primary keys, err = %d", err)
		log.Error(err.Error(), zap.Int64("collectionID", eNode.collectionID), zap.String("channel", eNode.channel))
		panic(err)
	}

	if iData.InsertRecord == nil {
		iData.InsertRecord = insertRecord
	} else {
		err := typeutil.MergeFieldData(iData.InsertRecord.FieldsData, insertRecord.FieldsData)
		if err != nil {
			log.Error("failed to merge field data", zap.Error(err))
			panic(err)
		}
		iData.InsertRecord.NumRows += insertRecord.NumRows
	}

	pks, err := segments.GetPrimaryKeys(msg, collection.Schema())
	if err != nil {
		log.Error("failed to get primary keys from insert message", zap.Error(err))
		panic(err)
	}

	iData.PrimaryKeys = append(iData.PrimaryKeys, pks...)
	iData.RowIDs = append(iData.RowIDs, msg.RowIDs...)
	iData.Timestamps = append(iData.Timestamps, msg.Timestamps...)
	log.Debug("pipeline embedding insert msg",
		zap.Int64("collectionID", eNode.collectionID),
		zap.Int64("segmentID", msg.SegmentID),
		zap.Int("insertRowNum", len(pks)),
		zap.Uint64("timestampMin", msg.BeginTimestamp),
		zap.Uint64("timestampMax", msg.EndTimestamp))
}

func (eNode *embeddingNode) bm25Embedding(runner function.FunctionRunner, msg *msgstream.InsertMsg, stats map[int64]*storage.BM25Stats) error {
	functionSchema := runner.GetSchema()
	inputFieldID := functionSchema.GetInputFieldIds()[0]
	outputFieldID := functionSchema.GetOutputFieldIds()[0]
	outputField := runner.GetOutputFields()[0]

	// TODO REMOVE CODE FOR TEST
	// REMOVE invalid vector field data used as placeholder
	msg.FieldsData = RemoveFieldData(msg.GetFieldsData(), outputFieldID)

	data, err := GetEmbeddingFieldData(msg.GetFieldsData(), inputFieldID)
	if data == nil || err != nil {
		return merr.WrapErrFieldNotFound(fmt.Sprint(inputFieldID))
	}

	output, err := runner.Run(data)
	if err != nil {
		return err
	}

	if _, ok := stats[outputFieldID]; !ok {
		stats[outputFieldID] = storage.NewBM25Stats()
	}

	sparseMaps, ok := output[0].([]map[uint32]float32)
	if !ok {
		return fmt.Errorf("bm25 runner return unknown type output")
	}
	stats[outputFieldID].Append(sparseMaps...)
	dim := 0
	sparseVector := lo.Map(sparseMaps, func(sparseMap map[uint32]float32, _ int) []byte {
		if len(sparseMap) > dim {
			dim = len(sparseMap)
		}
		return typeutil.CreateAndSortSparseFloatRow(sparseMap)
	})
	msg.FieldsData = append(msg.FieldsData, delegator.BuildSparseFieldData(outputField, int64(dim), sparseVector))
	return nil
}

func (eNode *embeddingNode) embedding(msg *msgstream.InsertMsg, stats map[int64]*storage.BM25Stats) error {
	for _, functionRunner := range eNode.functionRunners {
		functionSchema := functionRunner.GetSchema()
		switch functionSchema.GetType() {
		case schemapb.FunctionType_BM25:
			err := eNode.bm25Embedding(functionRunner, msg, stats)
			if err != nil {
				return err
			}
		default:
			log.Warn("pipeline embedding with unkonwn function type", zap.Any("type", functionSchema.GetType()))
			return fmt.Errorf("Unknown function type")
		}
	}

	return nil
}

func (eNode *embeddingNode) Operate(in Msg) Msg {
	nodeMsg := in.(*insertNodeMsg)
	nodeMsg.insertDatas = make(map[int64]*delegator.InsertData)

	collection := eNode.manager.Collection.Get(eNode.collectionID)
	if collection == nil {
		log.Error("insertNode with collection not exist", zap.Int64("collection", eNode.collectionID))
		panic("insertNode with collection not exist")
	}

	for _, msg := range nodeMsg.insertMsgs {
		eNode.addInsertData(nodeMsg.insertDatas, msg, collection)
	}

	return nodeMsg
}

func GetEmbeddingFieldData(datas []*schemapb.FieldData, fieldID int64) ([]string, error) {
	for _, data := range datas {
		if data.GetFieldId() == fieldID {
			return data.GetScalars().GetStringData().GetData(), nil
		}
	}
	return nil, fmt.Errorf("field%d not found", fieldID)
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

func RemoveFieldData(datas []*schemapb.FieldData, fieldID int64) []*schemapb.FieldData {
	for id, data := range datas {
		if data.GetFieldId() == fieldID {
			return append(datas[:id], datas[id+1:]...)
		}
	}
	return datas
}
