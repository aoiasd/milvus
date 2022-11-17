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
	"fmt"
	"sort"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/querynodev2"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

type NodeTask interface {
	execute() bool
}

type insertData struct {
	segment          Segment
	insertIDs        []UniqueID
	insertTimestamps []Timestamp
	insertRecords    []*schemapb.FieldData
	insertOffset     []int64
	insertPKs        []primaryKey
}

func newInsertData(segment Segment) *insertData {
	return &insertData{
		segment:          segment,
		insertIDs:        []UniqueID{},
		insertTimestamps: []Timestamp{},
		insertRecords:    []*schemapb.FieldData{},
		insertOffset:     []int64{},
		insertPKs:        []primaryKey{},
	}
}

type insertNodeTask struct {
	manager      Manager
	collectionID UniqueID
	vchannel     Channel

	msgs        []*InsertMsg
	insertDatas map[UniqueID]*insertData
}

func (t *insertNodeTask) newSegment(msg *InsertMsg, collection *Collection) Segment {
	startPosition := &internalpb.MsgPosition{
		ChannelName: msg.ShardName,
		Timestamp:   msg.BeginTs(),
	}
	segment, err := querynodev2.NewSegment(collection, msg.SegmentID, msg.PartitionID, msg.CollectionID, t.vchannel, segmentTypeGrowing, 0, startPosition)

	if err != nil {
		err = fmt.Errorf("insertNode addSegment failed, err = %s", err)
		log.Error(err.Error(), zap.Int64("collectionID", t.collectionID), zap.String("vchannel", t.vchannel))
		panic(err)
	}
	return segment
}

func (t *insertNodeTask) getSegment(msg *InsertMsg, collection *Collection) Segment {
	if collection.GetLoadType() == loadTypeCollection {
		collection.AddPartition(msg.PartitionID)
	}
	segment := t.manager.Get(msg.CollectionID)
	if segment == nil {
		segment := t.newSegment(msg, collection)
		t.manager.Put(segmentTypeGrowing, segment)
	}
	return segment
}

func (t *insertNodeTask) getPKs(msg *InsertMsg, schema *schemapb.CollectionSchema) []storage.PrimaryKey {
	var PKs []storage.PrimaryKey
	var err error
	if msg.IsRowBased() {
		PKs, err = getPKsFromRowBasedInsertMsg(msg, schema)
	} else {
		PKs, err = getPKsFromColumnBasedInsertMsg(msg, schema)
	}

	if err != nil {
		err = fmt.Errorf("failed to get primary keys, err = %d", err)
		log.Error(err.Error(), zap.Int64("collectionID", t.collectionID), zap.String("vchannel", t.vchannel))
		panic(err)
	}
	return PKs
}

func (t *insertNodeTask) addInsertData(msg *InsertMsg, segment Segment, collection *Collection) {
	insertRecord, err := storage.TransferInsertMsgToInsertRecord(collection.Schema(), msg)
	if err != nil {
		err = fmt.Errorf("failed to get primary keys, err = %d", err)
		log.Error(err.Error(), zap.Int64("collectionID", t.collectionID), zap.String("vchannel", t.vchannel))
		panic(err)
	}
	iData, ok := t.insertDatas[msg.SegmentID]
	if !ok {
		iData = newInsertData(segment)
		t.insertDatas[msg.SegmentID] = iData
		iData.insertRecords = insertRecord.FieldsData
	} else {
		typeutil.MergeFieldData(iData.insertRecords, insertRecord.FieldsData)
	}
	pks := t.getPKs(msg, collection.Schema())

	iData.insertIDs = append(iData.insertIDs, msg.RowIDs...)
	iData.insertTimestamps = append(iData.insertTimestamps, msg.Timestamps...)
	iData.insertPKs = append(iData.insertPKs, pks...)
}

func (t *insertNodeTask) getOffset(iData *insertData) {
	segment := iData.segment
	var num = len(iData.insertIDs)
	offset, err := segment.PreInsert(num)
	if err != nil {
		//TODO segment removed before preInsert
		// error occurs when cgo function `PreInsert` failed
		err = fmt.Errorf("segmentPreInsert failed, segmentID = %d, err = %s", segment.ID(), err)
		log.Error(err.Error(), zap.Int64("collectionID", t.collectionID), zap.String("vchannel", t.vchannel))
		panic(err)
	}

	iData.insertOffset[segment.ID()] = offset
	//TODO UpdateBloomFilter
}

func (t *insertNodeTask) insert(iData *insertData, segment Segment) {
	segment.Insert()
}

func (t *insertNodeTask) execute() {
	sort.Slice(t.msgs, func(i, j int) bool {
		return t.msgs[i].BeginTs() < t.msgs[j].BeginTs()
	})
	collection := t.manager.GetCollection(t.collectionID)

	//get InsertData and merge datas of same segment
	for _, msg := range t.msgs {
		segment := t.getSegment(msg, collection)
		t.addInsertData(msg, segment, collection)
	}

	for _, insertData := range t.insertDatas {
		t.getOffset(insertData)
		t.insert(insertData)
	}
}

type deleteNodeTask struct {
	msgs []*DeleteMsg
}

func (t *deleteNodeTask) execute() {}
