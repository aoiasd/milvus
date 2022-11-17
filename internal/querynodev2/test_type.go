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
	"context"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type SegmentType = commonpb.SegmentState
type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp

type IndexedFieldInfo struct{}
type searchRequest struct{}
type SearchResult struct{}
type RetrievePlan struct{}
type Manager interface {
	// Collection related

	// PutCollectionAndRef puts the given collection in,
	// and increases the ref count of the given collection,
	// returns the increased ref count
	PutCollectionAndRef(collectionID UniqueID, schema *schemapb.CollectionSchema) int32

	// UnrefCollection decreases the ref count of the given collection,
	// this will remove the collection if it sets the ref count to 0,
	// returns the decreased ref count
	UnrefCollection(collectionID UniqueID) int32
	GetCollection(collectionID UniqueID) *Collection

	// Segment related

	// Put puts the given segments in,
	// and increases the ref count of the corresponding collection,
	// dup segments will not increase the ref count
	Put(segmentType SegmentType, segments ...Segment)
	Get(segmentID UniqueID) Segment
	GetSealed(segmentID UniqueID) Segment
	GetGrowing(segmentID UniqueID) Segment
	// Remove removes the given segment,
	// and decreases the ref count of the corresponding collection,
	// will not decrease the ref count if the given segment not exists
	Remove(segmentID UniqueID, scope querypb.DataScope)
}

type Loader interface {
	// Load loads binlogs, and spawn segments
	Load(ctx context.Context, req *querypb.LoadSegmentsRequest, segmentType SegmentType) (map[int64]Segment, error)

	// LoadStreamDelta loads delete messages from stream, from the given position,
	// and applies these messages to the given segments
	LoadStreamDelta(ctx context.Context, collectionID int64, position *internalpb.MsgPosition, segments map[int64]Segment) error
}

type Segment interface {
	// Properties
	ID() int64
	Collection() int64
	Partition() int64
	Channel() string
	Version() int64
	StartPosition() *internalpb.MsgPosition
	Type() SegmentType

	// Index related
	AddIndex(fieldID int64, index *IndexedFieldInfo)
	GetIndex(fieldID int64) *IndexedFieldInfo
	HaveIndex(fieldID int64) bool

	// Insert related
	PreInsert(numOfRecords int) (int64, error)
	Insert(entityIDs []int64, timestamps []Timestamp, record *segcorepb.InsertRecord) error
	Delete(entityIDs []storage.PrimaryKey, timestamps []typeutil.Timestamp) error

	// Query related
	Search(searchReq *searchRequest) (*SearchResult, error)
	Retrieve(plan *RetrievePlan) (*segcorepb.RetrieveResults, error)
}

func NewSegment(collection *Collection,
	segmentID int64,
	partitionID int64,
	collectionID int64,
	channel string,
	segmentType SegmentType,
	version int64,
	startPosition *internalpb.MsgPosition) (Segment, error)

type Collection struct {
}

func (c *Collection) ID() UniqueID
func (c *Collection) Schema() *schemapb.CollectionSchema
func (c *Collection) GetPartitions() []int64
func (c *Collection) HasPartition(partitionID int64) bool
func (c *Collection) AddPartition(partitionID int64)
func (c *Collection) RemovePartition(partitionID int64)
func (c *Collection) GetLoadType() querypb.LoadType
func NewCollection(collectionID int64, schema *schemapb.CollectionSchema, loadType querypb.LoadType) *Collection
func DeleteCollection(collection *Collection)
