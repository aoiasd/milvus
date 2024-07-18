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

package syncmgr

import (
	"context"
	"fmt"
	"path"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type SyncChannelStatsTask struct {
	chunkManager storage.ChunkManager
	metaWriter   MetaWriter
	allocator    allocator.Interface

	metaData      map[int64]storage.EmbeddingMeta
	startPosition *msgpb.MsgPosition
	checkpoint    *msgpb.MsgPosition

	collectionID int64
	channel      string
	schema       *schemapb.CollectionSchema

	failureCallback func(err error)

	binlog map[int64]*datapb.FieldBinlog
}

func NewSyncChannelStatsTask() *SyncChannelStatsTask {
	return &SyncChannelStatsTask{}
}

func (t *SyncChannelStatsTask) SegmentID() int64 {
	return -1
}

func (t *SyncChannelStatsTask) Checkpoint() *msgpb.MsgPosition {
	return t.startPosition
}

func (t *SyncChannelStatsTask) StartPosition() *msgpb.MsgPosition {
	return t.startPosition
}

func (t *SyncChannelStatsTask) ChannelName() string {
	return t.channel
}

func (t *SyncChannelStatsTask) Run(ctx context.Context) error {
	data := map[string][]byte{}
	for _, field := range t.schema.Fields {
		// TODO if field was embedding field
		meta, ok := t.metaData[field.FieldID]
		if !ok {
			log.Warn("Failed to get embedding field meta data", zap.Int64("fieldID", field.GetFieldID()), zap.String("fieldName", field.GetName()))
		}

		logId, err := t.allocator.AllocOne()
		if err != nil {
			return err
		}

		path := path.Join(t.chunkManager.RootPath(), common.SegmentInsertLogPath, fmt.Sprint(t.collectionID), t.channel, fmt.Sprint(logId))

		bytes, err := meta.Serialize()
		if err != nil {
			return err
		}

		data[path] = bytes
		t.binlog[field.FieldID] = &datapb.FieldBinlog{
			FieldID: field.FieldID,
			// TODO ADD MEMORY SIZE
			Binlogs: []*datapb.Binlog{{LogID: logId, LogSize: int64(len(bytes))}},
		}
	}

	err := t.chunkManager.MultiWrite(ctx, data)
	if err != nil {
		log.Info("write meta to chunk manager failed", zap.Error(err))
		return err
	}

	err = t.metaWriter.UpdateSyncChannelStats(ctx, t)
	if err != nil {
		log.Warn("failed to update channel stats", zap.Error(err))
		return err
	}

	return nil
}

func (t *SyncChannelStatsTask) HandleError(err error) {
	if t.failureCallback != nil {
		t.failureCallback(err)
	}

}

func (t *SyncChannelStatsTask) WithChunkManager(cm storage.ChunkManager) *SyncChannelStatsTask {
	t.chunkManager = cm
	return t
}

func (t *SyncChannelStatsTask) WithAllocator(allocator allocator.Interface) *SyncChannelStatsTask {
	t.allocator = allocator
	return t
}

func (t *SyncChannelStatsTask) WithStartPosition(start *msgpb.MsgPosition) *SyncChannelStatsTask {
	t.startPosition = start
	return t
}

func (t *SyncChannelStatsTask) WithCheckpoint(cp *msgpb.MsgPosition) *SyncChannelStatsTask {
	t.checkpoint = cp
	return t
}

func (t *SyncChannelStatsTask) WithCollectionID(collID int64) *SyncChannelStatsTask {
	t.collectionID = collID
	return t
}

func (t *SyncChannelStatsTask) WithChannelName(chanName string) *SyncChannelStatsTask {
	t.channel = chanName
	return t
}

func (t *SyncChannelStatsTask) WithMetaData(data map[int64]storage.EmbeddingMeta) *SyncChannelStatsTask {
	t.metaData = data
	return t
}

func (t *SyncChannelStatsTask) WithSchema(schema *schemapb.CollectionSchema) *SyncChannelStatsTask {
	t.schema = schema
	return t
}

func (t *SyncChannelStatsTask) WithMetaWriter(writer MetaWriter) *SyncChannelStatsTask {
	t.metaWriter = writer
	return t
}
