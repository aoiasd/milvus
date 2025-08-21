package syncmgr

import (
	"context"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	storage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type LobWriter interface {
	WriteRecord()
	Complete()
}

type BatchLobWriter struct {
	schema       *schemapb.CollectionSchema
	columnGroups []storagecommon.ColumnGroup
	collectionID int64
	partitionID  int64
	segmentID    int64

	writer     storage.RecordWriter
	bufferSize int64

	chunkManager  storage.ChunkManager
	storageConfig *indexpb.StorageConfig
	// prefetched log ids
	ids []int64
}

func NewBatchLobWriter(schema *schemapb.CollectionSchema, collectionID, partitionID, segmentID, bufferSize int64) *BatchLobWriter {
	return &BatchLobWriter{
		schema:       schema,
		columnGroups: storagecommon.SplitLobBySchema(schema.GetFields()),
		collectionID: collectionID,
		partitionID:  partitionID,
		segmentID:    segmentID,
		bufferSize:   bufferSize,
	}
}

func (w *BatchLobWriter) createWriter() error {
	paths := make([]string, 0)
	for _, columnGroup := range w.columnGroups {
		path := metautil.BuildInsertLogPath(w.getRootPath(), w.collectionID, w.partitionID, w.segmentID, columnGroup.GroupID, w.nextID())
		paths = append(paths, path)
	}

	bucketName := paramtable.Get().ServiceParam.MinioCfg.BucketName.GetValue()
	writer, err := storage.NewPackedRecordWriter(bucketName, paths, w.schema, w.bufferSize, w.bufferSize, w.columnGroups, w.storageConfig)
	if err != nil {
		return err
	}
	w.writer = writer
	return nil
}

func (w *BatchLobWriter) nextID() int64 {
	if len(w.ids) == 0 {
		panic("pre-fetched ids exhausted")
	}
	r := w.ids[0]
	w.ids = w.ids[1:]
	return r
}

func (w *BatchLobWriter) getRootPath() string {
	if w.storageConfig != nil {
		return w.storageConfig.RootPath
	}
	return w.chunkManager.RootPath()
}

func (w *BatchLobWriter) serializeBinlog(lobData []*storage.InsertData) (storage.Record, error) {
	if len(lobData) == 0 {
		return nil, nil
	}
	arrowSchema, err := storage.ConvertToArrowSchema(w.schema)
	if err != nil {
		return nil, err
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()

	for _, chunk := range lobData {
		if err := storage.BuildRecord(builder, chunk, w.schema); err != nil {
			return nil, err
		}
	}

	rec := builder.NewRecord()
	field2Col := make(map[storage.FieldID]int, len(w.schema.GetFields()))

	for c, field := range w.schema.GetFields() {
		field2Col[field.FieldID] = c
	}
	return storage.NewSimpleArrowRecord(rec, field2Col), nil
}

func (w *BatchLobWriter) WriteRecord(ctx context.Context, lobData []*storage.InsertData) {
	if w.writer == nil {
		w.createWriter()
	}

	// w.writer.Write()
}
