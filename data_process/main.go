package main

import (
	"context"
	"encoding/json"
	sio "io"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/pingcap/log"
	"github.com/samber/lo"
	"go.uber.org/zap"
)

const ImportPath = "json_data/"

var collectionID = "454576124989538339"
var partitionID = "454576124989538340"
var prefix = path.Join("files", "insert_log", collectionID, partitionID) + "/"

var json_log_id atomic.Int64
var fields = []*schemapb.FieldSchema{
	{
		Name:     "id",
		FieldID:  100,
		DataType: schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "max_length",
				Value: "128",
			},
		},
	},
	{
		Name:     "vector",
		FieldID:  101,
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "1024",
			},
		},
	},
	{
		Name:     "$meta",
		FieldID:  102,
		DataType: schemapb.DataType_JSON,
	},
}

var options = []DataOption{
	NewDataOption(MoveOption),
	NewDataOption(MoveOption),
	NewDataOption(MoveJsonOption),
}

type DataOptionFunc func(field *schemapb.FieldSchema, target map[string]any, value any)

type DataOption struct {
	options []DataOptionFunc
}

func (o *DataOption) BuildData(field *schemapb.FieldSchema, target map[string]any, value any) {
	for _, option := range o.options {
		option(field, target, value)
	}
}

func NewDataOption(options ...DataOptionFunc) DataOption {
	return DataOption{
		options: options,
	}
}

func MoveOption(field *schemapb.FieldSchema, target map[string]any, value any) {
	target[field.GetName()] = value
}

func MoveJsonOption(field *schemapb.FieldSchema, target map[string]any, value any) {
	bytes := value.([]byte)
	dict := make(map[string]any)
	err := json.Unmarshal(bytes, &dict)
	if err != nil {
		panic(err)
	}

	movefunc := func(name string) {
		value, ok := dict[name]
		if ok {
			delete(dict, name)
		}
		target[name] = value
	}

	total := ""
	names := []string{"abstract", "text", "title", "question", "answer"}
	for _, name := range names {
		movefunc(name)
		total = total + target[name].(string) + " "
	}
	target[field.GetName()] = dict
}

type JsonImportDataBuilder struct {
	cli    storage.ChunkManager
	datas  []any
	logIds []int64
	row    int
	cap    int
}

func NewJsonImportDataBuilder(cli storage.ChunkManager, cap int) *JsonImportDataBuilder {
	return &JsonImportDataBuilder{
		datas:  make([]any, 0),
		logIds: make([]int64, 0),
		cli:    cli,
		cap:    cap,
	}
}

func (b *JsonImportDataBuilder) AppendRow(data map[string]any) {
	b.datas = append(b.datas, data)
	b.row++
	b.TryMarshal()
}

func (b *JsonImportDataBuilder) Marshal() error {
	bytes, err := json.Marshal(b.datas)
	if err != nil {
		return err
	}
	b.datas = make([]any, 0)
	b.row = 0
	logId := json_log_id.Add(1)
	dataPath := path.Join(ImportPath, strconv.FormatInt(logId, 10)+".json")

	err = b.cli.Write(context.Background(), dataPath, bytes)
	if err != nil {
		return err
	}

	b.logIds = append(b.logIds, logId)
	return nil
}

func (b *JsonImportDataBuilder) TryMarshal() error {
	if b.IsFull() {
		return b.Marshal()
	}
	return nil
}

func (b *JsonImportDataBuilder) IsFull() bool {
	return b.row >= b.cap
}

func getChunkManager() (storage.ChunkManager, error) {
	chunkManagerFactory := storage.NewChunkManagerFactoryWithParam(paramtable.Get())
	cli, err := chunkManagerFactory.NewPersistentStorageChunkManager(context.Background())
	if err != nil {
		return nil, err
	}
	return cli, nil
}

func getSegmentBlobPaths(cli storage.ChunkManager, prefix, segmentID string, fields []*schemapb.FieldSchema) []string {
	blobPaths := []string{}
	for _, field := range fields {
		path := path.Join(prefix, segmentID, strconv.FormatInt(field.FieldID, 10)) + "/"
		cli.WalkWithPrefix(context.Background(), path, false, func(chunkObjectInfo *storage.ChunkObjectInfo) bool {
			blobPaths = append(blobPaths, chunkObjectInfo.FilePath)
			return true
		})
	}
	return blobPaths
}

var cpLock = sync.Mutex{}

func SaveCheckpoint(segment string, builder *JsonImportDataBuilder) error {
	f, err := os.OpenFile("file_list.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.ModePerm)
	if err != nil {
		panic(err)
	}

	m := map[string]any{}
	m["segment"] = segment
	m["file"] = builder.logIds

	bytes, err := json.Marshal(m)
	if err != nil {
		return err
	}

	cpLock.Lock()
	defer cpLock.Unlock()
	_, err = f.Write(append(bytes, byte('\n')))
	if err != nil {
		return err
	}
	return nil
}

func BuildJson(binlogio io.BinlogIO, cli storage.ChunkManager, segment string) error {
	log.Info("build segment start", zap.String("segmentID", segment))
	start := time.Now()
	// save json every 20000 rows
	builder := NewJsonImportDataBuilder(cli, 20000)
	paths := getSegmentBlobPaths(cli, prefix, segment, fields)
	bytes, err := binlogio.Download(context.Background(), paths)
	if err != nil {
		return err
	}
	blobs := lo.Map(bytes, func(v []byte, i int) *storage.Blob {
		return &storage.Blob{Key: paths[i], Value: v}
	})
	reader, err := storage.NewTestBinlogDeserializeReader(blobs)
	if err != nil {
		return err
	}

	for {
		err := reader.Next()
		if err != nil {
			if err == sio.EOF {
				reader.Close()
				break
			} else {
				log.Warn("failed to iter through data", zap.Error(err))
				return err
			}
		}
		v := reader.Value()
		m := v.Value.(map[storage.FieldID]interface{})
		newdata := make(map[string]any)
		for i, field := range fields {
			options[i].BuildData(field, newdata, m[field.GetFieldID()])
		}
		builder.AppendRow(newdata)
	}
	builder.Marshal()

	err = SaveCheckpoint(segment, builder)
	if err != nil {
		return err
	}
	log.Info("build segment success", zap.String("segmentID", segment), zap.Duration("time", time.Since(start)))
	return nil
}

func main() {
	paramtable.Init()
	cli, err := getChunkManager()
	if err != nil {
		panic(err)
	}
	log.Info("test--", zap.String("prefix", prefix))
	segments := []string{"454576124989938451", "454576124989939489"}
	log.Info("test--", zap.Strings("segment", segments))

	binlogio := io.NewBinlogIO(cli)

	pool := conc.NewPool[any](2, conc.WithPreAlloc(true))
	futures := []*conc.Future[any]{}
	for _, segment := range segments {
		future := pool.Submit(func() (any, error) {
			err := BuildJson(binlogio, cli, segment)
			return nil, err
		})
		futures = append(futures, future)
	}
	err = conc.AwaitAll(futures...)
	if err != nil {
		panic(err)
	}
	log.Info("build finished", zap.Int("file_num", int(json_log_id.Load())))
}
