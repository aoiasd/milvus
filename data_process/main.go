package main

import (
	"bufio"
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
	backuppb "github.com/milvus-io/milvus/internal/proto/backpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/pingcap/log"
	"github.com/samber/lo"
	"go.uber.org/zap"
)

const ImportPath = "json_data/"
const CheckpointPath = "/tmp/milvus_file_list.log"
const BackupPath = "backup/backup_2024_12_17_09_48_53_650563337"

var collectionID = "454670119864893553"
var json_log_id atomic.Int64
var remain_seg atomic.Int32
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

func FieldExist(fieldID int64) bool {
	for _, field := range fields {
		if fieldID == field.GetFieldID() {
			return true
		}
	}
	return false
}

func ReadSegmentInfo(cli storage.ChunkManager, collection string) (map[int64][]string, error) {
	jsonBytes, err := cli.Read(context.Background(), path.Join(BackupPath, "meta/segment_meta.json"))
	if err != nil {
		return nil, err
	}

	log.Info("test--", zap.ByteString("bytes", jsonBytes))

	segInfos := backuppb.SegmentLevelBackupInfo{}
	err = json.Unmarshal(jsonBytes, &segInfos)
	if err != nil {
		return nil, err
	}

	result := map[int64][]string{}
	for _, info := range segInfos.Infos {
		paths := []string{}
		for _, field := range info.Binlogs {
			if !FieldExist(field.FieldID) {
				continue
			}
			for _, binlog := range field.Binlogs {
				paths = append(paths, binlog.GetLogPath())
			}

			result[info.SegmentId] = paths
		}
	}
	return result, nil
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

	target["total"] = total
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

var cpLock = sync.Mutex{}

type checkPoint struct {
	Segment  int64   `json:"segment"`
	Json_ids []int64 `json:"json_ids"`
}

func SaveCheckpoint(segment int64, builder *JsonImportDataBuilder) error {
	f, err := os.OpenFile(CheckpointPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.ModePerm)
	if err != nil {
		panic(err)
	}

	m := checkPoint{
		Segment:  segment,
		Json_ids: builder.logIds,
	}

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

func ReadCheckpoint(infos map[int64][]string) (map[int64][]string, error) {
	f, err := os.OpenFile(CheckpointPath, os.O_RDONLY, os.ModePerm)
	if err != nil {
		if os.IsNotExist(err) {
			return infos, nil
		}
		return nil, err
	}

	reader := bufio.NewReader(f)
	for {
		line, _, err := reader.ReadLine()
		if err != nil {
			if err == sio.EOF {
				break
			}
			return nil, err
		}

		m := &checkPoint{}
		err = json.Unmarshal(line, m)
		if err != nil {
			return nil, err
		}

		delete(infos, m.Segment)
	}
	return infos, nil
}

func BuildJson(binlogio io.BinlogIO, cli storage.ChunkManager, segment int64, paths []string) error {
	log.Info("build segment start", zap.Int64("segmentID", segment))
	start := time.Now()
	// save json every 20000 rows
	builder := NewJsonImportDataBuilder(cli, 20000)
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
	log.Info("build segment success", zap.Int64("segmentID", segment), zap.Duration("time", time.Since(start)), zap.Int32("remain", remain_seg.Add(-1)))
	return nil
}

func main() {
	paramtable.Init()
	cli, err := getChunkManager()
	if err != nil {
		panic(err)
	}

	info, err := ReadSegmentInfo(cli, collectionID)
	if err != nil {
		panic(err)
	}

	info, err = ReadCheckpoint(info)
	if err != nil {
		panic(err)
	}

	log.Info("start build json", zap.Int("segmentNum", len(info)))
	remain_seg.Store(int32(len(info)))
	log.Info("test--", zap.Any("info", info))

	binlogio := io.NewBinlogIO(cli)
	pool := conc.NewPool[any](2, conc.WithPreAlloc(true))
	futures := []*conc.Future[any]{}
	for segment, paths := range info {
		future := pool.Submit(func() (any, error) {
			err := BuildJson(binlogio, cli, segment, paths)
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
