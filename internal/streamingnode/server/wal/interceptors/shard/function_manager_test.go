package shard

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/log"
)

func TestFunctionRunnerManagerGetOrCreateCachesBySignature(t *testing.T) {
	const collectionID = int64(10001)
	manager := newFunctionRunnerManager(log.With())
	t.Cleanup(manager.Close)

	base := newFunctionManagerBM25Schema()
	baseKey, err := functionRunnerCacheKeyOf(collectionID, base)
	require.NoError(t, err)

	runners, err := manager.GetOrCreate(context.Background(), collectionID, base)
	require.NoError(t, err)
	require.Len(t, runners, 1)
	requireFunctionRunnerManagerEntry(t, manager, baseKey, functionRunnerStateReady)
	requireFunctionRunnerManagerEntryCount(t, manager, 1)

	schemaVersionChanged := cloneFunctionManagerSchema(base)
	schemaVersionChanged.Version = base.GetVersion() + 1
	versionChangedKey, err := functionRunnerCacheKeyOf(collectionID, schemaVersionChanged)
	require.NoError(t, err)
	require.Equal(t, baseKey, versionChangedKey)

	runnersWithNewSchemaVersion, err := manager.GetOrCreate(context.Background(), collectionID, schemaVersionChanged)
	require.NoError(t, err)
	require.Len(t, runnersWithNewSchemaVersion, 1)
	require.True(t, runners[0] == runnersWithNewSchemaVersion[0])
	requireFunctionRunnerManagerEntryCount(t, manager, 1)

	functionParamChanged := cloneFunctionManagerSchema(schemaVersionChanged)
	functionParamChanged.Functions[0].Params = []*commonpb.KeyValuePair{
		{Key: "rebuild", Value: "true"},
	}
	paramChangedKey, err := functionRunnerCacheKeyOf(collectionID, functionParamChanged)
	require.NoError(t, err)
	require.NotEqual(t, baseKey, paramChangedKey)

	runnersWithNewParam, err := manager.GetOrCreate(context.Background(), collectionID, functionParamChanged)
	require.NoError(t, err)
	require.Len(t, runnersWithNewParam, 1)
	require.False(t, runners[0] == runnersWithNewParam[0])
	requireFunctionRunnerManagerEntry(t, manager, paramChangedKey, functionRunnerStateReady)
	requireFunctionRunnerManagerEntryCount(t, manager, 2)
}

func TestFunctionRunnerManagerDropCollectionRemovesEntriesAndRunners(t *testing.T) {
	const collectionID = int64(10002)
	manager := newFunctionRunnerManager(log.With())
	t.Cleanup(manager.Close)

	base := newFunctionManagerBM25Schema()
	runners, err := manager.GetOrCreate(context.Background(), collectionID, base)
	require.NoError(t, err)
	require.Len(t, runners, 1)

	functionParamChanged := cloneFunctionManagerSchema(base)
	functionParamChanged.Functions[0].Params = []*commonpb.KeyValuePair{
		{Key: "rebuild", Value: "true"},
	}
	_, err = manager.GetOrCreate(context.Background(), collectionID, functionParamChanged)
	require.NoError(t, err)
	requireFunctionRunnerManagerEntryCount(t, manager, 2)

	manager.DropCollection(collectionID)
	requireFunctionRunnerManagerEntryCount(t, manager, 0)

	runnersAfterDrop, err := manager.GetOrCreate(context.Background(), collectionID, base)
	require.NoError(t, err)
	require.Len(t, runnersAfterDrop, 1)
	require.False(t, runners[0] == runnersAfterDrop[0])
	requireFunctionRunnerManagerEntryCount(t, manager, 1)
}

func TestFunctionRunnerManagerGetOrCreateHonorsContextDuringForegroundInit(t *testing.T) {
	const collectionID = int64(10003)
	manager := newFunctionRunnerManager(log.With())
	t.Cleanup(manager.Close)

	schema := newFunctionManagerBM25Schema()
	key, err := functionRunnerCacheKeyOf(collectionID, schema)
	require.NoError(t, err)
	_, shouldBuild := manager.beginInit(key, schema)
	require.True(t, shouldBuild)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	runners, err := manager.GetOrCreate(ctx, collectionID, schema)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, runners)
	requireFunctionRunnerManagerEntry(t, manager, key, functionRunnerStateInitializing)
}

func requireFunctionRunnerManagerEntry(t *testing.T, manager *functionRunnerManager, key functionRunnerCacheKey, state functionRunnerState) {
	t.Helper()

	manager.mu.Lock()
	defer manager.mu.Unlock()

	entry := manager.entries[key]
	require.NotNil(t, entry)
	require.Equal(t, state, entry.state)
}

func requireFunctionRunnerManagerEntryCount(t *testing.T, manager *functionRunnerManager, expected int) {
	t.Helper()

	manager.mu.Lock()
	defer manager.mu.Unlock()

	require.Len(t, manager.entries, expected)
}

func newFunctionManagerBM25Schema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:    "test",
		Version: 1,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID:  101,
				Name:     "text",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "analyzer_params", Value: "{}"},
				},
			},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
		},
		Functions: []*schemapb.FunctionSchema{
			{
				Id:             100,
				Name:           "bm25",
				Type:           schemapb.FunctionType_BM25,
				InputFieldIds:  []int64{101},
				OutputFieldIds: []int64{102},
			},
		},
	}
}

func cloneFunctionManagerSchema(schema *schemapb.CollectionSchema) *schemapb.CollectionSchema {
	return proto.Clone(schema).(*schemapb.CollectionSchema)
}
