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

package function

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestEmbeddingFunctionSignatureIgnoresSchemaVersionAndUnrelatedSchema(t *testing.T) {
	base := newBM25SignatureTestSchema()
	baseSignature, err := EmbeddingFunctionSignature(base)
	require.NoError(t, err)

	schemaVersionChanged := cloneCollectionSchema(base)
	schemaVersionChanged.Version = base.GetVersion() + 1
	signature, err := EmbeddingFunctionSignature(schemaVersionChanged)
	require.NoError(t, err)
	require.Equal(t, baseSignature, signature)

	unrelatedFieldAdded := cloneCollectionSchema(base)
	unrelatedFieldAdded.Fields = append(unrelatedFieldAdded.Fields, &schemapb.FieldSchema{
		FieldID:  200,
		Name:     "extra",
		DataType: schemapb.DataType_Int64,
	})
	signature, err = EmbeddingFunctionSignature(unrelatedFieldAdded)
	require.NoError(t, err)
	require.Equal(t, baseSignature, signature)

	nonCachedFunctionChanged := cloneCollectionSchema(base)
	nonCachedFunctionChanged.Functions = append(nonCachedFunctionChanged.Functions, &schemapb.FunctionSchema{
		Id:             200,
		Name:           "text_embedding",
		Type:           schemapb.FunctionType_TextEmbedding,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{103},
		Params: []*commonpb.KeyValuePair{
			{Key: "provider", Value: "mock"},
			{Key: "credential", Value: "changed"},
		},
	})
	signature, err = EmbeddingFunctionSignature(nonCachedFunctionChanged)
	require.NoError(t, err)
	require.Equal(t, baseSignature, signature)
}

func TestEmbeddingFunctionSignatureChangesWhenRunnerInputsChange(t *testing.T) {
	base := newBM25SignatureTestSchema()
	baseSignature, err := EmbeddingFunctionSignature(base)
	require.NoError(t, err)

	functionParamChanged := cloneCollectionSchema(base)
	functionParamChanged.Functions[0].Params = []*commonpb.KeyValuePair{
		{Key: "rebuild", Value: "true"},
	}
	signature, err := EmbeddingFunctionSignature(functionParamChanged)
	require.NoError(t, err)
	require.NotEqual(t, baseSignature, signature)

	analyzerParamChanged := cloneCollectionSchema(base)
	analyzerParamChanged.Fields[1].TypeParams = []*commonpb.KeyValuePair{
		{Key: analyzerParams, Value: `{"tokenizer": "standard"}`},
	}
	signature, err = EmbeddingFunctionSignature(analyzerParamChanged)
	require.NoError(t, err)
	require.NotEqual(t, baseSignature, signature)
}

func TestFunctionRunnerCacheReusesRunnerAcrossSchemaVersionChange(t *testing.T) {
	cache := NewFunctionRunnerCache()
	t.Cleanup(cache.Close)

	base := newBM25SignatureTestSchema()
	runners, err := cache.GetOrCreate(1, base)
	require.NoError(t, err)
	require.Len(t, runners, 1)
	require.Len(t, cache.entries, 1)

	schemaVersionChanged := cloneCollectionSchema(base)
	schemaVersionChanged.Version = base.GetVersion() + 1
	runnersWithNewSchemaVersion, err := cache.GetOrCreate(1, schemaVersionChanged)
	require.NoError(t, err)
	require.Len(t, runnersWithNewSchemaVersion, 1)
	require.True(t, runners[0] == runnersWithNewSchemaVersion[0])
	require.Len(t, cache.entries, 1)

	functionParamChanged := cloneCollectionSchema(schemaVersionChanged)
	functionParamChanged.Functions[0].Params = []*commonpb.KeyValuePair{
		{Key: "rebuild", Value: "true"},
	}
	runnersWithNewParam, err := cache.GetOrCreate(1, functionParamChanged)
	require.NoError(t, err)
	require.Len(t, runnersWithNewParam, 1)
	require.False(t, runners[0] == runnersWithNewParam[0])
	require.Len(t, cache.entries, 2)
}

func newBM25SignatureTestSchema() *schemapb.CollectionSchema {
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
					{Key: analyzerParams, Value: "{}"},
				},
			},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
			{FieldID: 103, Name: "dense", DataType: schemapb.DataType_FloatVector},
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

func cloneCollectionSchema(schema *schemapb.CollectionSchema) *schemapb.CollectionSchema {
	return proto.Clone(schema).(*schemapb.CollectionSchema)
}
