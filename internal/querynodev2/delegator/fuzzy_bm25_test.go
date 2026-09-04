// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package delegator

import (
	"context"
	"errors"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/textindex"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type fakeFuzzyGrowingSegment struct {
	segments.Segment
	id            int64
	collectionID  int64
	matches       [][]segments.TextTermMatch
	generation    uint64
	dataVersion   int32
	err           error
	fieldID       int64
	sourceTerms   [][]byte
	editDistance  uint32
	maxExpansions uint32
	prefixLength  uint32
}

func (s *fakeFuzzyGrowingSegment) ID() int64 {
	return s.id
}

func (s *fakeFuzzyGrowingSegment) Collection() int64 {
	return s.collectionID
}

func (s *fakeFuzzyGrowingSegment) ExpandTextTerms(
	fieldID int64,
	sourceTerms [][]byte,
	maxEditDistance, maxExpansions, prefixLength uint32,
) ([][]segments.TextTermMatch, uint64, int32, error) {
	s.fieldID = fieldID
	s.sourceTerms = sourceTerms
	s.editDistance = maxEditDistance
	s.maxExpansions = maxExpansions
	s.prefixLength = prefixLength
	return s.matches, s.generation, s.dataVersion, s.err
}

func TestBuildFuzzyBM25QueryTF(t *testing.T) {
	expanded := map[uint32][]*querypb.ExpandedTextTerm{
		0: {
			{Term: []byte("book"), EditDistance: 1},
			{Term: []byte("boon"), EditDistance: 0},
			{Term: []byte("ghost"), EditDistance: 0},
		},
		1: {
			{Term: []byte("book"), EditDistance: 0},
			{Term: []byte("back"), EditDistance: 0},
		},
	}

	rows := buildFuzzyBM25QueryTF([]map[uint32]float32{{0: 2, 1: 3}}, expanded)
	require.Len(t, rows, 1)
	assert.Equal(t, map[uint32]float32{
		typeutil.HashString2LessUint32("back"):  3,
		typeutil.HashString2LessUint32("book"):  5,
		typeutil.HashString2LessUint32("boon"):  2,
		typeutil.HashString2LessUint32("ghost"): 2,
	}, typeutil.SparseFloatBytesToMap(rows[0]))
}

func TestBuildFuzzyBM25IDFUsesGlobalStats(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  101,
				Name:     "text",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.EnableAnalyzerKey, Value: "true"},
				},
			},
			{FieldID: 103, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
		},
		Functions: []*schemapb.FunctionSchema{{
			Name:           "bm25",
			Type:           schemapb.FunctionType_BM25,
			InputFieldIds:  []int64{101},
			OutputFieldIds: []int64{103},
			Params: []*commonpb.KeyValuePair{
				{Key: common.EnableFuzzyKey, Value: "true"},
			},
		}},
	}
	runner, err := function.NewBM25FunctionRunner(schema, schema.GetFunctions()[0])
	require.NoError(t, err)
	defer runner.Close()

	oracle := NewIDFOracle(t.Name(), schema.GetFunctions()).(*idfOracle)
	defer oracle.Close()
	globalStats, err := oracle.current.GetStats(103)
	require.NoError(t, err)
	bookHash := typeutil.HashString2LessUint32("book")
	globalStats.Append(map[uint32]float32{bookHash: 1})
	globalStats.Append(map[uint32]float32{typeutil.HashString2LessUint32("global-only"): 3})

	growingSegment := &fakeFuzzyGrowingSegment{
		id:           20,
		collectionID: 1000,
		matches: [][]segments.TextTermMatch{{{
			Term: []byte("book"), EditDistance: 1,
		}}},
		generation: 200,
	}
	segmentManager := segments.NewMockSegmentManager(t)
	segmentManager.EXPECT().GetGrowing(int64(20)).Return(growingSegment).Once()
	sd := &shardDelegator{
		collectionID:   1000,
		vchannelName:   "channel",
		segmentManager: segmentManager,
	}
	sd.publishIDFOracle(oracle)

	placeholder, err := proto.Marshal(&commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{{
			Type:   commonpb.PlaceholderType_VarChar,
			Values: [][]byte{[]byte("bok")},
		}},
	})
	require.NoError(t, err)
	plan, err := proto.Marshal(&planpb.PlanNode{
		Node: &planpb.PlanNode_VectorAnns{
			VectorAnns: &planpb.VectorANNS{QueryInfo: &planpb.QueryInfo{}},
		},
	})
	require.NoError(t, err)
	req := &querypb.SearchRequest{Req: &internalpb.SearchRequest{
		CollectionID:       1000,
		FieldId:            103,
		PlaceholderGroup:   placeholder,
		SerializedExprPlan: plan,
		FuzzyBm25Options: &internalpb.FuzzyBM25SearchOptions{
			MaxEditDistance: 1,
			MaxExpansions:   50,
		},
	}}

	avgdl, err := sd.buildFuzzyBM25IDF(
		context.Background(), req, runner, nil,
		[]SegmentEntry{{SegmentID: 20, Level: datapb.SegmentLevel_L1}}, nil)
	require.NoError(t, err)
	assert.Equal(t, globalStats.GetAvgdl(), avgdl)
	require.Len(t, req.GetTextTermGenerations(), 1)
	assert.EqualValues(t, 20, req.GetTextTermGenerations()[0].GetSegmentID())

	rewritten := &commonpb.PlaceholderGroup{}
	require.NoError(t, proto.Unmarshal(req.GetReq().GetPlaceholderGroup(), rewritten))
	require.Len(t, rewritten.GetPlaceholders(), 1)
	require.Len(t, rewritten.GetPlaceholders()[0].GetValues(), 1)
	expectedTF := typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{bookHash: 1})
	assert.Equal(t, globalStats.BuildIDF(expectedTF), rewritten.GetPlaceholders()[0].GetValues()[0])
}

func TestMultiAnalyzerBranchesShareFuzzyVocabulary(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  101,
				Name:     "text",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.EnableAnalyzerKey, Value: "true"},
					{Key: "multi_analyzer_params", Value: `{"by_field":"language","analyzers":{"default":{"type":"standard"},"english":{"type":"english"}}}`},
				},
			},
			{FieldID: 102, Name: "language", DataType: schemapb.DataType_VarChar},
			{FieldID: 103, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
		},
		Functions: []*schemapb.FunctionSchema{{
			Name:           "bm25",
			Type:           schemapb.FunctionType_BM25,
			InputFieldIds:  []int64{101},
			OutputFieldIds: []int64{103},
		}},
	}
	runner, err := function.NewBM25FunctionRunner(schema, schema.GetFunctions()[0])
	require.NoError(t, err)
	defer runner.Close()

	materializer := runner.(function.TextTermMaterializer)
	_, batches, err := materializer.BatchRunWithTextTerms(
		[]string{"book", "books"},
		[]string{"english", "default"},
	)
	require.NoError(t, err)
	require.Len(t, batches, 1)
	assert.Equal(t, [][]byte{[]byte("book"), []byte("books")}, batches[0].Terms)

	artifact, err := textindex.BuildTextFst(batches[0].Terms)
	require.NoError(t, err)
	reader, err := textindex.LoadTextFstBytes(artifact.Data)
	require.NoError(t, err)
	defer reader.Close()

	analyzer := runner.(function.Analyzer)
	tokens, err := analyzer.BatchAnalyze(false, false, []string{"book"}, []string{"english"})
	require.NoError(t, err)
	require.Len(t, tokens, 1)
	require.Len(t, tokens[0], 1)
	matches, err := reader.FuzzySearch([]byte(tokens[0][0].GetToken()), 1, 2)
	require.NoError(t, err)
	require.Len(t, matches, 2)

	expanded := make([]*querypb.ExpandedTextTerm, 0, len(matches))
	for _, match := range matches {
		expanded = append(expanded, &querypb.ExpandedTextTerm{
			Term:         match.Term,
			EditDistance: match.EditDistance,
		})
	}
	rows := buildFuzzyBM25QueryTF(
		[]map[uint32]float32{{0: 1}},
		map[uint32][]*querypb.ExpandedTextTerm{0: expanded},
	)
	require.Len(t, rows, 1)
	assert.Equal(t, map[uint32]float32{
		typeutil.HashString2LessUint32("book"):  1,
		typeutil.HashString2LessUint32("books"): 1,
	}, typeutil.SparseFloatBytesToMap(rows[0]))
}

func TestFuzzySearchTargetsExcludeL0(t *testing.T) {
	sealed, growing := fuzzySearchTargets(
		[]SnapshotItem{{NodeID: 1, Segments: []SegmentEntry{
			{SegmentID: 10, Level: datapb.SegmentLevel_L1},
			{SegmentID: 11, Level: datapb.SegmentLevel_L0},
		}}},
		[]SegmentEntry{
			{SegmentID: 20, Level: datapb.SegmentLevel_L1},
			{SegmentID: 21, Level: datapb.SegmentLevel_L0},
		},
	)
	require.Len(t, sealed, 1)
	require.Len(t, sealed[0].Segments, 1)
	assert.EqualValues(t, 10, sealed[0].Segments[0].SegmentID)
	require.Len(t, growing, 1)
	assert.EqualValues(t, 20, growing[0].SegmentID)
}

func TestModifySearchRequestFiltersTextTermGenerations(t *testing.T) {
	sd := &shardDelegator{vchannelName: "channel"}
	modified := sd.modifySearchRequest(&querypb.SearchRequest{
		Req: &internalpb.SearchRequest{},
		TextTermGenerations: []*querypb.SegmentTextTermGeneration{
			{SegmentID: 10, Generation: 1},
			{SegmentID: 20, Generation: 2},
		},
	}, querypb.DataScope_Historical, []int64{20}, 100)

	require.Len(t, modified.GetTextTermGenerations(), 1)
	assert.EqualValues(t, 20, modified.GetTextTermGenerations()[0].GetSegmentID())
	assert.EqualValues(t, 2, modified.GetTextTermGenerations()[0].GetGeneration())
}

func TestExpandFuzzyBM25TermsPreservesPartialResultPolicy(t *testing.T) {
	params := paramtable.Get()
	require.NoError(t, params.Save(params.QueryNodeCfg.PartialResultRequiredDataRatio.Key, "0.5"))
	t.Cleanup(func() {
		require.NoError(t, params.Reset(params.QueryNodeCfg.PartialResultRequiredDataRatio.Key))
	})

	successWorker := cluster.NewMockWorker(t)
	successWorker.EXPECT().ExpandTextTerms(mock.Anything, mock.MatchedBy(func(req *querypb.ExpandTextTermsRequest) bool {
		segmentIDs := req.GetSegmentIDs()
		return req.GetScope() == querypb.DataScope_Historical &&
			req.GetMaxExpansions() == 50 &&
			len(segmentIDs) == 1 && segmentIDs[0] == 10
	})).Return(&querypb.ExpandTextTermsResponse{
		Status: merr.Success(),
		Terms: []*querypb.ExpandedTextTerm{{
			SourceIndex:  0,
			Term:         []byte("book"),
			EditDistance: 1,
		}},
		Generations: []*querypb.SegmentTextTermGeneration{{
			SegmentID:   10,
			Generation:  100,
			DataVersion: 3,
		}},
	}, nil).Once()

	workerManager := cluster.NewMockManager(t)
	workerManager.EXPECT().GetWorker(mock.Anything, int64(1)).Return(successWorker, nil).Once()
	workerManager.EXPECT().GetWorker(mock.Anything, int64(-1)).Return(nil, merr.ErrNodeNotFound).Once()

	sd := &shardDelegator{
		collectionID:  1000,
		vchannelName:  "channel",
		workerManager: workerManager,
	}
	sealed := []SnapshotItem{
		{NodeID: 1, Segments: []SegmentEntry{{SegmentID: 10, Level: datapb.SegmentLevel_L1}}},
		{NodeID: -1, Segments: []SegmentEntry{{SegmentID: 20, Level: datapb.SegmentLevel_L1}}},
	}

	expanded, generations, err := sd.expandFuzzyBM25Terms(
		context.Background(), 101, [][]byte{[]byte("bok")}, 1, 50, 0,
		sealed, nil, map[int64]int64{10: 100, 20: 100})
	require.NoError(t, err)
	require.Len(t, expanded[0], 1)
	assert.Equal(t, []byte("book"), expanded[0][0].GetTerm())
	require.Len(t, generations, 1)
	assert.EqualValues(t, 10, generations[0].GetSegmentID())
}

func TestExpandFuzzyBM25TermsGroupsSegmentsPerWorkerAndForwardsLimit(t *testing.T) {
	worker := cluster.NewMockWorker(t)
	worker.EXPECT().ExpandTextTerms(mock.Anything, mock.MatchedBy(func(req *querypb.ExpandTextTermsRequest) bool {
		return req.GetScope() == querypb.DataScope_Historical &&
			req.GetMaxEditDistance() == 2 &&
			req.GetMaxExpansions() == 7 &&
			req.GetPrefixLength() == 3 &&
			assert.ElementsMatch(t, []int64{10, 11}, req.GetSegmentIDs())
	})).Return(&querypb.ExpandTextTermsResponse{
		Status: merr.Success(),
		Terms: []*querypb.ExpandedTextTerm{{
			SourceIndex:  0,
			Term:         []byte("book"),
			EditDistance: 1,
		}},
		Generations: []*querypb.SegmentTextTermGeneration{
			{SegmentID: 10, Generation: 100, DataVersion: 3},
			{SegmentID: 11, Generation: 101, DataVersion: 4},
		},
	}, nil).Once()

	workerManager := cluster.NewMockManager(t)
	workerManager.EXPECT().GetWorker(mock.Anything, int64(1)).Return(worker, nil).Once()

	sd := &shardDelegator{
		collectionID:  1000,
		vchannelName:  "channel",
		workerManager: workerManager,
	}
	sealed := []SnapshotItem{{
		NodeID: 1,
		Segments: []SegmentEntry{
			{SegmentID: 10, Level: datapb.SegmentLevel_L1},
			{SegmentID: 11, Level: datapb.SegmentLevel_L1},
		},
	}}

	expanded, generations, err := sd.expandFuzzyBM25Terms(
		context.Background(), 101, [][]byte{[]byte("bok")}, 2, 7, 3,
		sealed, nil, map[int64]int64{10: 100, 11: 100})
	require.NoError(t, err)
	require.Len(t, expanded[0], 1)
	assert.Equal(t, []byte("book"), expanded[0][0].GetTerm())
	require.Len(t, generations, 2)
}

func TestExpandFuzzyBM25TermsKeepsGrowingLocalAndSealedOnWorker(t *testing.T) {
	worker := cluster.NewMockWorker(t)
	worker.EXPECT().ExpandTextTerms(mock.Anything, mock.MatchedBy(func(req *querypb.ExpandTextTermsRequest) bool {
		return req.GetScope() == querypb.DataScope_Historical &&
			req.GetMaxEditDistance() == 2 &&
			req.GetMaxExpansions() == 7 &&
			req.GetPrefixLength() == 3 &&
			assert.Equal(t, []int64{10}, req.GetSegmentIDs())
	})).Return(&querypb.ExpandTextTermsResponse{
		Status: merr.Success(),
		Terms: []*querypb.ExpandedTextTerm{{
			SourceIndex:  0,
			Term:         []byte("book"),
			EditDistance: 1,
		}},
		Generations: []*querypb.SegmentTextTermGeneration{{
			SegmentID: 10, Generation: 100, DataVersion: 3,
		}},
	}, nil).Once()

	workerManager := cluster.NewMockManager(t)
	workerManager.EXPECT().GetWorker(mock.Anything, int64(1)).Return(worker, nil).Once()

	growingSegment := &fakeFuzzyGrowingSegment{
		id:           20,
		collectionID: 1000,
		matches: [][]segments.TextTermMatch{{{
			Term: []byte("books"), EditDistance: 2,
		}}},
		generation:  200,
		dataVersion: 4,
	}
	segmentManager := segments.NewMockSegmentManager(t)
	segmentManager.EXPECT().GetGrowing(int64(20)).Return(growingSegment).Once()

	sd := &shardDelegator{
		collectionID:   1000,
		vchannelName:   "channel",
		workerManager:  workerManager,
		segmentManager: segmentManager,
	}
	expanded, generations, err := sd.expandFuzzyBM25Terms(
		context.Background(),
		101,
		[][]byte{[]byte("bok")},
		2,
		7,
		3,
		[]SnapshotItem{{
			NodeID:   1,
			Segments: []SegmentEntry{{SegmentID: 10, Level: datapb.SegmentLevel_L1}},
		}},
		[]SegmentEntry{{SegmentID: 20, Level: datapb.SegmentLevel_L1}},
		map[int64]int64{10: 100},
	)
	require.NoError(t, err)
	require.Len(t, expanded[0], 2)
	assert.ElementsMatch(t, [][]byte{[]byte("book"), []byte("books")}, [][]byte{
		expanded[0][0].GetTerm(), expanded[0][1].GetTerm(),
	})
	require.Len(t, generations, 2)
	assert.Equal(t, int64(101), growingSegment.fieldID)
	assert.Equal(t, [][]byte{[]byte("bok")}, growingSegment.sourceTerms)
	assert.EqualValues(t, 2, growingSegment.editDistance)
	assert.EqualValues(t, 7, growingSegment.maxExpansions)
	assert.EqualValues(t, 3, growingSegment.prefixLength)
}

func TestExpandFuzzyBM25TermsAppliesPartialPolicyToLocalGrowingFailure(t *testing.T) {
	params := paramtable.Get()
	require.NoError(t, params.Save(params.QueryNodeCfg.PartialResultRequiredDataRatio.Key, "0.5"))
	t.Cleanup(func() {
		require.NoError(t, params.Reset(params.QueryNodeCfg.PartialResultRequiredDataRatio.Key))
	})

	worker := cluster.NewMockWorker(t)
	worker.EXPECT().ExpandTextTerms(mock.Anything, mock.AnythingOfType("*querypb.ExpandTextTermsRequest")).
		Return(&querypb.ExpandTextTermsResponse{
			Status: merr.Success(),
			Terms: []*querypb.ExpandedTextTerm{{
				SourceIndex: 0, Term: []byte("book"), EditDistance: 1,
			}},
			Generations: []*querypb.SegmentTextTermGeneration{{
				SegmentID: 10, Generation: 100, DataVersion: 3,
			}},
		}, nil).Once()
	workerManager := cluster.NewMockManager(t)
	workerManager.EXPECT().GetWorker(mock.Anything, int64(1)).Return(worker, nil).Once()

	growingSegment := &fakeFuzzyGrowingSegment{
		id:           20,
		collectionID: 1000,
		err:          errors.New("local growing expansion failed"),
	}
	segmentManager := segments.NewMockSegmentManager(t)
	segmentManager.EXPECT().GetGrowing(int64(20)).Return(growingSegment).Once()

	sd := &shardDelegator{
		collectionID:   1000,
		vchannelName:   "channel",
		workerManager:  workerManager,
		segmentManager: segmentManager,
	}
	expanded, generations, err := sd.expandFuzzyBM25Terms(
		context.Background(),
		101,
		[][]byte{[]byte("bok")},
		1,
		50,
		0,
		[]SnapshotItem{{
			NodeID:   1,
			Segments: []SegmentEntry{{SegmentID: 10, Level: datapb.SegmentLevel_L1}},
		}},
		[]SegmentEntry{{SegmentID: 20, Level: datapb.SegmentLevel_L1}},
		map[int64]int64{10: 100},
	)
	require.NoError(t, err)
	require.Len(t, expanded[0], 1)
	assert.Equal(t, []byte("book"), expanded[0][0].GetTerm())
	require.Len(t, generations, 1)
	assert.EqualValues(t, 10, generations[0].GetSegmentID())
}

func TestExpandFuzzyBM25TermsPreservesPartialResultAfterWorkerFailure(t *testing.T) {
	params := paramtable.Get()
	require.NoError(t, params.Save(params.QueryNodeCfg.PartialResultRequiredDataRatio.Key, "0.5"))
	t.Cleanup(func() {
		require.NoError(t, params.Reset(params.QueryNodeCfg.PartialResultRequiredDataRatio.Key))
	})

	successWorker := cluster.NewMockWorker(t)
	successWorker.EXPECT().ExpandTextTerms(mock.Anything, mock.AnythingOfType("*querypb.ExpandTextTermsRequest")).
		Return(&querypb.ExpandTextTermsResponse{
			Status: merr.Success(),
			Terms: []*querypb.ExpandedTextTerm{{
				SourceIndex:  0,
				Term:         []byte("book"),
				EditDistance: 1,
			}},
			Generations: []*querypb.SegmentTextTermGeneration{{
				SegmentID:   10,
				Generation:  100,
				DataVersion: 3,
			}},
		}, nil).Once()
	failedWorker := cluster.NewMockWorker(t)
	failedWorker.EXPECT().ExpandTextTerms(mock.Anything, mock.AnythingOfType("*querypb.ExpandTextTermsRequest")).
		Return(nil, errors.New("term expansion failed")).Once()

	workerManager := cluster.NewMockManager(t)
	workerManager.EXPECT().GetWorker(mock.Anything, int64(1)).Return(successWorker, nil).Once()
	workerManager.EXPECT().GetWorker(mock.Anything, int64(2)).Return(failedWorker, nil).Once()

	sd := &shardDelegator{
		collectionID:  1000,
		vchannelName:  "channel",
		workerManager: workerManager,
	}
	sealed := []SnapshotItem{
		{NodeID: 1, Segments: []SegmentEntry{{SegmentID: 10, Level: datapb.SegmentLevel_L1}}},
		{NodeID: 2, Segments: []SegmentEntry{{SegmentID: 20, Level: datapb.SegmentLevel_L1}}},
	}

	expanded, generations, err := sd.expandFuzzyBM25Terms(
		context.Background(), 101, [][]byte{[]byte("bok")}, 1, 50, 0,
		sealed, nil, map[int64]int64{10: 100, 20: 100})
	require.NoError(t, err)
	require.Len(t, expanded[0], 1)
	assert.Equal(t, []byte("book"), expanded[0][0].GetTerm())
	require.Len(t, generations, 1)
	assert.EqualValues(t, 10, generations[0].GetSegmentID())
}

func TestSearchFuzzyBM25RetriesFromExpansionAfterGenerationMismatch(t *testing.T) {
	mockey.PatchConvey("retry the complete lexical preparation after search generation mismatch", t, func() {
		prepareCalls := 0
		executeCalls := 0
		mockey.Mock((*shardDelegator).prepareSearchFunction).To(func(
			_ *shardDelegator,
			_ context.Context,
			_ *querypb.SearchRequest,
			_ []SnapshotItem,
			_ []SegmentEntry,
			_ map[int64]int64,
		) (float64, bool, error) {
			prepareCalls++
			return 1, false, nil
		}).Build()
		mockey.Mock((*shardDelegator).executeSearchSubTasks).To(func(
			_ *shardDelegator,
			_ context.Context,
			_ *querypb.SearchRequest,
			_ []SnapshotItem,
			_ []SegmentEntry,
			_ map[int64]int64,
		) ([]*internalpb.SearchResults, error) {
			executeCalls++
			if executeCalls == 1 {
				return nil, merr.WrapErrServiceUnavailableMsg("text term generation changed")
			}
			return []*internalpb.SearchResults{{Status: merr.Success()}}, nil
		}).Build()

		sd := &shardDelegator{}
		results, err := sd.searchFuzzyBM25(
			context.Background(),
			&querypb.SearchRequest{Req: &internalpb.SearchRequest{}},
			nil,
			nil,
			nil,
		)
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, 2, prepareCalls)
		assert.Equal(t, 2, executeCalls)
	})
}

func TestSearchFuzzyBM25PreservesPartialResultPolicy(t *testing.T) {
	params := paramtable.Get()
	require.NoError(t, params.Save(params.QueryNodeCfg.PartialResultRequiredDataRatio.Key, "0.5"))
	t.Cleanup(func() {
		require.NoError(t, params.Reset(params.QueryNodeCfg.PartialResultRequiredDataRatio.Key))
	})

	mockey.PatchConvey("fuzzy search uses the ordinary partial-result policy", t, func() {
		mockey.Mock((*shardDelegator).prepareSearchFunction).To(func(
			_ *shardDelegator,
			_ context.Context,
			req *querypb.SearchRequest,
			_ []SnapshotItem,
			_ []SegmentEntry,
			_ map[int64]int64,
		) (float64, bool, error) {
			req.TextTermGenerations = []*querypb.SegmentTextTermGeneration{
				{SegmentID: 10, Generation: 1},
				{SegmentID: 20, Generation: 1},
			}
			return 1, false, nil
		}).Build()

		successWorker := cluster.NewMockWorker(t)
		successWorker.EXPECT().SearchSegments(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).
			Return(&internalpb.SearchResults{Status: merr.Success()}, nil).Once()
		failedWorker := cluster.NewMockWorker(t)
		failedWorker.EXPECT().SearchSegments(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).
			Return(nil, errors.New("worker unavailable")).Once()

		workerManager := cluster.NewMockManager(t)
		workerManager.EXPECT().GetWorker(mock.Anything, int64(1)).Return(successWorker, nil).Once()
		workerManager.EXPECT().GetWorker(mock.Anything, int64(2)).Return(failedWorker, nil).Once()

		sd := &shardDelegator{
			vchannelName:  "channel",
			workerManager: workerManager,
		}
		sealed := []SnapshotItem{
			{NodeID: 1, Segments: []SegmentEntry{{SegmentID: 10, Level: datapb.SegmentLevel_L1}}},
			{NodeID: 2, Segments: []SegmentEntry{{SegmentID: 20, Level: datapb.SegmentLevel_L1}}},
		}
		results, err := sd.searchFuzzyBM25(
			context.Background(),
			&querypb.SearchRequest{Req: &internalpb.SearchRequest{Base: &commonpb.MsgBase{}}},
			sealed,
			nil,
			map[int64]int64{10: 100, 20: 100},
		)

		require.NoError(t, err)
		require.Len(t, results, 1)
	})
}

func TestSearchFuzzyBM25UsesExpansionPartialTargets(t *testing.T) {
	mockey.PatchConvey("fuzzy search uses only segments served by term expansion", t, func() {
		mockey.Mock((*shardDelegator).prepareSearchFunction).To(func(
			_ *shardDelegator,
			_ context.Context,
			req *querypb.SearchRequest,
			_ []SnapshotItem,
			_ []SegmentEntry,
			_ map[int64]int64,
		) (float64, bool, error) {
			req.TextTermGenerations = []*querypb.SegmentTextTermGeneration{{SegmentID: 10, Generation: 1}}
			return 1, false, nil
		}).Build()

		successWorker := cluster.NewMockWorker(t)
		successWorker.EXPECT().SearchSegments(mock.Anything, mock.MatchedBy(func(req *querypb.SearchRequest) bool {
			return len(req.GetSegmentIDs()) == 1 && req.GetSegmentIDs()[0] == 10
		})).Return(&internalpb.SearchResults{Status: merr.Success()}, nil).Once()

		workerManager := cluster.NewMockManager(t)
		workerManager.EXPECT().GetWorker(mock.Anything, int64(1)).Return(successWorker, nil).Once()

		sd := &shardDelegator{
			vchannelName:  "channel",
			workerManager: workerManager,
		}
		sealed := []SnapshotItem{
			{NodeID: 1, Segments: []SegmentEntry{{SegmentID: 10, Level: datapb.SegmentLevel_L1}}},
			{NodeID: 2, Segments: []SegmentEntry{{SegmentID: 20, Level: datapb.SegmentLevel_L1}}},
		}
		results, err := sd.searchFuzzyBM25(
			context.Background(),
			&querypb.SearchRequest{Req: &internalpb.SearchRequest{Base: &commonpb.MsgBase{}}},
			sealed,
			nil,
			map[int64]int64{10: 100, 20: 100},
		)
		require.NoError(t, err)
		require.Len(t, results, 1)
	})
}
