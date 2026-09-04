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

package delegator

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/searchutil/optimizers"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type fuzzySegmentGeneration struct {
	generation  uint64
	dataVersion int32
}

type fuzzyTextTermExpander interface {
	ExpandTextTerms(
		fieldID int64,
		sourceTerms [][]byte,
		maxEditDistance, maxExpansions, prefixLength uint32,
	) ([][]segments.TextTermMatch, uint64, int32, error)
}

func (sd *shardDelegator) expandGrowingFuzzyBM25Terms(
	ctx context.Context,
	request *querypb.ExpandTextTermsRequest,
	growing []SegmentEntry,
) (*querypb.ExpandTextTermsResponse, error) {
	type candidateKey struct {
		sourceIndex uint32
		term        string
	}

	response := &querypb.ExpandTextTermsResponse{Status: merr.Success()}
	candidates := make(map[candidateKey]uint32)
	for _, entry := range growing {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		segment := sd.segmentManager.GetGrowing(entry.SegmentID)
		if segment == nil || segment.Collection() != request.GetCollectionID() {
			return nil, merr.WrapErrSegmentNotLoaded(
				entry.SegmentID, "local growing text term expansion target is unavailable")
		}
		expander, ok := segment.(fuzzyTextTermExpander)
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg(
				"growing segment %d does not support text term expansion", entry.SegmentID)
		}
		matches, generation, dataVersion, err := expander.ExpandTextTerms(
			request.GetFieldID(),
			request.GetSourceTerms(),
			request.GetMaxEditDistance(),
			request.GetMaxExpansions(),
			request.GetPrefixLength(),
		)
		if err != nil {
			return nil, err
		}
		response.Generations = append(response.Generations, &querypb.SegmentTextTermGeneration{
			SegmentID:   entry.SegmentID,
			Generation:  generation,
			DataVersion: dataVersion,
		})
		for sourceIndex, sourceMatches := range matches {
			for _, match := range sourceMatches {
				key := candidateKey{sourceIndex: uint32(sourceIndex), term: string(match.Term)}
				if distance, ok := candidates[key]; !ok || match.EditDistance < distance {
					candidates[key] = match.EditDistance
				}
			}
		}
	}
	for key, distance := range candidates {
		response.Terms = append(response.Terms, &querypb.ExpandedTextTerm{
			SourceIndex:  key.sourceIndex,
			Term:         []byte(key.term),
			EditDistance: distance,
		})
	}
	sort.Slice(response.Terms, func(i, j int) bool {
		if response.Terms[i].GetSourceIndex() != response.Terms[j].GetSourceIndex() {
			return response.Terms[i].GetSourceIndex() < response.Terms[j].GetSourceIndex()
		}
		if response.Terms[i].GetEditDistance() != response.Terms[j].GetEditDistance() {
			return response.Terms[i].GetEditDistance() < response.Terms[j].GetEditDistance()
		}
		return bytes.Compare(response.Terms[i].GetTerm(), response.Terms[j].GetTerm()) < 0
	})
	return response, nil
}

func buildFuzzyBM25QueryTF(
	queryTF []map[uint32]float32,
	expanded map[uint32][]*querypb.ExpandedTextTerm,
) [][]byte {
	tfRows := make([][]byte, len(queryTF))
	for queryIndex, sourceFrequencies := range queryTF {
		hashedTF := make(map[uint32]float32)
		for index, frequency := range sourceFrequencies {
			for _, match := range expanded[index] {
				hash := typeutil.HashString2LessUint32(string(match.GetTerm()))
				hashedTF[hash] += frequency
			}
		}
		tfRows[queryIndex] = typeutil.CreateAndSortSparseFloatRow(hashedTF)
	}
	return tfRows
}

func fuzzySearchTargets(sealed []SnapshotItem, growing []SegmentEntry) ([]SnapshotItem, []SegmentEntry) {
	filteredSealed := make([]SnapshotItem, 0, len(sealed))
	for _, item := range sealed {
		segments := lo.Filter(item.Segments, func(segment SegmentEntry, _ int) bool {
			return segment.Level != datapb.SegmentLevel_L0
		})
		if len(segments) > 0 {
			filteredSealed = append(filteredSealed, SnapshotItem{NodeID: item.NodeID, Segments: segments})
		}
	}
	filteredGrowing := lo.Filter(growing, func(segment SegmentEntry, _ int) bool {
		return segment.Level != datapb.SegmentLevel_L0
	})
	return filteredSealed, filteredGrowing
}

func fuzzySearchTargetsBySegmentIDs(
	sealed []SnapshotItem,
	growing []SegmentEntry,
	segmentIDs typeutil.Set[int64],
) ([]SnapshotItem, []SegmentEntry) {
	filteredSealed := make([]SnapshotItem, 0, len(sealed))
	for _, item := range sealed {
		segments := lo.Filter(item.Segments, func(segment SegmentEntry, _ int) bool {
			return segmentIDs.Contain(segment.SegmentID)
		})
		if len(segments) > 0 {
			filteredSealed = append(filteredSealed, SnapshotItem{NodeID: item.NodeID, Segments: segments})
		}
	}
	filteredGrowing := lo.Filter(growing, func(segment SegmentEntry, _ int) bool {
		return segmentIDs.Contain(segment.SegmentID)
	})
	return filteredSealed, filteredGrowing
}

func fuzzySealedRowCount(sealed []SnapshotItem, sealedRowCount map[int64]int64) map[int64]int64 {
	result := make(map[int64]int64)
	for _, item := range sealed {
		for _, segment := range item.Segments {
			if rowCount, ok := sealedRowCount[segment.SegmentID]; ok {
				result[segment.SegmentID] = rowCount
			}
		}
	}
	return result
}

func (sd *shardDelegator) expandFuzzyBM25Terms(
	ctx context.Context,
	inputFieldID int64,
	sourceTerms [][]byte,
	maxEditDistance uint32,
	maxExpansions uint32,
	prefixLength uint32,
	sealed []SnapshotItem,
	growing []SegmentEntry,
	sealedRowCount map[int64]int64,
) (map[uint32][]*querypb.ExpandedTextTerm, []*querypb.SegmentTextTermGeneration, error) {
	expansionSealed, expansionGrowing := fuzzySearchTargets(sealed, growing)
	expansionSealedRowCount := fuzzySealedRowCount(expansionSealed, sealedRowCount)
	request := &querypb.ExpandTextTermsRequest{
		Base:            &commonpb.MsgBase{SourceID: paramtable.GetNodeID()},
		CollectionID:    sd.collectionID,
		FieldID:         inputFieldID,
		SourceTerms:     sourceTerms,
		MaxEditDistance: maxEditDistance,
		MaxExpansions:   maxExpansions,
		PrefixLength:    prefixLength,
	}
	tasks, err := organizeSubTask(ctx, request, expansionSealed, nil, sd, true,
		func(req *querypb.ExpandTextTermsRequest, scope querypb.DataScope, segmentIDs []int64, targetID int64) *querypb.ExpandTextTermsRequest {
			return &querypb.ExpandTextTermsRequest{
				Base: &commonpb.MsgBase{
					SourceID: paramtable.GetNodeID(),
					TargetID: targetID,
				},
				CollectionID:    req.GetCollectionID(),
				FieldID:         req.GetFieldID(),
				SourceTerms:     req.GetSourceTerms(),
				MaxEditDistance: req.GetMaxEditDistance(),
				MaxExpansions:   req.GetMaxExpansions(),
				PrefixLength:    req.GetPrefixLength(),
				SegmentIDs:      segmentIDs,
				Scope:           scope,
			}
		})
	if err != nil {
		return nil, nil, err
	}

	type sealedExpansionOutcome struct {
		results []subTaskResult[*querypb.ExpandTextTermsResponse]
		err     error
	}
	sealedDone := make(chan sealedExpansionOutcome, 1)
	go func() {
		results, executeErr := executeSubTasksWithDetails(
			ctx,
			tasks,
			NewRowCountBasedEvaluator(expansionSealedRowCount),
			func(ctx context.Context, req *querypb.ExpandTextTermsRequest, worker cluster.Worker) (*querypb.ExpandTextTermsResponse, error) {
				response, workerErr := worker.ExpandTextTerms(ctx, req)
				if response == nil {
					response = &querypb.ExpandTextTermsResponse{Status: merr.Status(workerErr)}
				}
				return response, workerErr
			},
			"ExpandTextTerms",
			sd.getLogger(ctx),
		)
		sealedDone <- sealedExpansionOutcome{results: results, err: executeErr}
	}()

	growingIDs := lo.Map(expansionGrowing, func(entry SegmentEntry, _ int) int64 {
		return entry.SegmentID
	})
	var growingResult subTaskResult[*querypb.ExpandTextTermsResponse]
	if len(growingIDs) > 0 {
		growingResult.segments = growingIDs
		growingResult.result, growingResult.err = sd.expandGrowingFuzzyBM25Terms(ctx, request, expansionGrowing)
	}

	sealedOutcome := <-sealedDone
	if sealedOutcome.err != nil {
		return nil, nil, sealedOutcome.err
	}
	taskResults := sealedOutcome.results
	if len(growingIDs) > 0 {
		if growingResult.err == nil {
			taskResults = append(taskResults, growingResult)
		} else {
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
			successSegments := typeutil.NewSet[int64]()
			for _, result := range taskResults {
				successSegments.Insert(result.segments...)
			}
			shouldReturnPartial, accessedDataRatio := NewRowCountBasedEvaluator(expansionSealedRowCount)(
				"ExpandTextTerms", successSegments, growingIDs, []error{growingResult.err})
			if !shouldReturnPartial {
				return nil, nil, growingResult.err
			}
			sd.getLogger(ctx).Info(ctx, "local growing text term expansion returned a partial result",
				mlog.Float64("accessedDataRatio", accessedDataRatio),
				mlog.Int64s("failureSegmentList", growingIDs),
				mlog.Err(growingResult.err),
			)
		}
	}

	targetIDs := make([]int64, 0)
	for _, taskResult := range taskResults {
		targetIDs = append(targetIDs, taskResult.segments...)
	}
	expected := make(map[int64]struct{}, len(targetIDs))
	for _, segmentID := range targetIDs {
		expected[segmentID] = struct{}{}
	}
	served := make(map[int64]fuzzySegmentGeneration, len(targetIDs))
	candidates := make(map[uint32]map[string]*querypb.ExpandedTextTerm)
	for _, taskResult := range taskResults {
		response := taskResult.result
		for _, generation := range response.GetGenerations() {
			if _, ok := expected[generation.GetSegmentID()]; !ok || generation.GetGeneration() == 0 {
				return nil, nil, merr.WrapErrServiceInternalMsg(
					"expansion returned unexpected text term generation for segment %d", generation.GetSegmentID())
			}
			current := fuzzySegmentGeneration{
				generation:  generation.GetGeneration(),
				dataVersion: generation.GetDataVersion(),
			}
			if existing, ok := served[generation.GetSegmentID()]; ok && existing != current {
				return nil, nil, merr.WrapErrServiceInternalMsg(
					"expansion returned conflicting text term generations for segment %d", generation.GetSegmentID())
			}
			served[generation.GetSegmentID()] = current
		}
		for _, term := range response.GetTerms() {
			if int(term.GetSourceIndex()) >= len(sourceTerms) {
				return nil, nil, merr.WrapErrServiceInternalMsg("expansion returned invalid fuzzy source index %d", term.GetSourceIndex())
			}
			byTerm := candidates[term.GetSourceIndex()]
			if byTerm == nil {
				byTerm = make(map[string]*querypb.ExpandedTextTerm)
				candidates[term.GetSourceIndex()] = byTerm
			}
			key := string(term.GetTerm())
			if current, ok := byTerm[key]; !ok || term.GetEditDistance() < current.GetEditDistance() {
				byTerm[key] = &querypb.ExpandedTextTerm{
					SourceIndex:  term.GetSourceIndex(),
					Term:         bytes.Clone(term.GetTerm()),
					EditDistance: term.GetEditDistance(),
				}
			}
		}
	}
	if len(served) != len(expected) {
		missing := make([]int64, 0)
		for segmentID := range expected {
			if _, ok := served[segmentID]; !ok {
				missing = append(missing, segmentID)
			}
		}
		sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
		return nil, nil, merr.WrapErrServiceInternalMsg("text term expansion missed target segments %v", missing)
	}
	generations := make([]*querypb.SegmentTextTermGeneration, 0, len(served))
	for segmentID, generation := range served {
		generations = append(generations, &querypb.SegmentTextTermGeneration{
			SegmentID:   segmentID,
			Generation:  generation.generation,
			DataVersion: generation.dataVersion,
		})
	}
	sort.Slice(generations, func(i, j int) bool { return generations[i].GetSegmentID() < generations[j].GetSegmentID() })
	result := make(map[uint32][]*querypb.ExpandedTextTerm, len(candidates))
	for sourceIndex, byTerm := range candidates {
		for _, term := range byTerm {
			result[sourceIndex] = append(result[sourceIndex], term)
		}
	}
	return result, generations, nil
}

func (sd *shardDelegator) buildFuzzyBM25IDF(
	ctx context.Context,
	req *querypb.SearchRequest,
	runner function.FunctionRunner,
	sealed []SnapshotItem,
	growing []SegmentEntry,
	sealedRowCount map[int64]int64,
) (float64, error) {
	options := req.GetReq().GetFuzzyBm25Options()
	if options == nil || options.GetMaxExpansions() == 0 || options.GetMaxEditDistance() > 2 {
		return 0, merr.WrapErrServiceInternalMsg("invalid fuzzy BM25 options at delegator")
	}
	pb := &commonpb.PlaceholderGroup{}
	if err := proto.Unmarshal(req.GetReq().GetPlaceholderGroup(), pb); err != nil {
		return 0, merr.WrapErrParameterInvalidErr(err, "failed to unmarshal fuzzy BM25 placeholder group")
	}
	if len(pb.GetPlaceholders()) != 1 || len(pb.GetPlaceholders()[0].GetValues()) == 0 ||
		pb.GetPlaceholders()[0].GetType() != commonpb.PlaceholderType_VarChar {
		return 0, merr.WrapErrParameterInvalidMsg("please provide varchar/text for fuzzy BM25 search")
	}
	texts := funcutil.GetVarCharFromPlaceholder(pb.GetPlaceholders()[0])
	datas := []any{texts}
	if len(runner.GetInputFields()) == 2 {
		analyzerName := req.GetReq().GetAnalyzerName()
		if analyzerName == "" {
			analyzerName = "default"
		}
		analyzerNames := make([]string, len(texts))
		for i := range analyzerNames {
			analyzerNames[i] = analyzerName
		}
		datas = append(datas, analyzerNames)
	}
	analyzer, ok := runner.(function.Analyzer)
	if !ok {
		return 0, merr.WrapErrServiceInternalMsg("BM25 runner does not expose analyzer")
	}
	tokensByQuery, err := analyzer.BatchAnalyze(false, false, datas...)
	if err != nil {
		return 0, err
	}
	if len(runner.GetInputFields()) == 0 {
		return 0, merr.WrapErrServiceInternalMsg("BM25 runner has no input field")
	}

	sourceTerms := make([][]byte, 0)
	sourceIndex := make(map[string]uint32)
	queryTF := make([]map[uint32]float32, len(tokensByQuery))
	for queryIndex, tokens := range tokensByQuery {
		queryTF[queryIndex] = make(map[uint32]float32)
		for _, token := range tokens {
			term := token.GetToken()
			index, ok := sourceIndex[term]
			if !ok {
				index = uint32(len(sourceTerms))
				sourceIndex[term] = index
				sourceTerms = append(sourceTerms, []byte(term))
			}
			queryTF[queryIndex][index]++
		}
	}

	expanded, generations, err := sd.expandFuzzyBM25Terms(
		ctx,
		runner.GetInputFields()[0].GetFieldID(),
		sourceTerms,
		options.GetMaxEditDistance(),
		options.GetMaxExpansions(),
		options.GetPrefixLength(),
		sealed,
		growing,
		sealedRowCount,
	)
	if err != nil {
		return 0, err
	}
	idfOracle := sd.getIDFOracle()
	if idfOracle == nil {
		return 0, merr.WrapErrServiceInternalMsg("bm25 oracle is not initialized")
	}
	tfRows := buildFuzzyBM25QueryTF(queryTF, expanded)
	idfRows, avgdl, err := idfOracle.BuildIDF(req.GetReq().GetFieldId(), &schemapb.SparseFloatArray{
		Contents: tfRows,
	})
	if err != nil {
		return 0, err
	}
	if avgdl <= 0 {
		return 0, nil
	}
	for _, idf := range idfRows {
		metrics.QueryNodeSearchFTSNumTokens.WithLabelValues(
			paramtable.GetStringNodeID(), fmt.Sprint(sd.collectionID), fmt.Sprint(req.GetReq().GetFieldId())).
			Observe(float64(typeutil.SparseFloatRowElementCount(idf)))
	}
	if err := SetBM25Params(req.GetReq(), avgdl); err != nil {
		return 0, err
	}
	req.Req.PlaceholderGroup = funcutil.SparseVectorDataToPlaceholderGroupBytes(idfRows)
	req.TextTermGenerations = generations
	return avgdl, nil
}

func (sd *shardDelegator) searchFuzzyBM25(
	ctx context.Context,
	req *querypb.SearchRequest,
	sealed []SnapshotItem,
	growing []SegmentEntry,
	sealedRowCount map[int64]int64,
) ([]*internalpb.SearchResults, error) {
	sealed, growing = fuzzySearchTargets(sealed, growing)
	var lastErr error
	for attempt := 1; attempt <= 2; attempt++ {
		attemptReq := typeutil.Clone(req)
		fuzzyRowCount := fuzzySealedRowCount(sealed, sealedRowCount)
		_, skipSearch, err := sd.prepareSearchFunction(ctx, attemptReq, sealed, growing, fuzzyRowCount)
		if err != nil {
			lastErr = err
			if merr.IsRetryableErr(err) && attempt < 2 {
				sd.getLogger(ctx).Warn(ctx, "retry fuzzy BM25 from expansion after transient preparation failure",
					mlog.Int("attempt", attempt), mlog.Err(err))
				continue
			}
			break
		}
		if skipSearch {
			return []*internalpb.SearchResults{}, nil
		}
		servedSegmentIDs := typeutil.NewSet[int64]()
		for _, generation := range attemptReq.GetTextTermGenerations() {
			servedSegmentIDs.Insert(generation.GetSegmentID())
		}
		attemptSealed, attemptGrowing := fuzzySearchTargetsBySegmentIDs(sealed, growing, servedSegmentIDs)
		rowCounts := make([]int64, 0, len(servedSegmentIDs))
		for _, item := range attemptSealed {
			for _, segment := range item.Segments {
				rowCounts = append(rowCounts, sealedRowCount[segment.SegmentID])
			}
		}
		effectiveSegmentNum := optimizers.CalculateEffectiveSegmentNum(
			sd.queryHook, rowCounts, attemptReq.GetReq().GetTopk())
		if optimizers.ShouldUseTwoStageSearch(attemptReq, effectiveSegmentNum) {
			results, fallback, err := sd.twoStageSearch(ctx, attemptReq, attemptSealed, attemptGrowing, sealedRowCount)
			if err == nil && !fallback {
				return results, nil
			}
			if err != nil {
				lastErr = err
				if !merr.IsRetryableErr(err) || attempt == 2 {
					break
				}
				sd.getLogger(ctx).Warn(ctx, "retry fuzzy BM25 from expansion after transient two-stage search failure",
					mlog.Int("attempt", attempt), mlog.Err(err))
				continue
			}
			sd.getLogger(ctx).Debug(ctx, "Two-stage fuzzy BM25 search requested fallback, continuing with normal search")
		}
		const isSecondStageSearch = false
		attemptReq, err = optimizers.OptimizeSearchParams(
			ctx, attemptReq, sd.queryHook, effectiveSegmentNum, isSecondStageSearch, sd.getVectorFieldDim)
		if err != nil {
			return nil, err
		}
		results, err := sd.executeSearchSubTasks(ctx, attemptReq, attemptSealed, attemptGrowing, sealedRowCount)
		if err == nil {
			return results, nil
		}
		lastErr = err
		if !merr.IsRetryableErr(err) || attempt == 2 {
			break
		}
		sd.getLogger(ctx).Warn(ctx, "retry fuzzy BM25 from expansion after transient search failure",
			mlog.Int("attempt", attempt), mlog.Err(err))
	}
	return nil, lastErr
}
