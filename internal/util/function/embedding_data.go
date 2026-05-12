package function

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/bm25"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var defaultFunctionRunnerCache = NewFunctionRunnerCache()

type FunctionRunnerCache struct {
	mu      sync.RWMutex
	nextSeq uint64
	entries map[functionRunnerCacheKey]*functionRunnerCacheEntry
}

type functionRunnerCacheKey struct {
	collectionID int64
	signature    string
}

type functionRunnerCacheEntry struct {
	outputFieldIDs []int64
	runners        []FunctionRunner
	seq            uint64
}

func NewFunctionRunnerCache() *FunctionRunnerCache {
	return &FunctionRunnerCache{
		entries: make(map[functionRunnerCacheKey]*functionRunnerCacheEntry),
	}
}

func (c *FunctionRunnerCache) getEntry(collectionID int64, signature string) (*functionRunnerCacheEntry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if entry, ok := c.entries[functionRunnerCacheKey{collectionID: collectionID, signature: signature}]; ok {
		return entry, true
	}
	return nil, false
}

func (c *FunctionRunnerCache) getLatestEntry(collectionID int64) (*functionRunnerCacheEntry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.getLatestEntryLocked(collectionID)
}

func (c *FunctionRunnerCache) getLatestEntryLocked(collectionID int64) (*functionRunnerCacheEntry, bool) {
	var (
		latestSeq   uint64
		latestEntry *functionRunnerCacheEntry
		found       bool
	)
	for key, entry := range c.entries {
		if key.collectionID != collectionID {
			continue
		}
		if !found || entry.seq > latestSeq {
			latestSeq = entry.seq
			latestEntry = entry
			found = true
		}
	}
	return latestEntry, found
}

func (c *FunctionRunnerCache) GetOrCreate(collectionID int64, schema *schemapb.CollectionSchema) ([]FunctionRunner, error) {
	entry, err := c.getOrCreateEntry(collectionID, schema)
	if err != nil {
		return nil, err
	}
	return entry.runners, nil
}

func (c *FunctionRunnerCache) getOrCreateEntry(collectionID int64, schema *schemapb.CollectionSchema) (*functionRunnerCacheEntry, error) {
	if schema == nil {
		return nil, errors.New("collection schema is nil")
	}
	signature, err := EmbeddingFunctionSignature(schema)
	if err != nil {
		return nil, err
	}

	key := functionRunnerCacheKey{
		collectionID: collectionID,
		signature:    signature,
	}
	if entry, ok := c.getEntry(collectionID, signature); ok {
		return entry, nil
	}

	runners, err := BuildEmbeddingRunners(schema)
	if err != nil {
		return nil, err
	}
	outputFieldIDs, err := OutputFieldIDsFromRunners(runners)
	if err != nil {
		CloseRunners(runners)
		return nil, err
	}

	c.mu.Lock()
	if entry, ok := c.entries[key]; ok {
		c.mu.Unlock()
		CloseRunners(runners)
		return entry, nil
	}
	c.nextSeq++
	entry := &functionRunnerCacheEntry{
		outputFieldIDs: outputFieldIDs,
		runners:        runners,
		seq:            c.nextSeq,
	}
	c.entries[key] = entry
	c.mu.Unlock()

	return entry, nil
}

func (c *FunctionRunnerCache) Materialize(collectionID int64, body *msgpb.InsertRequest) (bool, error) {
	changed, ok, err := c.FillCachedFunctionFields(collectionID, body)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, fmt.Errorf("function runners for collection %d are not initialized", collectionID)
	}
	return changed, err
}

func (c *FunctionRunnerCache) FillCachedFunctionFields(collectionID int64, body *msgpb.InsertRequest) (bool, bool, error) {
	if body == nil {
		return false, false, errors.New("insert request is nil")
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, ok := c.getLatestEntryLocked(collectionID)
	if !ok {
		return false, false, nil
	}
	if len(entry.outputFieldIDs) == 0 || HasAllFieldDataByID(body.GetFieldsData(), entry.outputFieldIDs) {
		return false, true, nil
	}
	changed, err := FillFunctionFields(entry.runners, body)
	return changed, true, err
}

func (c *FunctionRunnerCache) DropCollection(collectionID int64) {
	c.mu.Lock()
	runners := make([]FunctionRunner, 0)
	for key, entry := range c.entries {
		if key.collectionID == collectionID {
			runners = append(runners, entry.runners...)
			delete(c.entries, key)
		}
	}
	c.mu.Unlock()

	CloseRunners(runners)
}

func (c *FunctionRunnerCache) DropCollectionSignature(collectionID int64, signature string) {
	key := functionRunnerCacheKey{
		collectionID: collectionID,
		signature:    signature,
	}
	c.mu.Lock()
	entry := c.entries[key]
	delete(c.entries, key)
	c.mu.Unlock()

	if entry != nil {
		CloseRunners(entry.runners)
	}
}

func (c *FunctionRunnerCache) Close() {
	c.mu.Lock()
	runners := make([]FunctionRunner, 0)
	for key, entry := range c.entries {
		runners = append(runners, entry.runners...)
		delete(c.entries, key)
	}
	c.mu.Unlock()

	CloseRunners(runners)
}

func BuildEmbeddingRunners(schema *schemapb.CollectionSchema) ([]FunctionRunner, error) {
	if schema == nil {
		return nil, errors.New("collection schema is nil")
	}

	schema = proto.Clone(schema).(*schemapb.CollectionSchema)
	runners := make([]FunctionRunner, 0)
	for _, fn := range schema.GetFunctions() {
		if !IsEmbeddingFunctionType(fn.GetType()) {
			continue
		}
		runner, err := NewFunctionRunner(schema, fn)
		if err != nil {
			CloseRunners(runners)
			return nil, err
		}
		if runner != nil {
			runners = append(runners, runner)
		}
	}
	return runners, nil
}

func EmbeddingOutputFieldIDs(schema *schemapb.CollectionSchema) ([]int64, error) {
	if schema == nil {
		return nil, errors.New("collection schema is nil")
	}

	outputFieldIDs := make([]int64, 0)
	for _, fn := range schema.GetFunctions() {
		if !IsEmbeddingFunctionType(fn.GetType()) {
			continue
		}
		outputField := typeutil.GetFunctionOutputField(schema, fn)
		if outputField == nil {
			return nil, fmt.Errorf("function %s output field not found", fn.GetName())
		}
		outputFieldIDs = append(outputFieldIDs, outputField.GetFieldID())
	}
	return outputFieldIDs, nil
}

func EmbeddingFunctionSignature(schema *schemapb.CollectionSchema) (string, error) {
	if schema == nil {
		return "", errors.New("collection schema is nil")
	}

	functions := lo.Filter(schema.GetFunctions(), func(fn *schemapb.FunctionSchema, _ int) bool {
		return IsEmbeddingFunctionType(fn.GetType())
	})
	sort.Slice(functions, func(i, j int) bool {
		if functions[i].GetId() != functions[j].GetId() {
			return functions[i].GetId() < functions[j].GetId()
		}
		if functions[i].GetName() != functions[j].GetName() {
			return functions[i].GetName() < functions[j].GetName()
		}
		return functions[i].GetType() < functions[j].GetType()
	})

	hasher := sha256.New()
	for _, fn := range functions {
		fmt.Fprintf(hasher, "fn:%d:%d:%s|", fn.GetId(), fn.GetType(), fn.GetName())
		writeInt64s(hasher, "input_ids", fn.GetInputFieldIds())
		writeStrings(hasher, "input_names", fn.GetInputFieldNames())
		writeInt64s(hasher, "output_ids", fn.GetOutputFieldIds())
		writeStrings(hasher, "output_names", fn.GetOutputFieldNames())
		writeKeyValuePairs(hasher, "fn_params", fn.GetParams())

		for _, fieldID := range fn.GetInputFieldIds() {
			field := typeutil.GetField(schema, fieldID)
			if field == nil {
				return "", fmt.Errorf("function %s input field %d not found", fn.GetName(), fieldID)
			}
			writeFieldSignature(hasher, "input", field)
		}
		for _, fieldName := range fn.GetInputFieldNames() {
			field := typeutil.GetFieldByName(schema, fieldName)
			if field == nil {
				return "", fmt.Errorf("function %s input field %s not found", fn.GetName(), fieldName)
			}
			writeFieldSignature(hasher, "input_name", field)
		}
		for _, fieldID := range fn.GetOutputFieldIds() {
			field := typeutil.GetField(schema, fieldID)
			if field == nil {
				return "", fmt.Errorf("function %s output field %d not found", fn.GetName(), fieldID)
			}
			writeFieldSignature(hasher, "output", field)
		}
		for _, fieldName := range fn.GetOutputFieldNames() {
			field := typeutil.GetFieldByName(schema, fieldName)
			if field == nil {
				return "", fmt.Errorf("function %s output field %s not found", fn.GetName(), fieldName)
			}
			writeFieldSignature(hasher, "output_name", field)
		}
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func writeFieldSignature(hasher hashWriter, prefix string, field *schemapb.FieldSchema) {
	fmt.Fprintf(hasher, "%s:%d:%s:%d:%d|", prefix, field.GetFieldID(), field.GetName(), field.GetDataType(), field.GetElementType())
	writeKeyValuePairs(hasher, prefix+"_type_params", field.GetTypeParams())
}

func writeKeyValuePairs(hasher hashWriter, prefix string, pairs []*commonpb.KeyValuePair) {
	cloned := append([]*commonpb.KeyValuePair(nil), pairs...)
	sort.Slice(cloned, func(i, j int) bool {
		if cloned[i].GetKey() != cloned[j].GetKey() {
			return cloned[i].GetKey() < cloned[j].GetKey()
		}
		return cloned[i].GetValue() < cloned[j].GetValue()
	})
	for _, pair := range cloned {
		fmt.Fprintf(hasher, "%s:%s=%s|", prefix, pair.GetKey(), pair.GetValue())
	}
}

func writeInt64s(hasher hashWriter, prefix string, values []int64) {
	for idx, value := range values {
		fmt.Fprintf(hasher, "%s:%d=%d|", prefix, idx, value)
	}
}

func writeStrings(hasher hashWriter, prefix string, values []string) {
	for idx, value := range values {
		fmt.Fprintf(hasher, "%s:%d=%s|", prefix, idx, value)
	}
}

type hashWriter interface {
	Write([]byte) (int, error)
}

// FillFunctionData fills function output fields before appending an insert message to WAL.
// The function runners are expected to have been initialized by collection create or recovery.
func FillFunctionData(collectionID int64, body *msgpb.InsertRequest) (bool, error) {
	return defaultFunctionRunnerCache.Materialize(collectionID, body)
}

// FillCachedFunctionData uses long-lived runners managed by WAL lifecycle.
// It returns ok=false when cache is absent so consumer pipelines can build a
// short-lived runner only for old messages without write-before embedding.
func FillCachedFunctionData(collectionID int64, body *msgpb.InsertRequest) (changed bool, ok bool, err error) {
	return defaultFunctionRunnerCache.FillCachedFunctionFields(collectionID, body)
}

func GetOrCreateFunctionRunners(collectionID int64, schema *schemapb.CollectionSchema) ([]FunctionRunner, error) {
	return defaultFunctionRunnerCache.GetOrCreate(collectionID, schema)
}

func DropFunctionRunners(collectionID int64) {
	defaultFunctionRunnerCache.DropCollection(collectionID)
}

func DropFunctionRunnersBySignature(collectionID int64, signature string) {
	defaultFunctionRunnerCache.DropCollectionSignature(collectionID, signature)
}

func FillFunctionFields(runners []FunctionRunner, body *msgpb.InsertRequest) (bool, error) {
	if body == nil {
		return false, errors.New("insert request is nil")
	}

	changed := false
	for _, runner := range runners {
		outputFields := runner.GetOutputFields()
		if len(outputFields) != 1 {
			return false, fmt.Errorf("function should have exactly one output field, got %d", len(outputFields))
		}
		outputField := outputFields[0]
		if HasFieldData(body.GetFieldsData(), outputField.GetFieldID()) {
			continue
		}

		output, err := RunFunction(runner, body)
		if err != nil {
			return false, err
		}
		body.FieldsData = append(body.FieldsData, output)
		changed = true
	}

	return changed, nil
}

func OutputFieldIDsFromRunners(runners []FunctionRunner) ([]int64, error) {
	outputFieldIDs := make([]int64, 0, len(runners))
	for _, runner := range runners {
		outputFields := runner.GetOutputFields()
		if len(outputFields) != 1 {
			return nil, fmt.Errorf("function should have exactly one output field, got %d", len(outputFields))
		}
		outputFieldIDs = append(outputFieldIDs, outputFields[0].GetFieldID())
	}
	return outputFieldIDs, nil
}

func IsEmbeddingFunctionType(functionType schemapb.FunctionType) bool {
	switch functionType {
	case schemapb.FunctionType_BM25, schemapb.FunctionType_MinHash:
		return true
	default:
		return false
	}
}

func RunFunction(runner FunctionRunner, body *msgpb.InsertRequest) (*schemapb.FieldData, error) {
	inputIDs := lo.Map(runner.GetInputFields(), func(field *schemapb.FieldSchema, _ int) int64 {
		return field.GetFieldID()
	})
	inputData, err := getStringFieldData(body.GetFieldsData(), inputIDs...)
	if err != nil {
		return nil, err
	}
	output, err := runner.BatchRun(inputData...)
	if err != nil {
		return nil, err
	}
	if len(output) == 0 {
		return nil, errors.New("function runner returned empty output")
	}

	outputFields := runner.GetOutputFields()
	if len(outputFields) != 1 {
		return nil, fmt.Errorf("function should have exactly one output field, got %d", len(outputFields))
	}
	outputField := outputFields[0]

	switch runner.GetSchema().GetType() {
	case schemapb.FunctionType_BM25:
		sparseArray, ok := output[0].(*schemapb.SparseFloatArray)
		if !ok {
			return nil, errors.New("BM25 runner returned non sparse-float-vector output")
		}
		return bm25.BuildSparseFieldData(outputField, sparseArray), nil
	case schemapb.FunctionType_MinHash:
		fieldData, ok := output[0].(*schemapb.FieldData)
		if !ok {
			return nil, errors.New("MinHash runner returned non field-data output")
		}
		fieldData.Type = outputField.GetDataType()
		fieldData.FieldName = outputField.GetName()
		fieldData.FieldId = outputField.GetFieldID()
		return fieldData, nil
	default:
		return nil, fmt.Errorf("unsupported embedding function type %s", runner.GetSchema().GetType().String())
	}
}

func HasAllFieldDataByID(fieldsData []*schemapb.FieldData, fieldIDs []int64) bool {
	for _, fieldID := range fieldIDs {
		if !HasFieldData(fieldsData, fieldID) {
			return false
		}
	}
	return true
}

func HasFieldData(fieldsData []*schemapb.FieldData, fieldID int64) bool {
	return GetFieldData(fieldsData, fieldID) != nil
}

func GetFieldData(fieldsData []*schemapb.FieldData, fieldID int64) *schemapb.FieldData {
	for _, fieldData := range fieldsData {
		if fieldData.GetFieldId() == fieldID {
			return fieldData
		}
	}
	return nil
}

func CloseRunners(runners []FunctionRunner) {
	for _, runner := range runners {
		if runner != nil {
			runner.Close()
		}
	}
}

func getStringFieldData(fieldsData []*schemapb.FieldData, fieldIDs ...int64) ([]any, error) {
	result := make([]any, 0, len(fieldIDs))
	for _, fieldID := range fieldIDs {
		fieldData := GetFieldData(fieldsData, fieldID)
		if fieldData == nil {
			return nil, fmt.Errorf("field %d not found", fieldID)
		}
		stringData := fieldData.GetScalars().GetStringData()
		if stringData == nil {
			return nil, fmt.Errorf("field %d is not string data", fieldID)
		}
		result = append(result, stringData.GetData())
	}
	return result, nil
}
