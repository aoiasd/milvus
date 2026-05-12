package shard

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v3/log"
)

type functionRunnerState int

const (
	functionRunnerStateMissing functionRunnerState = iota
	functionRunnerStateInitializing
	functionRunnerStateReady
	functionRunnerStateFailed
)

type functionRunnerCacheKey struct {
	CollectionID int64
	Signature    string
}

type functionRunnerEntry struct {
	schema      *schemapb.CollectionSchema
	runners     []function.FunctionRunner
	state       functionRunnerState
	lastErr     error
	initialDone chan struct{}
	retrying    bool
}

type functionRunnerManager struct {
	ctx    context.Context
	cancel context.CancelFunc
	logger *log.MLogger

	mu      sync.Mutex
	entries map[functionRunnerCacheKey]*functionRunnerEntry
}

func newFunctionRunnerManager(logger *log.MLogger) *functionRunnerManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &functionRunnerManager{
		ctx:     ctx,
		cancel:  cancel,
		logger:  logger,
		entries: make(map[functionRunnerCacheKey]*functionRunnerEntry),
	}
}

func (m *functionRunnerManager) Recover(collectionID int64, schema *schemapb.CollectionSchema) {
	if schema == nil {
		return
	}
	key, err := functionRunnerCacheKeyOf(collectionID, schema)
	if err != nil {
		m.logger.Warn("failed to build function runner cache key",
			zap.Int64("collectionID", collectionID),
			zap.Int32("schemaVersion", schema.GetVersion()),
			zap.Error(err))
		return
	}
	if _, shouldBuild := m.beginInit(key, schema); !shouldBuild {
		return
	}

	go m.finishInitAndRetry(key, schema)
}

func (m *functionRunnerManager) GetOrCreate(ctx context.Context, collectionID int64, schema *schemapb.CollectionSchema) ([]function.FunctionRunner, error) {
	if schema == nil {
		return nil, errors.New("collection schema is nil")
	}

	key, err := functionRunnerCacheKeyOf(collectionID, schema)
	if err != nil {
		return nil, err
	}
	_, shouldBuild := m.beginInit(key, schema)
	if shouldBuild {
		runners, err := m.buildRunners(collectionID, schema)
		m.finishInit(key, runners, err)
		if err != nil {
			m.scheduleRetry(key, schema)
			return nil, err
		}
		return runners, nil
	}

	m.mu.Lock()
	e := m.entries[key]
	if e != nil && e.state == functionRunnerStateReady {
		m.mu.Unlock()
		return m.buildRunners(collectionID, schema)
	}
	var done <-chan struct{}
	if e != nil {
		done = e.initialDone
	}
	m.mu.Unlock()

	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	m.mu.Lock()
	e = m.entries[key]
	if e != nil && e.state == functionRunnerStateReady {
		m.mu.Unlock()
		return m.buildRunners(collectionID, schema)
	}
	if e == nil {
		m.mu.Unlock()
		return nil, errors.New("function runner cache entry was removed")
	}
	if e.state != functionRunnerStateInitializing {
		m.mu.Unlock()
		return m.GetOrCreate(ctx, collectionID, schema)
	}
	lastErr := e.lastErr
	m.mu.Unlock()

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, errors.New("function runner initialization is in progress")
}

func (m *functionRunnerManager) DropCollection(collectionID int64) {
	var doneChannels []chan struct{}

	m.mu.Lock()
	for key, e := range m.entries {
		if key.CollectionID == collectionID {
			if e.initialDone != nil {
				doneChannels = append(doneChannels, e.initialDone)
				e.initialDone = nil
			}
			delete(m.entries, key)
		}
	}
	m.mu.Unlock()

	closeDoneChannels(doneChannels)
	function.DropFunctionRunners(collectionID)
}

func (m *functionRunnerManager) Invalidate(collectionID int64, signature string) {
	key := functionRunnerCacheKey{CollectionID: collectionID, Signature: signature}

	m.mu.Lock()
	e := m.entries[key]
	delete(m.entries, key)
	m.mu.Unlock()

	if e != nil {
		if e.initialDone != nil {
			close(e.initialDone)
		}
	}
}

func (m *functionRunnerManager) Close() {
	m.cancel()

	var doneChannels []chan struct{}
	collectionIDs := make(map[int64]struct{})
	m.mu.Lock()
	for key, e := range m.entries {
		collectionIDs[key.CollectionID] = struct{}{}
		if e.initialDone != nil {
			doneChannels = append(doneChannels, e.initialDone)
			e.initialDone = nil
		}
	}
	m.entries = make(map[functionRunnerCacheKey]*functionRunnerEntry)
	m.mu.Unlock()

	closeDoneChannels(doneChannels)
	for collectionID := range collectionIDs {
		function.DropFunctionRunners(collectionID)
	}
}

func (m *functionRunnerManager) beginInit(key functionRunnerCacheKey, schema *schemapb.CollectionSchema) (*functionRunnerEntry, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if e, ok := m.entries[key]; ok {
		switch e.state {
		case functionRunnerStateReady, functionRunnerStateInitializing:
			return e, false
		}
		e.schema = cloneSchema(schema)
		e.state = functionRunnerStateInitializing
		e.initialDone = make(chan struct{})
		return e, true
	}

	e := &functionRunnerEntry{
		schema:      cloneSchema(schema),
		state:       functionRunnerStateInitializing,
		initialDone: make(chan struct{}),
	}
	m.entries[key] = e
	return e, true
}

func (m *functionRunnerManager) finishInit(key functionRunnerCacheKey, runners []function.FunctionRunner, err error) {
	m.mu.Lock()
	e := m.entries[key]
	if e == nil {
		m.mu.Unlock()
		function.DropFunctionRunnersBySignature(key.CollectionID, key.Signature)
		return
	}
	e.runners = runners
	e.lastErr = err
	if err != nil {
		e.state = functionRunnerStateFailed
	} else {
		e.state = functionRunnerStateReady
	}
	if e.initialDone != nil {
		close(e.initialDone)
		e.initialDone = nil
	}
	m.mu.Unlock()
}

func (m *functionRunnerManager) finishInitAndRetry(key functionRunnerCacheKey, schema *schemapb.CollectionSchema) {
	runners, err := m.buildRunners(key.CollectionID, schema)
	m.finishInit(key, runners, err)
	if err == nil {
		return
	}
	m.logger.Warn("failed to initialize function runners, will retry",
		zap.Int64("collectionID", key.CollectionID),
		zap.String("signature", key.Signature),
		zap.Error(err))
	m.scheduleRetry(key, schema)
}

func (m *functionRunnerManager) scheduleRetry(key functionRunnerCacheKey, schema *schemapb.CollectionSchema) {
	m.mu.Lock()
	e := m.entries[key]
	if e == nil || e.retrying || e.state == functionRunnerStateReady {
		m.mu.Unlock()
		return
	}
	e.retrying = true
	m.mu.Unlock()

	go m.retryLoop(key, cloneSchema(schema))
}

func (m *functionRunnerManager) retryLoop(key functionRunnerCacheKey, schema *schemapb.CollectionSchema) {
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 100 * time.Millisecond
	bo.MaxInterval = 10 * time.Second
	bo.MaxElapsedTime = 0
	bo.Reset()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-time.After(bo.NextBackOff()):
		}

		if m.isReadyOrMissing(key) {
			return
		}

		runners, err := m.buildRunners(key.CollectionID, schema)
		if m.hasForegroundInit(key) {
			continue
		}
		if m.isReadyOrMissing(key) {
			return
		}
		if err == nil {
			if !m.finishRetryInit(key, runners, nil) {
				continue
			}
			m.logger.Info("function runners initialized after retry",
				zap.Int64("collectionID", key.CollectionID),
				zap.String("signature", key.Signature))
			return
		}

		if !m.finishRetryInit(key, runners, err) {
			continue
		}

		m.logger.Warn("retry function runner initialization failed",
			zap.Int64("collectionID", key.CollectionID),
			zap.String("signature", key.Signature),
			zap.Error(err))
	}
}

func (m *functionRunnerManager) finishRetryInit(key functionRunnerCacheKey, runners []function.FunctionRunner, err error) bool {
	m.mu.Lock()
	e := m.entries[key]
	if e == nil {
		m.mu.Unlock()
		function.DropFunctionRunnersBySignature(key.CollectionID, key.Signature)
		return false
	}
	if e.state == functionRunnerStateReady {
		e.retrying = false
		m.mu.Unlock()
		return false
	}
	if e.state == functionRunnerStateInitializing && e.initialDone != nil {
		m.mu.Unlock()
		return false
	}

	e.runners = runners
	e.lastErr = err
	if err != nil {
		e.state = functionRunnerStateFailed
	} else {
		e.state = functionRunnerStateReady
		e.retrying = false
	}
	m.mu.Unlock()
	return true
}

func (m *functionRunnerManager) isReadyOrMissing(key functionRunnerCacheKey) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	e := m.entries[key]
	if e == nil || e.state == functionRunnerStateReady {
		if e != nil {
			e.retrying = false
		}
		return true
	}
	return false
}

func (m *functionRunnerManager) hasForegroundInit(key functionRunnerCacheKey) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	e := m.entries[key]
	return e != nil && e.state == functionRunnerStateInitializing && e.initialDone != nil
}

func (m *functionRunnerManager) buildRunners(collectionID int64, schema *schemapb.CollectionSchema) ([]function.FunctionRunner, error) {
	return function.GetOrCreateFunctionRunners(collectionID, schema)
}

func closeDoneChannels(channels []chan struct{}) {
	for _, ch := range channels {
		close(ch)
	}
}

func functionRunnerCacheKeyOf(collectionID int64, schema *schemapb.CollectionSchema) (functionRunnerCacheKey, error) {
	signature, err := function.EmbeddingFunctionSignature(schema)
	if err != nil {
		return functionRunnerCacheKey{}, err
	}
	return functionRunnerCacheKey{
		CollectionID: collectionID,
		Signature:    signature,
	}, nil
}

func cloneSchema(schema *schemapb.CollectionSchema) *schemapb.CollectionSchema {
	if schema == nil {
		return nil
	}
	return proto.Clone(schema).(*schemapb.CollectionSchema)
}
