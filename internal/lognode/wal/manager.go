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

package wal

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"go.uber.org/zap"
)

// channel filter used for filter logger when boradcast
type ChannelFilter func(*WriteAheadLogger) bool

func WithTsPhysicalTime(ti time.Time) ChannelFilter {
	return func(wal *WriteAheadLogger) bool {
		return tsoutil.PhysicalTime(wal.GetLastTimestamp()).After(ti)
	}
}

type LoggerManager struct {
	loggers     map[string]*WriteAheadLogger
	tsAllocator TimestampAllocator
	factory     msgstream.Factory

	mu sync.RWMutex
}

func NewLoggerManger(factory msgstream.Factory) *LoggerManager {
	return &LoggerManager{
		loggers: make(map[string]*WriteAheadLogger),
		factory: factory,
	}
}

func (m *LoggerManager) Init(rc types.RootCoord) error {
	// TODO RET SIZE OPTION
	allocator, err := NewTimestampAllocator(1024, rc)
	if err != nil {
		return err
	}
	m.tsAllocator = allocator
	return nil
}

func (m *LoggerManager) AddLogger(ctx context.Context, pChannel string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	logger, ok := m.loggers[pChannel]
	if !ok {
		m.loggers[pChannel] = NewWriteAheadLogger(pChannel, m.tsAllocator)
		err := logger.Init(ctx, m.factory)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *LoggerManager) RemoveLogger(pChannel string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.loggers[pChannel]
	if ok {
		delete(m.loggers, pChannel)
	}
}

func (m *LoggerManager) Broadcast(ctx context.Context, msg msgstream.TsMsg, filters ...ChannelFilter) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	filterFunc := func(wal *WriteAheadLogger) bool {
		for _, filter := range filters {
			if filter(wal) {
				return true
			}
		}
		return false
	}

	errCombine := []error{}
	for channel, logger := range m.loggers {
		if !filterFunc(logger) {
			err := logger.Produce(ctx, msg)
			if err != nil {
				errCombine = append(errCombine, err)
				log.Warn("some channel could not work when board cast", zap.String("channel", channel))
			}
		}
	}
	return merr.Combine(errCombine...)
}

func (m *LoggerManager) Produce(ctx context.Context, msg msgstream.TsMsg, pChannel string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	logger, ok := m.loggers[pChannel]
	if !ok {
		return merr.WrapErrChannelNotFound(pChannel, "channel logger not added")
	}

	return logger.Produce(ctx, msg)
}
