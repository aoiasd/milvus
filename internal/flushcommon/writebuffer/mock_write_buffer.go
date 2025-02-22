// Code generated by mockery v2.46.0. DO NOT EDIT.

package writebuffer

import (
	context "context"

	msgpb "github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	mock "github.com/stretchr/testify/mock"

	msgstream "github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
)

// MockWriteBuffer is an autogenerated mock type for the WriteBuffer type
type MockWriteBuffer struct {
	mock.Mock
}

type MockWriteBuffer_Expecter struct {
	mock *mock.Mock
}

func (_m *MockWriteBuffer) EXPECT() *MockWriteBuffer_Expecter {
	return &MockWriteBuffer_Expecter{mock: &_m.Mock}
}

// BufferData provides a mock function with given fields: insertMsgs, deleteMsgs, startPos, endPos
func (_m *MockWriteBuffer) BufferData(insertMsgs []*InsertData, deleteMsgs []*msgstream.DeleteMsg, startPos *msgpb.MsgPosition, endPos *msgpb.MsgPosition) error {
	ret := _m.Called(insertMsgs, deleteMsgs, startPos, endPos)

	if len(ret) == 0 {
		panic("no return value specified for BufferData")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func([]*InsertData, []*msgstream.DeleteMsg, *msgpb.MsgPosition, *msgpb.MsgPosition) error); ok {
		r0 = rf(insertMsgs, deleteMsgs, startPos, endPos)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockWriteBuffer_BufferData_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'BufferData'
type MockWriteBuffer_BufferData_Call struct {
	*mock.Call
}

// BufferData is a helper method to define mock.On call
//   - insertMsgs []*InsertData
//   - deleteMsgs []*msgstream.DeleteMsg
//   - startPos *msgpb.MsgPosition
//   - endPos *msgpb.MsgPosition
func (_e *MockWriteBuffer_Expecter) BufferData(insertMsgs interface{}, deleteMsgs interface{}, startPos interface{}, endPos interface{}) *MockWriteBuffer_BufferData_Call {
	return &MockWriteBuffer_BufferData_Call{Call: _e.mock.On("BufferData", insertMsgs, deleteMsgs, startPos, endPos)}
}

func (_c *MockWriteBuffer_BufferData_Call) Run(run func(insertMsgs []*InsertData, deleteMsgs []*msgstream.DeleteMsg, startPos *msgpb.MsgPosition, endPos *msgpb.MsgPosition)) *MockWriteBuffer_BufferData_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].([]*InsertData), args[1].([]*msgstream.DeleteMsg), args[2].(*msgpb.MsgPosition), args[3].(*msgpb.MsgPosition))
	})
	return _c
}

func (_c *MockWriteBuffer_BufferData_Call) Return(_a0 error) *MockWriteBuffer_BufferData_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockWriteBuffer_BufferData_Call) RunAndReturn(run func([]*InsertData, []*msgstream.DeleteMsg, *msgpb.MsgPosition, *msgpb.MsgPosition) error) *MockWriteBuffer_BufferData_Call {
	_c.Call.Return(run)
	return _c
}

// Close provides a mock function with given fields: ctx, drop
func (_m *MockWriteBuffer) Close(ctx context.Context, drop bool) {
	_m.Called(ctx, drop)
}

// MockWriteBuffer_Close_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Close'
type MockWriteBuffer_Close_Call struct {
	*mock.Call
}

// Close is a helper method to define mock.On call
//   - ctx context.Context
//   - drop bool
func (_e *MockWriteBuffer_Expecter) Close(ctx interface{}, drop interface{}) *MockWriteBuffer_Close_Call {
	return &MockWriteBuffer_Close_Call{Call: _e.mock.On("Close", ctx, drop)}
}

func (_c *MockWriteBuffer_Close_Call) Run(run func(ctx context.Context, drop bool)) *MockWriteBuffer_Close_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(bool))
	})
	return _c
}

func (_c *MockWriteBuffer_Close_Call) Return() *MockWriteBuffer_Close_Call {
	_c.Call.Return()
	return _c
}

func (_c *MockWriteBuffer_Close_Call) RunAndReturn(run func(context.Context, bool)) *MockWriteBuffer_Close_Call {
	_c.Call.Return(run)
	return _c
}

// CreateNewGrowingSegment provides a mock function with given fields: partitionID, segmentID, startPos
func (_m *MockWriteBuffer) CreateNewGrowingSegment(partitionID int64, segmentID int64, startPos *msgpb.MsgPosition) {
	_m.Called(partitionID, segmentID, startPos)
}

// MockWriteBuffer_CreateNewGrowingSegment_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'CreateNewGrowingSegment'
type MockWriteBuffer_CreateNewGrowingSegment_Call struct {
	*mock.Call
}

// CreateNewGrowingSegment is a helper method to define mock.On call
//   - partitionID int64
//   - segmentID int64
//   - startPos *msgpb.MsgPosition
func (_e *MockWriteBuffer_Expecter) CreateNewGrowingSegment(partitionID interface{}, segmentID interface{}, startPos interface{}) *MockWriteBuffer_CreateNewGrowingSegment_Call {
	return &MockWriteBuffer_CreateNewGrowingSegment_Call{Call: _e.mock.On("CreateNewGrowingSegment", partitionID, segmentID, startPos)}
}

func (_c *MockWriteBuffer_CreateNewGrowingSegment_Call) Run(run func(partitionID int64, segmentID int64, startPos *msgpb.MsgPosition)) *MockWriteBuffer_CreateNewGrowingSegment_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(int64), args[1].(int64), args[2].(*msgpb.MsgPosition))
	})
	return _c
}

func (_c *MockWriteBuffer_CreateNewGrowingSegment_Call) Return() *MockWriteBuffer_CreateNewGrowingSegment_Call {
	_c.Call.Return()
	return _c
}

func (_c *MockWriteBuffer_CreateNewGrowingSegment_Call) RunAndReturn(run func(int64, int64, *msgpb.MsgPosition)) *MockWriteBuffer_CreateNewGrowingSegment_Call {
	_c.Call.Return(run)
	return _c
}

// DropPartitions provides a mock function with given fields: partitionIDs
func (_m *MockWriteBuffer) DropPartitions(partitionIDs []int64) {
	_m.Called(partitionIDs)
}

// MockWriteBuffer_DropPartitions_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'DropPartitions'
type MockWriteBuffer_DropPartitions_Call struct {
	*mock.Call
}

// DropPartitions is a helper method to define mock.On call
//   - partitionIDs []int64
func (_e *MockWriteBuffer_Expecter) DropPartitions(partitionIDs interface{}) *MockWriteBuffer_DropPartitions_Call {
	return &MockWriteBuffer_DropPartitions_Call{Call: _e.mock.On("DropPartitions", partitionIDs)}
}

func (_c *MockWriteBuffer_DropPartitions_Call) Run(run func(partitionIDs []int64)) *MockWriteBuffer_DropPartitions_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].([]int64))
	})
	return _c
}

func (_c *MockWriteBuffer_DropPartitions_Call) Return() *MockWriteBuffer_DropPartitions_Call {
	_c.Call.Return()
	return _c
}

func (_c *MockWriteBuffer_DropPartitions_Call) RunAndReturn(run func([]int64)) *MockWriteBuffer_DropPartitions_Call {
	_c.Call.Return(run)
	return _c
}

// EvictBuffer provides a mock function with given fields: policies
func (_m *MockWriteBuffer) EvictBuffer(policies ...SyncPolicy) {
	_va := make([]interface{}, len(policies))
	for _i := range policies {
		_va[_i] = policies[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, _va...)
	_m.Called(_ca...)
}

// MockWriteBuffer_EvictBuffer_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'EvictBuffer'
type MockWriteBuffer_EvictBuffer_Call struct {
	*mock.Call
}

// EvictBuffer is a helper method to define mock.On call
//   - policies ...SyncPolicy
func (_e *MockWriteBuffer_Expecter) EvictBuffer(policies ...interface{}) *MockWriteBuffer_EvictBuffer_Call {
	return &MockWriteBuffer_EvictBuffer_Call{Call: _e.mock.On("EvictBuffer",
		append([]interface{}{}, policies...)...)}
}

func (_c *MockWriteBuffer_EvictBuffer_Call) Run(run func(policies ...SyncPolicy)) *MockWriteBuffer_EvictBuffer_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]SyncPolicy, len(args)-0)
		for i, a := range args[0:] {
			if a != nil {
				variadicArgs[i] = a.(SyncPolicy)
			}
		}
		run(variadicArgs...)
	})
	return _c
}

func (_c *MockWriteBuffer_EvictBuffer_Call) Return() *MockWriteBuffer_EvictBuffer_Call {
	_c.Call.Return()
	return _c
}

func (_c *MockWriteBuffer_EvictBuffer_Call) RunAndReturn(run func(...SyncPolicy)) *MockWriteBuffer_EvictBuffer_Call {
	_c.Call.Return(run)
	return _c
}

// GetCheckpoint provides a mock function with given fields:
func (_m *MockWriteBuffer) GetCheckpoint() *msgpb.MsgPosition {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetCheckpoint")
	}

	var r0 *msgpb.MsgPosition
	if rf, ok := ret.Get(0).(func() *msgpb.MsgPosition); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*msgpb.MsgPosition)
		}
	}

	return r0
}

// MockWriteBuffer_GetCheckpoint_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetCheckpoint'
type MockWriteBuffer_GetCheckpoint_Call struct {
	*mock.Call
}

// GetCheckpoint is a helper method to define mock.On call
func (_e *MockWriteBuffer_Expecter) GetCheckpoint() *MockWriteBuffer_GetCheckpoint_Call {
	return &MockWriteBuffer_GetCheckpoint_Call{Call: _e.mock.On("GetCheckpoint")}
}

func (_c *MockWriteBuffer_GetCheckpoint_Call) Run(run func()) *MockWriteBuffer_GetCheckpoint_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockWriteBuffer_GetCheckpoint_Call) Return(_a0 *msgpb.MsgPosition) *MockWriteBuffer_GetCheckpoint_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockWriteBuffer_GetCheckpoint_Call) RunAndReturn(run func() *msgpb.MsgPosition) *MockWriteBuffer_GetCheckpoint_Call {
	_c.Call.Return(run)
	return _c
}

// GetFlushTimestamp provides a mock function with given fields:
func (_m *MockWriteBuffer) GetFlushTimestamp() uint64 {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetFlushTimestamp")
	}

	var r0 uint64
	if rf, ok := ret.Get(0).(func() uint64); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(uint64)
	}

	return r0
}

// MockWriteBuffer_GetFlushTimestamp_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetFlushTimestamp'
type MockWriteBuffer_GetFlushTimestamp_Call struct {
	*mock.Call
}

// GetFlushTimestamp is a helper method to define mock.On call
func (_e *MockWriteBuffer_Expecter) GetFlushTimestamp() *MockWriteBuffer_GetFlushTimestamp_Call {
	return &MockWriteBuffer_GetFlushTimestamp_Call{Call: _e.mock.On("GetFlushTimestamp")}
}

func (_c *MockWriteBuffer_GetFlushTimestamp_Call) Run(run func()) *MockWriteBuffer_GetFlushTimestamp_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockWriteBuffer_GetFlushTimestamp_Call) Return(_a0 uint64) *MockWriteBuffer_GetFlushTimestamp_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockWriteBuffer_GetFlushTimestamp_Call) RunAndReturn(run func() uint64) *MockWriteBuffer_GetFlushTimestamp_Call {
	_c.Call.Return(run)
	return _c
}

// HasSegment provides a mock function with given fields: segmentID
func (_m *MockWriteBuffer) HasSegment(segmentID int64) bool {
	ret := _m.Called(segmentID)

	if len(ret) == 0 {
		panic("no return value specified for HasSegment")
	}

	var r0 bool
	if rf, ok := ret.Get(0).(func(int64) bool); ok {
		r0 = rf(segmentID)
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// MockWriteBuffer_HasSegment_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'HasSegment'
type MockWriteBuffer_HasSegment_Call struct {
	*mock.Call
}

// HasSegment is a helper method to define mock.On call
//   - segmentID int64
func (_e *MockWriteBuffer_Expecter) HasSegment(segmentID interface{}) *MockWriteBuffer_HasSegment_Call {
	return &MockWriteBuffer_HasSegment_Call{Call: _e.mock.On("HasSegment", segmentID)}
}

func (_c *MockWriteBuffer_HasSegment_Call) Run(run func(segmentID int64)) *MockWriteBuffer_HasSegment_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(int64))
	})
	return _c
}

func (_c *MockWriteBuffer_HasSegment_Call) Return(_a0 bool) *MockWriteBuffer_HasSegment_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockWriteBuffer_HasSegment_Call) RunAndReturn(run func(int64) bool) *MockWriteBuffer_HasSegment_Call {
	_c.Call.Return(run)
	return _c
}

// MemorySize provides a mock function with given fields:
func (_m *MockWriteBuffer) MemorySize() int64 {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for MemorySize")
	}

	var r0 int64
	if rf, ok := ret.Get(0).(func() int64); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int64)
	}

	return r0
}

// MockWriteBuffer_MemorySize_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'MemorySize'
type MockWriteBuffer_MemorySize_Call struct {
	*mock.Call
}

// MemorySize is a helper method to define mock.On call
func (_e *MockWriteBuffer_Expecter) MemorySize() *MockWriteBuffer_MemorySize_Call {
	return &MockWriteBuffer_MemorySize_Call{Call: _e.mock.On("MemorySize")}
}

func (_c *MockWriteBuffer_MemorySize_Call) Run(run func()) *MockWriteBuffer_MemorySize_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockWriteBuffer_MemorySize_Call) Return(_a0 int64) *MockWriteBuffer_MemorySize_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockWriteBuffer_MemorySize_Call) RunAndReturn(run func() int64) *MockWriteBuffer_MemorySize_Call {
	_c.Call.Return(run)
	return _c
}

// SealSegments provides a mock function with given fields: ctx, segmentIDs
func (_m *MockWriteBuffer) SealSegments(ctx context.Context, segmentIDs []int64) error {
	ret := _m.Called(ctx, segmentIDs)

	if len(ret) == 0 {
		panic("no return value specified for SealSegments")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, []int64) error); ok {
		r0 = rf(ctx, segmentIDs)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockWriteBuffer_SealSegments_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SealSegments'
type MockWriteBuffer_SealSegments_Call struct {
	*mock.Call
}

// SealSegments is a helper method to define mock.On call
//   - ctx context.Context
//   - segmentIDs []int64
func (_e *MockWriteBuffer_Expecter) SealSegments(ctx interface{}, segmentIDs interface{}) *MockWriteBuffer_SealSegments_Call {
	return &MockWriteBuffer_SealSegments_Call{Call: _e.mock.On("SealSegments", ctx, segmentIDs)}
}

func (_c *MockWriteBuffer_SealSegments_Call) Run(run func(ctx context.Context, segmentIDs []int64)) *MockWriteBuffer_SealSegments_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].([]int64))
	})
	return _c
}

func (_c *MockWriteBuffer_SealSegments_Call) Return(_a0 error) *MockWriteBuffer_SealSegments_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockWriteBuffer_SealSegments_Call) RunAndReturn(run func(context.Context, []int64) error) *MockWriteBuffer_SealSegments_Call {
	_c.Call.Return(run)
	return _c
}

// SetFlushTimestamp provides a mock function with given fields: flushTs
func (_m *MockWriteBuffer) SetFlushTimestamp(flushTs uint64) {
	_m.Called(flushTs)
}

// MockWriteBuffer_SetFlushTimestamp_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetFlushTimestamp'
type MockWriteBuffer_SetFlushTimestamp_Call struct {
	*mock.Call
}

// SetFlushTimestamp is a helper method to define mock.On call
//   - flushTs uint64
func (_e *MockWriteBuffer_Expecter) SetFlushTimestamp(flushTs interface{}) *MockWriteBuffer_SetFlushTimestamp_Call {
	return &MockWriteBuffer_SetFlushTimestamp_Call{Call: _e.mock.On("SetFlushTimestamp", flushTs)}
}

func (_c *MockWriteBuffer_SetFlushTimestamp_Call) Run(run func(flushTs uint64)) *MockWriteBuffer_SetFlushTimestamp_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(uint64))
	})
	return _c
}

func (_c *MockWriteBuffer_SetFlushTimestamp_Call) Return() *MockWriteBuffer_SetFlushTimestamp_Call {
	_c.Call.Return()
	return _c
}

func (_c *MockWriteBuffer_SetFlushTimestamp_Call) RunAndReturn(run func(uint64)) *MockWriteBuffer_SetFlushTimestamp_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockWriteBuffer creates a new instance of MockWriteBuffer. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockWriteBuffer(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockWriteBuffer {
	mock := &MockWriteBuffer{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
