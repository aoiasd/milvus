// Code generated by mockery v2.46.0. DO NOT EDIT.

package mock_streamingpb

import (
	context "context"

	mock "github.com/stretchr/testify/mock"
	metadata "google.golang.org/grpc/metadata"

	streamingpb "github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

// MockStreamingCoordBroadcastService_WatchServer is an autogenerated mock type for the StreamingCoordBroadcastService_WatchServer type
type MockStreamingCoordBroadcastService_WatchServer struct {
	mock.Mock
}

type MockStreamingCoordBroadcastService_WatchServer_Expecter struct {
	mock *mock.Mock
}

func (_m *MockStreamingCoordBroadcastService_WatchServer) EXPECT() *MockStreamingCoordBroadcastService_WatchServer_Expecter {
	return &MockStreamingCoordBroadcastService_WatchServer_Expecter{mock: &_m.Mock}
}

// Context provides a mock function with given fields:
func (_m *MockStreamingCoordBroadcastService_WatchServer) Context() context.Context {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Context")
	}

	var r0 context.Context
	if rf, ok := ret.Get(0).(func() context.Context); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(context.Context)
		}
	}

	return r0
}

// MockStreamingCoordBroadcastService_WatchServer_Context_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Context'
type MockStreamingCoordBroadcastService_WatchServer_Context_Call struct {
	*mock.Call
}

// Context is a helper method to define mock.On call
func (_e *MockStreamingCoordBroadcastService_WatchServer_Expecter) Context() *MockStreamingCoordBroadcastService_WatchServer_Context_Call {
	return &MockStreamingCoordBroadcastService_WatchServer_Context_Call{Call: _e.mock.On("Context")}
}

func (_c *MockStreamingCoordBroadcastService_WatchServer_Context_Call) Run(run func()) *MockStreamingCoordBroadcastService_WatchServer_Context_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockStreamingCoordBroadcastService_WatchServer_Context_Call) Return(_a0 context.Context) *MockStreamingCoordBroadcastService_WatchServer_Context_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockStreamingCoordBroadcastService_WatchServer_Context_Call) RunAndReturn(run func() context.Context) *MockStreamingCoordBroadcastService_WatchServer_Context_Call {
	_c.Call.Return(run)
	return _c
}

// Recv provides a mock function with given fields:
func (_m *MockStreamingCoordBroadcastService_WatchServer) Recv() (*streamingpb.BroadcastWatchRequest, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Recv")
	}

	var r0 *streamingpb.BroadcastWatchRequest
	var r1 error
	if rf, ok := ret.Get(0).(func() (*streamingpb.BroadcastWatchRequest, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() *streamingpb.BroadcastWatchRequest); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*streamingpb.BroadcastWatchRequest)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockStreamingCoordBroadcastService_WatchServer_Recv_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Recv'
type MockStreamingCoordBroadcastService_WatchServer_Recv_Call struct {
	*mock.Call
}

// Recv is a helper method to define mock.On call
func (_e *MockStreamingCoordBroadcastService_WatchServer_Expecter) Recv() *MockStreamingCoordBroadcastService_WatchServer_Recv_Call {
	return &MockStreamingCoordBroadcastService_WatchServer_Recv_Call{Call: _e.mock.On("Recv")}
}

func (_c *MockStreamingCoordBroadcastService_WatchServer_Recv_Call) Run(run func()) *MockStreamingCoordBroadcastService_WatchServer_Recv_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockStreamingCoordBroadcastService_WatchServer_Recv_Call) Return(_a0 *streamingpb.BroadcastWatchRequest, _a1 error) *MockStreamingCoordBroadcastService_WatchServer_Recv_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockStreamingCoordBroadcastService_WatchServer_Recv_Call) RunAndReturn(run func() (*streamingpb.BroadcastWatchRequest, error)) *MockStreamingCoordBroadcastService_WatchServer_Recv_Call {
	_c.Call.Return(run)
	return _c
}

// RecvMsg provides a mock function with given fields: m
func (_m *MockStreamingCoordBroadcastService_WatchServer) RecvMsg(m interface{}) error {
	ret := _m.Called(m)

	if len(ret) == 0 {
		panic("no return value specified for RecvMsg")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}) error); ok {
		r0 = rf(m)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockStreamingCoordBroadcastService_WatchServer_RecvMsg_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'RecvMsg'
type MockStreamingCoordBroadcastService_WatchServer_RecvMsg_Call struct {
	*mock.Call
}

// RecvMsg is a helper method to define mock.On call
//   - m interface{}
func (_e *MockStreamingCoordBroadcastService_WatchServer_Expecter) RecvMsg(m interface{}) *MockStreamingCoordBroadcastService_WatchServer_RecvMsg_Call {
	return &MockStreamingCoordBroadcastService_WatchServer_RecvMsg_Call{Call: _e.mock.On("RecvMsg", m)}
}

func (_c *MockStreamingCoordBroadcastService_WatchServer_RecvMsg_Call) Run(run func(m interface{})) *MockStreamingCoordBroadcastService_WatchServer_RecvMsg_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(interface{}))
	})
	return _c
}

func (_c *MockStreamingCoordBroadcastService_WatchServer_RecvMsg_Call) Return(_a0 error) *MockStreamingCoordBroadcastService_WatchServer_RecvMsg_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockStreamingCoordBroadcastService_WatchServer_RecvMsg_Call) RunAndReturn(run func(interface{}) error) *MockStreamingCoordBroadcastService_WatchServer_RecvMsg_Call {
	_c.Call.Return(run)
	return _c
}

// Send provides a mock function with given fields: _a0
func (_m *MockStreamingCoordBroadcastService_WatchServer) Send(_a0 *streamingpb.BroadcastWatchResponse) error {
	ret := _m.Called(_a0)

	if len(ret) == 0 {
		panic("no return value specified for Send")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*streamingpb.BroadcastWatchResponse) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockStreamingCoordBroadcastService_WatchServer_Send_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Send'
type MockStreamingCoordBroadcastService_WatchServer_Send_Call struct {
	*mock.Call
}

// Send is a helper method to define mock.On call
//   - _a0 *streamingpb.BroadcastWatchResponse
func (_e *MockStreamingCoordBroadcastService_WatchServer_Expecter) Send(_a0 interface{}) *MockStreamingCoordBroadcastService_WatchServer_Send_Call {
	return &MockStreamingCoordBroadcastService_WatchServer_Send_Call{Call: _e.mock.On("Send", _a0)}
}

func (_c *MockStreamingCoordBroadcastService_WatchServer_Send_Call) Run(run func(_a0 *streamingpb.BroadcastWatchResponse)) *MockStreamingCoordBroadcastService_WatchServer_Send_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*streamingpb.BroadcastWatchResponse))
	})
	return _c
}

func (_c *MockStreamingCoordBroadcastService_WatchServer_Send_Call) Return(_a0 error) *MockStreamingCoordBroadcastService_WatchServer_Send_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockStreamingCoordBroadcastService_WatchServer_Send_Call) RunAndReturn(run func(*streamingpb.BroadcastWatchResponse) error) *MockStreamingCoordBroadcastService_WatchServer_Send_Call {
	_c.Call.Return(run)
	return _c
}

// SendHeader provides a mock function with given fields: _a0
func (_m *MockStreamingCoordBroadcastService_WatchServer) SendHeader(_a0 metadata.MD) error {
	ret := _m.Called(_a0)

	if len(ret) == 0 {
		panic("no return value specified for SendHeader")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(metadata.MD) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockStreamingCoordBroadcastService_WatchServer_SendHeader_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SendHeader'
type MockStreamingCoordBroadcastService_WatchServer_SendHeader_Call struct {
	*mock.Call
}

// SendHeader is a helper method to define mock.On call
//   - _a0 metadata.MD
func (_e *MockStreamingCoordBroadcastService_WatchServer_Expecter) SendHeader(_a0 interface{}) *MockStreamingCoordBroadcastService_WatchServer_SendHeader_Call {
	return &MockStreamingCoordBroadcastService_WatchServer_SendHeader_Call{Call: _e.mock.On("SendHeader", _a0)}
}

func (_c *MockStreamingCoordBroadcastService_WatchServer_SendHeader_Call) Run(run func(_a0 metadata.MD)) *MockStreamingCoordBroadcastService_WatchServer_SendHeader_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(metadata.MD))
	})
	return _c
}

func (_c *MockStreamingCoordBroadcastService_WatchServer_SendHeader_Call) Return(_a0 error) *MockStreamingCoordBroadcastService_WatchServer_SendHeader_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockStreamingCoordBroadcastService_WatchServer_SendHeader_Call) RunAndReturn(run func(metadata.MD) error) *MockStreamingCoordBroadcastService_WatchServer_SendHeader_Call {
	_c.Call.Return(run)
	return _c
}

// SendMsg provides a mock function with given fields: m
func (_m *MockStreamingCoordBroadcastService_WatchServer) SendMsg(m interface{}) error {
	ret := _m.Called(m)

	if len(ret) == 0 {
		panic("no return value specified for SendMsg")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}) error); ok {
		r0 = rf(m)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockStreamingCoordBroadcastService_WatchServer_SendMsg_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SendMsg'
type MockStreamingCoordBroadcastService_WatchServer_SendMsg_Call struct {
	*mock.Call
}

// SendMsg is a helper method to define mock.On call
//   - m interface{}
func (_e *MockStreamingCoordBroadcastService_WatchServer_Expecter) SendMsg(m interface{}) *MockStreamingCoordBroadcastService_WatchServer_SendMsg_Call {
	return &MockStreamingCoordBroadcastService_WatchServer_SendMsg_Call{Call: _e.mock.On("SendMsg", m)}
}

func (_c *MockStreamingCoordBroadcastService_WatchServer_SendMsg_Call) Run(run func(m interface{})) *MockStreamingCoordBroadcastService_WatchServer_SendMsg_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(interface{}))
	})
	return _c
}

func (_c *MockStreamingCoordBroadcastService_WatchServer_SendMsg_Call) Return(_a0 error) *MockStreamingCoordBroadcastService_WatchServer_SendMsg_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockStreamingCoordBroadcastService_WatchServer_SendMsg_Call) RunAndReturn(run func(interface{}) error) *MockStreamingCoordBroadcastService_WatchServer_SendMsg_Call {
	_c.Call.Return(run)
	return _c
}

// SetHeader provides a mock function with given fields: _a0
func (_m *MockStreamingCoordBroadcastService_WatchServer) SetHeader(_a0 metadata.MD) error {
	ret := _m.Called(_a0)

	if len(ret) == 0 {
		panic("no return value specified for SetHeader")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(metadata.MD) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockStreamingCoordBroadcastService_WatchServer_SetHeader_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetHeader'
type MockStreamingCoordBroadcastService_WatchServer_SetHeader_Call struct {
	*mock.Call
}

// SetHeader is a helper method to define mock.On call
//   - _a0 metadata.MD
func (_e *MockStreamingCoordBroadcastService_WatchServer_Expecter) SetHeader(_a0 interface{}) *MockStreamingCoordBroadcastService_WatchServer_SetHeader_Call {
	return &MockStreamingCoordBroadcastService_WatchServer_SetHeader_Call{Call: _e.mock.On("SetHeader", _a0)}
}

func (_c *MockStreamingCoordBroadcastService_WatchServer_SetHeader_Call) Run(run func(_a0 metadata.MD)) *MockStreamingCoordBroadcastService_WatchServer_SetHeader_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(metadata.MD))
	})
	return _c
}

func (_c *MockStreamingCoordBroadcastService_WatchServer_SetHeader_Call) Return(_a0 error) *MockStreamingCoordBroadcastService_WatchServer_SetHeader_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockStreamingCoordBroadcastService_WatchServer_SetHeader_Call) RunAndReturn(run func(metadata.MD) error) *MockStreamingCoordBroadcastService_WatchServer_SetHeader_Call {
	_c.Call.Return(run)
	return _c
}

// SetTrailer provides a mock function with given fields: _a0
func (_m *MockStreamingCoordBroadcastService_WatchServer) SetTrailer(_a0 metadata.MD) {
	_m.Called(_a0)
}

// MockStreamingCoordBroadcastService_WatchServer_SetTrailer_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetTrailer'
type MockStreamingCoordBroadcastService_WatchServer_SetTrailer_Call struct {
	*mock.Call
}

// SetTrailer is a helper method to define mock.On call
//   - _a0 metadata.MD
func (_e *MockStreamingCoordBroadcastService_WatchServer_Expecter) SetTrailer(_a0 interface{}) *MockStreamingCoordBroadcastService_WatchServer_SetTrailer_Call {
	return &MockStreamingCoordBroadcastService_WatchServer_SetTrailer_Call{Call: _e.mock.On("SetTrailer", _a0)}
}

func (_c *MockStreamingCoordBroadcastService_WatchServer_SetTrailer_Call) Run(run func(_a0 metadata.MD)) *MockStreamingCoordBroadcastService_WatchServer_SetTrailer_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(metadata.MD))
	})
	return _c
}

func (_c *MockStreamingCoordBroadcastService_WatchServer_SetTrailer_Call) Return() *MockStreamingCoordBroadcastService_WatchServer_SetTrailer_Call {
	_c.Call.Return()
	return _c
}

func (_c *MockStreamingCoordBroadcastService_WatchServer_SetTrailer_Call) RunAndReturn(run func(metadata.MD)) *MockStreamingCoordBroadcastService_WatchServer_SetTrailer_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockStreamingCoordBroadcastService_WatchServer creates a new instance of MockStreamingCoordBroadcastService_WatchServer. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockStreamingCoordBroadcastService_WatchServer(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockStreamingCoordBroadcastService_WatchServer {
	mock := &MockStreamingCoordBroadcastService_WatchServer{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
