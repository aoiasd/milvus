// Code generated by protoc-gen-go. DO NOT EDIT.
// source: log_coord.proto

package logpb

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	commonpb "github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	msgpb "github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type WatchChannelRequest struct {
	Base                 *commonpb.MsgBase `protobuf:"bytes,1,opt,name=base,proto3" json:"base,omitempty"`
	PChannel             string            `protobuf:"bytes,2,opt,name=pChannel,proto3" json:"pChannel,omitempty"`
	XXX_NoUnkeyedLiteral struct{}          `json:"-"`
	XXX_unrecognized     []byte            `json:"-"`
	XXX_sizecache        int32             `json:"-"`
}

func (m *WatchChannelRequest) Reset()         { *m = WatchChannelRequest{} }
func (m *WatchChannelRequest) String() string { return proto.CompactTextString(m) }
func (*WatchChannelRequest) ProtoMessage()    {}
func (*WatchChannelRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_5674412121b98d7f, []int{0}
}

func (m *WatchChannelRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_WatchChannelRequest.Unmarshal(m, b)
}
func (m *WatchChannelRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_WatchChannelRequest.Marshal(b, m, deterministic)
}
func (m *WatchChannelRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_WatchChannelRequest.Merge(m, src)
}
func (m *WatchChannelRequest) XXX_Size() int {
	return xxx_messageInfo_WatchChannelRequest.Size(m)
}
func (m *WatchChannelRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_WatchChannelRequest.DiscardUnknown(m)
}

var xxx_messageInfo_WatchChannelRequest proto.InternalMessageInfo

func (m *WatchChannelRequest) GetBase() *commonpb.MsgBase {
	if m != nil {
		return m.Base
	}
	return nil
}

func (m *WatchChannelRequest) GetPChannel() string {
	if m != nil {
		return m.PChannel
	}
	return ""
}

type InsertRequest struct {
	Base                 *commonpb.MsgBase    `protobuf:"bytes,1,opt,name=base,proto3" json:"base,omitempty"`
	Msg                  *msgpb.InsertRequest `protobuf:"bytes,2,opt,name=msg,proto3" json:"msg,omitempty"`
	VChannels            []string             `protobuf:"bytes,3,rep,name=vChannels,proto3" json:"vChannels,omitempty"`
	XXX_NoUnkeyedLiteral struct{}             `json:"-"`
	XXX_unrecognized     []byte               `json:"-"`
	XXX_sizecache        int32                `json:"-"`
}

func (m *InsertRequest) Reset()         { *m = InsertRequest{} }
func (m *InsertRequest) String() string { return proto.CompactTextString(m) }
func (*InsertRequest) ProtoMessage()    {}
func (*InsertRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_5674412121b98d7f, []int{1}
}

func (m *InsertRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_InsertRequest.Unmarshal(m, b)
}
func (m *InsertRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_InsertRequest.Marshal(b, m, deterministic)
}
func (m *InsertRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_InsertRequest.Merge(m, src)
}
func (m *InsertRequest) XXX_Size() int {
	return xxx_messageInfo_InsertRequest.Size(m)
}
func (m *InsertRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_InsertRequest.DiscardUnknown(m)
}

var xxx_messageInfo_InsertRequest proto.InternalMessageInfo

func (m *InsertRequest) GetBase() *commonpb.MsgBase {
	if m != nil {
		return m.Base
	}
	return nil
}

func (m *InsertRequest) GetMsg() *msgpb.InsertRequest {
	if m != nil {
		return m.Msg
	}
	return nil
}

func (m *InsertRequest) GetVChannels() []string {
	if m != nil {
		return m.VChannels
	}
	return nil
}

func init() {
	proto.RegisterType((*WatchChannelRequest)(nil), "milvus.proto.index.WatchChannelRequest")
	proto.RegisterType((*InsertRequest)(nil), "milvus.proto.index.InsertRequest")
}

func init() { proto.RegisterFile("log_coord.proto", fileDescriptor_5674412121b98d7f) }

var fileDescriptor_5674412121b98d7f = []byte{
	// 287 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x9c, 0x90, 0xd1, 0x4a, 0x84, 0x40,
	0x14, 0x86, 0x33, 0x63, 0xcb, 0xd9, 0x8d, 0x60, 0xba, 0x11, 0x5b, 0xc8, 0xbc, 0xc9, 0x9b, 0x74,
	0xb3, 0x37, 0xd8, 0xae, 0x8a, 0xea, 0xc2, 0xa0, 0xa0, 0x9b, 0x18, 0x75, 0x18, 0x05, 0x9d, 0x63,
	0x9e, 0x71, 0xe9, 0x39, 0x7a, 0x8f, 0xde, 0x31, 0x74, 0xa4, 0x92, 0x84, 0x60, 0xef, 0xce, 0x7f,
	0xe6, 0x3f, 0x1f, 0xff, 0xfc, 0xe4, 0xa8, 0x04, 0xf1, 0x9a, 0x02, 0x34, 0x59, 0x50, 0x37, 0xa0,
	0x80, 0xd2, 0xaa, 0x28, 0x37, 0x2d, 0x6a, 0x15, 0x14, 0x32, 0xe3, 0xef, 0xce, 0x22, 0x85, 0xaa,
	0x02, 0xa9, 0x77, 0x8e, 0x55, 0xa1, 0xd0, 0xa3, 0x97, 0x92, 0xe3, 0x67, 0xa6, 0xd2, 0xfc, 0x3a,
	0x67, 0x52, 0xf2, 0x32, 0xe6, 0x6f, 0x2d, 0x47, 0x45, 0x57, 0x64, 0x2f, 0x61, 0xc8, 0x6d, 0xc3,
	0x35, 0xfc, 0x79, 0xb4, 0x0c, 0x46, 0xc8, 0x81, 0x75, 0x8f, 0x62, 0xcd, 0x90, 0xc7, 0xbd, 0x93,
	0x3a, 0xe4, 0xa0, 0x1e, 0x20, 0xf6, 0xae, 0x6b, 0xf8, 0x56, 0xfc, 0xad, 0xbd, 0x0f, 0x83, 0x1c,
	0xde, 0x48, 0xe4, 0x8d, 0xda, 0x9e, 0x7f, 0x49, 0xcc, 0x0a, 0x45, 0x8f, 0x9e, 0x47, 0xa7, 0xe3,
	0x83, 0xee, 0x3b, 0x23, 0x7e, 0xdc, 0x79, 0xe9, 0x92, 0x58, 0x9b, 0x21, 0x02, 0xda, 0xa6, 0x6b,
	0xfa, 0x56, 0xfc, 0xb3, 0x88, 0x3e, 0x0d, 0xb2, 0x7f, 0x07, 0xe2, 0x01, 0x32, 0x4e, 0x9f, 0xc8,
	0xe2, 0x77, 0x0b, 0xf4, 0x3c, 0xf8, 0xdb, 0x61, 0x30, 0xd1, 0x93, 0x73, 0x32, 0x99, 0xfc, 0x51,
	0x31, 0xd5, 0xa2, 0xb7, 0x43, 0x6f, 0xc9, 0x4c, 0xe7, 0xa2, 0x67, 0x53, 0xc4, 0x51, 0xe6, 0x7f,
	0x58, 0xeb, 0xe8, 0x65, 0x25, 0x0a, 0x95, 0xb7, 0x49, 0xf7, 0x12, 0x6a, 0xeb, 0x45, 0x01, 0xc3,
	0x14, 0x16, 0x52, 0xf1, 0x46, 0xb2, 0x32, 0xec, 0xaf, 0xc3, 0x12, 0x44, 0x9d, 0x24, 0xb3, 0x5e,
	0x5c, 0x7d, 0x05, 0x00, 0x00, 0xff, 0xff, 0xb6, 0x09, 0x0b, 0xe5, 0x24, 0x02, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// LogNodeClient is the client API for LogNode service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type LogNodeClient interface {
	WatchChannel(ctx context.Context, in *WatchChannelRequest, opts ...grpc.CallOption) (*commonpb.Status, error)
	Insert(ctx context.Context, in *InsertRequest, opts ...grpc.CallOption) (*commonpb.Status, error)
}

type logNodeClient struct {
	cc *grpc.ClientConn
}

func NewLogNodeClient(cc *grpc.ClientConn) LogNodeClient {
	return &logNodeClient{cc}
}

func (c *logNodeClient) WatchChannel(ctx context.Context, in *WatchChannelRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	out := new(commonpb.Status)
	err := c.cc.Invoke(ctx, "/milvus.proto.index.LogNode/WatchChannel", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *logNodeClient) Insert(ctx context.Context, in *InsertRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	out := new(commonpb.Status)
	err := c.cc.Invoke(ctx, "/milvus.proto.index.LogNode/Insert", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// LogNodeServer is the server API for LogNode service.
type LogNodeServer interface {
	WatchChannel(context.Context, *WatchChannelRequest) (*commonpb.Status, error)
	Insert(context.Context, *InsertRequest) (*commonpb.Status, error)
}

// UnimplementedLogNodeServer can be embedded to have forward compatible implementations.
type UnimplementedLogNodeServer struct {
}

func (*UnimplementedLogNodeServer) WatchChannel(ctx context.Context, req *WatchChannelRequest) (*commonpb.Status, error) {
	return nil, status.Errorf(codes.Unimplemented, "method WatchChannel not implemented")
}
func (*UnimplementedLogNodeServer) Insert(ctx context.Context, req *InsertRequest) (*commonpb.Status, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Insert not implemented")
}

func RegisterLogNodeServer(s *grpc.Server, srv LogNodeServer) {
	s.RegisterService(&_LogNode_serviceDesc, srv)
}

func _LogNode_WatchChannel_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(WatchChannelRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LogNodeServer).WatchChannel(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/milvus.proto.index.LogNode/WatchChannel",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LogNodeServer).WatchChannel(ctx, req.(*WatchChannelRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _LogNode_Insert_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(InsertRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LogNodeServer).Insert(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/milvus.proto.index.LogNode/Insert",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LogNodeServer).Insert(ctx, req.(*InsertRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _LogNode_serviceDesc = grpc.ServiceDesc{
	ServiceName: "milvus.proto.index.LogNode",
	HandlerType: (*LogNodeServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "WatchChannel",
			Handler:    _LogNode_WatchChannel_Handler,
		},
		{
			MethodName: "Insert",
			Handler:    _LogNode_Insert_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "log_coord.proto",
}
