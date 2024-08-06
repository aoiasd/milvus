// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.33.0
// 	protoc        v3.21.4
// source: backup_header.proto

package backend

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type BackupHeader struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Version number for backup format
	Version int32 `protobuf:"varint,1,opt,name=version,proto3" json:"version,omitempty"`
	// instance name, as rootPath for key prefix
	Instance string `protobuf:"bytes,2,opt,name=instance,proto3" json:"instance,omitempty"`
	// MetaPath used in keys
	MetaPath string `protobuf:"bytes,3,opt,name=meta_path,json=metaPath,proto3" json:"meta_path,omitempty"`
	// Entries record number of key-value in backup
	Entries int64 `protobuf:"varint,4,opt,name=entries,proto3" json:"entries,omitempty"`
	// Component is the backup target
	Component string `protobuf:"bytes,5,opt,name=component,proto3" json:"component,omitempty"`
	// Extra property reserved
	Extra []byte `protobuf:"bytes,6,opt,name=extra,proto3" json:"extra,omitempty"`
}

func (x *BackupHeader) Reset() {
	*x = BackupHeader{}
	if protoimpl.UnsafeEnabled {
		mi := &file_backup_header_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BackupHeader) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BackupHeader) ProtoMessage() {}

func (x *BackupHeader) ProtoReflect() protoreflect.Message {
	mi := &file_backup_header_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BackupHeader.ProtoReflect.Descriptor instead.
func (*BackupHeader) Descriptor() ([]byte, []int) {
	return file_backup_header_proto_rawDescGZIP(), []int{0}
}

func (x *BackupHeader) GetVersion() int32 {
	if x != nil {
		return x.Version
	}
	return 0
}

func (x *BackupHeader) GetInstance() string {
	if x != nil {
		return x.Instance
	}
	return ""
}

func (x *BackupHeader) GetMetaPath() string {
	if x != nil {
		return x.MetaPath
	}
	return ""
}

func (x *BackupHeader) GetEntries() int64 {
	if x != nil {
		return x.Entries
	}
	return 0
}

func (x *BackupHeader) GetComponent() string {
	if x != nil {
		return x.Component
	}
	return ""
}

func (x *BackupHeader) GetExtra() []byte {
	if x != nil {
		return x.Extra
	}
	return nil
}

var File_backup_header_proto protoreflect.FileDescriptor

var file_backup_header_proto_rawDesc = []byte{
	0x0a, 0x13, 0x62, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x5f, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x28, 0x6d, 0x69, 0x6c, 0x76, 0x75, 0x73, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x2e, 0x63, 0x6d, 0x64, 0x2e, 0x74, 0x6f, 0x6f, 0x6c, 0x73, 0x2e, 0x6d, 0x69,
	0x67, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x62, 0x61, 0x63, 0x6b, 0x65, 0x6e, 0x64, 0x22,
	0xaf, 0x01, 0x0a, 0x0c, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x48, 0x65, 0x61, 0x64, 0x65, 0x72,
	0x12, 0x18, 0x0a, 0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x05, 0x52, 0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x1a, 0x0a, 0x08, 0x69, 0x6e,
	0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x69, 0x6e,
	0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x12, 0x1b, 0x0a, 0x09, 0x6d, 0x65, 0x74, 0x61, 0x5f, 0x70,
	0x61, 0x74, 0x68, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x6d, 0x65, 0x74, 0x61, 0x50,
	0x61, 0x74, 0x68, 0x12, 0x18, 0x0a, 0x07, 0x65, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x18, 0x04,
	0x20, 0x01, 0x28, 0x03, 0x52, 0x07, 0x65, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x12, 0x1c, 0x0a,
	0x09, 0x63, 0x6f, 0x6d, 0x70, 0x6f, 0x6e, 0x65, 0x6e, 0x74, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x09, 0x63, 0x6f, 0x6d, 0x70, 0x6f, 0x6e, 0x65, 0x6e, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x65,
	0x78, 0x74, 0x72, 0x61, 0x18, 0x06, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x05, 0x65, 0x78, 0x74, 0x72,
	0x61, 0x42, 0x48, 0x5a, 0x46, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f,
	0x6d, 0x69, 0x6c, 0x76, 0x75, 0x73, 0x2d, 0x69, 0x6f, 0x2f, 0x6d, 0x69, 0x6c, 0x76, 0x75, 0x73,
	0x2f, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f,
	0x63, 0x6d, 0x64, 0x2f, 0x74, 0x6f, 0x6f, 0x6c, 0x73, 0x2f, 0x6d, 0x69, 0x67, 0x72, 0x61, 0x74,
	0x69, 0x6f, 0x6e, 0x2f, 0x62, 0x61, 0x63, 0x6b, 0x65, 0x6e, 0x64, 0x62, 0x06, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x33,
}

var (
	file_backup_header_proto_rawDescOnce sync.Once
	file_backup_header_proto_rawDescData = file_backup_header_proto_rawDesc
)

func file_backup_header_proto_rawDescGZIP() []byte {
	file_backup_header_proto_rawDescOnce.Do(func() {
		file_backup_header_proto_rawDescData = protoimpl.X.CompressGZIP(file_backup_header_proto_rawDescData)
	})
	return file_backup_header_proto_rawDescData
}

var file_backup_header_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_backup_header_proto_goTypes = []interface{}{
	(*BackupHeader)(nil), // 0: milvus.proto.cmd.tools.migration.backend.BackupHeader
}
var file_backup_header_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_backup_header_proto_init() }
func file_backup_header_proto_init() {
	if File_backup_header_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_backup_header_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BackupHeader); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_backup_header_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_backup_header_proto_goTypes,
		DependencyIndexes: file_backup_header_proto_depIdxs,
		MessageInfos:      file_backup_header_proto_msgTypes,
	}.Build()
	File_backup_header_proto = out.File
	file_backup_header_proto_rawDesc = nil
	file_backup_header_proto_goTypes = nil
	file_backup_header_proto_depIdxs = nil
}