package writebuffer

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
)

type LobBuffer struct {
	BufferBase
	collSchema *schemapb.CollectionSchema

	buffers []map[storage.FieldID]storage.FieldData
	// writer
}
