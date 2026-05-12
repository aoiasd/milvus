package shard

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

type collectionSchemaProvider interface {
	GetAllCollectionSchemas() map[int64]*schemapb.CollectionSchema
}

type collectionSchemaGetter interface {
	GetCollectionSchema(collectionID int64, schemaVersion int32) (*schemapb.CollectionSchema, error)
}

func (impl *shardInterceptor) materializeFunctionFields(ctx context.Context, insertMsg message.MutableInsertMessageV1, collectionID int64, schemaVersion int32) error {
	schemaGetter, ok := impl.shardManager.(collectionSchemaGetter)
	if !ok {
		return nil
	}

	schema, err := schemaGetter.GetCollectionSchema(collectionID, schemaVersion)
	if err != nil {
		if errors.Is(err, shards.ErrCollectionSchemaNotFound) {
			return nil
		}
		return err
	}

	outputFieldIDs, err := function.EmbeddingOutputFieldIDs(schema)
	if err != nil {
		return err
	}
	if len(outputFieldIDs) == 0 {
		return nil
	}

	body := insertMsg.MustBody()
	if function.HasAllFieldDataByID(body.GetFieldsData(), outputFieldIDs) {
		return nil
	}

	runners, err := impl.functionManager.GetOrCreate(ctx, collectionID, schema)
	if err != nil {
		return err
	}
	changed, err := function.FillFunctionFields(runners, body)
	if err != nil {
		return err
	}
	if changed {
		insertMsg.OverwriteBody(body)
	}
	return nil
}
