package shard

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
)

func NewInterceptorBuilder() interceptors.InterceptorBuilder {
	return &interceptorBuilder{}
}

type interceptorBuilder struct{}

func (b *interceptorBuilder) Build(param *interceptors.InterceptorBuildParam) interceptors.Interceptor {
	functionManager := newFunctionRunnerManager(param.ShardManager.Logger())
	shardInterceptor := &shardInterceptor{
		shardManager:    param.ShardManager,
		functionManager: functionManager,
	}
	if schemaProvider, ok := param.ShardManager.(collectionSchemaProvider); ok {
		for collectionID, schema := range schemaProvider.GetAllCollectionSchemas() {
			functionManager.Recover(collectionID, schema)
		}
	}
	shardInterceptor.initOpTable()
	return shardInterceptor
}
