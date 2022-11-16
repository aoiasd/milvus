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

package querynodev2

import (
	"context"

	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/querynodev2"
	"github.com/milvus-io/milvus/internal/util/pipeline"
)

type Pipeline struct {
	*pipeline.StreamPipeline

	collectionID UniqueID
	channel      Channel
}

func NewPipeLine(ctx context.Context,
	collectionID UniqueID,
	channel Channel,
	manager Manager,
	tSatSafeReplica querynodev2.TSafeReplicaInterface,
	factory msgstream.Factory,
) (*Pipeline, error) {
	dmStream, err := factory.NewTtMsgStream(ctx)
	if err != nil {
		return nil, err
	}

	p := &Pipeline{
		collectionID:   collectionID,
		channel:        channel,
		StreamPipeline: pipeline.NewPipelineWithStream(dmStream, nodeCtxTtInterval, enableTtChecker),
	}

	return p, nil
}
