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
	"time"

	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	// TODO: better to be configured
	nodeCtxTtInterval  = 2 * time.Minute
	enableTtChecker    = true
	loadTypeCollection = querypb.LoadType_LoadCollection
	loadTypePartition  = querypb.LoadType_LoadPartition
)

type (
	// UniqueID is an identifier that is guaranteed to be unique among all the collections, partitions and segments
	UniqueID = typeutil.UniqueID
	// Channel is the virtual channel
	Channel = string
	// Timestamp is timestamp
	Timestamp = typeutil.Timestamp

	loadType = querypb.LoadType

	InsertMsg = msgstream.InsertMsg
	DeleteMsg = msgstream.DeleteMsg
)

type TimeRange struct {
	timestampMin Timestamp
	timestampMax Timestamp
}
