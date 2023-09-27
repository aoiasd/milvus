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

package grpcindexnodeclient

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"google.golang.org/grpc"
)

// Client is the grpc client of LogNode.
type Client struct {
	grpcClient grpcclient.GrpcClient[logpb.LogNodeClient]
	addr       string
}

func NewClient(ctx context.Context, addr string, nodeID int64) (*Client, error) {
	if addr == "" {
		return nil, fmt.Errorf("addr is empty")
	}
	config := &paramtable.Get().LogNodeGrpcClientCfg
	client := &Client{
		addr:       addr,
		grpcClient: grpcclient.NewClientBase[logpb.LogNodeClient](config, "milvus.proto.query.LogNode"),
	}
	// node shall specify node id
	client.grpcClient.SetRole(fmt.Sprintf("%s-%d", typeutil.LogNodeRole, nodeID))
	client.grpcClient.SetGetAddrFunc(client.getAddr)
	client.grpcClient.SetNewGrpcClientFunc(client.newGrpcClient)
	client.grpcClient.SetNodeID(nodeID)

	return client, nil
}

// Close close QueryNode's grpc client
func (c *Client) Close() error {
	return c.grpcClient.Close()
}

func (c *Client) newGrpcClient(cc *grpc.ClientConn) logpb.LogNodeClient {
	return logpb.NewLogNodeClient(cc)
}

func (c *Client) getAddr() (string, error) {
	return c.addr, nil
}

func wrapGrpcCall[T any](ctx context.Context, c *Client, call func(grpcClient logpb.LogNodeClient) (*T, error)) (*T, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client logpb.LogNodeClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return call(client)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*T), err
}

// GetComponentStates gets the component states of LogNode.
func (c *Client) GetComponentStates(ctx context.Context, _ *milvuspb.GetComponentStatesRequest, _ ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	return wrapGrpcCall(ctx, c, func(client logpb.LogNodeClient) (*milvuspb.ComponentStates, error) {
		return client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	})
}

// WatchChannel watch channel
func (c *Client) WatchChannel(ctx context.Context, req *logpb.WatchChannelRequest, options ...grpc.CallOption) (*commonpb.Status, error) {
	return wrapGrpcCall(ctx, c, func(client logpb.LogNodeClient) (*commonpb.Status, error) {
		return client.WatchChannel(ctx, req, options...)
	})
}

func (c *Client) Insert(ctx context.Context, req *logpb.InsertRequest, options ...grpc.CallOption) (*commonpb.Status, error) {
	return wrapGrpcCall(ctx, c, func(client logpb.LogNodeClient) (*commonpb.Status, error) {
		return client.Insert(ctx, req, options...)
	})
}
