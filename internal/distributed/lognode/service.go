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

package grpclognode

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	rcc "github.com/milvus-io/milvus/internal/distributed/rootcoord/client"
	ln "github.com/milvus-io/milvus/internal/lognode"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/componentutil"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/tracer"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/interceptor"
	"github.com/milvus-io/milvus/pkg/util/logutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Server is the grpc server of lognode.
type Server struct {
	// server
	serverID atomic.Int64
	factory  dependency.Factory
	lognode  types.LogNodeComponent

	// rpc
	grpcServer  *grpc.Server
	grpcErrChan chan error

	// external component client
	etcdCli *clientv3.Client

	// component client
	rootCoord types.RootCoordClient

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// NewServer create a new QueryCoord grpc server.
func NewServer(ctx context.Context, factory dependency.Factory) (*Server, error) {
	ctx, cancel := context.WithCancel(ctx)
	svr := ln.NewLogNode(ctx, factory)

	return &Server{
		lognode:     svr,
		ctx:         ctx,
		cancel:      cancel,
		factory:     factory,
		grpcErrChan: make(chan error),
	}, nil
}

func (s *Server) initRootCoord() error {
	var err error
	if s.rootCoord == nil {
		s.rootCoord, err = rcc.NewClient(s.ctx, paramtable.Get().EtcdCfg.MetaRootPath.GetValue(), s.etcdCli)
		if err != nil {
			log.Error("QueryCoord try to new RootCoord client failed", zap.Error(err))
			return err
		}
	}

	log.Debug("QueryCoord try to wait for RootCoord ready")
	err = componentutil.WaitForComponentHealthy(s.ctx, s.rootCoord, "RootCoord", 1000000, time.Millisecond*200)
	if err != nil {
		log.Error("QueryCoord wait for RootCoord ready failed", zap.Error(err))
		panic(err)
	}

	err = s.lognode.SetRootCoord(s.rootCoord)
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) initEtcd(params *paramtable.ComponentParam) error {
	etcdConfig := &params.EtcdCfg
	etcdCli, err := etcd.GetEtcdClient(
		etcdConfig.UseEmbedEtcd.GetAsBool(),
		etcdConfig.EtcdUseSSL.GetAsBool(),
		etcdConfig.Endpoints.GetAsStrings(),
		etcdConfig.EtcdTLSCert.GetValue(),
		etcdConfig.EtcdTLSKey.GetValue(),
		etcdConfig.EtcdTLSCACert.GetValue(),
		etcdConfig.EtcdTLSMinVersion.GetValue())
	if err != nil {
		log.Debug("QueryCoord connect to etcd failed", zap.Error(err))
		return err
	}

	s.etcdCli = etcdCli
	s.lognode.SetEtcdClient(etcdCli)
	return nil
}

func (s *Server) initRpc(params *paramtable.ComponentParam) error {
	rpcParams := &params.LogNodeGrpcServerCfg

	s.wg.Add(1)
	go s.startGrpcLoop(rpcParams.Port.GetAsInt())
	// wait for grpc server loop start
	err := <-s.grpcErrChan
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) startGrpcLoop(grpcPort int) {
	defer s.wg.Done()
	Params := &paramtable.Get().LogNodeGrpcServerCfg
	var kaep = keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}

	var kasp = keepalive.ServerParameters{
		Time:    60 * time.Second, // Ping the client if it is idle for 60 seconds to ensure the connection is still active
		Timeout: 10 * time.Second, // Wait 10 second for the ping ack before assuming the connection is dead
	}
	log.Debug("network", zap.String("port", strconv.Itoa(grpcPort)))

	var lis net.Listener
	var err error
	err = retry.Do(s.ctx, func() error {
		addr := ":" + strconv.Itoa(grpcPort)
		lis, err = net.Listen("tcp", addr)
		if err == nil {
			s.lognode.SetAddress(fmt.Sprintf("%s:%d", Params.IP, lis.Addr().(*net.TCPAddr).Port))
		} else {
			// set port=0 to get next available port
			grpcPort = 0
		}
		return err
	}, retry.Attempts(10))

	if err != nil {
		log.Error("QueryNode GrpcServer:failed to listen", zap.Error(err))
		s.grpcErrChan <- err
		return
	}

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	opts := tracer.GetInterceptorOpts()
	s.grpcServer = grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
		grpc.MaxRecvMsgSize(Params.ServerMaxRecvSize.GetAsInt()),
		grpc.MaxSendMsgSize(Params.ServerMaxSendSize.GetAsInt()),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			otelgrpc.UnaryServerInterceptor(opts...),
			logutil.UnaryTraceLoggerInterceptor,
			interceptor.ClusterValidationUnaryServerInterceptor(),
			interceptor.ServerIDValidationUnaryServerInterceptor(func() int64 {
				if s.serverID.Load() == 0 {
					s.serverID.Store(paramtable.GetNodeID())
				}
				return s.serverID.Load()
			}),
		)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			otelgrpc.StreamServerInterceptor(opts...),
			logutil.StreamTraceLoggerInterceptor,
			interceptor.ClusterValidationStreamServerInterceptor(),
			interceptor.ServerIDValidationStreamServerInterceptor(func() int64 {
				if s.serverID.Load() == 0 {
					s.serverID.Store(paramtable.GetNodeID())
				}
				return s.serverID.Load()
			}),
		)))
	logpb.RegisterLogNodeServer(s.grpcServer, s)

	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErrChan <- err
	}
}

func (s *Server) init() error {
	params := paramtable.Get()

	if err := s.initEtcd(params); err != nil {
		return err
	}

	if err := s.initRpc(params); err != nil {
		return err
	}

	if err := s.initRootCoord(); err != nil {
		return err
	}

	if err := s.lognode.Init(); err != nil {
		return err
	}
	return nil
}

func (s *Server) start() error {
	if err := s.lognode.Register(); err != nil {
		return err
	}

	return s.lognode.Start()
}

func (s *Server) Run() error {
	if err := s.init(); err != nil {
		return err
	}
	log.Debug("lognode init done ...")

	if err := s.start(); err != nil {
		return err
	}
	log.Debug("lognode start done ...")
	return nil
}

func (s *Server) Stop() error {
	Params := &paramtable.Get().QueryCoordGrpcServerCfg
	log.Debug("lognode stop", zap.String("Address", Params.GetAddress()))
	if s.etcdCli != nil {
		defer s.etcdCli.Close()
	}
	err := s.lognode.Stop()
	s.cancel()
	if s.grpcServer != nil {
		log.Debug("Graceful stop grpc server...")
		s.grpcServer.GracefulStop()
	}
	return err
}

// RPC Method
func (s *Server) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	return s.lognode.GetComponentStates(ctx, req)
}

func (s *Server) WatchChannel(ctx context.Context, req *logpb.WatchChannelRequest) (*commonpb.Status, error) {
	return s.lognode.WatchChannel(ctx, req)
}

func (s *Server) Insert(ctx context.Context, req *logpb.InsertRequest) (*commonpb.Status, error) {
	return s.lognode.Insert(ctx, req)
}
