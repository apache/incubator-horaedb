/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/incubator-horaedb-meta/pkg/coderr"
	"github.com/apache/incubator-horaedb-meta/pkg/log"
	"github.com/apache/incubator-horaedb-meta/server/cluster"
	"github.com/apache/incubator-horaedb-meta/server/cluster/metadata"
	"github.com/apache/incubator-horaedb-meta/server/config"
	"github.com/apache/incubator-horaedb-meta/server/etcdutil"
	"github.com/apache/incubator-horaedb-meta/server/limiter"
	"github.com/apache/incubator-horaedb-meta/server/member"
	metagrpc "github.com/apache/incubator-horaedb-meta/server/service/grpc"
	"github.com/apache/incubator-horaedb-meta/server/service/http"
	"github.com/apache/incubator-horaedb-meta/server/status"
	"github.com/apache/incubator-horaedb-meta/server/storage"
	"github.com/apache/incubator-horaedb-proto/golang/pkg/metaservicepb"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type Server struct {
	isClosed int32
	status   *status.ServerStatus

	cfg *config.Config

	etcdCfg *embed.Config

	// The fields below are initialized after Run of server is called.
	clusterManager cluster.Manager
	flowLimiter    *limiter.FlowLimiter

	// member describes membership in horaemeta cluster.
	member  *member.Member
	etcdCli *clientv3.Client
	etcdSrv *embed.Etcd

	// httpService contains http server and api set.
	httpService *http.Service

	// bgJobWg can be used to join with the background jobs.
	bgJobWg sync.WaitGroup
	// bgJobCancel can be used to cancel all pending background jobs.
	bgJobCancel func()
}

// CreateServer creates the server instance without starting any services or background jobs.
func CreateServer(cfg *config.Config) (*Server, error) {
	etcdCfg, err := cfg.GenEtcdConfig()
	if err != nil {
		return nil, err
	}

	srv := &Server{
		isClosed: 0,
		status:   status.NewServerStatus(),
		cfg:      cfg,
		etcdCfg:  etcdCfg,

		clusterManager: nil,
		flowLimiter:    nil,
		member:         nil,
		etcdCli:        nil,
		etcdSrv:        nil,
		httpService:    nil,
		bgJobWg:        sync.WaitGroup{},
		bgJobCancel:    nil,
	}

	grpcService := metagrpc.NewService(cfg.GrpcHandleTimeout(), srv)
	etcdCfg.ServiceRegister = func(grpcSrv *grpc.Server) {
		grpcSrv.RegisterService(&metaservicepb.MetaRpcService_ServiceDesc, grpcService)
	}

	return srv, nil
}

// Run runs the services and background jobs.
func (srv *Server) Run(ctx context.Context) error {
	// If enableEmbedEtcd is true, the grpc server is started in the same process as the etcd server.
	if srv.cfg.EnableEmbedEtcd {
		if err := srv.startEmbedEtcd(ctx); err != nil {
			srv.status.Set(status.Terminated)
			return err
		}
	} else {
		// If enableEmbedEtcd is false, the grpc server is started in a separate process.
		go func() {
			if err := srv.startGrpcServer(ctx); err != nil {
				srv.status.Set(status.Terminated)
				log.Fatal("Grpc serve failed", zap.Error(err))
			}
		}()
	}
	if err := srv.initEtcdClient(srv.cfg.EnableEmbedEtcd); err != nil {
		srv.status.Set(status.Terminated)
		return err
	}

	if err := srv.startServer(ctx); err != nil {
		srv.status.Set(status.Terminated)
		return err
	}

	srv.startBgJobs(ctx)
	srv.status.Set(status.StatusRunning)
	return nil
}

func (srv *Server) Close() {
	atomic.StoreInt32(&srv.isClosed, 1)

	srv.stopBgJobs()

	if srv.etcdCli != nil {
		err := srv.etcdCli.Close()
		if err != nil {
			log.Error("fail to close etcdCli", zap.Error(err))
		}
	}

	err := srv.httpService.Stop()
	if err != nil {
		log.Error("fail to close http server", zap.Error(err))
	}
}

func (srv *Server) IsClosed() bool {
	return atomic.LoadInt32(&srv.isClosed) == 1
}

func (srv *Server) startEmbedEtcd(ctx context.Context) error {
	etcdSrv, err := embed.StartEtcd(srv.etcdCfg)
	if err != nil {
		return ErrStartEtcd.WithCause(err)
	}

	newCtx, cancel := context.WithTimeout(ctx, srv.cfg.EtcdStartTimeout())
	defer cancel()

	select {
	case <-etcdSrv.Server.ReadyNotify():
	case <-newCtx.Done():
		return ErrStartEtcdTimeout.WithMessagef("timeout is:%v", srv.cfg.EtcdStartTimeout())
	}
	srv.etcdSrv = etcdSrv

	return nil
}

func (srv *Server) initEtcdClient(enableEmbedEtcd bool) error {
	// If enableEmbedEtcd is false, we should add tls config to connect remote Etcd server.
	var tlsConfig *tls.Config
	if !enableEmbedEtcd {
		tlsInfo := transport.TLSInfo{
			TrustedCAFile: srv.cfg.EtcdCaCertPath,
			CertFile:      srv.cfg.EtcdCertPath,
			KeyFile:       srv.cfg.EtcdKeyPath,
		}
		clientConfig, err := tlsInfo.ClientConfig()
		if err != nil {
			return ErrCreateEtcdClient.WithCause(err)
		}
		tlsConfig = clientConfig
	}

	etcdEndpoints := make([]string, 0, len(srv.etcdCfg.AdvertiseClientUrls))
	for _, url := range srv.etcdCfg.AdvertiseClientUrls {
		etcdEndpoints = append(etcdEndpoints, url.String())
	}
	lgc := log.GetLoggerConfig()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: srv.cfg.EtcdCallTimeout(),
		LogConfig:   lgc,
		TLS:         tlsConfig,
	})
	if err != nil {
		return ErrCreateEtcdClient.WithCause(err)
	}
	srv.etcdCli = client

	if srv.etcdSrv != nil {
		etcdLeaderGetter := &etcdutil.LeaderGetterWrapper{Server: srv.etcdSrv.Server}
		srv.member = member.NewMember(srv.cfg.StorageRootPath, uint64(srv.etcdSrv.Server.ID()), srv.cfg.NodeName, srv.etcdCfg.AdvertiseClientUrls[0].String(), client, etcdLeaderGetter, srv.cfg.EtcdCallTimeout())
	} else {
		endpoint := fmt.Sprintf("http://%s:%d", srv.cfg.Addr, srv.cfg.GrpcPort)
		srv.member = member.NewMember(srv.cfg.StorageRootPath, 0, srv.cfg.NodeName, endpoint, client, nil, srv.cfg.EtcdCallTimeout())
	}
	return nil
}

func (srv *Server) startGrpcServer(_ context.Context) error {
	opts := srv.buildGrpcOptions()
	server := grpc.NewServer(opts...)

	grpcService := metagrpc.NewService(srv.cfg.GrpcHandleTimeout(), srv)
	server.RegisterService(&metaservicepb.MetaRpcService_ServiceDesc, grpcService)
	addr := fmt.Sprintf(":%d", srv.cfg.GrpcPort)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return errors.Wrapf(err, "listen on %s failed", addr)
	}

	if err = server.Serve(lis); err != nil {
		return errors.Wrap(err, "serve failed")
	}

	return nil
}

// startServer starts involved services.
func (srv *Server) startServer(_ context.Context) error {
	if srv.cfg.MaxScanLimit <= 1 {
		return ErrStartServer.WithMessagef("scan limit must be greater than 1")
	}

	storage := storage.NewStorageWithEtcdBackend(srv.etcdCli, srv.cfg.StorageRootPath,
		storage.Options{
			MaxScanLimit: srv.cfg.MaxScanLimit,
			MinScanLimit: srv.cfg.MinScanLimit,
			MaxOpsPerTxn: srv.cfg.MaxOpsPerTxn,
		})

	topologyType, err := metadata.ParseTopologyType(srv.cfg.TopologyType)
	if err != nil {
		return err
	}

	manager, err := cluster.NewManagerImpl(storage, srv.etcdCli, srv.etcdCli, srv.cfg.StorageRootPath, srv.cfg.IDAllocatorStep, topologyType)
	if err != nil {
		return err
	}
	srv.clusterManager = manager
	srv.flowLimiter = limiter.NewFlowLimiter(srv.cfg.FlowLimiter)

	api := http.NewAPI(manager, srv.status, http.NewForwardClient(srv.member, srv.cfg.HTTPPort), srv.flowLimiter, srv.etcdCli)
	httpService := http.NewHTTPService(srv.cfg.HTTPPort, time.Second*10, time.Second*10, api.NewAPIRouter())
	go func() {
		err := httpService.Start()
		if err != nil {
			log.Error("start http service failed", zap.Error(err))
		}
	}()
	srv.httpService = httpService

	log.Info("server started")
	return nil
}

func (srv *Server) startBgJobs(ctx context.Context) {
	var bgJobCtx context.Context
	bgJobCtx, srv.bgJobCancel = context.WithCancel(ctx)

	go srv.watchLeader(bgJobCtx)
	go srv.watchEtcdLeaderPriority(bgJobCtx)
}

func (srv *Server) stopBgJobs() {
	srv.bgJobCancel()
	srv.bgJobWg.Wait()
}

// watchLeader watches whether the leader of the cluster exists.
// Every node campaigns the leadership if it finds the leader is offline and the leader should keep the leadership after
// election. And Keep the leader node also be the leader of the etcd cluster during election.
func (srv *Server) watchLeader(ctx context.Context) {
	srv.bgJobWg.Add(1)
	defer srv.bgJobWg.Done()

	watchCtx := &leaderWatchContext{
		srv,
	}
	// If enable embed etcd, we should watch the leader of the etcd cluster.
	watcher := member.NewLeaderWatcher(watchCtx, srv.member, srv.cfg.LeaseTTLSec, srv.cfg.EnableEmbedEtcd)

	callbacks := &leadershipEventCallbacks{
		srv: srv,
	}
	watcher.Watch(ctx, callbacks)
}

func (srv *Server) watchEtcdLeaderPriority(_ context.Context) {
	srv.bgJobWg.Add(1)
	defer srv.bgJobWg.Done()
}

func (srv *Server) createDefaultCluster(ctx context.Context) error {
	resp, err := srv.member.GetLeaderAddr(ctx)
	if err != nil {
		log.Warn("get leader failed", zap.Error(err))
	}

	// Create default cluster by the leader.
	if resp.IsLocal {
		topologyType, err := metadata.ParseTopologyType(srv.cfg.TopologyType)
		if err != nil {
			return err
		}
		defaultCluster, err := srv.clusterManager.CreateCluster(ctx, srv.cfg.DefaultClusterName,
			metadata.CreateClusterOpts{
				NodeCount:                   uint32(srv.cfg.DefaultClusterNodeCount),
				ShardTotal:                  uint32(srv.cfg.DefaultClusterShardTotal),
				EnableSchedule:              srv.cfg.EnableSchedule,
				TopologyType:                topologyType,
				ProcedureExecutingBatchSize: srv.cfg.ProcedureExecutingBatchSize,
			})
		if err != nil {
			log.Warn("create default cluster failed", zap.Error(err))
			if coderr.Is(err, metadata.ErrClusterAlreadyExists.Code()) {
				_, err = srv.clusterManager.GetCluster(ctx, srv.cfg.DefaultClusterName)
				if err != nil {
					return errors.WithMessage(err, "get default cluster failed")
				}
			}
		} else {
			log.Info("create default cluster succeed", zap.String("cluster", defaultCluster.GetMetadata().Name()))
		}
	}
	return nil
}

func (srv *Server) buildGrpcOptions() []grpc.ServerOption {
	keepalivePolicy := keepalive.EnforcementPolicy{
		MinTime:             time.Duration(srv.cfg.GrpcServiceKeepAlivePingMinIntervalSec) * time.Second,
		PermitWithoutStream: true,
	}
	opts := []grpc.ServerOption{
		grpc.MaxSendMsgSize(srv.cfg.GrpcServiceMaxSendMsgSize),
		grpc.MaxRecvMsgSize(srv.cfg.GrpcServiceMaxSendMsgSize),
		grpc.KeepaliveEnforcementPolicy(keepalivePolicy),
	}
	return opts
}

type leaderWatchContext struct {
	srv *Server
}

func (ctx *leaderWatchContext) ShouldStop() bool {
	return ctx.srv.IsClosed()
}

func (ctx *leaderWatchContext) EtcdLeaderID() (uint64, error) {
	if ctx.srv.etcdSrv != nil {
		return ctx.srv.etcdSrv.Server.Lead(), nil
	}
	return 0, member.ErrGetLeader.WithMessagef("etcd server is not set")
}

func (srv *Server) GetClusterManager() cluster.Manager {
	return srv.clusterManager
}

func (srv *Server) GetLeader(ctx context.Context) (member.GetLeaderAddrResp, error) {
	// Get leader with cache.
	return srv.member.GetLeaderAddr(ctx)
}

func (srv *Server) GetFlowLimiter() (*limiter.FlowLimiter, error) {
	if srv.flowLimiter == nil {
		return nil, ErrFlowLimiterNotFound.WithMessagef("")
	}
	return srv.flowLimiter, nil
}

type leadershipEventCallbacks struct {
	srv *Server
}

func (c *leadershipEventCallbacks) AfterElected(ctx context.Context) {
	if err := c.srv.clusterManager.Start(ctx); err != nil {
		panic(fmt.Sprintf("cluster manager fail to start, err:%v", err))
	}
	if err := c.srv.createDefaultCluster(ctx); err != nil {
		panic(fmt.Sprintf("create default cluster failed, err:%v", err))
	}
}

func (c *leadershipEventCallbacks) BeforeTransfer(ctx context.Context) {
	if err := c.srv.clusterManager.Stop(ctx); err != nil {
		panic(fmt.Sprintf("cluster manager fail to stop, err:%v", err))
	}
}
