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

package grpc

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/apache/incubator-horaedb-meta/pkg/coderr"
	"github.com/apache/incubator-horaedb-meta/pkg/log"
	"github.com/apache/incubator-horaedb-meta/server/cluster"
	"github.com/apache/incubator-horaedb-meta/server/cluster/metadata"
	"github.com/apache/incubator-horaedb-meta/server/coordinator"
	"github.com/apache/incubator-horaedb-meta/server/limiter"
	"github.com/apache/incubator-horaedb-meta/server/member"
	"github.com/apache/incubator-horaedb-meta/server/storage"
	"github.com/apache/incubator-horaedb-proto/golang/pkg/clusterpb"
	"github.com/apache/incubator-horaedb-proto/golang/pkg/commonpb"
	"github.com/apache/incubator-horaedb-proto/golang/pkg/metaservicepb"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Service struct {
	metaservicepb.UnimplementedMetaRpcServiceServer
	opTimeout time.Duration
	h         Handler

	// Store as map[string]*grpc.ClientConn
	// TODO: remove unavailable connection
	conns sync.Map
}

func NewService(opTimeout time.Duration, h Handler) *Service {
	return &Service{
		UnimplementedMetaRpcServiceServer: metaservicepb.UnimplementedMetaRpcServiceServer{},
		opTimeout:                         opTimeout,
		h:                                 h,
		conns:                             sync.Map{},
	}
}

// Handler is needed by grpc service to process the requests.
type Handler interface {
	GetClusterManager() cluster.Manager
	GetLeader(ctx context.Context) (member.GetLeaderAddrResp, error)
	GetFlowLimiter() (*limiter.FlowLimiter, error)
	// TODO: define the methods for handling other grpc requests.
}

// NodeHeartbeat implements gRPC HoraeMetaServer.
func (s *Service) NodeHeartbeat(ctx context.Context, req *metaservicepb.NodeHeartbeatRequest) (*metaservicepb.NodeHeartbeatResponse, error) {
	metaClient, err := s.getForwardedMetaClient(ctx)
	if err != nil {
		return &metaservicepb.NodeHeartbeatResponse{Header: responseHeader(err, "grpc heartbeat")}, nil
	}

	// Forward request to the leader.
	if metaClient != nil {
		return metaClient.NodeHeartbeat(ctx, req)
	}

	shardInfos := make([]metadata.ShardInfo, 0, len(req.Info.ShardInfos))
	for _, shardInfo := range req.Info.ShardInfos {
		shardInfos = append(shardInfos, metadata.ConvertShardsInfoPB(shardInfo))
	}

	registeredNode := metadata.RegisteredNode{
		Node: storage.Node{
			Name: req.Info.Endpoint,
			NodeStats: storage.NodeStats{
				Lease:       req.GetInfo().Lease,
				Zone:        req.GetInfo().Zone,
				NodeVersion: req.GetInfo().BinaryVersion,
			},
			LastTouchTime: uint64(time.Now().UnixMilli()),
			State:         storage.NodeStateOnline,
		}, ShardInfos: shardInfos,
	}

	log.Info("[NodeHeartbeat]", zap.String("clusterName", req.GetHeader().ClusterName), zap.String("name", req.Info.Endpoint), zap.String("info", fmt.Sprintf("%+v", registeredNode)))

	err = s.h.GetClusterManager().RegisterNode(ctx, req.GetHeader().GetClusterName(), registeredNode)
	if err != nil {
		return &metaservicepb.NodeHeartbeatResponse{Header: responseHeader(err, "grpc heartbeat")}, nil
	}

	return &metaservicepb.NodeHeartbeatResponse{
		Header: okResponseHeader(),
	}, nil
}

// AllocSchemaID implements gRPC HoraeMetaServer.
func (s *Service) AllocSchemaID(ctx context.Context, req *metaservicepb.AllocSchemaIdRequest) (*metaservicepb.AllocSchemaIdResponse, error) {
	metaClient, err := s.getForwardedMetaClient(ctx)
	if err != nil {
		return &metaservicepb.AllocSchemaIdResponse{Header: responseHeader(err, "grpc alloc schema id")}, nil
	}

	// Forward request to the leader.
	if metaClient != nil {
		return metaClient.AllocSchemaID(ctx, req)
	}

	log.Info("[AllocSchemaID]", zap.String("schemaName", req.GetName()), zap.String("clusterName", req.GetHeader().GetClusterName()))

	schemaID, _, err := s.h.GetClusterManager().AllocSchemaID(ctx, req.GetHeader().GetClusterName(), req.GetName())
	if err != nil {
		return &metaservicepb.AllocSchemaIdResponse{Header: responseHeader(err, "grpc alloc schema id")}, nil
	}

	return &metaservicepb.AllocSchemaIdResponse{
		Header: okResponseHeader(),
		Name:   req.GetName(),
		Id:     uint32(schemaID),
	}, nil
}

// GetTablesOfShards implements gRPC HoraeMetaServer.
func (s *Service) GetTablesOfShards(ctx context.Context, req *metaservicepb.GetTablesOfShardsRequest) (*metaservicepb.GetTablesOfShardsResponse, error) {
	metaClient, err := s.getForwardedMetaClient(ctx)
	if err != nil {
		return &metaservicepb.GetTablesOfShardsResponse{Header: responseHeader(err, "grpc get tables of shards")}, nil
	}

	// Forward request to the leader.
	if metaClient != nil {
		return metaClient.GetTablesOfShards(ctx, req)
	}

	log.Info("[GetTablesOfShards]", zap.String("clusterName", req.GetHeader().GetClusterName()), zap.String("shardIDs", fmt.Sprint(req.ShardIds)))

	shardIDs := make([]storage.ShardID, 0, len(req.GetShardIds()))
	for _, shardID := range req.GetShardIds() {
		shardIDs = append(shardIDs, storage.ShardID(shardID))
	}

	tables, err := s.h.GetClusterManager().GetTablesByShardIDs(req.GetHeader().GetClusterName(), req.GetHeader().GetNode(), shardIDs)
	if err != nil {
		return &metaservicepb.GetTablesOfShardsResponse{Header: responseHeader(err, "grpc get tables of shards")}, nil
	}

	result := convertToGetTablesOfShardsResponse(tables)
	return result, nil
}

// CreateTable implements gRPC HoraeMetaServer.
func (s *Service) CreateTable(ctx context.Context, req *metaservicepb.CreateTableRequest) (*metaservicepb.CreateTableResponse, error) {
	start := time.Now()
	// Since there may be too many table creation requests, a flow limiter is added here.
	if ok, err := s.allow(); !ok {
		return &metaservicepb.CreateTableResponse{Header: responseHeader(err, "create table grpc request is rejected by flow limiter")}, nil
	}

	metaClient, err := s.getForwardedMetaClient(ctx)
	if err != nil {
		return &metaservicepb.CreateTableResponse{Header: responseHeader(err, err.Error())}, nil
	}

	// Forward request to the leader.
	if metaClient != nil {
		return metaClient.CreateTable(ctx, req)
	}

	log.Info("[CreateTable]", zap.String("schemaName", req.SchemaName), zap.String("clusterName", req.GetHeader().ClusterName), zap.String("tableName", req.GetName()))

	clusterManager := s.h.GetClusterManager()
	c, err := clusterManager.GetCluster(ctx, req.GetHeader().GetClusterName())
	if err != nil {
		log.Error("fail to create table", zap.Error(err))
		return &metaservicepb.CreateTableResponse{Header: responseHeader(err, err.Error())}, nil
	}

	errorCh := make(chan error, 1)
	resultCh := make(chan metadata.CreateTableResult, 1)

	onSucceeded := func(ret metadata.CreateTableResult) error {
		resultCh <- ret
		return nil
	}
	onFailed := func(err error) error {
		errorCh <- err
		return nil
	}

	p, err := c.GetProcedureFactory().MakeCreateTableProcedure(ctx, coordinator.CreateTableRequest{
		ClusterMetadata: c.GetMetadata(),
		SourceReq:       req,
		OnSucceeded:     onSucceeded,
		OnFailed:        onFailed,
	})
	if err != nil {
		log.Error("fail to create table, factory create procedure", zap.Error(err))
		return &metaservicepb.CreateTableResponse{Header: responseHeader(err, err.Error())}, nil
	}

	err = c.GetProcedureManager().Submit(ctx, p)
	if err != nil {
		log.Error("fail to create table, manager submit procedure", zap.Error(err))
		return &metaservicepb.CreateTableResponse{Header: responseHeader(err, err.Error())}, nil
	}

	select {
	case ret := <-resultCh:
		log.Info("create table finish", zap.String("tableName", req.Name), zap.Int64("costTime", time.Since(start).Milliseconds()))
		return &metaservicepb.CreateTableResponse{
			Header: okResponseHeader(),
			CreatedTable: &metaservicepb.TableInfo{
				Id:         uint64(ret.Table.ID),
				Name:       ret.Table.Name,
				SchemaId:   uint32(ret.Table.SchemaID),
				SchemaName: req.GetSchemaName(),
			},
			ShardInfo: &metaservicepb.ShardInfo{
				Id:      uint32(ret.ShardVersionUpdate.ShardID),
				Role:    clusterpb.ShardRole_LEADER,
				Version: ret.ShardVersionUpdate.LatestVersion,
			},
		}, nil
	case err = <-errorCh:
		log.Warn("create table failed", zap.String("tableName", req.Name), zap.Int64("costTime", time.Since(start).Milliseconds()), zap.Error(err))
		return &metaservicepb.CreateTableResponse{Header: responseHeader(err, err.Error())}, nil
	}
}

// DropTable implements gRPC HoraeMetaServer.
func (s *Service) DropTable(ctx context.Context, req *metaservicepb.DropTableRequest) (*metaservicepb.DropTableResponse, error) {
	start := time.Now()
	// Since there may be too many table dropping requests, a flow limiter is added here.
	if ok, err := s.allow(); !ok {
		return &metaservicepb.DropTableResponse{Header: responseHeader(err, "drop table grpc request is rejected by flow limiter")}, nil
	}

	metaClient, err := s.getForwardedMetaClient(ctx)
	if err != nil {
		return &metaservicepb.DropTableResponse{Header: responseHeader(err, "drop table")}, nil
	}

	// Forward request to the leader.
	if metaClient != nil {
		return metaClient.DropTable(ctx, req)
	}

	log.Info("[DropTable]", zap.String("schemaName", req.SchemaName), zap.String("clusterName", req.GetHeader().ClusterName), zap.String("tableName", req.Name))

	clusterManager := s.h.GetClusterManager()
	c, err := clusterManager.GetCluster(ctx, req.GetHeader().GetClusterName())
	if err != nil {
		log.Error("fail to drop table", zap.Error(err))
		return &metaservicepb.DropTableResponse{Header: responseHeader(err, "drop table")}, nil
	}

	errorCh := make(chan error, 1)
	resultCh := make(chan metadata.TableInfo, 1)

	onSucceeded := func(ret metadata.TableInfo) error {
		resultCh <- ret
		return nil
	}
	onFailed := func(err error) error {
		errorCh <- err
		return nil
	}
	procedure, ok, err := c.GetProcedureFactory().CreateDropTableProcedure(ctx, coordinator.DropTableRequest{
		ClusterMetadata: c.GetMetadata(),
		ClusterSnapshot: c.GetMetadata().GetClusterSnapshot(),
		SourceReq:       req,
		OnSucceeded:     onSucceeded,
		OnFailed:        onFailed,
	})
	if err != nil {
		log.Error("fail to drop table", zap.Error(err))
		return &metaservicepb.DropTableResponse{Header: responseHeader(err, "drop table")}, nil
	}
	if !ok {
		log.Warn("table may have been dropped already")
		return &metaservicepb.DropTableResponse{Header: okResponseHeader()}, nil
	}

	err = c.GetProcedureManager().Submit(ctx, procedure)
	if err != nil {
		log.Error("fail to drop table, manager submit procedure", zap.Error(err), zap.Int64("costTime", time.Since(start).Milliseconds()))
		return &metaservicepb.DropTableResponse{Header: responseHeader(err, "drop table")}, nil
	}

	select {
	case ret := <-resultCh:
		log.Info("drop table finish", zap.String("tableName", req.Name), zap.Int64("costTime", time.Since(start).Milliseconds()))
		return &metaservicepb.DropTableResponse{
			Header:       okResponseHeader(),
			DroppedTable: metadata.ConvertTableInfoToPB(ret),
		}, nil
	case err = <-errorCh:
		log.Info("drop table failed", zap.String("tableName", req.Name), zap.Int64("costTime", time.Since(start).Milliseconds()))
		return &metaservicepb.DropTableResponse{Header: responseHeader(err, "drop table")}, nil
	}
}

// RouteTables implements gRPC HoraeMetaServer.
func (s *Service) RouteTables(ctx context.Context, req *metaservicepb.RouteTablesRequest) (*metaservicepb.RouteTablesResponse, error) {
	// Since there may be too many table routing requests, a flow limiter is added here.
	if ok, err := s.allow(); !ok {
		return &metaservicepb.RouteTablesResponse{Header: responseHeader(err, "routeTables grpc request is rejected by flow limiter")}, nil
	}

	metaClient, err := s.getForwardedMetaClient(ctx)
	if err != nil {
		return &metaservicepb.RouteTablesResponse{Header: responseHeader(err, "grpc routeTables")}, nil
	}

	log.Debug("[RouteTable]", zap.String("schemaName", req.SchemaName), zap.String("clusterName", req.GetHeader().ClusterName), zap.String("tableNames", strings.Join(req.TableNames, ",")))

	// Forward request to the leader.
	if metaClient != nil {
		return metaClient.RouteTables(ctx, req)
	}

	routeTableResult, err := s.h.GetClusterManager().RouteTables(ctx, req.GetHeader().GetClusterName(), req.GetSchemaName(), req.GetTableNames())
	if err != nil {
		return &metaservicepb.RouteTablesResponse{Header: responseHeader(err, "grpc routeTables")}, nil
	}

	return convertRouteTableResult(routeTableResult), nil
}

// GetNodes implements gRPC HoraeMetaServer.
func (s *Service) GetNodes(ctx context.Context, req *metaservicepb.GetNodesRequest) (*metaservicepb.GetNodesResponse, error) {
	metaClient, err := s.getForwardedMetaClient(ctx)
	if err != nil {
		return &metaservicepb.GetNodesResponse{Header: responseHeader(err, "grpc get nodes")}, nil
	}

	// Forward request to the leader.
	if metaClient != nil {
		return metaClient.GetNodes(ctx, req)
	}

	log.Info("[GetNodes]", zap.String("clusterName", req.GetHeader().ClusterName))

	nodesResult, err := s.h.GetClusterManager().GetNodeShards(ctx, req.GetHeader().GetClusterName())
	if err != nil {
		log.Error("fail to get nodes", zap.Error(err))
		return &metaservicepb.GetNodesResponse{Header: responseHeader(err, "grpc get nodes")}, nil
	}

	return convertToGetNodesResponse(nodesResult), nil
}

func convertToGetTablesOfShardsResponse(shardTables map[storage.ShardID]metadata.ShardTables) *metaservicepb.GetTablesOfShardsResponse {
	tablesByShard := make(map[uint32]*metaservicepb.TablesOfShard, len(shardTables))
	for id, shardTable := range shardTables {
		tables := make([]*metaservicepb.TableInfo, 0, len(shardTable.Tables))
		for _, table := range shardTable.Tables {
			tables = append(tables, metadata.ConvertTableInfoToPB(table))
		}
		tablesByShard[uint32(id)] = &metaservicepb.TablesOfShard{
			ShardInfo: metadata.ConvertShardsInfoToPB(shardTable.Shard),
			Tables:    tables,
		}
	}
	return &metaservicepb.GetTablesOfShardsResponse{
		Header:        okResponseHeader(),
		TablesByShard: tablesByShard,
	}
}

func convertRouteTableResult(routeTablesResult metadata.RouteTablesResult) *metaservicepb.RouteTablesResponse {
	entries := make(map[string]*metaservicepb.RouteEntry, len(routeTablesResult.RouteEntries))
	for tableName, entry := range routeTablesResult.RouteEntries {
		nodeShards := make([]*metaservicepb.NodeShard, 0, len(entry.NodeShards))
		for _, nodeShard := range entry.NodeShards {
			nodeShards = append(nodeShards, &metaservicepb.NodeShard{
				Endpoint: nodeShard.ShardNode.NodeName,
				ShardInfo: &metaservicepb.ShardInfo{
					Id:   uint32(nodeShard.ShardNode.ID),
					Role: storage.ConvertShardRoleToPB(nodeShard.ShardNode.ShardRole),
				},
			})
		}

		entries[tableName] = &metaservicepb.RouteEntry{
			Table:      metadata.ConvertTableInfoToPB(entry.Table),
			NodeShards: nodeShards,
		}
	}

	return &metaservicepb.RouteTablesResponse{
		Header:                 okResponseHeader(),
		ClusterTopologyVersion: routeTablesResult.ClusterViewVersion,
		Entries:                entries,
	}
}

func convertToGetNodesResponse(nodesResult metadata.GetNodeShardsResult) *metaservicepb.GetNodesResponse {
	nodeShards := make([]*metaservicepb.NodeShard, 0, len(nodesResult.NodeShards))
	for _, shardNodeWithVersion := range nodesResult.NodeShards {
		nodeShards = append(nodeShards, &metaservicepb.NodeShard{
			Endpoint: shardNodeWithVersion.ShardNode.NodeName,
			ShardInfo: &metaservicepb.ShardInfo{
				Id:   uint32(shardNodeWithVersion.ShardNode.ID),
				Role: storage.ConvertShardRoleToPB(shardNodeWithVersion.ShardNode.ShardRole),
			},
		})
	}
	return &metaservicepb.GetNodesResponse{
		Header:                 okResponseHeader(),
		ClusterTopologyVersion: nodesResult.ClusterTopologyVersion,
		NodeShards:             nodeShards,
	}
}

func okResponseHeader() *commonpb.ResponseHeader {
	return responseHeader(nil, "")
}

func responseHeader(err error, msg string) *commonpb.ResponseHeader {
	if err == nil {
		return &commonpb.ResponseHeader{Code: uint32(coderr.Ok.ToInt()), Error: msg}
	}

	code, ok := coderr.GetCauseCode(err)
	if ok {
		return &commonpb.ResponseHeader{Code: uint32(code), Error: msg}
	}

	return &commonpb.ResponseHeader{Code: uint32(coderr.Internal.ToInt()), Error: msg}
}

func (s *Service) allow() (bool, error) {
	flowLimiter, err := s.h.GetFlowLimiter()
	if err != nil {
		return false, errors.WithMessage(err, "get flow limiter failed")
	}
	if !flowLimiter.Allow() {
		return false, ErrFlowLimit.WithMessagef("the current flow has reached the threshold")
	}
	return true, nil
}
