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

package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/pprof"

	"github.com/apache/incubator-horaedb-meta/pkg/coderr"
	"github.com/apache/incubator-horaedb-meta/pkg/log"
	"github.com/apache/incubator-horaedb-meta/server/cluster"
	"github.com/apache/incubator-horaedb-meta/server/cluster/metadata"
	"github.com/apache/incubator-horaedb-meta/server/config"
	"github.com/apache/incubator-horaedb-meta/server/coordinator"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/procedure"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/scheduler"
	"github.com/apache/incubator-horaedb-meta/server/limiter"
	"github.com/apache/incubator-horaedb-meta/server/member"
	"github.com/apache/incubator-horaedb-meta/server/status"
	"github.com/apache/incubator-horaedb-meta/server/storage"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func NewAPI(clusterManager cluster.Manager, serverStatus *status.ServerStatus, forwardClient *ForwardClient, flowLimiter *limiter.FlowLimiter, etcdClient *clientv3.Client) *API {
	return &API{
		clusterManager: clusterManager,
		serverStatus:   serverStatus,
		forwardClient:  forwardClient,
		flowLimiter:    flowLimiter,
		etcdAPI:        NewEtcdAPI(etcdClient, forwardClient),
	}
}

func (a *API) NewAPIRouter() *Router {
	router := New().WithPrefix(apiPrefix).WithInstrumentation(printRequestInfo)

	// Register API.
	router.Post("/getShardTables", wrap(a.getShardTables, true, a.forwardClient))
	router.Post("/transferLeader", wrap(a.transferLeader, true, a.forwardClient))
	router.Post("/split", wrap(a.split, true, a.forwardClient))
	router.Post("/route", wrap(a.route, true, a.forwardClient))
	router.Del("/table", wrap(a.dropTable, true, a.forwardClient))
	router.Post("/getNodeShards", wrap(a.getNodeShards, true, a.forwardClient))
	router.Del("/nodeShards", wrap(a.dropNodeShards, true, a.forwardClient))
	router.Get("/flowLimiter", wrap(a.getFlowLimiter, true, a.forwardClient))
	router.Put("/flowLimiter", wrap(a.updateFlowLimiter, true, a.forwardClient))
	router.Get("/health", wrap(a.health, false, a.forwardClient))

	// Register cluster API.
	router.Get("/clusters", wrap(a.listClusters, true, a.forwardClient))
	router.Post("/clusters", wrap(a.createCluster, true, a.forwardClient))
	router.Put(fmt.Sprintf("/clusters/:%s", clusterNameParam), wrap(a.updateCluster, true, a.forwardClient))
	router.Get(fmt.Sprintf("/clusters/:%s/procedure", clusterNameParam), wrap(a.listProcedures, true, a.forwardClient))
	router.Get(fmt.Sprintf("/clusters/:%s/shardAffinities", clusterNameParam), wrap(a.listShardAffinities, true, a.forwardClient))
	router.Post(fmt.Sprintf("/clusters/:%s/shardAffinities", clusterNameParam), wrap(a.addShardAffinities, true, a.forwardClient))
	router.Del(fmt.Sprintf("/clusters/:%s/shardAffinities", clusterNameParam), wrap(a.removeShardAffinities, true, a.forwardClient))
	router.Post("/table/query", wrap(a.queryTable, true, a.forwardClient))

	// Register debug API.
	router.DebugGet("/pprof/profile", pprof.Profile)
	router.DebugGet("/pprof/symbol", pprof.Symbol)
	router.DebugGet("/pprof/trace", pprof.Trace)
	router.DebugGet("/pprof/heap", a.pprofHeap)
	router.DebugGet("/pprof/allocs", a.pprofAllocs)
	router.DebugGet("/pprof/block", a.pprofBlock)
	router.DebugGet("/pprof/goroutine", a.pprofGoroutine)
	router.DebugGet("/pprof/threadCreate", a.pprofThreadCreate)
	router.DebugGet(fmt.Sprintf("/diagnose/:%s/shards", clusterNameParam), wrap(a.diagnoseShards, true, a.forwardClient))
	router.DebugGet("/leader", wrap(a.getLeader, false, a.forwardClient))
	router.DebugGet(fmt.Sprintf("/clusters/:%s/enableSchedule", clusterNameParam), wrap(a.getEnableSchedule, true, a.forwardClient))
	router.DebugPut(fmt.Sprintf("/clusters/:%s/enableSchedule", clusterNameParam), wrap(a.updateEnableSchedule, true, a.forwardClient))

	// Register ETCD API.
	router.Post("/etcd/promoteLearner", wrap(a.etcdAPI.promoteLearner, false, a.forwardClient))
	router.Put("/etcd/member", wrap(a.etcdAPI.addMember, false, a.forwardClient))
	router.Get("/etcd/member", wrap(a.etcdAPI.getMember, false, a.forwardClient))
	router.Post("/etcd/member", wrap(a.etcdAPI.updateMember, false, a.forwardClient))
	router.Del("/etcd/member", wrap(a.etcdAPI.removeMember, false, a.forwardClient))
	router.Post("/etcd/moveLeader", wrap(a.etcdAPI.moveLeader, false, a.forwardClient))

	return router
}

func (a *API) getLeader(req *http.Request) apiFuncResult {
	leaderAddr, err := a.forwardClient.GetLeaderAddr(req.Context())
	if err != nil {
		return errResult(member.ErrGetLeader.WithCause(err))
	}
	return okResult(leaderAddr)
}

func (a *API) getShardTables(req *http.Request) apiFuncResult {
	var getShardTablesReq GetShardTablesRequest
	err := json.NewDecoder(req.Body).Decode(&getShardTablesReq)
	if err != nil {
		return errResult(ErrParseRequest.WithCause(err))
	}

	c, err := a.clusterManager.GetCluster(req.Context(), getShardTablesReq.ClusterName)
	if err != nil {
		return errResult(ErrParseRequest.WithCause(err))
	}

	// If ShardIDs in the request is empty, query with all shardIDs in the cluster.
	shardIDs := make([]storage.ShardID, len(getShardTablesReq.ShardIDs))
	if len(getShardTablesReq.ShardIDs) != 0 {
		for _, shardID := range getShardTablesReq.ShardIDs {
			shardIDs = append(shardIDs, storage.ShardID(shardID))
		}
	} else {
		shardViewsMapping := c.GetMetadata().GetClusterSnapshot().Topology.ShardViewsMapping
		for shardID := range shardViewsMapping {
			shardIDs = append(shardIDs, shardID)
		}
	}

	shardTables := c.GetMetadata().GetShardTables(shardIDs)
	return okResult(shardTables)
}

func (a *API) transferLeader(req *http.Request) apiFuncResult {
	var transferLeaderRequest TransferLeaderRequest
	err := json.NewDecoder(req.Body).Decode(&transferLeaderRequest)
	if err != nil {
		return errResult(ErrParseRequest.WithCause(err))
	}
	log.Info("transfer leader request", zap.String("request", fmt.Sprintf("%+v", transferLeaderRequest)))

	c, err := a.clusterManager.GetCluster(req.Context(), transferLeaderRequest.ClusterName)
	if err != nil {
		log.Error("get cluster failed", zap.String("clusterName", transferLeaderRequest.ClusterName), zap.Error(err))
		return errResult(ErrGetCluster.WithMessagef("clusterName:%s, err:%s", transferLeaderRequest.ClusterName, err.Error()))
	}

	transferLeaderProcedure, err := c.GetProcedureFactory().CreateTransferLeaderProcedure(req.Context(), coordinator.TransferLeaderRequest{
		Snapshot:          c.GetMetadata().GetClusterSnapshot(),
		ShardID:           storage.ShardID(transferLeaderRequest.ShardID),
		OldLeaderNodeName: transferLeaderRequest.OldLeaderNodeName,
		NewLeaderNodeName: transferLeaderRequest.NewLeaderNodeName,
	})
	if err != nil {
		log.Error("create transfer leader procedure failed", zap.Error(err))
		return errResult(ErrCreateProcedure.WithCause(err))
	}
	err = c.GetProcedureManager().Submit(req.Context(), transferLeaderProcedure)
	if err != nil {
		log.Error("submit transfer leader procedure failed", zap.Error(err))
		return errResult(ErrSubmitProcedure.WithCause(err))
	}

	return okResult(statusSuccess)
}

func (a *API) route(req *http.Request) apiFuncResult {
	var routeRequest RouteRequest
	err := json.NewDecoder(req.Body).Decode(&routeRequest)
	if err != nil {
		return errResult(ErrParseRequest.WithCause(err))
	}

	result, err := a.clusterManager.RouteTables(context.Background(), routeRequest.ClusterName, routeRequest.SchemaName, routeRequest.Tables)
	if err != nil {
		return errResult(ErrRoute.WithCause(err))
	}

	return okResult(result)
}

func (a *API) getNodeShards(req *http.Request) apiFuncResult {
	var nodeShardsRequest NodeShardsRequest
	err := json.NewDecoder(req.Body).Decode(&nodeShardsRequest)
	if err != nil {
		return errResult(ErrParseRequest.WithCause(err))
	}

	result, err := a.clusterManager.GetNodeShards(context.Background(), nodeShardsRequest.ClusterName)
	if err != nil {
		return errResult(ErrGetNodeShards.WithCause(err))
	}

	return okResult(result)
}

func (a *API) dropNodeShards(req *http.Request) apiFuncResult {
	var dropNodeShardsRequest DropNodeShardsRequest
	err := json.NewDecoder(req.Body).Decode(&dropNodeShardsRequest)
	if err != nil {
		return errResult(ErrParseRequest.WithCause(err))
	}

	c, err := a.clusterManager.GetCluster(req.Context(), dropNodeShardsRequest.ClusterName)
	if err != nil {
		log.Error("get cluster failed", zap.String("clusterName", dropNodeShardsRequest.ClusterName), zap.Error(err))
		return errResult(ErrGetCluster.WithMessagef("clusterName:%s, err:%s", dropNodeShardsRequest.ClusterName, err.Error()))
	}

	targetShardNodes := make([]storage.ShardNode, 0, len(dropNodeShardsRequest.ShardIDs))
	getShardNodeResult := c.GetMetadata().GetShardNodes()
	for _, shardNode := range getShardNodeResult.ShardNodes {
		for _, shardID := range dropNodeShardsRequest.ShardIDs {
			if shardNode.ID == storage.ShardID(shardID) {
				targetShardNodes = append(targetShardNodes, shardNode)
			}
		}
	}

	if err := c.GetMetadata().DropShardNodes(req.Context(), targetShardNodes); err != nil {
		return errResult(ErrDropNodeShards.WithCause(err))
	}

	return okResult(targetShardNodes)
}

func (a *API) dropTable(req *http.Request) apiFuncResult {
	var dropTableRequest DropTableRequest
	err := json.NewDecoder(req.Body).Decode(&dropTableRequest)
	if err != nil {
		return errResult(ErrParseRequest.WithCause(err))
	}
	log.Info("drop table request", zap.String("request", fmt.Sprintf("%+v", dropTableRequest)))

	if err := a.clusterManager.DropTable(context.Background(), dropTableRequest.ClusterName, dropTableRequest.SchemaName, dropTableRequest.Table); err != nil {
		log.Error("drop table failed", zap.Error(err))
		return errResult(ErrTable.WithCause(err))
	}

	return okResult(statusSuccess)
}

func (a *API) split(req *http.Request) apiFuncResult {
	var splitRequest SplitRequest
	err := json.NewDecoder(req.Body).Decode(&splitRequest)
	if err != nil {
		return errResult(ErrParseRequest.WithCause(err))
	}

	log.Info("split request", zap.String("request", fmt.Sprintf("%+v", splitRequest)))

	ctx := context.Background()

	c, err := a.clusterManager.GetCluster(ctx, splitRequest.ClusterName)
	if err != nil {
		return errResult(ErrGetCluster.WithCausef(err, "clusterName:%s", splitRequest.ClusterName))
	}

	newShardID, err := c.GetMetadata().AllocShardID(ctx)
	if err != nil {
		return errResult(ErrAllocShardID.WithCause(err))
	}

	splitProcedure, err := c.GetProcedureFactory().CreateSplitProcedure(ctx, coordinator.SplitRequest{
		ClusterMetadata: c.GetMetadata(),
		SchemaName:      splitRequest.SchemaName,
		TableNames:      splitRequest.SplitTables,
		Snapshot:        c.GetMetadata().GetClusterSnapshot(),
		ShardID:         storage.ShardID(splitRequest.ShardID),
		NewShardID:      storage.ShardID(newShardID),
		TargetNodeName:  splitRequest.NodeName,
	})
	if err != nil {
		log.Error("create split procedure failed", zap.Error(err))
		return errResult(ErrCreateProcedure.WithCause(err))
	}

	if err := c.GetProcedureManager().Submit(ctx, splitProcedure); err != nil {
		log.Error("submit split procedure failed", zap.Error(err))
		return errResult(ErrSubmitProcedure.WithCause(err))
	}

	return okResult(newShardID)
}

func (a *API) listClusters(req *http.Request) apiFuncResult {
	clusters, err := a.clusterManager.ListClusters(req.Context())
	if err != nil {
		return errResult(ErrGetCluster.WithCause(err))
	}

	clusterMetadatas := make([]storage.Cluster, 0, len(clusters))
	for i := 0; i < len(clusters); i++ {
		storageMetadata := clusters[i].GetMetadata().GetStorageMetadata()
		clusterMetadatas = append(clusterMetadatas, storageMetadata)
	}

	return okResult(clusterMetadatas)
}

func (a *API) createCluster(req *http.Request) apiFuncResult {
	var createClusterRequest CreateClusterRequest
	err := json.NewDecoder(req.Body).Decode(&createClusterRequest)
	if err != nil {
		return errResult(ErrParseRequest.WithCause(err))
	}

	log.Info("create cluster request", zap.String("request", fmt.Sprintf("%+v", createClusterRequest)))

	if createClusterRequest.ProcedureExecutingBatchSize == 0 {
		return errResult(ErrInvalidParamsForCreateCluster.WithMessagef("expect positive procedureExecutingBatchSize"))
	}

	if _, err := a.clusterManager.GetCluster(req.Context(), createClusterRequest.Name); err == nil {
		return errResult(ErrAlreadyExistsCluster.WithMessagef("cluster:%s", createClusterRequest.Name))
	}

	topologyType, err := metadata.ParseTopologyType(createClusterRequest.TopologyType)
	if err != nil {
		return errResult(ErrParseRequest.WithCause(err))
	}

	ctx := context.Background()
	createClusterOpts := metadata.CreateClusterOpts{
		NodeCount:                   createClusterRequest.NodeCount,
		ShardTotal:                  createClusterRequest.ShardTotal,
		EnableSchedule:              createClusterRequest.EnableSchedule,
		TopologyType:                topologyType,
		ProcedureExecutingBatchSize: createClusterRequest.ProcedureExecutingBatchSize,
	}
	c, err := a.clusterManager.CreateCluster(ctx, createClusterRequest.Name, createClusterOpts)
	if err != nil {
		log.Error("create cluster failed", zap.Error(err))
		return errResult(metadata.ErrCreateCluster.WithCause(err))
	}

	return okResult(c.GetMetadata().GetClusterID())
}

func (a *API) updateCluster(req *http.Request) apiFuncResult {
	clusterName := Param(req.Context(), clusterNameParam)
	if len(clusterName) == 0 {
		return errResult(ErrInvalidClusterName.WithMessagef("clusterName can't be empty"))
	}

	var updateClusterRequest UpdateClusterRequest
	err := json.NewDecoder(req.Body).Decode(&updateClusterRequest)
	if err != nil {
		return errResult(ErrParseRequest.WithCause(err))
	}

	log.Info("update cluster request", zap.String("request", fmt.Sprintf("%+v", updateClusterRequest)))

	c, err := a.clusterManager.GetCluster(req.Context(), clusterName)
	if err != nil {
		return errResult(ErrGetCluster.WithCausef(err, "clusterName:%s", clusterName))
	}

	topologyType, err := metadata.ParseTopologyType(updateClusterRequest.TopologyType)
	if err != nil {
		return errResult(ErrParseTopology.WithCause(err))
	}

	if err := a.clusterManager.UpdateCluster(req.Context(), clusterName, metadata.UpdateClusterOpts{
		TopologyType:                topologyType,
		ProcedureExecutingBatchSize: updateClusterRequest.ProcedureExecutingBatchSize,
	}); err != nil {
		return errResult(metadata.ErrUpdateCluster.WithCause(err))
	}

	return okResult(c.GetMetadata().GetClusterID())
}

func (a *API) getFlowLimiter(_ *http.Request) apiFuncResult {
	limiter := a.flowLimiter.GetConfig()
	return okResult(limiter)
}

func (a *API) updateFlowLimiter(req *http.Request) apiFuncResult {
	var updateFlowLimiterRequest UpdateFlowLimiterRequest
	err := json.NewDecoder(req.Body).Decode(&updateFlowLimiterRequest)
	if err != nil {
		return errResult(ErrParseRequest.WithCause(err))
	}

	log.Info("update flow limiter request", zap.String("request", fmt.Sprintf("%+v", updateFlowLimiterRequest)))

	newLimiterConfig := config.LimiterConfig{
		Enable: updateFlowLimiterRequest.Enable,
		Limit:  updateFlowLimiterRequest.Limit,
		Burst:  updateFlowLimiterRequest.Burst,
	}

	if err := a.flowLimiter.UpdateLimiter(newLimiterConfig); err != nil {
		return errResult(ErrUpdateFlowLimiter.WithCause(err))
	}

	return okResult(statusSuccess)
}

func (a *API) listProcedures(req *http.Request) apiFuncResult {
	ctx := req.Context()
	clusterName := Param(ctx, clusterNameParam)
	if len(clusterName) == 0 {
		return errResult(ErrInvalidClusterName.WithMessagef("clusterName can't be empty when list cluster procedures"))
	}

	c, err := a.clusterManager.GetCluster(ctx, clusterName)
	if err != nil {
		return errResult(ErrGetCluster.WithCausef(err, "list procedures of cluster"))
	}

	infos, err := c.GetProcedureManager().ListRunningProcedure(ctx)
	if err != nil {
		return errResult(procedure.ErrListRunningProcedure.WithMessagef("list cluster's running procedures, clusterName:%s", clusterName))
	}

	return okResult(infos)
}

func (a *API) listShardAffinities(req *http.Request) apiFuncResult {
	ctx := req.Context()
	clusterName := Param(ctx, clusterNameParam)
	if len(clusterName) == 0 {
		return errResult(ErrInvalidClusterName.WithMessagef("clusterName can't be empty when list shard affinities"))
	}

	c, err := a.clusterManager.GetCluster(ctx, clusterName)
	if err != nil {
		return errResult(ErrGetCluster.WithCausef(err, "list shard affinities"))
	}

	affinityRules, err := c.GetSchedulerManager().ListShardAffinityRules(ctx)
	if err != nil {
		return errResult(ErrListAffinityRules.WithCause(err))
	}

	return okResult(affinityRules)
}

func (a *API) addShardAffinities(req *http.Request) apiFuncResult {
	ctx := req.Context()
	clusterName := Param(ctx, clusterNameParam)
	if len(clusterName) == 0 {
		return errResult(ErrInvalidClusterName.WithMessagef("clusterName can't be empty when add shard affinities"))
	}

	var affinities []scheduler.ShardAffinity
	err := json.NewDecoder(req.Body).Decode(&affinities)
	if err != nil {
		return errResult(ErrParseRequest.WithCause(err))
	}

	log.Info("try to apply shard affinity rule", zap.String("cluster", clusterName), zap.String("affinity", fmt.Sprintf("%+v", affinities)))

	c, err := a.clusterManager.GetCluster(ctx, clusterName)
	if err != nil {
		return errResult(ErrGetCluster.WithCausef(err, "add shard affinities for cluster:%s", clusterName))
	}

	err = c.GetSchedulerManager().AddShardAffinityRule(ctx, scheduler.ShardAffinityRule{Affinities: affinities})
	if err != nil {
		return errResult(ErrAddAffinityRule.WithCause(err))
	}

	log.Info("finish applying shard affinity rule", zap.String("cluster", clusterName), zap.String("rules", fmt.Sprintf("%+v", affinities)))

	return okResult(nil)
}

func (a *API) removeShardAffinities(req *http.Request) apiFuncResult {
	ctx := req.Context()
	clusterName := Param(ctx, clusterNameParam)
	if len(clusterName) == 0 {
		return errResult(ErrInvalidClusterName.WithMessagef("clusterName can't be empty when remove shard affinities"))
	}

	var decodedReq RemoveShardAffinitiesRequest
	err := json.NewDecoder(req.Body).Decode(&decodedReq)
	if err != nil {
		return errResult(ErrParseRequest.WithCause(err))
	}

	c, err := a.clusterManager.GetCluster(ctx, clusterName)
	if err != nil {
		return errResult(ErrGetCluster.WithCause(err))
	}

	for _, shardID := range decodedReq.ShardIDs {
		log.Info("try to remove shard affinity rule", zap.String("cluster", clusterName), zap.Int("shardID", int(shardID)))
		err := c.GetSchedulerManager().RemoveShardAffinityRule(ctx, shardID)
		if err != nil {
			return errResult(ErrRemoveAffinityRule.WithCause(err))
		}
	}

	return okResult(nil)
}

func (a *API) queryTable(r *http.Request) apiFuncResult {
	var req QueryTableRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return errResult(ErrParseRequest.WithCause(err))
	}

	if len(req.Names) != 0 {
		tables, err := a.clusterManager.GetTables(req.ClusterName, req.SchemaName, req.Names)
		if err != nil {
			return errResult(ErrTable.WithCause(err))
		}
		return okResult(tables)
	}

	ids := make([]storage.TableID, 0, len(req.IDs))
	for _, id := range req.IDs {
		ids = append(ids, storage.TableID(id))
	}

	tables, err := a.clusterManager.GetTablesByIDs(req.ClusterName, ids)
	if err != nil {
		return errResult(ErrTable.WithCause(err))
	}
	return okResult(tables)
}

func (a *API) getEnableSchedule(r *http.Request) apiFuncResult {
	ctx := r.Context()
	clusterName := Param(ctx, clusterNameParam)
	if len(clusterName) == 0 {
		clusterName = config.DefaultClusterName
	}

	c, err := a.clusterManager.GetCluster(ctx, clusterName)
	if err != nil {
		return errResult(ErrGetCluster.WithCause(err))
	}

	enableSchedule, err := c.GetSchedulerManager().GetEnableSchedule(r.Context())
	if err != nil {
		return errResult(ErrGetEnableSchedule.WithCause(err))
	}

	return okResult(enableSchedule)
}

func (a *API) updateEnableSchedule(r *http.Request) apiFuncResult {
	ctx := r.Context()
	clusterName := Param(ctx, clusterNameParam)
	if len(clusterName) == 0 {
		clusterName = config.DefaultClusterName
	}

	c, err := a.clusterManager.GetCluster(ctx, clusterName)
	if err != nil {
		return errResult(ErrGetCluster.WithCause(err))
	}

	var req UpdateEnableScheduleRequest
	err = json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return errResult(ErrParseRequest.WithCause(err))
	}

	err = c.GetSchedulerManager().UpdateEnableSchedule(r.Context(), req.Enable)
	if err != nil {
		return errResult(ErrUpdateEnableSchedule.WithCause(err))
	}

	return okResult(req.Enable)
}

func (a *API) diagnoseShards(req *http.Request) apiFuncResult {
	ctx := req.Context()
	clusterName := Param(ctx, clusterNameParam)
	if len(clusterName) == 0 {
		clusterName = config.DefaultClusterName
	}

	c, err := a.clusterManager.GetCluster(ctx, clusterName)
	if err != nil {
		return errResult(ErrGetCluster.WithCause(err))
	}

	registeredNodes, err := a.clusterManager.ListRegisteredNodes(ctx, clusterName)
	if err != nil {
		return errResult(ErrGetCluster.WithCause(err))
	}

	ret := DiagnoseShardResult{
		UnregisteredShards: []storage.ShardID{},
		UnreadyShards:      make(map[storage.ShardID]DiagnoseShardStatus),
	}
	shards := c.GetShards()

	registeredShards := make(map[storage.ShardID]struct{}, len(shards))
	// Check if there are unready shards.
	for _, node := range registeredNodes {
		for _, shardInfo := range node.ShardInfos {
			if shardInfo.Status != storage.ShardStatusReady {
				ret.UnreadyShards[shardInfo.ID] = DiagnoseShardStatus{
					NodeName: node.Node.Name,
					Status:   storage.ConvertShardStatusToString(shardInfo.Status),
				}
			}
			registeredShards[shardInfo.ID] = struct{}{}
		}
	}

	// Check if there are unregistered shards.
	for _, shard := range shards {
		if _, ok := registeredShards[shard]; !ok {
			ret.UnregisteredShards = append(ret.UnregisteredShards, shard)
		}
	}

	return okResult(ret)
}

func (a *API) health(_ *http.Request) apiFuncResult {
	isServerHealthy := a.serverStatus.IsHealthy()
	if isServerHealthy {
		return okResult(nil)
	}
	return errResult(ErrHealthCheck.WithMessagef("server heath check failed, status is %v", a.serverStatus.Get()))
}

func (a *API) pprofHeap(writer http.ResponseWriter, req *http.Request) {
	pprof.Handler("heap").ServeHTTP(writer, req)
}

func (a *API) pprofAllocs(writer http.ResponseWriter, req *http.Request) {
	pprof.Handler("allocs").ServeHTTP(writer, req)
}

func (a *API) pprofBlock(writer http.ResponseWriter, req *http.Request) {
	pprof.Handler("block").ServeHTTP(writer, req)
}

func (a *API) pprofGoroutine(writer http.ResponseWriter, req *http.Request) {
	pprof.Handler("goroutine").ServeHTTP(writer, req)
}

func (a *API) pprofThreadCreate(writer http.ResponseWriter, req *http.Request) {
	pprof.Handler("threadcreate").ServeHTTP(writer, req)
}

// printRequestInfo used for printing every request information.
func printRequestInfo(handlerName string, handler http.HandlerFunc) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		body := ""
		bodyByte, err := io.ReadAll(request.Body)
		if err != nil {
			log.Error("read request body failed", zap.Error(err))
			return
		}
		body = string(bodyByte)
		newBody := io.NopCloser(bytes.NewReader(bodyByte))
		request.Body = newBody
		log.Info("receive http request", zap.String("handlerName", handlerName), zap.String("client host", request.RemoteAddr), zap.String("method", request.Method), zap.String("params", request.Form.Encode()), zap.String("body", body))
		handler.ServeHTTP(writer, request)
	}
}

func respondForward(w http.ResponseWriter, response *http.Response) {
	b, err := io.ReadAll(response.Body)
	if err != nil {
		log.Error("read response failed", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for key, valArr := range response.Header {
		for _, val := range valArr {
			w.Header().Add(key, val)
		}
	}
	w.WriteHeader(response.StatusCode)
	if n, err := w.Write(b); err != nil {
		log.Error("write response failed", zap.Int("msg", n), zap.Error(err))
	}
}

func respond(w http.ResponseWriter, data interface{}) {
	statusMessage := statusSuccess
	b, err := json.Marshal(&response{
		Status: statusMessage,
		Data:   data,
		Error:  "",
		Msg:    "",
	})
	if err != nil {
		log.Error("marshal json response failed", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if n, err := w.Write(b); err != nil {
		log.Error("write response failed", zap.Int("msg", n), zap.Error(err))
	}
}

func respondError(w http.ResponseWriter, apiErr coderr.CodeError) {
	b, err := json.Marshal(&response{
		Status: statusError,
		Data:   nil,
		Error:  apiErr.Error(),
		// FIXME: remove the redundant Msg field.
		Msg: apiErr.Error(),
	})
	if err != nil {
		log.Error("marshal json response failed", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(apiErr.Code().ToHTTPCode())
	if n, err := w.Write(b); err != nil {
		log.Error("write response failed", zap.Int("msg", n), zap.Error(err))
	}
}

func wrap(f apiFunc, needForward bool, forwardClient *ForwardClient) http.HandlerFunc {
	hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if needForward {
			resp, isLeader, err := forwardClient.forwardToLeader(r)
			if err != nil {
				log.Error("forward to leader failed", zap.Error(err))
				respondError(w, ErrForwardToLeader.WithCause(err))
				return
			}
			if !isLeader {
				// nolint:staticcheck
				defer resp.Body.Close()
				respondForward(w, resp)
				return
			}
		}
		result := f(r)
		if result.err != nil {
			respondError(w, result.err)
			return
		}
		respond(w, result.data)
	})
	return hf
}
