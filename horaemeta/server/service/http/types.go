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
	"net/http"

	"github.com/apache/incubator-horaedb-meta/pkg/coderr"
	"github.com/apache/incubator-horaedb-meta/server/cluster"
	"github.com/apache/incubator-horaedb-meta/server/limiter"
	"github.com/apache/incubator-horaedb-meta/server/status"
	"github.com/apache/incubator-horaedb-meta/server/storage"
)

const (
	statusSuccess    string = "success"
	statusError      string = "error"
	clusterNameParam string = "cluster"

	apiPrefix string = "/api/v1"
)

type response struct {
	Status string      `json:"status"`
	Data   interface{} `json:"data,omitempty"`
	Error  string      `json:"error,omitempty"`
	Msg    string      `json:"msg,omitempty"`
}

type apiFuncResult struct {
	data   interface{}
	err    coderr.CodeError
	errMsg string
}

func okResult(data interface{}) apiFuncResult {
	return apiFuncResult{
		data:   data,
		err:    nil,
		errMsg: "",
	}
}

func errResult(err coderr.CodeError, errMsg string) apiFuncResult {
	return apiFuncResult{
		data:   nil,
		err:    err,
		errMsg: errMsg,
	}
}

type apiFunc func(r *http.Request) apiFuncResult

type API struct {
	clusterManager cluster.Manager

	serverStatus *status.ServerStatus

	forwardClient *ForwardClient
	flowLimiter   *limiter.FlowLimiter

	etcdAPI EtcdAPI
}

type DiagnoseShardStatus struct {
	NodeName string `json:"nodeName"`
	Status   string `json:"status"`
}

type DiagnoseShardResult struct {
	// shardID -> nodeName
	UnregisteredShards []storage.ShardID                       `json:"unregisteredShards"`
	UnreadyShards      map[storage.ShardID]DiagnoseShardStatus `json:"unreadyShards"`
}

type QueryTableRequest struct {
	ClusterName string   `json:"clusterName"`
	SchemaName  string   `json:"schemaName"`
	Names       []string `json:"names"`
	IDs         []uint64 `json:"ids"`
}

type GetShardTablesRequest struct {
	ClusterName string   `json:"clusterName"`
	ShardIDs    []uint32 `json:"shardIDs"`
}

type TransferLeaderRequest struct {
	ClusterName       string `json:"clusterName"`
	ShardID           uint32 `json:"shardID"`
	OldLeaderNodeName string `json:"OldLeaderNodeName"`
	NewLeaderNodeName string `json:"newLeaderNodeName"`
}

type RouteRequest struct {
	ClusterName string   `json:"clusterName"`
	SchemaName  string   `json:"schemaName"`
	Tables      []string `json:"table"`
}

type NodeShardsRequest struct {
	ClusterName string `json:"clusterName"`
}

type DropNodeShardsRequest struct {
	ClusterName string   `json:"clusterName"`
	ShardIDs    []uint32 `json:"shardIDs"`
}

type DropTableRequest struct {
	ClusterName string `json:"clusterName"`
	SchemaName  string `json:"schemaName"`
	Table       string `json:"table"`
}

type SplitRequest struct {
	ClusterName string   `json:"clusterName"`
	SchemaName  string   `json:"schemaName"`
	ShardID     uint32   `json:"shardID"`
	SplitTables []string `json:"splitTables"`
	NodeName    string   `json:"nodeName"`
}

type CreateClusterRequest struct {
	Name                        string `json:"Name"`
	NodeCount                   uint32 `json:"NodeCount"`
	ShardTotal                  uint32 `json:"ShardTotal"`
	EnableSchedule              bool   `json:"enableSchedule"`
	TopologyType                string `json:"topologyType"`
	ProcedureExecutingBatchSize uint32 `json:"procedureExecutingBatchSize"`
}

type UpdateClusterRequest struct {
	NodeCount                   uint32 `json:"nodeCount"`
	ShardTotal                  uint32 `json:"shardTotal"`
	EnableSchedule              bool   `json:"enableSchedule"`
	TopologyType                string `json:"topologyType"`
	ProcedureExecutingBatchSize uint32 `json:"procedureExecutingBatchSize"`
}

type UpdateFlowLimiterRequest struct {
	Enable bool `json:"enable"`
	Limit  int  `json:"limit"`
	Burst  int  `json:"burst"`
}

type UpdateEnableScheduleRequest struct {
	Enable bool `json:"enable"`
}

type RemoveShardAffinitiesRequest struct {
	ShardIDs []storage.ShardID `json:"shardIDs"`
}

type DiagnoseTableRequest struct {
	ClusterName string   `json:"clusterName"`
	SchemaName  string   `json:"schemaName"`
	Names       []string `json:"names"`
}

type TableInfo struct {
	TableName string          `json:"tableName"`
	NodeName  string          `json:"nodeName"`
	ShardID   storage.ShardID `json:"shardID"`
}

type DiagnoseTable struct {
	Tables          []TableInfo            `json:"tables"`
	PartitionTables map[string][]TableInfo `json:"partitionTables"`
}

type DiagnoseTableResult struct {
	UnExistedTables          []string            `json:"unExistedTables"`
	UnExistedPartitionTables map[string][]string `json:"unExistedPartitionTables"`
	NoShardTables            []string            `json:"noShardTables"`
	NoShardPartitionTables   map[string][]string `json:"noShardPartitionTables"`
	ExistedTables            DiagnoseTable       `json:"existedTables"`
}
