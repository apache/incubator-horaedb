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

package metadata

import (
	"time"

	"github.com/apache/incubator-horaedb-meta/server/storage"
	"github.com/apache/incubator-horaedb-proto/golang/pkg/metaservicepb"
)

const (
	expiredThreshold = time.Second * 10
	MinShardID       = 0
)

type Snapshot struct {
	Topology        Topology
	RegisteredNodes []RegisteredNode
}

type TableInfo struct {
	ID            storage.TableID
	Name          string
	SchemaID      storage.SchemaID
	SchemaName    string
	PartitionInfo storage.PartitionInfo
	CreatedAt     uint64
}

type ShardTables struct {
	Shard  ShardInfo
	Tables []TableInfo
}

type ShardInfo struct {
	ID   storage.ShardID
	Role storage.ShardRole
	// ShardViewVersion
	Version uint64
	// The open state of the shard, which is used to determine whether the shard needs to be opened again.
	Status storage.ShardStatus
}

type ShardNodeWithVersion struct {
	ShardInfo ShardInfo
	ShardNode storage.ShardNode
}

type CreateClusterOpts struct {
	NodeCount                   uint32
	ShardTotal                  uint32
	EnableSchedule              bool
	TopologyType                storage.TopologyType
	ProcedureExecutingBatchSize uint32
}

type UpdateClusterOpts struct {
	TopologyType                storage.TopologyType
	ProcedureExecutingBatchSize uint32
}

type CreateTableMetadataRequest struct {
	SchemaName    string
	TableName     string
	PartitionInfo storage.PartitionInfo
}

type CreateTableMetadataResult struct {
	Table storage.Table
}

type CreateTableRequest struct {
	ShardID       storage.ShardID
	LatestVersion uint64
	SchemaName    string
	TableName     string
	PartitionInfo storage.PartitionInfo
}

type CreateTableResult struct {
	Table              storage.Table
	ShardVersionUpdate ShardVersionUpdate
}

type DropTableRequest struct {
	SchemaName    string
	TableName     string
	ShardID       storage.ShardID
	LatestVersion uint64
}

type DropTableMetadataResult struct {
	Table storage.Table
}

type OpenTableRequest struct {
	SchemaName string
	TableName  string
	ShardID    storage.ShardID
	NodeName   string
}

type CloseTableRequest struct {
	SchemaName string
	TableName  string
	ShardID    storage.ShardID
	NodeName   string
}

type MigrateTableRequest struct {
	SchemaName string
	TableNames []string
	OldShardID storage.ShardID
	// TODO: refactor migrate table request, simplify params.
	latestOldShardVersion uint64
	NewShardID            storage.ShardID
	latestNewShardVersion uint64
}

type ShardVersionUpdate struct {
	ShardID       storage.ShardID
	LatestVersion uint64
}

type RouteEntry struct {
	Table      TableInfo
	NodeShards []ShardNodeWithVersion
}

type RouteTablesResult struct {
	ClusterViewVersion uint64
	RouteEntries       map[string]RouteEntry
}

type GetNodeShardsResult struct {
	ClusterTopologyVersion uint64
	NodeShards             []ShardNodeWithVersion
}

type RegisteredNode struct {
	Node       storage.Node
	ShardInfos []ShardInfo
}

func NewRegisteredNode(meta storage.Node, shardInfos []ShardInfo) RegisteredNode {
	return RegisteredNode{
		meta,
		shardInfos,
	}
}

func (n RegisteredNode) IsExpired(now time.Time) bool {
	expiredTime := time.UnixMilli(int64(n.Node.LastTouchTime)).Add(expiredThreshold)

	return now.After(expiredTime)
}

func ConvertShardsInfoToPB(shard ShardInfo) *metaservicepb.ShardInfo {
	status := storage.ConvertShardStatusToPB(shard.Status)
	return &metaservicepb.ShardInfo{
		Id:      uint32(shard.ID),
		Role:    storage.ConvertShardRoleToPB(shard.Role),
		Version: shard.Version,
		Status:  &status,
	}
}

func ConvertShardsInfoPB(shard *metaservicepb.ShardInfo) ShardInfo {
	return ShardInfo{
		ID:      storage.ShardID(shard.Id),
		Role:    storage.ConvertShardRolePB(shard.Role),
		Version: shard.Version,
		Status:  storage.ConvertShardStatusPB(shard.Status),
	}
}

func ConvertTableInfoToPB(table TableInfo) *metaservicepb.TableInfo {
	return &metaservicepb.TableInfo{
		Id:            uint64(table.ID),
		Name:          table.Name,
		SchemaId:      uint32(table.SchemaID),
		SchemaName:    table.SchemaName,
		PartitionInfo: table.PartitionInfo.Info,
	}
}

func ParseTopologyType(rawString string) (storage.TopologyType, error) {
	switch rawString {
	case storage.TopologyTypeStatic:
		return storage.TopologyTypeStatic, nil
	case storage.TopologyTypeDynamic:
		return storage.TopologyTypeDynamic, nil
	}

	return "", ErrParseTopologyType.WithMessagef("raw type:%s", rawString)
}
