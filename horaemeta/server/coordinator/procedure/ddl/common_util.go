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

package ddl

import (
	"context"

	"github.com/apache/incubator-horaedb-meta/pkg/log"
	"github.com/apache/incubator-horaedb-meta/server/cluster/metadata"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/eventdispatch"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/procedure"
	"github.com/apache/incubator-horaedb-meta/server/storage"
	"github.com/apache/incubator-horaedb-proto/golang/pkg/metaservicepb"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

func CreateTableOnShard(ctx context.Context, c *metadata.ClusterMetadata, dispatch eventdispatch.Dispatch, shardID storage.ShardID, request eventdispatch.CreateTableOnShardRequest) (uint64, error) {
	log.Debug("CreateTableOnShard", zap.Uint64("version", request.UpdateShardInfo.CurrShardInfo.Version))
	shardNodes, err := c.GetShardNodesByShardID(shardID)
	if err != nil {
		return 0, errors.WithMessage(err, "cluster get shardNode by id")
	}
	// TODO: consider followers
	var leader storage.ShardNode
	found := false
	for _, shardNode := range shardNodes {
		if shardNode.ShardRole == storage.ShardRoleLeader {
			found = true
			leader = shardNode
			break
		}
	}
	if !found {
		return 0, procedure.ErrShardLeaderNotFound.WithMessagef("create table on shard, shardID:%d", shardID)
	}

	latestVersion, err := dispatch.CreateTableOnShard(ctx, leader.NodeName, request)
	if err != nil {
		return 0, errors.WithMessage(err, "create table on shard")
	}
	return latestVersion, nil
}

func BuildCreateTableRequest(table storage.Table, shardVersionUpdate metadata.ShardVersionUpdate, req *metaservicepb.CreateTableRequest) eventdispatch.CreateTableOnShardRequest {
	return eventdispatch.CreateTableOnShardRequest{
		UpdateShardInfo: eventdispatch.UpdateShardInfo{
			CurrShardInfo: metadata.ShardInfo{
				ID: shardVersionUpdate.ShardID,
				// TODO: dispatch CreateTableOnShard to followers?
				Role:    storage.ShardRoleLeader,
				Version: shardVersionUpdate.LatestVersion,
				// FIXME: There is no need to update status here, but it must be set. Shall we provide another struct without status field?
				Status: storage.ShardStatusUnknown,
			},
		},
		TableInfo: metadata.TableInfo{
			ID:            table.ID,
			Name:          table.Name,
			SchemaID:      table.SchemaID,
			SchemaName:    req.GetSchemaName(),
			PartitionInfo: table.PartitionInfo,
			CreatedAt:     table.CreatedAt,
		},
		EncodedSchema:    req.EncodedSchema,
		Engine:           req.Engine,
		CreateIfNotExist: req.CreateIfNotExist,
		Options:          req.Options,
	}
}

func GetTableMetadata(clusterMetadata *metadata.ClusterMetadata, schemaName, tableName string) (storage.Table, error) {
	var emptyTable storage.Table
	table, exists, err := clusterMetadata.GetTable(schemaName, tableName)
	if err != nil {
		return emptyTable, err
	}
	if !exists {
		return emptyTable, procedure.ErrTableNotExists.WithMessagef("get table metadata, table:%s", tableName)
	}
	return table, nil
}

// BuildShardVersionUpdate builds metadata.ShardVersionUpdate to assist DDL on the shard.
//
// And if no error is thrown, the returned boolean value is used to tell whether this table is allocated to shard.
// In some cases, we need to use this value to determine whether DDL can be executed normally。
func BuildShardVersionUpdate(table storage.Table, clusterMetadata *metadata.ClusterMetadata, shardVersions map[storage.ShardID]uint64) (metadata.ShardVersionUpdate, bool, error) {
	var versionUpdate metadata.ShardVersionUpdate
	shardNodesResult, err := clusterMetadata.GetShardNodeByTableIDs([]storage.TableID{table.ID})
	if err != nil {
		return versionUpdate, false, err
	}

	var leader storage.ShardNode
	found := false
	for _, shardNode := range shardNodesResult.ShardNodes[table.ID] {
		if shardNode.ShardRole == storage.ShardRoleLeader {
			found = true
			leader = shardNode
			break
		}
	}

	if !found {
		log.Warn("table can't find leader shard", zap.String("tableName", table.Name))
		return versionUpdate, false, nil
	}

	latestVersion, exists := shardVersions[leader.ID]
	if !exists {
		return versionUpdate, false, metadata.ErrShardNotFound.WithMessagef("shard not found in shardVersions when build shard version update, shardID:%d", leader.ID)
	}

	versionUpdate = metadata.ShardVersionUpdate{
		ShardID:       leader.ID,
		LatestVersion: latestVersion,
	}
	return versionUpdate, true, nil
}

func DropTableOnShard(ctx context.Context, clusterMetadata *metadata.ClusterMetadata, dispatch eventdispatch.Dispatch, schemaName string, table storage.Table, version metadata.ShardVersionUpdate) (uint64, error) {
	shardNodes, err := clusterMetadata.GetShardNodesByShardID(version.ShardID)
	if err != nil {
		return 0, errors.WithMessage(err, "cluster get shard by shard id")
	}

	tableInfo := metadata.TableInfo{
		ID:            table.ID,
		Name:          table.Name,
		SchemaID:      table.SchemaID,
		SchemaName:    schemaName,
		PartitionInfo: storage.PartitionInfo{Info: nil},
		CreatedAt:     0,
	}

	var latestVersion uint64
	for _, shardNode := range shardNodes {
		latestVersion, err = dispatch.DropTableOnShard(ctx, shardNode.NodeName, eventdispatch.DropTableOnShardRequest{
			UpdateShardInfo: eventdispatch.UpdateShardInfo{
				CurrShardInfo: metadata.ShardInfo{
					ID:      version.ShardID,
					Role:    storage.ShardRoleLeader,
					Version: version.LatestVersion,
					// FIXME: We have no need to update the status, but it must be set. Maybe we should provide another struct without status field.
					Status: storage.ShardStatusUnknown,
				},
			},
			TableInfo: tableInfo,
		})
		if err != nil {
			return 0, errors.WithMessage(err, "dispatch drop table on shard")
		}
	}

	return latestVersion, nil
}
