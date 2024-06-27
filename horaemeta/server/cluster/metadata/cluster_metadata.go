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
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"path"
	"sort"
	"sync"

	"github.com/apache/incubator-horaedb-meta/pkg/coderr"
	"github.com/apache/incubator-horaedb-meta/server/id"
	"github.com/apache/incubator-horaedb-meta/server/storage"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	AllocSchemaIDPrefix = "SchemaID"
	AllocTableIDPrefix  = "TableID"
)

type ClusterMetadata struct {
	logger    *zap.Logger
	clusterID storage.ClusterID

	// RWMutex is used to protect following fields.
	// TODO: Encapsulated maps as a specific struct.
	lock     sync.RWMutex
	metaData storage.Cluster

	tableManager    TableManager
	topologyManager TopologyManager

	// Manage the registered nodes from heartbeat.
	registeredNodesCache map[string]RegisteredNode // nodeName -> NodeName

	storage      storage.Storage
	kv           clientv3.KV
	shardIDAlloc id.Allocator
}

func NewClusterMetadata(logger *zap.Logger, meta storage.Cluster, storage storage.Storage, kv clientv3.KV, rootPath string, idAllocatorStep uint) *ClusterMetadata {
	schemaIDAlloc := id.NewAllocatorImpl(logger, kv, path.Join(rootPath, meta.Name, AllocSchemaIDPrefix), idAllocatorStep)
	tableIDAlloc := id.NewAllocatorImpl(logger, kv, path.Join(rootPath, meta.Name, AllocTableIDPrefix), idAllocatorStep)
	// FIXME: Load ShardTopology when cluster create, pass exist ShardID to allocator.
	shardIDAlloc := id.NewReusableAllocatorImpl([]uint64{}, MinShardID)

	cluster := &ClusterMetadata{
		logger:               logger,
		clusterID:            meta.ID,
		lock:                 sync.RWMutex{},
		metaData:             meta,
		tableManager:         NewTableManagerImpl(logger, storage, meta.ID, schemaIDAlloc, tableIDAlloc),
		topologyManager:      NewTopologyManagerImpl(logger, storage, meta.ID, shardIDAlloc),
		registeredNodesCache: map[string]RegisteredNode{},
		storage:              storage,
		kv:                   kv,
		shardIDAlloc:         shardIDAlloc,
	}

	return cluster
}

// Initialize the cluster view and shard view of the cluster.
// It will be used when we create the cluster.
func (c *ClusterMetadata) Init(ctx context.Context) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	createShardViews := make([]CreateShardView, 0, c.metaData.ShardTotal)
	for i := uint32(0); i < c.metaData.ShardTotal; i++ {
		shardID, err := c.AllocShardID(ctx)
		if err != nil {
			return errors.WithMessage(err, "alloc shard id failed")
		}
		createShardViews = append(createShardViews, CreateShardView{
			ShardID: storage.ShardID(shardID),
			Tables:  []storage.TableID{},
		})
	}
	if err := c.topologyManager.CreateShardViews(ctx, createShardViews); err != nil {
		return errors.WithMessage(err, "create shard view")
	}

	return c.topologyManager.InitClusterView(ctx)
}

// Load cluster NodeName from storage into memory.
func (c *ClusterMetadata) Load(ctx context.Context) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if err := c.tableManager.Load(ctx); err != nil {
		return errors.WithMessage(err, "load table manager")
	}

	schemas := c.tableManager.GetSchemas()
	if err := c.topologyManager.Load(ctx, schemas); err != nil {
		return errors.WithMessage(err, "load topology manager")
	}

	return nil
}

func (c *ClusterMetadata) GetClusterID() storage.ClusterID {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.clusterID
}

func (c *ClusterMetadata) Name() string {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.metaData.Name
}

func (c *ClusterMetadata) GetShardTables(shardIDs []storage.ShardID) map[storage.ShardID]ShardTables {
	shardTableIDs := c.topologyManager.GetTableIDs(shardIDs)

	result := make(map[storage.ShardID]ShardTables, len(shardIDs))

	schemas := c.tableManager.GetSchemas()
	schemaByID := make(map[storage.SchemaID]storage.Schema)
	for _, schema := range schemas {
		schemaByID[schema.ID] = schema
	}

	for shardID, shardTableID := range shardTableIDs {
		tables := c.tableManager.GetTablesByIDs(shardTableID.TableIDs)
		tableInfos := make([]TableInfo, 0, len(tables))
		for _, table := range tables {
			schema, ok := schemaByID[table.SchemaID]
			if !ok {
				c.logger.Warn("schema not exits", zap.Uint64("schemaID", uint64(table.SchemaID)))
			}
			tableInfos = append(tableInfos, TableInfo{
				ID:            table.ID,
				Name:          table.Name,
				SchemaID:      table.SchemaID,
				SchemaName:    schema.Name,
				PartitionInfo: table.PartitionInfo,
				CreatedAt:     table.CreatedAt,
			})
		}
		result[shardID] = ShardTables{
			Shard: ShardInfo{
				ID:      shardID,
				Role:    storage.ShardRoleLeader,
				Version: shardTableID.Version,
				Status:  storage.ShardStatusUnknown,
			},
			Tables: tableInfos,
		}
	}

	for _, shardID := range shardIDs {
		_, exists := result[shardID]
		if !exists {
			result[shardID] = ShardTables{
				Shard: ShardInfo{
					ID:      shardID,
					Role:    storage.ShardRoleLeader,
					Version: 0,
					Status:  storage.ShardStatusUnknown,
				},
				Tables: []TableInfo{},
			}
		}
	}
	return result
}

// DropTable will drop table metadata and all mapping of this table.
// If the table to be dropped has been opened multiple times, all its mapping will be dropped.
func (c *ClusterMetadata) DropTable(ctx context.Context, request DropTableRequest) error {
	c.logger.Info("drop table start", zap.String("cluster", c.Name()), zap.String("schemaName", request.SchemaName), zap.String("tableName", request.TableName))

	if !c.ensureClusterStable() {
		return ErrClusterStateInvalid.WithMessagef("invalid cluster state, cluster state must be stable")
	}

	table, ok, err := c.tableManager.GetTable(request.SchemaName, request.TableName)
	if err != nil {
		return coderr.Wrapf(err, "get table before dropping, table:%s", request.TableName)
	}
	if !ok {
		return ErrTableNotFound.WithMessagef("drop a non-exist table, table:%s", request.TableName)
	}

	// Drop table.
	err = c.tableManager.DropTable(ctx, request.SchemaName, request.TableName)
	if err != nil {
		return coderr.Wrapf(err, "drop table:%s", request.TableName)
	}

	// Remove dropped table in shard view.
	err = c.topologyManager.RemoveTable(ctx, request.ShardID, request.LatestVersion, []storage.TableID{table.ID})
	if err != nil {
		return coderr.Wrapf(err, "topology manager remove table:%s", request.TableName)
	}

	c.logger.Info("drop table success", zap.String("cluster", c.Name()), zap.String("schemaName", request.SchemaName), zap.String("tableName", request.TableName))
	return nil
}

// MigrateTable used to migrate tables from old shard to new shard.
// The mapping relationship between table and shard will be modified.
func (c *ClusterMetadata) MigrateTable(ctx context.Context, request MigrateTableRequest) error {
	c.logger.Info("migrate table", zap.String("request", fmt.Sprintf("%v", request)))

	if !c.ensureClusterStable() {
		return ErrClusterStateInvalid.WithMessagef("invalid cluster state, cluster state must be stable")
	}

	tables := make([]storage.Table, 0, len(request.TableNames))
	tableIDs := make([]storage.TableID, 0, len(request.TableNames))

	for _, tableName := range request.TableNames {
		table, exists, err := c.tableManager.GetTable(request.SchemaName, tableName)
		if err != nil {
			c.logger.Error("get table", zap.Error(err), zap.String("schemaName", request.SchemaName), zap.String("tableName", tableName))
			return err
		}

		if !exists {
			c.logger.Error("the table to be closed does not exist", zap.String("schemaName", request.SchemaName), zap.String("tableName", tableName))
			return ErrTableNotFound.WithMessagef("migrate a non-exist table, schema:%s, table:%s", request.SchemaName, tableName)
		}

		tables = append(tables, table)
		tableIDs = append(tableIDs, table.ID)
	}

	if err := c.topologyManager.RemoveTable(ctx, request.OldShardID, request.latestOldShardVersion, tableIDs); err != nil {
		c.logger.Error("remove table from topology")
		return err
	}

	if err := c.topologyManager.AddTable(ctx, request.NewShardID, request.latestNewShardVersion, tables); err != nil {
		c.logger.Error("add table from topology")
		return err
	}

	c.logger.Info("migrate table finish", zap.String("request", fmt.Sprintf("%v", request)))
	return nil
}

// GetOrCreateSchema the second output parameter bool: returns true if the schema was newly created.
func (c *ClusterMetadata) GetOrCreateSchema(ctx context.Context, schemaName string) (storage.Schema, bool, error) {
	return c.tableManager.GetOrCreateSchema(ctx, schemaName)
}

// GetTable the second output parameter bool: returns true if the table exists.
func (c *ClusterMetadata) GetTable(schemaName, tableName string) (storage.Table, bool, error) {
	return c.tableManager.GetTable(schemaName, tableName)
}

// GetTableShard get the shard where the table actually exists.
func (c *ClusterMetadata) GetTableShard(ctx context.Context, table storage.Table) (storage.ShardID, bool) {
	return c.topologyManager.GetTableShardID(ctx, table)
}

func (c *ClusterMetadata) CreateTableMetadata(ctx context.Context, request CreateTableMetadataRequest) (CreateTableMetadataResult, error) {
	c.logger.Info("create table start", zap.String("cluster", c.Name()), zap.String("schemaName", request.SchemaName), zap.String("tableName", request.TableName))

	var createRes CreateTableMetadataResult
	if !c.ensureClusterStable() {
		return createRes, ErrClusterStateInvalid.WithMessagef("create tables on an unstable cluster:%s", c.metaData.Name)
	}

	_, exists, err := c.tableManager.GetTable(request.SchemaName, request.TableName)
	if err != nil {
		return createRes, err
	}
	if exists {
		return createRes, ErrTableAlreadyExists.WithMessagef("table to create already exists, table:%s", request.TableName)
	}

	// Create table in table manager.
	table, err := c.tableManager.CreateTable(ctx, request.SchemaName, request.TableName, request.PartitionInfo)
	if err != nil {
		return createRes, errors.WithMessage(err, "table manager create table")
	}

	c.logger.Info("create table metadata succeed", zap.String("cluster", c.Name()), zap.String("table", fmt.Sprintf("%+v", table)))

	createRes = CreateTableMetadataResult{
		Table: table,
	}
	return createRes, nil
}

func (c *ClusterMetadata) AddTableTopology(ctx context.Context, shardVersionUpdate ShardVersionUpdate, table storage.Table) error {
	c.logger.Info("add table topology start", zap.String("cluster", c.Name()), zap.String("tableName", table.Name))

	if !c.ensureClusterStable() {
		return ErrClusterStateInvalid.WithMessagef("add table topology on an unstable cluster, table:%s", table.Name)
	}

	// Add table to topology manager.
	err := c.topologyManager.AddTable(ctx, shardVersionUpdate.ShardID, shardVersionUpdate.LatestVersion, []storage.Table{table})
	if err != nil {
		return errors.WithMessage(err, "topology manager add table")
	}

	c.logger.Info("add table topology succeed", zap.String("cluster", c.Name()), zap.String("table", fmt.Sprintf("%+v", table)), zap.String("shardVersionUpdate", fmt.Sprintf("%+v", shardVersionUpdate)))
	return nil
}

func (c *ClusterMetadata) DropTableMetadata(ctx context.Context, schemaName, tableName string) (DropTableMetadataResult, error) {
	c.logger.Info("drop table start", zap.String("cluster", c.Name()), zap.String("schemaName", schemaName), zap.String("tableName", tableName))

	var dropRes DropTableMetadataResult
	if !c.ensureClusterStable() {
		return dropRes, ErrClusterStateInvalid.WithMessagef("drop table on an unstable cluster, table:%s", tableName)
	}

	table, ok, err := c.tableManager.GetTable(schemaName, tableName)
	if err != nil {
		return dropRes, errors.WithMessage(err, "get table")
	}
	if !ok {
		return dropRes, ErrTableNotFound.WithMessagef("drop a non-exist table:%s", tableName)
	}

	err = c.tableManager.DropTable(ctx, schemaName, tableName)
	if err != nil {
		return dropRes, coderr.Wrapf(err, "failed to drop table:%s", tableName)
	}

	c.logger.Info("drop table metadata success", zap.String("cluster", c.Name()), zap.String("schemaName", schemaName), zap.String("tableName", tableName), zap.String("result", fmt.Sprintf("%+v", table)))
	dropRes = DropTableMetadataResult{Table: table}
	return dropRes, nil
}

func (c *ClusterMetadata) CreateTable(ctx context.Context, request CreateTableRequest) (CreateTableResult, error) {
	c.logger.Info("create table start", zap.String("cluster", c.Name()), zap.String("schemaName", request.SchemaName), zap.String("tableName", request.TableName))

	var createRes CreateTableResult
	if !c.ensureClusterStable() {
		return createRes, ErrClusterStateInvalid.WithMessagef("create table on an unstable cluster, table:%s", request.TableName)
	}

	_, exists, err := c.tableManager.GetTable(request.SchemaName, request.TableName)
	if err != nil {
		return createRes, err
	}
	if exists {
		return createRes, ErrTableAlreadyExists.WithMessagef("create an tableName:%s", request.TableName)
	}

	// Create table in table manager.
	table, err := c.tableManager.CreateTable(ctx, request.SchemaName, request.TableName, request.PartitionInfo)
	if err != nil {
		return createRes, coderr.Wrapf(err, "table manager fails to create table:%s", request.TableName)
	}

	// Add table to topology manager.
	err = c.topologyManager.AddTable(ctx, request.ShardID, request.LatestVersion, []storage.Table{table})
	if err != nil {
		return createRes, coderr.Wrapf(err, "topology manager add table failed when creating table:%s", request.TableName)
	}

	createRes = CreateTableResult{
		Table: table,
		ShardVersionUpdate: ShardVersionUpdate{
			ShardID:       request.ShardID,
			LatestVersion: request.LatestVersion,
		},
	}
	c.logger.Info("create table succeed", zap.String("cluster", c.Name()), zap.String("result", fmt.Sprintf("%+v", createRes)))
	return createRes, nil
}

func (c *ClusterMetadata) GetTableAssignedShard(ctx context.Context, schemaName string, tableName string) (storage.ShardID, bool, error) {
	schema, exists := c.tableManager.GetSchema(schemaName)
	if !exists {
		return 0, false, ErrSchemaNotFound.WithMessagef("get table assigned shard, schema:", schemaName)
	}
	shardIDs, exists := c.topologyManager.GetTableAssignedShard(ctx, schema.ID, tableName)
	return shardIDs, exists, nil
}

func (c *ClusterMetadata) AssignTableToShard(ctx context.Context, schemaName string, tableName string, shardID storage.ShardID) error {
	schema, exists := c.tableManager.GetSchema(schemaName)
	if !exists {
		return ErrSchemaNotFound.WithMessagef("assign table to a shard, schema:%s", schemaName)
	}
	return c.topologyManager.AssignTableToShard(ctx, schema.ID, tableName, shardID)
}

func (c *ClusterMetadata) DeleteTableAssignedShard(ctx context.Context, schemaName string, tableName string) error {
	schema, exists := c.tableManager.GetSchema(schemaName)
	if !exists {
		return ErrSchemaNotFound.WithMessagef("delete table's assigned shard, schema:%s", schemaName)
	}
	return c.topologyManager.DeleteTableAssignedShard(ctx, schema.ID, tableName)
}

func (c *ClusterMetadata) GetShards() []storage.ShardID {
	return c.topologyManager.GetShards()
}

func (c *ClusterMetadata) GetShardNodesByShardID(id storage.ShardID) ([]storage.ShardNode, error) {
	return c.topologyManager.GetShardNodesByID(id)
}

func (c *ClusterMetadata) GetShardNodeByTableIDs(tableIDs []storage.TableID) (GetShardNodesByTableIDsResult, error) {
	return c.topologyManager.GetShardNodesByTableIDs(tableIDs)
}

func (c *ClusterMetadata) RegisterNode(ctx context.Context, registeredNode RegisteredNode) error {
	registeredNode.Node.State = storage.NodeStateOnline
	err := c.storage.CreateOrUpdateNode(ctx, storage.CreateOrUpdateNodeRequest{
		ClusterID: c.clusterID,
		Node:      registeredNode.Node,
	})
	if err != nil {
		return errors.WithMessage(err, "create or update registered node")
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	// When the number of nodes in the cluster reaches the threshold, modify the cluster status to prepare.
	// TODO: Consider the design of the entire cluster state, which may require refactoring.
	if uint32(len(c.registeredNodesCache)) >= c.metaData.MinNodeCount && c.topologyManager.GetClusterState() == storage.ClusterStateEmpty {
		if err := c.UpdateClusterView(ctx, storage.ClusterStatePrepare, []storage.ShardNode{}); err != nil {
			c.logger.Error("update cluster view failed", zap.Error(err))
		}
	}

	// Update shard node mapping.
	// Check whether to update persistence data.
	oldCache, exists := c.registeredNodesCache[registeredNode.Node.Name]
	c.registeredNodesCache[registeredNode.Node.Name] = registeredNode
	enableUpdateWhenStable := c.metaData.TopologyType == storage.TopologyTypeDynamic
	if !enableUpdateWhenStable && c.topologyManager.GetClusterState() == storage.ClusterStateStable {
		return nil
	}
	if exists && !needUpdate(oldCache, registeredNode) {
		// Check whether the shard versions need to be corrected.
		c.maybeCorrectShardVersion(ctx, registeredNode)
		return nil
	}

	shardNodes := make(map[string][]storage.ShardNode, 1)
	shardNodes[registeredNode.Node.Name] = make([]storage.ShardNode, 0, len(registeredNode.ShardInfos))
	for _, shardInfo := range registeredNode.ShardInfos {
		shardNodes[registeredNode.Node.Name] = append(shardNodes[registeredNode.Node.Name], storage.ShardNode{
			ID:        shardInfo.ID,
			ShardRole: shardInfo.Role,
			NodeName:  registeredNode.Node.Name,
		})
	}

	if err := c.UpdateClusterViewByNode(ctx, shardNodes); err != nil {
		return errors.WithMessage(err, "update cluster view failed")
	}

	return nil
}

func (c *ClusterMetadata) GetRegisteredNodes() []RegisteredNode {
	c.lock.RLock()
	defer c.lock.RUnlock()

	nodes := make([]RegisteredNode, 0, len(c.registeredNodesCache))
	for _, node := range c.registeredNodesCache {
		nodes = append(nodes, node)
	}
	return nodes
}

func (c *ClusterMetadata) GetRegisteredNodeByName(nodeName string) (RegisteredNode, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	registeredNode, ok := c.registeredNodesCache[nodeName]
	return registeredNode, ok
}

func (c *ClusterMetadata) AllocShardID(ctx context.Context) (uint32, error) {
	id, err := c.shardIDAlloc.Alloc(ctx)
	if err != nil {
		return 0, errors.WithMessage(err, "alloc shard id")
	}
	return uint32(id), nil
}

func (c *ClusterMetadata) RouteTables(_ context.Context, schemaName string, tableNames []string) (RouteTablesResult, error) {
	routeEntries := make(map[string]RouteEntry, len(tableNames))
	tables := make(map[storage.TableID]storage.Table, len(tableNames))
	tableIDs := make([]storage.TableID, 0, len(tableNames))
	for _, tableName := range tableNames {
		table, exists, err := c.tableManager.GetTable(schemaName, tableName)
		if err != nil {
			return RouteTablesResult{}, errors.WithMessage(err, "table manager get table")
		}
		if !exists {
			continue
		}

		// TODO: Adapt to the current implementation of the partition table, which may need to be reconstructed later.
		if !table.IsPartitioned() {
			tables[table.ID] = table
			tableIDs = append(tableIDs, table.ID)
		} else {
			routeEntries[table.Name] = RouteEntry{
				Table: TableInfo{
					ID:            table.ID,
					Name:          table.Name,
					SchemaID:      table.SchemaID,
					SchemaName:    schemaName,
					PartitionInfo: table.PartitionInfo,
					CreatedAt:     table.CreatedAt,
				},
				NodeShards: nil,
			}
		}
	}

	tableShardNodesWithShardViewVersion, err := c.topologyManager.GetShardNodesByTableIDs(tableIDs)
	if err != nil {
		return RouteTablesResult{}, errors.WithMessage(err, "topology get shard nodes by table ids")
	}
	for tableID, value := range tableShardNodesWithShardViewVersion.ShardNodes {
		nodeShards := make([]ShardNodeWithVersion, 0, len(value))
		for _, shardNode := range value {
			nodeShards = append(nodeShards, ShardNodeWithVersion{
				ShardInfo: ShardInfo{
					ID:      shardNode.ID,
					Role:    shardNode.ShardRole,
					Version: tableShardNodesWithShardViewVersion.Version[shardNode.ID],
					Status:  storage.ShardStatusUnknown,
				},
				ShardNode: shardNode,
			})
		}
		// If nodeShards length bigger than 1, randomly select a nodeShard.
		nodeShardsResult := nodeShards
		if len(nodeShards) > 1 {
			selectIndex, err2 := rand.Int(rand.Reader, big.NewInt(int64(len(nodeShards))))
			if err2 != nil {
				return RouteTablesResult{}, errors.WithMessage(err2, "generate random node index")
			}
			nodeShardsResult = []ShardNodeWithVersion{nodeShards[selectIndex.Uint64()]}
		}
		table := tables[tableID]
		routeEntries[table.Name] = RouteEntry{
			Table: TableInfo{
				ID:            table.ID,
				Name:          table.Name,
				SchemaID:      table.SchemaID,
				SchemaName:    schemaName,
				PartitionInfo: table.PartitionInfo,
				CreatedAt:     table.CreatedAt,
			},
			NodeShards: nodeShardsResult,
		}
	}
	return RouteTablesResult{
		ClusterViewVersion: c.topologyManager.GetVersion(),
		RouteEntries:       routeEntries,
	}, nil
}

func (c *ClusterMetadata) GetNodeShards(_ context.Context) (GetNodeShardsResult, error) {
	getNodeShardsResult := c.topologyManager.GetShardNodes()

	shardNodesWithVersion := make([]ShardNodeWithVersion, 0, len(getNodeShardsResult.ShardNodes))

	for _, shardNode := range getNodeShardsResult.ShardNodes {
		shardNodesWithVersion = append(shardNodesWithVersion, ShardNodeWithVersion{
			ShardInfo: ShardInfo{
				ID:      shardNode.ID,
				Role:    shardNode.ShardRole,
				Version: getNodeShardsResult.Versions[shardNode.ID],
				Status:  storage.ShardStatusUnknown,
			},
			ShardNode: shardNode,
		})
	}

	return GetNodeShardsResult{
		ClusterTopologyVersion: c.topologyManager.GetVersion(),
		NodeShards:             shardNodesWithVersion,
	}, nil
}

func (c *ClusterMetadata) GetClusterViewVersion() uint64 {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.topologyManager.GetVersion()
}

func (c *ClusterMetadata) GetClusterMinNodeCount() uint32 {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.metaData.MinNodeCount
}

func (c *ClusterMetadata) GetTotalShardNum() uint32 {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.metaData.ShardTotal
}

func (c *ClusterMetadata) GetTopologyType() storage.TopologyType {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.metaData.TopologyType
}

func (c *ClusterMetadata) GetProcedureExecutingBatchSize() uint32 {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.metaData.ProcedureExecutingBatchSize
}

func (c *ClusterMetadata) GetCreateTime() uint64 {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.metaData.CreatedAt
}

func (c *ClusterMetadata) GetClusterState() storage.ClusterState {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.topologyManager.GetClusterState()
}

func (c *ClusterMetadata) ensureClusterStable() bool {
	return c.GetClusterState() == storage.ClusterStateStable
}

func (c *ClusterMetadata) GetClusterView() storage.ClusterView {
	return c.topologyManager.GetClusterView()
}

func (c *ClusterMetadata) UpdateClusterView(ctx context.Context, state storage.ClusterState, shardNodes []storage.ShardNode) error {
	if err := c.topologyManager.UpdateClusterView(ctx, state, shardNodes); err != nil {
		return errors.WithMessage(err, "update cluster view")
	}
	return nil
}

func (c *ClusterMetadata) UpdateClusterViewByNode(ctx context.Context, shardNodes map[string][]storage.ShardNode) error {
	if err := c.topologyManager.UpdateClusterViewByNode(ctx, shardNodes); err != nil {
		return errors.WithMessage(err, "update cluster view")
	}
	return nil
}

func (c *ClusterMetadata) DropShardNodes(ctx context.Context, shardNodes []storage.ShardNode) error {
	if err := c.topologyManager.DropShardNodes(ctx, shardNodes); err != nil {
		return errors.WithMessage(err, "drop shard nodes")
	}
	return nil
}

func (c *ClusterMetadata) CreateShardViews(ctx context.Context, views []CreateShardView) error {
	if err := c.topologyManager.CreateShardViews(ctx, views); err != nil {
		return errors.WithMessage(err, "topology manager create shard views")
	}

	return nil
}

func (c *ClusterMetadata) GetClusterSnapshot() Snapshot {
	return Snapshot{
		Topology:        c.topologyManager.GetTopology(),
		RegisteredNodes: c.GetRegisteredNodes(),
	}
}

func (c *ClusterMetadata) GetStorageMetadata() storage.Cluster {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.metaData
}

// LoadMetadata load cluster metadata from storage.
func (c *ClusterMetadata) LoadMetadata(ctx context.Context) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	metadata, err := c.storage.GetCluster(ctx, c.clusterID)
	if err != nil {
		return errors.WithMessage(err, "get cluster")
	}
	c.metaData = metadata
	return nil
}

func (c *ClusterMetadata) GetShardNodes() GetShardNodesResult {
	return c.topologyManager.GetShardNodes()
}

func (c *ClusterMetadata) GetTables(schemaName string, tableNames []string) ([]storage.Table, error) {
	return c.tableManager.GetTables(schemaName, tableNames)
}

func (c *ClusterMetadata) GetTablesByIDs(tableIDs []storage.TableID) []storage.Table {
	return c.tableManager.GetTablesByIDs(tableIDs)
}

func needUpdate(oldCache RegisteredNode, registeredNode RegisteredNode) bool {
	if len(oldCache.ShardInfos) >= 50 {
		return !sortCompare(oldCache.ShardInfos, registeredNode.ShardInfos)
	}
	return !simpleCompare(oldCache.ShardInfos, registeredNode.ShardInfos)
}

// sortCompare compare if they are the same by sorted slice, return true when they are the same.
func sortCompare(oldShardInfos, newShardInfos []ShardInfo) bool {
	if len(oldShardInfos) != len(newShardInfos) {
		return false
	}
	oldShardIDs := make([]storage.ShardID, 0, len(oldShardInfos))
	for i := 0; i < len(oldShardInfos); i++ {
		oldShardIDs = append(oldShardIDs, oldShardInfos[i].ID)
	}
	sort.Slice(oldShardIDs, func(i, j int) bool {
		return oldShardIDs[i] < oldShardIDs[j]
	})
	curShardIDs := make([]storage.ShardID, 0, len(newShardInfos))
	for i := 0; i < len(newShardInfos); i++ {
		curShardIDs = append(curShardIDs, newShardInfos[i].ID)
	}
	sort.Slice(curShardIDs, func(i, j int) bool {
		return curShardIDs[i] < curShardIDs[j]
	})
	for i := 0; i < len(curShardIDs); i++ {
		if curShardIDs[i] != oldShardIDs[i] {
			return false
		}
	}
	return true
}

// simpleCompare compare if they are the same by simple loop, return true when they are the same.
func simpleCompare(oldShardInfos, newShardInfos []ShardInfo) bool {
	if len(oldShardInfos) != len(newShardInfos) {
		return false
	}
L1:
	for i := 0; i < len(newShardInfos); i++ {
		for j := 0; j < len(newShardInfos); j++ {
			if oldShardInfos[i].ID == newShardInfos[j].ID {
				continue L1
			}
		}
		return false
	}

	return true
}

func (c *ClusterMetadata) maybeCorrectShardVersion(ctx context.Context, node RegisteredNode) {
	topology := c.topologyManager.GetTopology()
	for _, shardInfo := range node.ShardInfos {
		oldShardView, ok := topology.ShardViewsMapping[shardInfo.ID]
		if !ok {
			c.logger.Error("shard out found in topology", zap.Uint32("shardID", uint32(shardInfo.ID)))
			return
		}
		if oldShardView.Version != shardInfo.Version {
			c.logger.Warn("shard version mismatch", zap.Uint32("shardID", uint32(shardInfo.ID)), zap.Uint64("metaVersion", oldShardView.Version), zap.Uint64("nodeVersion", shardInfo.Version))
		}
		if oldShardView.Version < shardInfo.Version {
			// Shard version in meta not equal to HoraeDB, it is needed to be corrected.
			// Update with expect value.
			c.logger.Info("try to update shard version", zap.Uint32("shardID", uint32(shardInfo.ID)), zap.Uint64("expectVersion", oldShardView.Version), zap.Uint64("newVersion", shardInfo.Version))
			if err := c.topologyManager.UpdateShardVersionWithExpect(ctx, shardInfo.ID, shardInfo.Version, oldShardView.Version); err != nil {
				c.logger.Warn("update shard version with expect failed", zap.Uint32("shardID", uint32(shardInfo.ID)), zap.Uint64("expectVersion", oldShardView.Version), zap.Uint64("newVersion", shardInfo.Version))
			}
			// TODO: Maybe we need do some thing to ensure HoraeDB status after update shard version.
		}
	}
}
