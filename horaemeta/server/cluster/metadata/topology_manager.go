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
	"fmt"
	"sync"

	"github.com/apache/incubator-horaedb-meta/server/id"
	"github.com/apache/incubator-horaedb-meta/server/storage"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// TopologyManager manages the cluster topology, including the mapping relationship between shards, nodes, and tables.
type TopologyManager interface {
	// Load load cluster topology from storage.
	Load(ctx context.Context, schemas []storage.Schema) error
	// GetVersion get cluster view version.
	GetVersion() uint64
	// GetClusterState get cluster view state.
	GetClusterState() storage.ClusterState
	// GetTableIDs get shardNode and tablesIDs with shardID and nodeName.
	GetTableIDs(shardIDs []storage.ShardID) map[storage.ShardID]ShardTableIDs
	// AddTable add table to cluster topology.
	AddTable(ctx context.Context, shardID storage.ShardID, latestVersion uint64, tables []storage.Table) error
	// RemoveTable remove table on target shards from cluster topology.
	RemoveTable(ctx context.Context, shardID storage.ShardID, latestVersion uint64, tableIDs []storage.TableID) error
	// GetTableShardID get the shardID of the shard where the table is located.
	GetTableShardID(ctx context.Context, table storage.Table) (storage.ShardID, bool)
	// AssignTableToShard persistent table shard mapping, it is used to store assign results and make the table creation idempotent.
	AssignTableToShard(ctx context.Context, schemaID storage.SchemaID, tableName string, shardID storage.ShardID) error
	// GetTableAssignedShard get table assign result.
	GetTableAssignedShard(ctx context.Context, schemaID storage.SchemaID, tableName string) (storage.ShardID, bool)
	// DeleteTableAssignedShard delete table assign result.
	DeleteTableAssignedShard(ctx context.Context, schemaID storage.SchemaID, tableName string) error
	// GetShards get all shards in cluster topology.
	GetShards() []storage.ShardID
	// GetShardNodesByID get shardNodes with shardID.
	GetShardNodesByID(shardID storage.ShardID) ([]storage.ShardNode, error)
	// GetShardNodesByTableIDs get shardNodes with tableIDs.
	GetShardNodesByTableIDs(tableID []storage.TableID) (GetShardNodesByTableIDsResult, error)
	// GetShardNodes get all shardNodes in cluster topology.
	GetShardNodes() GetShardNodesResult
	// DropShardNodes drop target shardNodes in cluster topology.
	DropShardNodes(ctx context.Context, shardNodes []storage.ShardNode) error
	// InitClusterView init cluster view when create new cluster.
	InitClusterView(ctx context.Context) error
	// UpdateClusterView update cluster view with shardNodes.
	UpdateClusterView(ctx context.Context, state storage.ClusterState, shardNodes []storage.ShardNode) error
	// UpdateClusterViewByNode update cluster view with target shardNodes, it will only update shardNodes corresponding the node name.
	UpdateClusterViewByNode(ctx context.Context, shardNodes map[string][]storage.ShardNode) error
	// GetClusterView return current cluster view.
	GetClusterView() storage.ClusterView
	// CreateShardViews create shardViews.
	CreateShardViews(ctx context.Context, shardViews []CreateShardView) error
	// UpdateShardVersionWithExpect update shard version when pre version is same as expect version.
	UpdateShardVersionWithExpect(ctx context.Context, shardID storage.ShardID, version uint64, expect uint64) error
	// GetTopology get current topology snapshot.
	GetTopology() Topology
}

type ShardTableIDs struct {
	TableIDs []storage.TableID
	Version  uint64
}

type GetShardTablesByNodeResult struct {
	ShardTableIDs map[storage.ShardID]ShardTableIDs
}

type GetShardNodesByTableIDsResult struct {
	ShardNodes map[storage.TableID][]storage.ShardNode
	Version    map[storage.ShardID]uint64
}

type GetShardNodesResult struct {
	ShardNodes []storage.ShardNode
	Versions   map[storage.ShardID]uint64
}

type CreateShardView struct {
	ShardID storage.ShardID
	Tables  []storage.TableID
}

type Topology struct {
	ShardViewsMapping map[storage.ShardID]storage.ShardView
	ClusterView       storage.ClusterView
}

func (t *Topology) IsStable() bool {
	if t.ClusterView.State != storage.ClusterStateStable {
		return false
	}
	if len(t.ClusterView.ShardNodes) != len(t.ShardViewsMapping) {
		return false
	}
	return true
}

func (t *Topology) IsPrepareFinished() bool {
	if t.ClusterView.State != storage.ClusterStatePrepare {
		return false
	}
	if len(t.ShardViewsMapping) != len(t.ClusterView.ShardNodes) {
		return false
	}
	return true
}

type TopologyManagerImpl struct {
	logger       *zap.Logger
	storage      storage.Storage
	clusterID    storage.ClusterID
	shardIDAlloc id.Allocator

	// RWMutex is used to protect following fields.
	lock              sync.RWMutex
	clusterView       *storage.ClusterView                    // ClusterView in memory.
	shardNodesMapping map[storage.ShardID][]storage.ShardNode // ShardID -> nodes of the shard
	nodeShardsMapping map[string][]storage.ShardNode          // nodeName -> shards of the NodeName
	// ShardView in memory.
	shardTablesMapping map[storage.ShardID]*storage.ShardView // ShardID -> shardTopology
	tableShardMapping  map[storage.TableID][]storage.ShardID  // tableID -> ShardID
	// Table assign result in memory.
	tableAssignMapping map[storage.SchemaID]map[string]storage.ShardID // tableName -> shardID

	nodes map[string]storage.Node // NodeName in memory.
}

func NewTopologyManagerImpl(logger *zap.Logger, storage storage.Storage, clusterID storage.ClusterID, shardIDAlloc id.Allocator) TopologyManager {
	return &TopologyManagerImpl{
		logger:       logger,
		storage:      storage,
		clusterID:    clusterID,
		shardIDAlloc: shardIDAlloc,
		lock:         sync.RWMutex{},
		// The following fields will be initialized in the Load method.
		clusterView:        nil,
		shardNodesMapping:  nil,
		nodeShardsMapping:  nil,
		shardTablesMapping: nil,
		tableShardMapping:  nil,
		tableAssignMapping: nil,
		nodes:              nil,
	}
}

func (m *TopologyManagerImpl) Load(ctx context.Context, schemas []storage.Schema) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if err := m.loadClusterView(ctx); err != nil {
		return errors.WithMessage(err, "load cluster view")
	}

	if err := m.loadShardViews(ctx); err != nil {
		return errors.WithMessage(err, "load shard views")
	}

	if err := m.loadNodes(ctx); err != nil {
		return errors.WithMessage(err, "load nodes")
	}

	if err := m.loadAssignTable(ctx, schemas); err != nil {
		return errors.WithMessage(err, "load assign table")
	}

	return nil
}

func (m *TopologyManagerImpl) GetVersion() uint64 {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.clusterView.Version
}

func (m *TopologyManagerImpl) GetClusterState() storage.ClusterState {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.clusterView.State
}

func (m *TopologyManagerImpl) GetTableIDs(shardIDs []storage.ShardID) map[storage.ShardID]ShardTableIDs {
	m.lock.RLock()
	defer m.lock.RUnlock()

	shardTableIDs := make(map[storage.ShardID]ShardTableIDs, len(shardIDs))
	for _, shardID := range shardIDs {
		shardView := m.shardTablesMapping[shardID]
		shardTableIDs[shardID] = ShardTableIDs{
			TableIDs: shardView.TableIDs,
			Version:  shardView.Version,
		}
	}

	return shardTableIDs
}

func (m *TopologyManagerImpl) AddTable(ctx context.Context, shardID storage.ShardID, latestVersion uint64, tables []storage.Table) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	shardView, ok := m.shardTablesMapping[shardID]
	if !ok {
		return ErrShardNotFound.WithMessagef("add table, shardID:%d", shardID)
	}

	tableIDsToAdd := make([]storage.TableID, 0, len(tables))
	for _, table := range tables {
		tableIDsToAdd = append(tableIDsToAdd, table.ID)
	}

	tableIDs := make([]storage.TableID, 0, len(shardView.TableIDs)+1)
	tableIDs = append(tableIDs, shardView.TableIDs...)
	tableIDs = append(tableIDs, tableIDsToAdd...)

	newShardView := storage.NewShardView(shardID, latestVersion, tableIDs)

	// Update shard view in storage.
	err := m.storage.UpdateShardView(ctx, storage.UpdateShardViewRequest{
		ClusterID:   m.clusterID,
		ShardView:   newShardView,
		PrevVersion: shardView.Version,
	})
	if err != nil {
		return errors.WithMessage(err, "storage update shard view")
	}

	// Update shard view in memory.
	m.shardTablesMapping[shardID] = &newShardView
	for _, tableID := range tableIDsToAdd {
		_, exists := m.tableShardMapping[tableID]
		if !exists {
			m.tableShardMapping[tableID] = []storage.ShardID{shardID}
		} else {
			m.tableShardMapping[tableID] = append(m.tableShardMapping[tableID], shardID)
		}
	}

	return nil
}

func (m *TopologyManagerImpl) RemoveTable(ctx context.Context, shardID storage.ShardID, latestVersion uint64, tableIDs []storage.TableID) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	shardView, ok := m.shardTablesMapping[shardID]
	if !ok {
		return ErrShardNotFound.WithMessagef("remove table, shardID:%d", shardID)
	}

	newTableIDs := make([]storage.TableID, 0, len(shardView.TableIDs))
	for _, tableID := range shardView.TableIDs {
		for _, tableIDToRemove := range tableIDs {
			if tableID != tableIDToRemove {
				newTableIDs = append(newTableIDs, tableID)
			}
		}
	}

	// Update shardView in storage.
	newShardView := storage.NewShardView(shardView.ShardID, latestVersion, newTableIDs)
	if err := m.storage.UpdateShardView(ctx, storage.UpdateShardViewRequest{
		ClusterID:   m.clusterID,
		ShardView:   newShardView,
		PrevVersion: shardView.Version,
	}); err != nil {
		return errors.WithMessage(err, "storage update shard view")
	}

	// Update shardView in memory.
	shardView.Version = latestVersion
	shardView.TableIDs = newTableIDs
	for _, tableID := range tableIDs {
		delete(m.tableShardMapping, tableID)
	}

	for i, tableID := range m.shardTablesMapping[shardID].TableIDs {
		for _, tableIDToRemove := range tableIDs {
			if tableIDToRemove == tableID {
				lastElementIndex := len(m.shardTablesMapping[shardID].TableIDs) - 1
				m.shardTablesMapping[shardID].TableIDs[i] = m.shardTablesMapping[shardID].TableIDs[lastElementIndex]
				m.shardTablesMapping[shardID].TableIDs = append(m.shardTablesMapping[shardID].TableIDs[:lastElementIndex], m.shardTablesMapping[shardID].TableIDs[lastElementIndex+1:]...)
			}
		}
	}

	return nil
}

func (m *TopologyManagerImpl) GetTableShardID(_ context.Context, table storage.Table) (storage.ShardID, bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	shardIDs, exists := m.tableShardMapping[table.ID]
	if exists {
		return shardIDs[0], true
	}

	return 0, false
}

func (m *TopologyManagerImpl) AssignTableToShard(ctx context.Context, schemaID storage.SchemaID, tableName string, shardID storage.ShardID) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if err := m.storage.AssignTableToShard(ctx, storage.AssignTableToShardRequest{
		ClusterID: m.clusterID,
		SchemaID:  schemaID,
		TableName: tableName,
		ShardID:   shardID,
	}); err != nil {
		return errors.WithMessage(err, "storage assign table")
	}

	// Update cache im memory.
	if _, exists := m.tableAssignMapping[schemaID]; !exists {
		m.tableAssignMapping[schemaID] = make(map[string]storage.ShardID, 0)
	}

	m.tableAssignMapping[schemaID][tableName] = shardID

	return nil
}

func (m *TopologyManagerImpl) GetTableAssignedShard(_ context.Context, schemaID storage.SchemaID, tableName string) (storage.ShardID, bool) {
	assignResult, exists := m.tableAssignMapping[schemaID][tableName]
	return assignResult, exists
}

func (m *TopologyManagerImpl) DeleteTableAssignedShard(ctx context.Context, schemaID storage.SchemaID, tableName string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if err := m.storage.DeleteTableAssignedShard(ctx, storage.DeleteTableAssignedRequest{
		ClusterID: m.clusterID,
		SchemaID:  schemaID,
		TableName: tableName,
	}); err != nil {
		return errors.WithMessage(err, "storage delete assign table")
	}

	// Update cache im memory.
	delete(m.tableAssignMapping[schemaID], tableName)

	return nil
}

func (m *TopologyManagerImpl) GetShards() []storage.ShardID {
	m.lock.RLock()
	defer m.lock.RUnlock()

	shards := make([]storage.ShardID, 0, len(m.shardTablesMapping))
	for _, shardView := range m.shardTablesMapping {
		shards = append(shards, shardView.ShardID)
	}

	return shards
}

func (m *TopologyManagerImpl) GetShardNodesByID(shardID storage.ShardID) ([]storage.ShardNode, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	shardNodes, ok := m.shardNodesMapping[shardID]
	if !ok {
		return nil, ErrShardNotFound.WithMessagef("get shard nodes by shardID:%d", shardID)
	}

	return shardNodes, nil
}

func (m *TopologyManagerImpl) GetShardNodesByTableIDs(tableIDs []storage.TableID) (GetShardNodesByTableIDsResult, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	tableShardNodes := make(map[storage.TableID][]storage.ShardNode, len(tableIDs))
	shardViewVersions := make(map[storage.ShardID]uint64, 0)
	for _, tableID := range tableIDs {
		shardIDs, ok := m.tableShardMapping[tableID]
		// If the table is not assigned to any shard, return an empty slice.
		if !ok {
			tableShardNodes[tableID] = []storage.ShardNode{}
			continue
		}

		for _, shardID := range shardIDs {
			shardNodes, ok := m.shardNodesMapping[shardID]
			if !ok {
				// If the shard is not assigned to any node, return an empty slice.
				tableShardNodes[tableID] = []storage.ShardNode{}
				continue
			}

			if _, exists := tableShardNodes[tableID]; !exists {
				tableShardNodes[tableID] = shardNodes
			} else {
				tableShardNodes[tableID] = append(tableShardNodes[tableID], shardNodes...)
			}

			_, ok = shardViewVersions[shardID]
			if !ok {
				shardViewVersions[shardID] = m.shardTablesMapping[shardID].Version
			}
		}
	}

	return GetShardNodesByTableIDsResult{
		ShardNodes: tableShardNodes,
		Version:    shardViewVersions,
	}, nil
}

func (m *TopologyManagerImpl) GetShardNodes() GetShardNodesResult {
	m.lock.RLock()
	defer m.lock.RUnlock()

	shardNodes := make([]storage.ShardNode, 0, len(m.shardNodesMapping))
	shardViewVersions := make(map[storage.ShardID]uint64, len(m.shardTablesMapping))
	for _, shardNode := range m.shardNodesMapping {
		shardNodes = append(shardNodes, shardNode...)
	}
	for shardID, shardView := range m.shardTablesMapping {
		shardViewVersions[shardID] = shardView.Version
	}

	return GetShardNodesResult{
		ShardNodes: shardNodes,
		Versions:   shardViewVersions,
	}
}

func (m *TopologyManagerImpl) DropShardNodes(ctx context.Context, shardNodes []storage.ShardNode) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	newShardNodes := make([]storage.ShardNode, 0, len(m.clusterView.ShardNodes))

	for i := 0; i < len(m.clusterView.ShardNodes); i++ {
		if !contains(shardNodes, m.clusterView.ShardNodes[i]) {
			newShardNodes = append(newShardNodes, m.clusterView.ShardNodes[i])
		}
	}

	return m.updateClusterViewWithLock(ctx, m.clusterView.State, newShardNodes)
}

func contains(shardNodes []storage.ShardNode, originShardNode storage.ShardNode) bool {
	for _, dropShardNode := range shardNodes {
		if originShardNode.NodeName == dropShardNode.NodeName && originShardNode.ID == dropShardNode.ID {
			return true
		}
	}
	return false
}

func (m *TopologyManagerImpl) InitClusterView(ctx context.Context) error {
	clusterView := storage.NewClusterView(m.clusterID, 0, storage.ClusterStateEmpty, []storage.ShardNode{})

	err := m.storage.CreateClusterView(ctx, storage.CreateClusterViewRequest{ClusterView: clusterView})
	if err != nil {
		return errors.WithMessage(err, "storage create cluster view")
	}

	m.lock.Lock()
	defer m.lock.Unlock()
	// Load cluster view into memory.
	if err := m.loadClusterView(ctx); err != nil {
		return errors.WithMessage(err, "load cluster view")
	}
	return nil
}

func (m *TopologyManagerImpl) UpdateClusterView(ctx context.Context, state storage.ClusterState, shardNodes []storage.ShardNode) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	return m.updateClusterViewWithLock(ctx, state, shardNodes)
}

func (m *TopologyManagerImpl) updateClusterViewWithLock(ctx context.Context, state storage.ClusterState, shardNodes []storage.ShardNode) error {
	// Update cluster view in storage.
	newClusterView := storage.NewClusterView(m.clusterID, m.clusterView.Version+1, state, shardNodes)
	if err := m.storage.UpdateClusterView(ctx, storage.UpdateClusterViewRequest{
		ClusterID:     m.clusterID,
		ClusterView:   newClusterView,
		LatestVersion: m.clusterView.Version,
	}); err != nil {
		return errors.WithMessage(err, "storage update cluster view")
	}

	// Load cluster view into memory.
	if err := m.loadClusterView(ctx); err != nil {
		return errors.WithMessage(err, "load cluster view")
	}
	return nil
}

func (m *TopologyManagerImpl) UpdateClusterViewByNode(ctx context.Context, shardNodes map[string][]storage.ShardNode) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	newShardNodes := make([]storage.ShardNode, 0, len(m.clusterView.ShardNodes))
	for _, shardNode := range shardNodes {
		newShardNodes = append(newShardNodes, shardNode...)
	}

	originShardNodes := m.clusterView.ShardNodes
	for _, shardNode := range originShardNodes {
		if _, exists := shardNodes[shardNode.NodeName]; !exists {
			newShardNodes = append(newShardNodes, shardNode)
		}
	}

	return m.updateClusterViewWithLock(ctx, m.clusterView.State, newShardNodes)
}

func (m *TopologyManagerImpl) GetClusterView() storage.ClusterView {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return *m.clusterView
}

func (m *TopologyManagerImpl) CreateShardViews(ctx context.Context, createShardViews []CreateShardView) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	// Create shard view in storage.
	shardViews := make([]storage.ShardView, 0, len(createShardViews))
	for _, createShardView := range createShardViews {
		shardViews = append(shardViews, storage.NewShardView(createShardView.ShardID, 0, createShardView.Tables))
	}
	if err := m.storage.CreateShardViews(ctx, storage.CreateShardViewsRequest{
		ClusterID:  m.clusterID,
		ShardViews: shardViews,
	}); err != nil {
		return errors.WithMessage(err, "storage create shard view")
	}

	// Load shard view into memory.
	if err := m.loadShardViews(ctx); err != nil {
		return errors.WithMessage(err, "load shard view")
	}
	return nil
}

func (m *TopologyManagerImpl) UpdateShardVersionWithExpect(ctx context.Context, shardID storage.ShardID, version uint64, expect uint64) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	shardView, ok := m.shardTablesMapping[shardID]
	if !ok {
		return ErrShardNotFound.WithMessagef("update shard version, shardID:%d", shardID)
	}

	newShardView := storage.NewShardView(shardID, version, shardView.TableIDs)
	if err := m.storage.UpdateShardView(ctx, storage.UpdateShardViewRequest{
		ClusterID:   m.clusterID,
		ShardView:   newShardView,
		PrevVersion: expect,
	}); err != nil {
		return errors.WithMessage(err, "storage update shard view")
	}

	// Update shard view into memory.
	m.shardTablesMapping[shardID] = &newShardView

	return nil
}

func (m *TopologyManagerImpl) GetTopology() Topology {
	m.lock.RLock()
	defer m.lock.RUnlock()

	shardViewsMapping := make(map[storage.ShardID]storage.ShardView, len(m.shardTablesMapping))
	for shardID, view := range m.shardTablesMapping {
		shardViewsMapping[shardID] = storage.ShardView{
			ShardID:   view.ShardID,
			Version:   view.Version,
			TableIDs:  view.TableIDs,
			CreatedAt: view.CreatedAt,
		}
	}

	return Topology{
		ShardViewsMapping: shardViewsMapping,
		ClusterView:       *m.clusterView,
	}
}

func (m *TopologyManagerImpl) loadClusterView(ctx context.Context) error {
	clusterViewResult, err := m.storage.GetClusterView(ctx, storage.GetClusterViewRequest{
		ClusterID: m.clusterID,
	})
	if err != nil {
		return errors.WithMessage(err, "storage get cluster view")
	}
	m.logger.Debug("load cluster view", zap.String("clusterViews", fmt.Sprintf("%+v", clusterViewResult)))

	m.shardNodesMapping = make(map[storage.ShardID][]storage.ShardNode, len(clusterViewResult.ClusterView.ShardNodes))
	m.nodeShardsMapping = make(map[string][]storage.ShardNode, len(clusterViewResult.ClusterView.ShardNodes))
	for _, shardNode := range clusterViewResult.ClusterView.ShardNodes {
		m.shardNodesMapping[shardNode.ID] = append(m.shardNodesMapping[shardNode.ID], shardNode)
		m.nodeShardsMapping[shardNode.NodeName] = append(m.nodeShardsMapping[shardNode.NodeName], shardNode)
	}
	m.clusterView = &clusterViewResult.ClusterView

	return nil
}

func (m *TopologyManagerImpl) loadShardViews(ctx context.Context) error {
	shardViewsResult, err := m.storage.ListShardViews(ctx, storage.ListShardViewsRequest{
		ClusterID: m.clusterID,
		ShardIDs:  []storage.ShardID{},
	})
	if err != nil {
		return errors.WithMessage(err, "storage list shard views")
	}
	m.logger.Debug("load shard views", zap.Int32("clusterID", int32(m.clusterID)), zap.String("shardViews", fmt.Sprintf("%+v", shardViewsResult)))

	// Reset data in memory.
	m.shardTablesMapping = make(map[storage.ShardID]*storage.ShardView, len(shardViewsResult.ShardViews))
	m.tableShardMapping = make(map[storage.TableID][]storage.ShardID, 0)
	for _, shardView := range shardViewsResult.ShardViews {
		view := shardView
		m.shardTablesMapping[shardView.ShardID] = &view
		for _, tableID := range shardView.TableIDs {
			if _, exists := m.tableShardMapping[tableID]; !exists {
				m.tableShardMapping[tableID] = []storage.ShardID{}
			}
			m.tableShardMapping[tableID] = append(m.tableShardMapping[tableID], shardView.ShardID)
		}
	}

	return nil
}

func (m *TopologyManagerImpl) loadAssignTable(ctx context.Context, schemas []storage.Schema) error {
	m.tableAssignMapping = make(map[storage.SchemaID]map[string]storage.ShardID, len(schemas))
	for _, schema := range schemas {
		m.tableAssignMapping[schema.ID] = make(map[string]storage.ShardID, 0)

		listAssignTableResult, err := m.storage.ListTableAssignedShard(ctx, storage.ListAssignTableRequest{ClusterID: m.clusterID, SchemaID: schema.ID})
		if err != nil {
			return errors.WithMessage(err, "storage list assign table")
		}
		for _, assignTable := range listAssignTableResult.TableAssigns {
			m.tableAssignMapping[schema.ID][assignTable.TableName] = assignTable.ShardID
		}
	}

	return nil
}

func (m *TopologyManagerImpl) loadNodes(ctx context.Context) error {
	nodesResult, err := m.storage.ListNodes(ctx, storage.ListNodesRequest{ClusterID: m.clusterID})
	if err != nil {
		return errors.WithMessage(err, "storage list nodes")
	}

	// Reset data in memory.
	m.nodes = make(map[string]storage.Node, len(nodesResult.Nodes))
	for _, node := range nodesResult.Nodes {
		m.nodes[node.Name] = node
	}

	return nil
}
