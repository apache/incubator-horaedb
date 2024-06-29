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

package cluster

import (
	"context"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/apache/incubator-horaedb-meta/pkg/log"
	"github.com/apache/incubator-horaedb-meta/server/cluster/metadata"
	"github.com/apache/incubator-horaedb-meta/server/id"
	"github.com/apache/incubator-horaedb-meta/server/storage"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	AllocClusterIDPrefix = "ClusterID"
)

type Manager interface {
	// Start must be called before manager is used.
	Start(ctx context.Context) error
	// Stop must be called before manager is dropped.
	Stop(ctx context.Context) error

	ListClusters(ctx context.Context) ([]*Cluster, error)
	CreateCluster(ctx context.Context, clusterName string, opts metadata.CreateClusterOpts) (*Cluster, error)
	UpdateCluster(ctx context.Context, clusterName string, opt metadata.UpdateClusterOpts) error
	GetCluster(ctx context.Context, clusterName string) (*Cluster, error)
	// AllocSchemaID means get or create schema.
	// The second output parameter bool: Returns true if the table was newly created.
	AllocSchemaID(ctx context.Context, clusterName, schemaName string) (storage.SchemaID, bool, error)
	GetTables(clusterName, schemaName string, tableNames []string) ([]metadata.TableInfo, error)
	GetTablesByIDs(clusterName string, tableID []storage.TableID) ([]metadata.TableInfo, error)
	GetTablesByShardIDs(clusterName, nodeName string, shardIDs []storage.ShardID) (map[storage.ShardID]metadata.ShardTables, error)
	DropTable(ctx context.Context, clusterName, schemaName, tableName string) error
	RouteTables(ctx context.Context, clusterName, schemaName string, tableNames []string) (metadata.RouteTablesResult, error)
	GetNodeShards(ctx context.Context, clusterName string) (metadata.GetNodeShardsResult, error)

	RegisterNode(ctx context.Context, clusterName string, registeredNode metadata.RegisteredNode) error
	GetRegisteredNode(ctx context.Context, clusterName string, node string) (metadata.RegisteredNode, error)
	ListRegisteredNodes(ctx context.Context, clusterName string) ([]metadata.RegisteredNode, error)
}

type managerImpl struct {
	// RWMutex is used to protect clusters when creating new cluster.
	lock     sync.RWMutex
	running  bool
	clusters map[string]*Cluster

	storage         storage.Storage
	kv              clientv3.KV
	client          *clientv3.Client
	alloc           id.Allocator
	rootPath        string
	idAllocatorStep uint

	// TODO: topologyType is used to be compatible with cluster data changes and needs to be deleted later.
	topologyType storage.TopologyType
}

func NewManagerImpl(storage storage.Storage, kv clientv3.KV, client *clientv3.Client, rootPath string, idAllocatorStep uint, topologyType storage.TopologyType) (Manager, error) {
	alloc := id.NewAllocatorImpl(log.GetLogger(), kv, path.Join(rootPath, AllocClusterIDPrefix), idAllocatorStep)

	manager := &managerImpl{
		lock:     sync.RWMutex{},
		running:  false,
		clusters: map[string]*Cluster{},

		kv:              kv,
		storage:         storage,
		client:          client,
		alloc:           alloc,
		rootPath:        rootPath,
		idAllocatorStep: idAllocatorStep,
		topologyType:    topologyType,
	}

	return manager, nil
}

func (m *managerImpl) ListClusters(_ context.Context) ([]*Cluster, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	clusters := make([]*Cluster, 0, len(m.clusters))
	for _, cluster := range m.clusters {
		clusters = append(clusters, cluster)
	}
	return clusters, nil
}

func (m *managerImpl) CreateCluster(ctx context.Context, clusterName string, opts metadata.CreateClusterOpts) (*Cluster, error) {
	if opts.NodeCount < 1 {
		log.Error("cluster's nodeCount must > 0", zap.String("clusterName", clusterName))
		return nil, metadata.ErrCreateCluster.WithCausef("nodeCount must > 0")
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	cluster, ok := m.clusters[clusterName]
	if ok {
		return cluster, metadata.ErrClusterAlreadyExists
	}

	clusterID, err := m.allocClusterID(ctx)
	if err != nil {
		log.Error("fail to alloc cluster id", zap.Error(err), zap.String("clusterName", clusterName))
		return nil, errors.WithMessagef(err, "cluster manager CreateCluster, clusterName:%s", clusterName)
	}

	createTime := time.Now().UnixMilli()
	clusterMetadataStorage := storage.Cluster{
		ID:                          clusterID,
		Name:                        clusterName,
		MinNodeCount:                opts.NodeCount,
		ShardTotal:                  opts.ShardTotal,
		TopologyType:                opts.TopologyType,
		ProcedureExecutingBatchSize: opts.ProcedureExecutingBatchSize,
		CreatedAt:                   uint64(createTime),
		ModifiedAt:                  uint64(createTime),
	}
	err = m.storage.CreateCluster(ctx, storage.CreateClusterRequest{
		Cluster: clusterMetadataStorage,
	})
	if err != nil {
		log.Error("fail to create cluster", zap.Error(err), zap.String("clusterName", clusterName))
		return nil, errors.WithMessage(err, "cluster create cluster")
	}

	logger := log.With(zap.String("clusterName", clusterName))

	clusterMetadata := metadata.NewClusterMetadata(logger, clusterMetadataStorage, m.storage, m.kv, m.rootPath, m.idAllocatorStep)

	if err = clusterMetadata.Init(ctx); err != nil {
		log.Error("fail to init cluster", zap.Error(err), zap.String("clusterName", clusterName))
		return nil, errors.WithMessage(err, "cluster init")
	}

	if err := clusterMetadata.Load(ctx); err != nil {
		log.Error("fail to load cluster", zap.Error(err), zap.String("clusterName", clusterName))
		return nil, errors.WithMessage(err, "cluster load")
	}

	c, err := NewCluster(logger, clusterMetadata, m.client, m.rootPath)
	if err != nil {
		return nil, errors.WithMessage(err, "new cluster")
	}
	m.clusters[clusterName] = c

	if err := c.Start(ctx); err != nil {
		return nil, errors.WithMessage(err, "start cluster")
	}

	return c, nil
}

func (m *managerImpl) UpdateCluster(ctx context.Context, clusterName string, opt metadata.UpdateClusterOpts) error {
	c, err := m.getCluster(clusterName)
	if err != nil {
		log.Error("get cluster", zap.Error(err))
		return err
	}

	err = m.storage.UpdateCluster(ctx, storage.UpdateClusterRequest{Cluster: storage.Cluster{
		ID:                          c.GetMetadata().GetClusterID(),
		Name:                        c.GetMetadata().Name(),
		MinNodeCount:                c.GetMetadata().GetClusterMinNodeCount(),
		ShardTotal:                  c.GetMetadata().GetTotalShardNum(),
		TopologyType:                opt.TopologyType,
		ProcedureExecutingBatchSize: opt.ProcedureExecutingBatchSize,
		CreatedAt:                   c.GetMetadata().GetCreateTime(),
		ModifiedAt:                  uint64(time.Now().UnixMilli()),
	}})
	if err != nil {
		log.Error("update cluster", zap.Error(err))
		return err
	}

	if err := c.GetMetadata().LoadMetadata(ctx); err != nil {
		log.Error("fail to load cluster", zap.Error(err), zap.String("clusterName", clusterName))
		return err
	}

	return nil
}

func (m *managerImpl) GetCluster(_ context.Context, clusterName string) (*Cluster, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	cluster, exist := m.clusters[clusterName]
	if exist {
		return cluster, nil
	}
	return nil, metadata.ErrClusterNotFound
}

func (m *managerImpl) AllocSchemaID(ctx context.Context, clusterName, schemaName string) (storage.SchemaID, bool, error) {
	cluster, err := m.getCluster(clusterName)
	if err != nil {
		return 0, false, errors.WithMessage(err, "get cluster")
	}

	// create new schema
	schema, exists, err := cluster.metadata.GetOrCreateSchema(ctx, schemaName)
	if err != nil {
		log.Error("fail to create schema", zap.Error(err))
		return 0, false, errors.WithMessage(err, "get or create schema")
	}
	return schema.ID, exists, nil
}

func (m *managerImpl) GetTables(clusterName, schemaName string, tableNames []string) ([]metadata.TableInfo, error) {
	cluster, err := m.getCluster(clusterName)
	if err != nil {
		return []metadata.TableInfo{}, errors.WithMessage(err, "get cluster")
	}

	tables, err := cluster.metadata.GetTables(schemaName, tableNames)
	if err != nil {
		return []metadata.TableInfo{}, errors.WithMessage(err, "metadata get tables")
	}

	tableInfos := make([]metadata.TableInfo, 0, len(tables))
	for _, table := range tables {
		tableInfos = append(tableInfos, metadata.TableInfo{
			ID:            table.ID,
			Name:          table.Name,
			SchemaID:      table.SchemaID,
			SchemaName:    schemaName,
			CreatedAt:     table.CreatedAt,
			PartitionInfo: table.PartitionInfo,
		})
	}
	return tableInfos, nil
}

func (m *managerImpl) GetTablesByIDs(clusterName string, tableIDs []storage.TableID) ([]metadata.TableInfo, error) {
	cluster, err := m.getCluster(clusterName)
	if err != nil {
		return []metadata.TableInfo{}, errors.WithMessage(err, "get cluster")
	}

	tables := cluster.metadata.GetTablesByIDs(tableIDs)
	tableInfos := make([]metadata.TableInfo, 0, len(tables))
	for _, table := range tables {
		tableInfos = append(tableInfos, metadata.TableInfo{
			ID:       table.ID,
			Name:     table.Name,
			SchemaID: table.SchemaID,
			// FIXME: We need the schema name here.
			SchemaName:    "",
			PartitionInfo: table.PartitionInfo,
			CreatedAt:     table.CreatedAt,
		})
	}
	return tableInfos, nil
}

func (m *managerImpl) GetTablesByShardIDs(clusterName, _ string, shardIDs []storage.ShardID) (map[storage.ShardID]metadata.ShardTables, error) {
	cluster, err := m.getCluster(clusterName)
	if err != nil {
		return nil, errors.WithMessage(err, "get cluster")
	}

	shardTables := cluster.metadata.GetShardTables(shardIDs)
	return shardTables, nil
}

// DropTable is only used for the HTTP interface.
// It only deletes the table data in ETCD and does not initiate a table deletion request to HoraeDB.
func (m *managerImpl) DropTable(ctx context.Context, clusterName, schemaName, tableName string) error {
	cluster, err := m.getCluster(clusterName)
	if err != nil {
		return errors.WithMessage(err, "get cluster")
	}

	table, ok, err := cluster.metadata.GetTable(schemaName, tableName)
	if !ok {
		return metadata.ErrTableNotFound
	}
	if err != nil {
		return errors.WithMessage(err, "get table")
	}

	// If the table is partitioned, delete the table metadata directly.
	if table.IsPartitioned() {
		_, err = cluster.metadata.DropTableMetadata(ctx, schemaName, tableName)
		if err != nil {
			return errors.WithMessage(err, "cluster drop table metadata")
		}
		return nil
	}

	// If the table is not a partition table, delete the table metadata and remove the table from the shard.
	// So we need to check if the table has been assigned to a shard.
	getShardNodeResult, err := cluster.metadata.GetShardNodeByTableIDs([]storage.TableID{table.ID})
	if err != nil {
		return errors.WithMessage(err, "get shard node by tableID")
	}

	if _, ok := getShardNodeResult.ShardNodes[table.ID]; !ok {
		return metadata.ErrShardNotFound
	}

	if len(getShardNodeResult.ShardNodes[table.ID]) != 1 || len(getShardNodeResult.Version) != 1 {
		return metadata.ErrShardNotFound
	}

	shardID := getShardNodeResult.ShardNodes[table.ID][0].ID
	version, ok := getShardNodeResult.Version[shardID]
	if !ok {
		return metadata.ErrVersionNotFound
	}

	err = cluster.metadata.DropTable(ctx, metadata.DropTableRequest{
		SchemaName:    schemaName,
		TableName:     tableName,
		ShardID:       shardID,
		LatestVersion: version,
	})
	if err != nil {
		return errors.WithMessage(err, "cluster drop table")
	}

	return nil
}

func (m *managerImpl) RegisterNode(ctx context.Context, clusterName string, registeredNode metadata.RegisteredNode) error {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if !m.running {
		return nil
	}

	cluster, err := m.getCluster(clusterName)
	if err != nil {
		return errors.WithMessage(err, "get cluster")
	}

	err = cluster.metadata.RegisterNode(ctx, registeredNode)
	if err != nil {
		return errors.WithMessage(err, "cluster register node")
	}

	return nil
}

func (m *managerImpl) GetRegisteredNode(_ context.Context, clusterName string, nodeName string) (metadata.RegisteredNode, error) {
	var registeredNode metadata.RegisteredNode
	cluster, err := m.getCluster(clusterName)
	if err != nil {
		log.Error("get cluster", zap.Error(err), zap.String("clusterName", clusterName))
		return registeredNode, errors.WithMessage(err, "get cluster")
	}

	registeredNode, ok := cluster.metadata.GetRegisteredNodeByName(nodeName)
	if !ok {
		return registeredNode, metadata.ErrNodeNotFound.WithCausef("registeredNode is not found, registeredNode:%s, cluster:%s", nodeName, clusterName)
	}

	return registeredNode, nil
}

func (m *managerImpl) ListRegisteredNodes(_ context.Context, clusterName string) ([]metadata.RegisteredNode, error) {
	cluster, err := m.getCluster(clusterName)
	if err != nil {
		return []metadata.RegisteredNode{}, errors.WithMessage(err, "get cluster")
	}

	nodes := cluster.metadata.GetRegisteredNodes()
	return nodes, nil
}

func (m *managerImpl) getCluster(clusterName string) (*Cluster, error) {
	m.lock.RLock()
	cluster, ok := m.clusters[clusterName]
	m.lock.RUnlock()
	if !ok {
		return nil, metadata.ErrClusterNotFound.WithCausef("cluster name:%s", clusterName)
	}
	return cluster, nil
}

func (m *managerImpl) allocClusterID(ctx context.Context) (storage.ClusterID, error) {
	ID, err := m.alloc.Alloc(ctx)
	if err != nil {
		return 0, errors.WithMessagef(err, "alloc cluster id")
	}
	return storage.ClusterID(ID), nil
}

func (m *managerImpl) Start(ctx context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.running {
		log.Warn("cluster manager has already been started")
		return nil
	}

	clusters, err := m.storage.ListClusters(ctx)
	if err != nil {
		log.Error("cluster manager fail to start, fail to list clusters", zap.Error(err))
		return errors.WithMessage(err, "cluster manager start")
	}

	m.clusters = make(map[string]*Cluster, len(clusters.Clusters))
	for _, metadataStorage := range clusters.Clusters {
		logger := log.With(zap.String("clusterName", metadataStorage.Name))
		clusterMetadata := metadata.NewClusterMetadata(logger, metadataStorage, m.storage, m.kv, m.rootPath, m.idAllocatorStep)
		if err = clusterMetadata.Load(ctx); err != nil {
			log.Error("fail to load cluster", zap.String("cluster", clusterMetadata.Name()), zap.Error(err))
			return errors.WithMessage(err, "fail to load cluster")
		}

		// TODO: topologyType is used to be compatible with cluster data changes and needs to be deleted later
		if clusterMetadata.GetStorageMetadata().TopologyType == storage.TopologyTypeUnknown {
			req := storage.UpdateClusterRequest{
				Cluster: storage.Cluster{
					ID:                          metadataStorage.ID,
					Name:                        metadataStorage.Name,
					MinNodeCount:                metadataStorage.MinNodeCount,
					ShardTotal:                  metadataStorage.ShardTotal,
					TopologyType:                m.topologyType,
					ProcedureExecutingBatchSize: metadataStorage.ProcedureExecutingBatchSize,
					CreatedAt:                   metadataStorage.CreatedAt,
					ModifiedAt:                  uint64(time.Now().UnixMilli()),
				},
			}
			if err := m.storage.UpdateCluster(ctx, req); err != nil {
				return errors.WithMessagef(err, "update cluster topology type failed, clusterName:%s", clusterMetadata.Name())
			}
			log.Info("update cluster topology type successfully", zap.String("request", fmt.Sprintf("%v", req)))
			if err := clusterMetadata.LoadMetadata(ctx); err != nil {
				log.Error("fail to load cluster", zap.String("clusterName", clusterMetadata.Name()), zap.Error(err))
				return err
			}
		}

		log.Info("open cluster successfully", zap.String("cluster", clusterMetadata.Name()))
		c, err := NewCluster(logger, clusterMetadata, m.client, m.rootPath)
		if err != nil {
			return errors.WithMessage(err, "new cluster")
		}
		m.clusters[clusterMetadata.Name()] = c
		if err := c.Start(ctx); err != nil {
			return errors.WithMessage(err, "start cluster")
		}
	}

	m.running = true

	return nil
}

func (m *managerImpl) Stop(ctx context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if !m.running {
		return nil
	}

	for _, cluster := range m.clusters {
		if err := cluster.Stop(ctx); err != nil {
			return errors.WithMessage(err, "stop cluster")
		}
	}

	m.clusters = make(map[string]*Cluster)
	m.running = false
	return nil
}

func (m *managerImpl) RouteTables(ctx context.Context, clusterName, schemaName string, tableNames []string) (metadata.RouteTablesResult, error) {
	cluster, err := m.getCluster(clusterName)
	if err != nil {
		return metadata.RouteTablesResult{}, errors.WithMessage(err, "get cluster")
	}

	ret, err := cluster.metadata.RouteTables(ctx, schemaName, tableNames)
	if err != nil {
		return metadata.RouteTablesResult{}, errors.WithMessage(err, "cluster route tables")
	}

	return ret, nil
}

func (m *managerImpl) GetNodeShards(ctx context.Context, clusterName string) (metadata.GetNodeShardsResult, error) {
	cluster, err := m.getCluster(clusterName)
	if err != nil {
		return metadata.GetNodeShardsResult{}, errors.WithMessage(err, "get cluster")
	}

	ret, err := cluster.metadata.GetNodeShards(ctx)
	if err != nil {
		return metadata.GetNodeShardsResult{}, errors.WithMessage(err, "cluster get NodeShards")
	}

	return ret, nil
}
