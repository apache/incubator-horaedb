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
	"time"

	"github.com/apache/incubator-horaedb-meta/pkg/coderr"
	"github.com/apache/incubator-horaedb-meta/server/id"
	"github.com/apache/incubator-horaedb-meta/server/storage"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// TableManager manages table metadata by schema.
type TableManager interface {
	// Load load table meta data from storage.
	Load(ctx context.Context) error
	// GetTable get table with schemaName and tableName, the second output parameter bool: returns true if the table exists.
	GetTable(schemaName string, tableName string) (storage.Table, bool, error)
	// GetTables get tables with schemaName and tableNames.
	GetTables(schemaName string, tableNames []string) ([]storage.Table, error)
	// GetTablesByIDs get tables with tableIDs.
	GetTablesByIDs(tableIDs []storage.TableID) []storage.Table
	// CreateTable create table with schemaName and tableName.
	CreateTable(ctx context.Context, schemaName string, tableName string, partitionInfo storage.PartitionInfo) (storage.Table, error)
	// DropTable drop table with schemaName and tableName.
	DropTable(ctx context.Context, schemaName string, tableName string) error
	// GetSchema get schema with schemaName.
	GetSchema(schemaName string) (storage.Schema, bool)
	// GetSchemaByID get schema with schemaName.
	GetSchemaByID(schemaID storage.SchemaID) (storage.Schema, bool)
	// GetSchemas get all schemas in cluster.
	GetSchemas() []storage.Schema
	// GetOrCreateSchema get or create schema with schemaName.
	GetOrCreateSchema(ctx context.Context, schemaName string) (storage.Schema, bool, error)
}

type Tables struct {
	tables     map[string]storage.Table          // tableName -> table
	tablesByID map[storage.TableID]storage.Table // tableID -> table
}

type TableManagerImpl struct {
	logger        *zap.Logger
	storage       storage.Storage
	clusterID     storage.ClusterID
	schemaIDAlloc id.Allocator
	tableIDAlloc  id.Allocator

	// RWMutex is used to protect following fields.
	lock         sync.RWMutex
	schemas      map[string]storage.Schema    // schemaName -> schema
	schemaTables map[storage.SchemaID]*Tables // schemaName -> tables
}

func NewTableManagerImpl(logger *zap.Logger, storage storage.Storage, clusterID storage.ClusterID, schemaIDAlloc id.Allocator, tableIDAlloc id.Allocator) TableManager {
	return &TableManagerImpl{
		logger:        logger,
		storage:       storage,
		clusterID:     clusterID,
		schemaIDAlloc: schemaIDAlloc,
		tableIDAlloc:  tableIDAlloc,
		lock:          sync.RWMutex{},
		// It will be initialized in loadSchemas.
		schemas: nil,
		// It will be initialized in loadTables.
		schemaTables: nil,
	}
}

func (m *TableManagerImpl) Load(ctx context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if err := m.loadSchemas(ctx); err != nil {
		return errors.WithMessage(err, "load schemas")
	}

	if err := m.loadTables(ctx); err != nil {
		return errors.WithMessage(err, "load tables")
	}

	return nil
}

func (m *TableManagerImpl) GetTable(schemaName, tableName string) (storage.Table, bool, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.getTable(schemaName, tableName)
}

func (m *TableManagerImpl) GetTables(schemaName string, tableNames []string) ([]storage.Table, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.getTables(schemaName, tableNames)
}

func (m *TableManagerImpl) GetTablesByIDs(tableIDs []storage.TableID) []storage.Table {
	m.lock.RLock()
	defer m.lock.RUnlock()

	result := make([]storage.Table, 0, len(tableIDs))
	for _, tables := range m.schemaTables {
		for _, tableID := range tableIDs {
			table, ok := tables.tablesByID[tableID]
			if !ok {
				m.logger.Warn("table not exists", zap.Uint64("tableID", uint64(tableID)))
				continue
			}
			result = append(result, table)
		}
	}

	return result
}

func (m *TableManagerImpl) CreateTable(ctx context.Context, schemaName string, tableName string, partitionInfo storage.PartitionInfo) (storage.Table, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	var emptyTable storage.Table
	_, exists, err := m.getTable(schemaName, tableName)
	if err != nil {
		return emptyTable, errors.WithMessage(err, "get table")
	}
	if exists {
		return emptyTable, ErrTableAlreadyExists.WithMessagef("table to create already exist, tableName:%s", tableName)
	}

	// Create table in storage.
	schema, ok := m.schemas[schemaName]
	if !ok {
		return emptyTable, ErrSchemaNotFound.WithMessagef("create table, schema:%s", schemaName)
	}

	id, err := m.tableIDAlloc.Alloc(ctx)
	if err != nil {
		return emptyTable, coderr.Wrapf(err, "alloc table id, table name:%s", tableName)
	}

	table := storage.Table{
		ID:            storage.TableID(id),
		Name:          tableName,
		SchemaID:      schema.ID,
		CreatedAt:     uint64(time.Now().UnixMilli()),
		PartitionInfo: partitionInfo,
	}
	err = m.storage.CreateTable(ctx, storage.CreateTableRequest{
		ClusterID: m.clusterID,
		SchemaID:  schema.ID,
		Table:     table,
	})
	if err != nil {
		return emptyTable, coderr.Wrapf(err, "storage create table failed")
	}

	// Update table in memory.
	_, ok = m.schemaTables[schema.ID]
	if !ok {
		m.schemaTables[schema.ID] = &Tables{
			tables:     make(map[string]storage.Table),
			tablesByID: make(map[storage.TableID]storage.Table),
		}
	}
	tables := m.schemaTables[schema.ID]
	tables.tables[tableName] = table
	tables.tablesByID[table.ID] = table

	return table, nil
}

func (m *TableManagerImpl) DropTable(ctx context.Context, schemaName string, tableName string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	schema, ok := m.schemas[schemaName]
	if !ok {
		return nil
	}

	table, ok := m.schemaTables[schema.ID].tables[tableName]
	if !ok {
		return nil
	}

	// Delete table in storage.
	err := m.storage.DeleteTable(ctx, storage.DeleteTableRequest{
		ClusterID: m.clusterID,
		SchemaID:  schema.ID,
		TableName: tableName,
	})
	if err != nil {
		return coderr.Wrapf(err, "storage delete table")
	}

	tables := m.schemaTables[schema.ID]
	delete(tables.tables, tableName)
	delete(tables.tablesByID, table.ID)
	return nil
}

func (m *TableManagerImpl) GetSchema(schemaName string) (storage.Schema, bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	schema, ok := m.schemas[schemaName]
	return schema, ok
}

func (m *TableManagerImpl) GetSchemaByID(schemaID storage.SchemaID) (storage.Schema, bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	for _, schema := range m.schemas {
		if schema.ID == schemaID {
			return schema, true
		}
	}

	var emptySchema storage.Schema
	return emptySchema, false
}

func (m *TableManagerImpl) GetSchemas() []storage.Schema {
	m.lock.RLock()
	defer m.lock.RUnlock()

	schemas := make([]storage.Schema, len(m.schemas))

	for _, schema := range m.schemas {
		schemas = append(schemas, schema)
	}

	return schemas
}

func (m *TableManagerImpl) GetOrCreateSchema(ctx context.Context, schemaName string) (storage.Schema, bool, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	schema, ok := m.schemas[schemaName]
	if ok {
		return schema, true, nil
	}

	id, err := m.schemaIDAlloc.Alloc(ctx)
	if err != nil {
		return storage.Schema{}, false, errors.WithMessage(err, "alloc schema id")
	}

	schema = storage.Schema{
		ID:        storage.SchemaID(id),
		ClusterID: m.clusterID,
		Name:      schemaName,
		CreatedAt: uint64(time.Now().UnixMilli()),
	}

	// Create schema in storage.
	if err = m.storage.CreateSchema(ctx, storage.CreateSchemaRequest{
		ClusterID: m.clusterID,
		Schema:    schema,
	}); err != nil {
		return storage.Schema{}, false, errors.WithMessage(err, "storage create schema")
	}
	// Update schema in memory.
	m.schemas[schemaName] = schema
	return schema, false, nil
}

func (m *TableManagerImpl) loadSchemas(ctx context.Context) error {
	schemasResult, err := m.storage.ListSchemas(ctx, storage.ListSchemasRequest{ClusterID: m.clusterID})
	if err != nil {
		return errors.WithMessage(err, "list schemas")
	}
	m.logger.Debug("load schema", zap.String("data", fmt.Sprintf("%+v", schemasResult)))

	// Reset data in memory.
	m.schemas = make(map[string]storage.Schema, len(schemasResult.Schemas))
	for _, schema := range schemasResult.Schemas {
		m.schemas[schema.Name] = schema
	}

	return nil
}

func (m *TableManagerImpl) loadTables(ctx context.Context) error {
	// Reset data in memory.
	m.schemaTables = make(map[storage.SchemaID]*Tables, len(m.schemas))
	for _, schema := range m.schemas {
		tablesResult, err := m.storage.ListTables(ctx, storage.ListTableRequest{
			ClusterID: m.clusterID,
			SchemaID:  schema.ID,
		})
		if err != nil {
			return errors.WithMessage(err, "list tables")
		}
		m.logger.Debug("load table", zap.String("schema", fmt.Sprintf("%+v", schema)), zap.String("tables", fmt.Sprintf("%+v", tablesResult)))

		for _, table := range tablesResult.Tables {
			tables, ok := m.schemaTables[table.SchemaID]
			if !ok {
				tables = &Tables{
					tables:     make(map[string]storage.Table, 0),
					tablesByID: make(map[storage.TableID]storage.Table, 0),
				}
				m.schemaTables[table.SchemaID] = tables
			}

			tables.tables[table.Name] = table
			tables.tablesByID[table.ID] = table
		}
	}
	return nil
}

func (m *TableManagerImpl) getTable(schemaName, tableName string) (storage.Table, bool, error) {
	schema, ok := m.schemas[schemaName]
	var emptyTable storage.Table
	if !ok {
		return emptyTable, false, ErrSchemaNotFound.WithMessagef("get table, schema:%s", schemaName)
	}

	tables, ok := m.schemaTables[schema.ID]
	if !ok {
		return emptyTable, false, nil
	}

	table, ok := tables.tables[tableName]
	return table, ok, nil
}

func (m *TableManagerImpl) getTables(schemaName string, tableNames []string) ([]storage.Table, error) {
	schema, ok := m.schemas[schemaName]
	if !ok {
		return []storage.Table{}, ErrSchemaNotFound.WithMessagef("get tables, schema:%s", schemaName)
	}

	schemaTables, ok := m.schemaTables[schema.ID]
	if !ok {
		return []storage.Table{}, nil
	}

	tables := make([]storage.Table, 0, len(tableNames))
	for _, tableName := range tableNames {
		if table, ok := schemaTables.tables[tableName]; ok {
			tables = append(tables, table)
		}
	}

	return tables, nil
}
