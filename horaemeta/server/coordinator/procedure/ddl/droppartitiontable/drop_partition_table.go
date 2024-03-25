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

package droppartitiontable

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/apache/incubator-horaedb-meta/pkg/coderr"
	"github.com/apache/incubator-horaedb-meta/pkg/log"
	"github.com/apache/incubator-horaedb-meta/server/cluster/metadata"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/eventdispatch"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/procedure"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/procedure/ddl"
	"github.com/apache/incubator-horaedb-meta/server/storage"
	"github.com/apache/incubator-horaedb-proto/golang/pkg/metaservicepb"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// fsm state change:
// ┌────────┐     ┌────────────────┐     ┌────────────────────┐      ┌───────────┐
// │ Begin  ├─────▶  DropDataTable ├─────▶ DropPartitionTable ├──────▶  Finish   │
// └────────┘     └────────────────┘     └────────────────────┘      └───────────┘
const (
	eventDropDataTable      = "EventDropDataTable"
	eventDropPartitionTable = "EventDropPartitionTable"
	eventFinish             = "EventFinish"

	stateBegin              = "StateBegin"
	stateDropDataTable      = "StateDropDataTable"
	stateDropPartitionTable = "StateDropPartitionTable"
	stateFinish             = "StateFinish"
)

var (
	createDropPartitionTableEvents = fsm.Events{
		{Name: eventDropDataTable, Src: []string{stateBegin}, Dst: stateDropDataTable},
		{Name: eventDropPartitionTable, Src: []string{stateDropDataTable}, Dst: stateDropPartitionTable},
		{Name: eventFinish, Src: []string{stateDropPartitionTable}, Dst: stateFinish},
	}
	createDropPartitionTableCallbacks = fsm.Callbacks{
		eventDropDataTable:      dropDataTablesCallback,
		eventDropPartitionTable: dropPartitionTableCallback,
		eventFinish:             finishCallback,
	}
)

type Procedure struct {
	fsm                *fsm.FSM
	params             ProcedureParams
	relatedVersionInfo procedure.RelatedVersionInfo

	// Protect the state.
	lock  sync.RWMutex
	state procedure.State
}

type ProcedureParams struct {
	ID              uint64
	ClusterMetadata *metadata.ClusterMetadata
	ClusterSnapshot metadata.Snapshot
	Dispatch        eventdispatch.Dispatch
	Storage         procedure.Storage
	SourceReq       *metaservicepb.DropTableRequest
	OnSucceeded     func(result metadata.TableInfo) error
	OnFailed        func(error) error
}

func NewProcedure(params ProcedureParams) (*Procedure, bool, error) {
	fsm := fsm.NewFSM(
		stateBegin,
		createDropPartitionTableEvents,
		createDropPartitionTableCallbacks,
	)
	relatedVersionInfo, err := buildRelatedVersionInfo(params)
	if err != nil {
		return nil, false, err
	}

	return &Procedure{
		fsm:                fsm,
		params:             params,
		relatedVersionInfo: relatedVersionInfo,
		lock:               sync.RWMutex{},
		state:              stateBegin,
	}, true, nil
}

func buildRelatedVersionInfo(params ProcedureParams) (procedure.RelatedVersionInfo, error) {
	tableShardMapping := make(map[storage.TableID]storage.ShardID, len(params.SourceReq.PartitionTableInfo.GetSubTableNames()))
	for shardID, shardView := range params.ClusterSnapshot.Topology.ShardViewsMapping {
		for _, tableID := range shardView.TableIDs {
			tableShardMapping[tableID] = shardID
		}
	}

	var relatedVersionInfo procedure.RelatedVersionInfo
	shardViewWithVersion := make(map[storage.ShardID]uint64, 0)
	for _, subTableName := range params.SourceReq.PartitionTableInfo.GetSubTableNames() {
		table, exists, err := params.ClusterMetadata.GetTable(params.SourceReq.GetSchemaName(), subTableName)
		if err != nil {
			return relatedVersionInfo, coderr.Wrapf(err, "get sub table, tableName:%s", subTableName)
		}
		if !exists {
			continue
		}
		shardID, exists := tableShardMapping[table.ID]
		if !exists {
			continue
		}
		shardView, exists := params.ClusterSnapshot.Topology.ShardViewsMapping[shardID]
		if !exists {
			return relatedVersionInfo, metadata.ErrShardNotFound.WithMessagef("shard not found in topology, shardID:%d", shardID)
		}
		shardViewWithVersion[shardID] = shardView.Version
	}

	relatedVersionInfo = procedure.RelatedVersionInfo{
		ClusterID:        params.ClusterSnapshot.Topology.ClusterView.ClusterID,
		ShardWithVersion: shardViewWithVersion,
		ClusterVersion:   params.ClusterSnapshot.Topology.ClusterView.Version,
	}
	return relatedVersionInfo, nil
}

func (p *Procedure) ID() uint64 {
	return p.params.ID
}

func (p *Procedure) Kind() procedure.Kind {
	return procedure.DropPartitionTable
}

func (p *Procedure) RelatedVersionInfo() procedure.RelatedVersionInfo {
	return p.relatedVersionInfo
}

func (p *Procedure) Priority() procedure.Priority {
	return procedure.PriorityMed
}

func (p *Procedure) Start(ctx context.Context) error {
	p.updateStateWithLock(procedure.StateRunning)

	dropPartitionTableRequest := &callbackRequest{
		ctx: ctx,
		p:   p,
		// FIXME: shall we initialize the table at the first?
		table: nil,
	}

	for {
		switch p.fsm.Current() {
		case stateBegin:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "drop partition table procedure persist")
			}
			if err := p.fsm.Event(eventDropDataTable, dropPartitionTableRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				_ = p.params.OnFailed(err)
				return errors.WithMessage(err, "drop partition table procedure")
			}
		case stateDropDataTable:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "drop partition table procedure persist")
			}
			if err := p.fsm.Event(eventDropPartitionTable, dropPartitionTableRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				_ = p.params.OnFailed(err)
				return errors.WithMessage(err, "drop partition table procedure drop data table")
			}
		case stateDropPartitionTable:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "drop partition table procedure persist")
			}
			if err := p.fsm.Event(eventFinish, dropPartitionTableRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				_ = p.params.OnFailed(err)
				return errors.WithMessage(err, "drop partition table procedure drop partition table")
			}
		case stateFinish:
			p.updateStateWithLock(procedure.StateFinished)
			if err := p.persist(ctx); err != nil {
				_ = p.params.OnFailed(err)
				return errors.WithMessage(err, "drop partition table procedure persist")
			}
			return nil
		}
	}
}

func (p *Procedure) Cancel(_ context.Context) error {
	p.updateStateWithLock(procedure.StateCancelled)
	return nil
}

func (p *Procedure) State() procedure.State {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return p.state
}

func (p *Procedure) updateStateWithLock(state procedure.State) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.state = state
}

func (p *Procedure) persist(ctx context.Context) error {
	meta, err := p.convertToMeta()
	if err != nil {
		return errors.WithMessage(err, "convert to meta")
	}
	err = p.params.Storage.CreateOrUpdate(ctx, meta)
	if err != nil {
		return errors.WithMessage(err, "createOrUpdate procedure storage")
	}
	return nil
}

func (p *Procedure) convertToMeta() (procedure.Meta, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	rawData := rawData{
		ID:               p.params.ID,
		FsmState:         p.fsm.Current(),
		State:            p.state,
		DropTableRequest: p.params.SourceReq,
	}
	rawDataBytes, err := json.Marshal(rawData)
	if err != nil {
		var emptyMeta procedure.Meta
		return emptyMeta, procedure.ErrEncodeRawData.WithMessagef("decode procedure.Meta, procedureID:%d, err:%v", p.params.ID, err)
	}

	meta := procedure.Meta{
		ID:    p.params.ID,
		Kind:  procedure.DropPartitionTable,
		State: p.state,

		RawData: rawDataBytes,
	}

	return meta, nil
}

type rawData struct {
	ID       uint64
	FsmState string
	State    procedure.State

	DropTableRequest *metaservicepb.DropTableRequest
}

type callbackRequest struct {
	ctx context.Context
	p   *Procedure

	table *storage.Table
}

func (d *callbackRequest) schemaName() string {
	return d.p.params.SourceReq.GetSchemaName()
}

func (d *callbackRequest) tableName() string {
	return d.p.params.SourceReq.GetName()
}

// 1. Drop data tables in target nodes.
func dropDataTablesCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, coderr.Wrapf(err, "get request from event"))
		return
	}
	params := req.p.params

	if len(params.SourceReq.PartitionTableInfo.SubTableNames) == 0 {
		procedure.CancelEventWithLog(event, procedure.ErrEmptyPartitionNames.WithMessagef("dropped partition contains no sub tables, table:%s", params.SourceReq.Name))
		return
	}

	shardVersions := req.p.relatedVersionInfo.ShardWithVersion
	g, _ := errgroup.WithContext(req.ctx)

	// shardID -> tableNames
	shardTables := make(map[storage.ShardID][]string)
	for _, tableName := range params.SourceReq.PartitionTableInfo.GetSubTableNames() {
		table, err := ddl.GetTableMetadata(params.ClusterMetadata, req.schemaName(), tableName)
		if err != nil {
			log.Warn("get table metadata failed", zap.String("tableName", tableName))
			continue
		}

		shardVersionUpdate, shardExists, err := ddl.BuildShardVersionUpdate(table, params.ClusterMetadata, shardVersions)
		if err != nil {
			log.Error("get shard version by table", zap.String("tableName", tableName), zap.Error(err))
			procedure.CancelEventWithLog(event, coderr.Wrapf(err, "build shard version update, table:%s", tableName))
			return
		}
		// If the shard corresponding to this table does not exist, it means that the actual table creation failed.
		// In order to ensure that the table can be deleted normally, we need to directly delete the metadata of the table.
		if !shardExists {
			_, err := params.ClusterMetadata.DropTableMetadata(req.ctx, req.schemaName(), tableName)
			if err != nil {
				procedure.CancelEventWithLog(event, coderr.Wrapf(err, "drop table metadata, table:%s", tableName))
				return
			}
			continue
		}

		shardTables[shardVersionUpdate.ShardID] = append(shardTables[shardVersionUpdate.ShardID], tableName)
	}

	for shardID, tableNames := range shardTables {
		shardID := shardID
		tableNames := tableNames
		shardVersion := shardVersions[shardID]
		g.Go(func() error {
			return dispatchDropDataTable(req, params.Dispatch, params.ClusterMetadata, shardID, params.SourceReq.GetSchemaName(), tableNames, shardVersion)
		})
	}

	err = g.Wait()
	if err != nil {
		procedure.CancelEventWithLog(event, err)
		return
	}
}

// 2. Drop partition table in target node.
func dropPartitionTableCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, coderr.Wrapf(err, "get request from event"))
		return
	}

	dropTableMetadataResult, err := req.p.params.ClusterMetadata.DropTableMetadata(req.ctx, req.schemaName(), req.tableName())
	if err != nil {
		procedure.CancelEventWithLog(event, coderr.Wrapf(err, "drop table meta data, table:%s", req.tableName()))
		return
	}

	req.table = &dropTableMetadataResult.Table
}

func finishCallback(event *fsm.Event) {
	request, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, coderr.Wrapf(err, "get request from event"))
		return
	}
	log.Info("drop partition table finish")

	tableInfo := metadata.TableInfo{
		ID:            request.table.ID,
		Name:          request.table.Name,
		SchemaID:      request.table.SchemaID,
		SchemaName:    request.p.params.SourceReq.GetSchemaName(),
		PartitionInfo: storage.PartitionInfo{Info: nil},
		CreatedAt:     0,
	}

	if err = request.p.params.OnSucceeded(tableInfo); err != nil {
		procedure.CancelEventWithLog(event, coderr.Wrapf(err, "drop partition table on succeeded"))
		return
	}
}

func dispatchDropDataTable(req *callbackRequest, dispatch eventdispatch.Dispatch, clusterMetadata *metadata.ClusterMetadata, shardID storage.ShardID, schema string, tableNames []string, shardVersion uint64) error {
	for _, tableName := range tableNames {
		table, err := ddl.GetTableMetadata(clusterMetadata, req.schemaName(), tableName)
		if err != nil {
			return coderr.Wrapf(err, "get table metadata, table:%s", tableName)
		}

		shardVersionUpdate := metadata.ShardVersionUpdate{
			ShardID:       shardID,
			LatestVersion: shardVersion,
		}

		latestShardVersion, err := ddl.DropTableOnShard(req.ctx, clusterMetadata, dispatch, schema, table, shardVersionUpdate)
		if err != nil {
			return coderr.Wrapf(err, "drop table, table:%s", tableName)
		}

		err = clusterMetadata.DropTable(req.ctx, metadata.DropTableRequest{
			SchemaName:    req.schemaName(),
			TableName:     tableName,
			ShardID:       shardID,
			LatestVersion: latestShardVersion,
		})
		if err != nil {
			return coderr.Wrapf(err, "drop table, table:%s", tableName)
		}

		shardVersion++
	}
	return nil
}
