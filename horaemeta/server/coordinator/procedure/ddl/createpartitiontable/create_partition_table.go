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

package createpartitiontable

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/apache/incubator-horaedb-meta/pkg/assert"
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
)

// fsm state change:
// ┌────────┐     ┌──────────────────────┐     ┌────────────────────┐     ┌───────────┐
// │ Begin  ├─────▶ CreatePartitionTable ├─────▶  CreateDataTables  ├──────▶  Finish  │
// └────────┘     └──────────────────────┘     └────────────────────┘     └───────────┘
const (
	eventCreatePartitionTable = "EventCreatePartitionTable"
	eventCreateSubTables      = "EventCreateSubTables"
	eventFinish               = "EventFinish"

	stateBegin                = "StateBegin"
	stateCreatePartitionTable = "StateCreatePartitionTable"
	stateCreateSubTables      = "StateCreateSubTables"
	stateFinish               = "StateFinish"
)

var (
	createPartitionTableEvents = fsm.Events{
		{Name: eventCreatePartitionTable, Src: []string{stateBegin}, Dst: stateCreatePartitionTable},
		{Name: eventCreateSubTables, Src: []string{stateCreatePartitionTable}, Dst: stateCreateSubTables},
		{Name: eventFinish, Src: []string{stateCreateSubTables}, Dst: stateFinish},
	}
	createPartitionTableCallbacks = fsm.Callbacks{
		eventCreatePartitionTable: createPartitionTableCallback,
		eventCreateSubTables:      createDataTablesCallback,
		eventFinish:               finishCallback,
	}
)

type Procedure struct {
	fsm                        *fsm.FSM
	params                     ProcedureParams
	relatedVersionInfo         procedure.RelatedVersionInfo
	createPartitionTableResult *metadata.CreateTableMetadataResult

	lock  sync.RWMutex
	state procedure.State
}

type ProcedureParams struct {
	ID              uint64
	ClusterMetadata *metadata.ClusterMetadata
	ClusterSnapshot metadata.Snapshot
	Dispatch        eventdispatch.Dispatch
	Storage         procedure.Storage
	SourceReq       *metaservicepb.CreateTableRequest
	SubTablesShards []metadata.ShardNodeWithVersion
	OnSucceeded     func(metadata.CreateTableResult) error
	OnFailed        func(error) error
}

func NewProcedure(params ProcedureParams) (procedure.Procedure, error) {
	relatedVersionInfo, err := buildRelatedVersionInfo(params)
	if err != nil {
		return nil, err
	}

	fsm := fsm.NewFSM(
		stateBegin,
		createPartitionTableEvents,
		createPartitionTableCallbacks,
	)

	return &Procedure{
		fsm:                        fsm,
		params:                     params,
		relatedVersionInfo:         relatedVersionInfo,
		createPartitionTableResult: nil,
		lock:                       sync.RWMutex{},
		state:                      procedure.StateInit,
	}, nil
}

func buildRelatedVersionInfo(params ProcedureParams) (procedure.RelatedVersionInfo, error) {
	shardWithVersion := make(map[storage.ShardID]uint64, len(params.SubTablesShards))

	var info procedure.RelatedVersionInfo
	for _, subTableShard := range params.SubTablesShards {
		shardView, exists := params.ClusterSnapshot.Topology.ShardViewsMapping[subTableShard.ShardInfo.ID]
		if !exists {
			return info, metadata.ErrShardNotFound.WithMessagef("build related version info, shardID:%d", subTableShard.ShardInfo.ID)
		}
		shardWithVersion[shardView.ShardID] = shardView.Version
	}

	info = procedure.RelatedVersionInfo{
		ClusterID:        params.ClusterSnapshot.Topology.ClusterView.ClusterID,
		ShardWithVersion: shardWithVersion,
		ClusterVersion:   params.ClusterSnapshot.Topology.ClusterView.Version,
	}
	return info, nil
}

func (p *Procedure) ID() uint64 {
	return p.params.ID
}

func (p *Procedure) Kind() procedure.Kind {
	return procedure.CreatePartitionTable
}

func (p *Procedure) RelatedVersionInfo() procedure.RelatedVersionInfo {
	return p.relatedVersionInfo
}

func (p *Procedure) Priority() procedure.Priority {
	return procedure.PriorityLow
}

func (p *Procedure) Start(ctx context.Context) error {
	p.updateStateWithLock(procedure.StateRunning)

	createPartitionTableRequest := &callbackRequest{
		ctx: ctx,
		p:   p,
	}

	for {
		switch p.fsm.Current() {
		case stateBegin:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "persist create partition table procedure")
			}
			if err := p.fsm.Event(eventCreatePartitionTable, createPartitionTableRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				_ = p.params.OnFailed(err)
				return errors.WithMessage(err, "create partition table")
			}
		case stateCreatePartitionTable:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "persist create partition table procedure")
			}
			if err := p.fsm.Event(eventCreateSubTables, createPartitionTableRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				_ = p.params.OnFailed(err)
				return errors.WithMessage(err, "create data tables")
			}
		case stateCreateSubTables:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "persist create partition table procedure")
			}
			if err := p.fsm.Event(eventFinish, createPartitionTableRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				_ = p.params.OnFailed(err)
				return errors.WithMessage(err, "update table shard metadata")
			}
		case stateFinish:
			// TODO: The state update sequence here is inconsistent with the previous one. Consider reconstructing the state update logic of the state machine.
			p.updateStateWithLock(procedure.StateFinished)
			if err := p.persist(ctx); err != nil {
				_ = p.params.OnFailed(err)
				return errors.WithMessage(err, "create partition table procedure persist")
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

type callbackRequest struct {
	ctx context.Context
	p   *Procedure
}

// 1. Create partition table in target node.
func createPartitionTableCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, coderr.Wrapf(err, "get request from event"))
		return
	}
	params := req.p.params

	createTableMetadataResult, err := params.ClusterMetadata.CreateTableMetadata(req.ctx, metadata.CreateTableMetadataRequest{
		SchemaName:    params.SourceReq.GetSchemaName(),
		TableName:     params.SourceReq.GetName(),
		PartitionInfo: storage.PartitionInfo{Info: params.SourceReq.PartitionTableInfo.GetPartitionInfo()},
	})
	if err != nil {
		procedure.CancelEventWithLog(event, coderr.Wrapf(err, "create table metadata"))
		return
	}
	req.p.createPartitionTableResult = &createTableMetadataResult
}

// 2. Create data tables in target nodes.
func createDataTablesCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, coderr.Wrapf(err, "get request from event"))
		return
	}
	params := req.p.params
	if len(params.SubTablesShards) != len(params.SourceReq.GetPartitionTableInfo().SubTableNames) {
		panic(fmt.Sprintf("shards number must be equal to sub tables number, shardNumber:%d, subTableNumber:%d", len(params.SubTablesShards), len(params.SourceReq.GetPartitionTableInfo().SubTableNames)))
	}

	shardVersions := req.p.relatedVersionInfo.ShardWithVersion
	shardTableMetaDatas := make(map[storage.ShardID][]metadata.CreateTableMetadataRequest, 0)
	for i, subTableShard := range params.SubTablesShards {
		tableMetaData := metadata.CreateTableMetadataRequest{
			SchemaName:    params.SourceReq.GetSchemaName(),
			TableName:     params.SourceReq.GetPartitionTableInfo().SubTableNames[i],
			PartitionInfo: storage.PartitionInfo{Info: nil},
		}
		shardTableMetaDatas[subTableShard.ShardInfo.ID] = append(shardTableMetaDatas[subTableShard.ShardInfo.ID], tableMetaData)
	}
	succeedCh := make(chan bool)
	errCh := make(chan error)
	for shardID, tableMetaDatas := range shardTableMetaDatas {
		shardVersion := shardVersions[shardID]
		go createDataTables(req, shardID, tableMetaDatas, shardVersion, succeedCh, errCh)
	}

	goRoutineNumber := len(shardTableMetaDatas)
	for {
		select {
		case err := <-errCh:
			procedure.CancelEventWithLog(event, coderr.Wrapf(err, "create data tables"))
			return
		case <-succeedCh:
			goRoutineNumber--
			if goRoutineNumber == 0 {
				return
			}
		}
	}
}

func createDataTables(req *callbackRequest, shardID storage.ShardID, tableMetaDatas []metadata.CreateTableMetadataRequest, shardVersion uint64, succeedCh chan bool, errCh chan error) {
	params := req.p.params

	for _, tableMetaData := range tableMetaDatas {
		result, err := params.ClusterMetadata.CreateTableMetadata(req.ctx, tableMetaData)
		if err != nil {
			errCh <- errors.WithMessage(err, "create table metadata")
			return
		}

		shardVersionUpdate := metadata.ShardVersionUpdate{
			ShardID:       shardID,
			LatestVersion: shardVersion,
		}

		latestShardVersion, err := ddl.CreateTableOnShard(req.ctx, params.ClusterMetadata, params.Dispatch, shardID, ddl.BuildCreateTableRequest(result.Table, shardVersionUpdate, params.SourceReq))
		if err != nil {
			errCh <- errors.WithMessage(err, "dispatch create table on shard")
			return
		}

		err = params.ClusterMetadata.AddTableTopology(req.ctx, metadata.ShardVersionUpdate{
			ShardID:       shardID,
			LatestVersion: latestShardVersion,
		}, result.Table)
		if err != nil {
			errCh <- errors.WithMessage(err, "create table metadata")
			return
		}
		shardVersion++
	}
	succeedCh <- true
}

func finishCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, coderr.Wrapf(err, "get request from event"))
		return
	}
	log.Info("create partition table finish", zap.String("tableName", req.p.params.SourceReq.GetName()))

	assert.Assert(req.p.createPartitionTableResult != nil)
	var versionUpdate metadata.ShardVersionUpdate
	if err := req.p.params.OnSucceeded(metadata.CreateTableResult{
		Table:              req.p.createPartitionTableResult.Table,
		ShardVersionUpdate: versionUpdate,
	}); err != nil {
		procedure.CancelEventWithLog(event, coderr.Wrapf(err, "create partition table on succeeded"))
		return
	}
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

// TODO: Replace rawData with structure defined by proto.
type rawData struct {
	ID       uint64
	FsmState string
	State    procedure.State

	CreateTableResult    *metadata.CreateTableResult
	PartitionTableShards []metadata.ShardNodeWithVersion
	SubTablesShards      []metadata.ShardNodeWithVersion
}

func (p *Procedure) convertToMeta() (procedure.Meta, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	rawData := rawData{
		ID:                   p.params.ID,
		FsmState:             p.fsm.Current(),
		State:                p.state,
		CreateTableResult:    nil,
		PartitionTableShards: []metadata.ShardNodeWithVersion{},
		SubTablesShards:      p.params.SubTablesShards,
	}
	rawDataBytes, err := json.Marshal(rawData)
	if err != nil {
		var emptyMeta procedure.Meta
		return emptyMeta, procedure.ErrEncodeRawData.WithMessagef("convert to procedure.Meta, procedureID:%v, err:%v", p.params.ID, err)
	}

	meta := procedure.Meta{
		ID:    p.params.ID,
		Kind:  procedure.CreatePartitionTable,
		State: p.state,

		RawData: rawDataBytes,
	}

	return meta, nil
}
