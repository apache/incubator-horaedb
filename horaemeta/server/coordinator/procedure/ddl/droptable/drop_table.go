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

package droptable

import (
	"context"
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

const (
	eventPrepare = "EventPrepare"
	eventFailed  = "EventFailed"
	eventSuccess = "EventSuccess"

	stateBegin   = "StateBegin"
	stateWaiting = "StateWaiting"
	stateFinish  = "StateFinish"
	stateFailed  = "StateFailed"
)

var (
	dropTableEvents = fsm.Events{
		{Name: eventPrepare, Src: []string{stateBegin}, Dst: stateWaiting},
		{Name: eventSuccess, Src: []string{stateWaiting}, Dst: stateFinish},
		{Name: eventFailed, Src: []string{stateWaiting}, Dst: stateFailed},
	}
	dropTableCallbacks = fsm.Callbacks{
		eventPrepare: prepareCallback,
		eventFailed:  failedCallback,
		eventSuccess: successCallback,
	}
)

func prepareCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, coderr.Wrapf(err, "get request from event"))
		return
	}
	params := req.p.params

	table, err := ddl.GetTableMetadata(params.ClusterMetadata, params.SourceReq.GetSchemaName(), params.SourceReq.GetName())
	if err != nil {
		procedure.CancelEventWithLog(event, coderr.Wrapf(err, "get table metadata, table:%s", params.SourceReq.GetName()))
		return
	}
	req.droppedTable = &metadata.TableInfo{
		ID:            table.ID,
		Name:          table.Name,
		SchemaID:      table.SchemaID,
		SchemaName:    params.SourceReq.GetSchemaName(),
		PartitionInfo: table.PartitionInfo,
		CreatedAt:     table.CreatedAt,
	}

	shardVersionUpdate, shardExists, err := ddl.BuildShardVersionUpdate(table, params.ClusterMetadata, req.p.relatedVersionInfo.ShardWithVersion)
	if err != nil {
		log.Error("get shard version by table", zap.String("tableName", params.SourceReq.GetName()), zap.Bool("shardExists", shardExists), zap.Error(err))
		procedure.CancelEventWithLog(event, coderr.Wrapf(err, "get shard version by table name, table:%s, shardExists:%v", params.SourceReq.GetName(), shardExists))
		return
	}
	// If the shard corresponding to this table does not exist, it means that the actual table creation failed.
	// In order to ensure that the table can be deleted normally, we need to directly delete the metadata of the table.
	if !shardExists {
		// Try to drop table with the latest shard version.
		err = params.ClusterMetadata.DropTable(req.ctx, metadata.DropTableRequest{
			SchemaName:    params.SourceReq.GetSchemaName(),
			TableName:     params.SourceReq.GetName(),
			ShardID:       shardVersionUpdate.ShardID,
			LatestVersion: shardVersionUpdate.LatestVersion,
		})
		if err != nil {
			procedure.CancelEventWithLog(event, coderr.Wrapf(err, "drop table metadata, table:%s", params.SourceReq.GetName()))
			return
		}
		return
	}

	latestShardVersion, err := ddl.DropTableOnShard(req.ctx, params.ClusterMetadata, params.Dispatch, params.SourceReq.GetSchemaName(), table, shardVersionUpdate)
	if err != nil {
		procedure.CancelEventWithLog(event, coderr.Wrapf(err, "dispatch drop table on shard"))
		return
	}

	log.Debug("dispatch dropTableOnShard finish", zap.String("tableName", params.SourceReq.GetName()), zap.Uint64("procedureID", params.ID))

	if err = params.ClusterMetadata.DropTable(req.ctx, metadata.DropTableRequest{
		SchemaName:    params.SourceReq.GetSchemaName(),
		TableName:     params.SourceReq.GetName(),
		ShardID:       shardVersionUpdate.ShardID,
		LatestVersion: latestShardVersion,
	}); err != nil {
		procedure.CancelEventWithLog(event, coderr.Wrapf(err, "cluster drop table"))
		return
	}

	log.Debug("drop table finish", zap.String("tableName", params.SourceReq.GetName()), zap.Uint64("procedureID", params.ID))
}

func successCallback(event *fsm.Event) {
	req := event.Args[0].(*callbackRequest)

	assert.Assert(req.droppedTable != nil)
	if err := req.p.params.OnSucceeded(*req.droppedTable); err != nil {
		log.Error("exec success callback failed")
	}
}

func failedCallback(event *fsm.Event) {
	req := event.Args[0].(*callbackRequest)

	if err := req.p.params.OnFailed(event.Err); err != nil {
		log.Error("exec failed callback failed")
	}
}

// callbackRequest is fsm callbacks param.
type callbackRequest struct {
	ctx context.Context
	p   *Procedure

	droppedTable *metadata.TableInfo
}

type ProcedureParams struct {
	ID              uint64
	Dispatch        eventdispatch.Dispatch
	ClusterMetadata *metadata.ClusterMetadata
	ClusterSnapshot metadata.Snapshot

	SourceReq   *metaservicepb.DropTableRequest
	OnSucceeded func(metadata.TableInfo) error
	OnFailed    func(error) error
}

func NewDropTableProcedure(params ProcedureParams) (procedure.Procedure, bool, error) {
	table, exists, err := params.ClusterMetadata.GetTable(params.SourceReq.GetSchemaName(), params.SourceReq.GetName())
	if err != nil {
		log.Error("get table", zap.Error(err))
		return nil, false, err
	}
	if !exists {
		log.Warn("drop non-existing table", zap.String("schema", params.SourceReq.GetSchemaName()), zap.String("table", params.SourceReq.GetName()))
		return nil, false, nil
	}

	shardID, err := findShardID(table.ID, params)
	if err != nil {
		return nil, false, err
	}

	relatedVersionInfo, err := buildRelatedVersionInfo(params, shardID)
	if err != nil {
		return nil, false, err
	}

	fsm := fsm.NewFSM(
		stateBegin,
		dropTableEvents,
		dropTableCallbacks,
	)

	return &Procedure{
		fsm:                fsm,
		shardID:            shardID,
		relatedVersionInfo: relatedVersionInfo,
		params:             params,
		lock:               sync.RWMutex{},
		state:              procedure.StateInit,
	}, true, nil
}

func buildRelatedVersionInfo(params ProcedureParams, shardID storage.ShardID) (procedure.RelatedVersionInfo, error) {
	shardWithVersion := make(map[storage.ShardID]uint64, 1)
	shardView, exists := params.ClusterSnapshot.Topology.ShardViewsMapping[shardID]

	var info procedure.RelatedVersionInfo
	if !exists {
		return info, metadata.ErrShardNotFound.WithMessagef("build related version info, shardID:%d", shardID)
	}

	shardWithVersion[shardID] = shardView.Version
	info = procedure.RelatedVersionInfo{
		ClusterID:        params.ClusterSnapshot.Topology.ClusterView.ClusterID,
		ShardWithVersion: shardWithVersion,
		ClusterVersion:   params.ClusterSnapshot.Topology.ClusterView.Version,
	}
	return info, nil
}

func findShardID(tableID storage.TableID, params ProcedureParams) (storage.ShardID, error) {
	for _, shardView := range params.ClusterSnapshot.Topology.ShardViewsMapping {
		for _, id := range shardView.TableIDs {
			if tableID == id {
				return shardView.ShardID, nil
			}
		}
	}

	return 0, metadata.ErrShardNotFound.WithMessagef("The shard corresponding to the table was not found, schema:%s, table:%s", params.SourceReq.GetSchemaName(), params.SourceReq.GetName())
}

type Procedure struct {
	fsm                *fsm.FSM
	shardID            storage.ShardID
	relatedVersionInfo procedure.RelatedVersionInfo
	params             ProcedureParams

	lock  sync.RWMutex
	state procedure.State
}

func (p *Procedure) RelatedVersionInfo() procedure.RelatedVersionInfo {
	return p.relatedVersionInfo
}

func (p *Procedure) Priority() procedure.Priority {
	return procedure.PriorityLow
}

func (p *Procedure) ID() uint64 {
	return p.params.ID
}

func (p *Procedure) Kind() procedure.Kind {
	return procedure.DropTable
}

func (p *Procedure) Start(ctx context.Context) error {
	p.updateState(procedure.StateRunning)

	req := &callbackRequest{
		ctx:          ctx,
		p:            p,
		droppedTable: nil,
	}

	if err := p.fsm.Event(eventPrepare, req); err != nil {
		err1 := p.fsm.Event(eventFailed, req)
		p.updateState(procedure.StateFailed)
		if err1 != nil {
			err = coderr.Wrapf(err, "send eventFailed, err:%v", err1)
		}
		return errors.WithMessage(err, "send eventPrepare")
	}

	if err := p.fsm.Event(eventSuccess, req); err != nil {
		return errors.WithMessage(err, "send eventSuccess")
	}

	p.updateState(procedure.StateFinished)
	return nil
}

func (p *Procedure) Cancel(_ context.Context) error {
	p.updateState(procedure.StateCancelled)
	return nil
}

func (p *Procedure) State() procedure.State {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return p.state
}

func (p *Procedure) updateState(state procedure.State) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.state = state
}
