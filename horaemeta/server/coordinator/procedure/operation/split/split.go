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

package split

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/apache/incubator-horaedb-meta/pkg/coderr"
	"github.com/apache/incubator-horaedb-meta/pkg/log"
	"github.com/apache/incubator-horaedb-meta/server/cluster/metadata"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/eventdispatch"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/procedure"
	"github.com/apache/incubator-horaedb-meta/server/storage"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Fsm state change: begin -> CreateNewShardView -> UpdateShardTables -> OpenNewShard -> Finish
// CreateNewShardView will create new shard metadata.
// UpdateShardTables will update shard tables mapping between the old and new shard.
// OpenNewShard will send open shard request to new shard leader.
const (
	eventCreateNewShardView = "EventCreateNewShardView"
	eventUpdateShardTables  = "EventUpdateShardTables"
	eventOpenNewShard       = "EventOpenNewShard"
	eventFinish             = "EventFinish"

	stateBegin              = "StateBegin"
	stateCreateNewShardView = "StateCreateNewShardView"
	stateUpdateShardTables  = "StateUpdateShardTables"
	stateOpenNewShard       = "StateOpenNewShard"
	stateFinish             = "StateFinish"
)

var (
	splitEvents = fsm.Events{
		{Name: eventCreateNewShardView, Src: []string{stateBegin}, Dst: stateCreateNewShardView},
		{Name: eventUpdateShardTables, Src: []string{stateCreateNewShardView}, Dst: stateUpdateShardTables},
		{Name: eventOpenNewShard, Src: []string{stateUpdateShardTables}, Dst: stateOpenNewShard},
		{Name: eventFinish, Src: []string{stateOpenNewShard}, Dst: stateFinish},
	}
	splitCallbacks = fsm.Callbacks{
		eventCreateNewShardView: createShardViewCallback,
		eventUpdateShardTables:  updateShardTablesCallback,
		eventOpenNewShard:       openShardCallback,
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
	ID uint64

	Dispatch eventdispatch.Dispatch
	Storage  procedure.Storage

	ClusterMetadata *metadata.ClusterMetadata
	ClusterSnapshot metadata.Snapshot

	ShardID    storage.ShardID
	NewShardID storage.ShardID

	SchemaName     string
	TableNames     []string
	TargetNodeName string
}

func NewProcedure(params ProcedureParams) (procedure.Procedure, error) {
	if err := validateClusterTopology(params.ClusterSnapshot.Topology, params.ShardID); err != nil {
		return nil, err
	}

	relatedVersionInfo, err := buildRelatedVersionInfo(params)
	if err != nil {
		return nil, err
	}

	splitFsm := fsm.NewFSM(
		stateBegin,
		splitEvents,
		splitCallbacks,
	)

	return &Procedure{
		fsm:                splitFsm,
		params:             params,
		relatedVersionInfo: relatedVersionInfo,
		lock:               sync.RWMutex{},
		state:              procedure.StateInit,
	}, nil
}

func buildRelatedVersionInfo(params ProcedureParams) (procedure.RelatedVersionInfo, error) {
	shardWithVersion := make(map[storage.ShardID]uint64, 0)

	shardView, exists := params.ClusterSnapshot.Topology.ShardViewsMapping[params.ShardID]
	var info procedure.RelatedVersionInfo
	if !exists {
		return info, metadata.ErrShardNotFound.WithMessagef("build related version info, shardID:%d", params.ShardID)
	}
	shardWithVersion[params.ShardID] = shardView.Version
	shardWithVersion[params.NewShardID] = 0

	info = procedure.RelatedVersionInfo{
		ClusterID:        params.ClusterSnapshot.Topology.ClusterView.ClusterID,
		ShardWithVersion: shardWithVersion,
		ClusterVersion:   params.ClusterSnapshot.Topology.ClusterView.Version,
	}
	return info, nil
}

func validateClusterTopology(topology metadata.Topology, shardID storage.ShardID) error {
	// Validate cluster state.
	curState := topology.ClusterView.State
	if curState != storage.ClusterStateStable {
		return metadata.ErrClusterStateInvalid.WithMessagef("create procedure on unstable cluster")
	}

	_, found := topology.ShardViewsMapping[shardID]
	if !found {
		return metadata.ErrShardNotFound.WithMessagef("create procedure on non-existent shard, shardID:%d", shardID)
	}

	found = false
	for _, shardNode := range topology.ClusterView.ShardNodes {
		if shardNode.ShardRole == storage.ShardRoleLeader {
			found = true
		}
	}
	if !found {
		return procedure.ErrShardLeaderNotFound.WithMessagef("create procedure on a shard without leader, shardID:%d", shardID)
	}
	return nil
}

type callbackRequest struct {
	ctx context.Context
	p   *Procedure
}

func (p *Procedure) ID() uint64 {
	return p.params.ID
}

func (p *Procedure) Kind() procedure.Kind {
	return procedure.Split
}

func (p *Procedure) RelatedVersionInfo() procedure.RelatedVersionInfo {
	return p.relatedVersionInfo
}

func (p *Procedure) Priority() procedure.Priority {
	return procedure.PriorityHigh
}

func (p *Procedure) Start(ctx context.Context) error {
	p.updateStateWithLock(procedure.StateRunning)

	splitCallbackRequest := callbackRequest{
		ctx: ctx,
		p:   p,
	}

	for {
		switch p.fsm.Current() {
		case stateBegin:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "split procedure persist")
			}
			if err := p.fsm.Event(eventCreateNewShardView, splitCallbackRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "split procedure create new shard view")
			}
		case stateCreateNewShardView:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "split procedure persist")
			}
			if err := p.fsm.Event(eventUpdateShardTables, splitCallbackRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "split procedure create new shard")
			}
		case stateUpdateShardTables:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "split procedure persist")
			}
			if err := p.fsm.Event(eventOpenNewShard, splitCallbackRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "split procedure create shard tables")
			}
		case stateOpenNewShard:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "split procedure persist")
			}
			if err := p.fsm.Event(eventFinish, splitCallbackRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "split procedure delete shard tables")
			}
		case stateFinish:
			// TODO: The state update sequence here is inconsistent with the previous one. Consider reconstructing the state update logic of the state machine.
			p.updateStateWithLock(procedure.StateFinished)
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "split procedure persist")
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

func createShardViewCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, coderr.Wrapf(err, "get request from event"))
		return
	}

	if err := req.p.params.ClusterMetadata.CreateShardViews(req.ctx, []metadata.CreateShardView{{
		ShardID: req.p.params.NewShardID,
		Tables:  []storage.TableID{},
	}}); err != nil {
		procedure.CancelEventWithLog(event, coderr.Wrapf(err, "create shard views"))
		return
	}
}

func updateShardTablesCallback(event *fsm.Event) {
	request, err := procedure.GetRequestFromEvent[callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, coderr.Wrapf(err, "get request from event"))
		return
	}

	if err := request.p.params.ClusterMetadata.MigrateTable(request.ctx, metadata.MigrateTableRequest{
		SchemaName: request.p.params.SchemaName,
		TableNames: request.p.params.TableNames,
		OldShardID: request.p.params.ShardID,
		NewShardID: request.p.params.NewShardID,
	}); err != nil {
		procedure.CancelEventWithLog(event, coderr.Wrapf(err, "update shard tables"))
		return
	}
}

func openShardCallback(event *fsm.Event) {
	request, err := procedure.GetRequestFromEvent[callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, coderr.Wrapf(err, "get request from event"))
		return
	}
	ctx := request.ctx

	// Send open new shard request to CSE.
	if err := request.p.params.Dispatch.OpenShard(ctx, request.p.params.TargetNodeName, eventdispatch.OpenShardRequest{
		Shard: metadata.ShardInfo{
			ID:      request.p.params.NewShardID,
			Role:    storage.ShardRoleLeader,
			Version: 0,
			Status:  storage.ShardStatusUnknown,
		},
	}); err != nil {
		procedure.CancelEventWithLog(event, coderr.Wrapf(err, "open shard failed"))
		return
	}
}

func finishCallback(event *fsm.Event) {
	request, err := procedure.GetRequestFromEvent[callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, coderr.Wrapf(err, "get request from event"))
		return
	}
	log.Info("split procedure finish", zap.Uint32("shardID", uint32(request.p.params.ShardID)), zap.Uint32("newShardID", uint32(request.p.params.NewShardID)))
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

type rawData struct {
	SchemaName     string
	TableNames     []string
	ShardID        uint32
	NewShardID     uint32
	TargetNodeName string
}

func (p *Procedure) convertToMeta() (procedure.Meta, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	rawData := rawData{
		SchemaName:     p.params.SchemaName,
		TableNames:     p.params.TableNames,
		ShardID:        uint32(p.params.ShardID),
		NewShardID:     uint32(p.params.NewShardID),
		TargetNodeName: p.params.TargetNodeName,
	}
	rawDataBytes, err := json.Marshal(rawData)
	if err != nil {
		var emptyMeta procedure.Meta
		return emptyMeta, procedure.ErrEncodeRawData.WithMessagef("decode procedure.Meta, procedureID:%v, err:%v", p.params.ShardID, err)
	}

	meta := procedure.Meta{
		ID:    p.params.ID,
		Kind:  procedure.Split,
		State: p.state,

		RawData: rawDataBytes,
	}

	return meta, nil
}
