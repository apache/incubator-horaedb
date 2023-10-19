// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package transferleader

import (
	"context"
	"fmt"
	"sync"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// BatchTransferLeaderProcedure is a proxy procedure contains a batch of TransferLeaderProcedure.
// It is used to support concurrent execution of a batch of TransferLeaderProcedure with same version.
type BatchTransferLeaderProcedure struct {
	id                 uint64
	batch              []procedure.Procedure
	relatedVersionInfo procedure.RelatedVersionInfo

	// Protect the state.
	lock  sync.RWMutex
	state procedure.State
}

func NewBatchTransferLeaderProcedure(id uint64, batch []procedure.Procedure) (procedure.Procedure, error) {
	if len(batch) == 0 {
		return nil, procedure.ErrEmptyBatchProcedure
	}

	relateVersionInfo, err := buildBatchRelatedVersionInfo(batch)
	if err != nil {
		return nil, err
	}

	return &BatchTransferLeaderProcedure{
		id:                 id,
		batch:              batch,
		relatedVersionInfo: relateVersionInfo,
		lock:               sync.RWMutex{},
		state:              procedure.StateInit,
	}, nil
}

func buildBatchRelatedVersionInfo(batch []procedure.Procedure) (procedure.RelatedVersionInfo, error) {
	var emptyInfo procedure.RelatedVersionInfo
	if len(batch) == 0 {
		return emptyInfo, nil
	}

	result := procedure.RelatedVersionInfo{
		ClusterID:        batch[0].RelatedVersionInfo().ClusterID,
		ShardWithVersion: map[storage.ShardID]uint64{},
		ClusterVersion:   batch[0].RelatedVersionInfo().ClusterVersion,
	}

	// The version of this batch of procedures must be the same.
	for _, p := range batch {
		if p.RelatedVersionInfo().ClusterID != result.ClusterID {
			return emptyInfo, errors.WithMessage(procedure.ErrMergeBatchProcedure, "procedure clusterID in the same batch is inconsistent")
		}
		if p.RelatedVersionInfo().ClusterVersion != result.ClusterVersion {
			return emptyInfo, errors.WithMessage(procedure.ErrMergeBatchProcedure, "procedure clusterVersion in the same batch is inconsistent")
		}
		// The ShardVersion of the same shard must be consistent.
		for shardID, version := range p.RelatedVersionInfo().ShardWithVersion {
			if resultVersion, exists := result.ShardWithVersion[shardID]; exists {
				if version != resultVersion {
					return emptyInfo, errors.WithMessage(procedure.ErrMergeBatchProcedure, fmt.Sprintf("procedure shardVersion in the same batch is inconsistent, shardID:%d, expectedShardVersion:%d, shardVersion:%d", shardID, version, resultVersion))
				}
			} else {
				result.ShardWithVersion[shardID] = version
			}
		}
	}

	return result, nil
}

func (p *BatchTransferLeaderProcedure) ID() uint64 {
	return p.id
}

func (p *BatchTransferLeaderProcedure) Typ() procedure.Typ {
	return procedure.TransferLeader
}

func (p *BatchTransferLeaderProcedure) Start(ctx context.Context) error {
	// Start procedures with multiple goroutine.
	g, _ := errgroup.WithContext(ctx)
	for _, p := range p.batch {
		p := p
		g.Go(func() error {
			err := p.Start(ctx)
			if err != nil {
				log.Error("procedure start failed", zap.Error(err), zap.String("procedure", fmt.Sprintf("%v", p)), zap.Error(err))
			}
			return err
		})
	}

	if err := g.Wait(); err != nil {
		p.updateStateWithLock(procedure.StateFailed)
		return err
	}

	p.updateStateWithLock(procedure.StateFinished)
	return nil
}

func (p *BatchTransferLeaderProcedure) Cancel(_ context.Context) error {
	p.updateStateWithLock(procedure.StateCancelled)
	return nil
}

func (p *BatchTransferLeaderProcedure) State() procedure.State {
	return p.state
}

func (p *BatchTransferLeaderProcedure) RelatedVersionInfo() procedure.RelatedVersionInfo {
	return p.relatedVersionInfo
}

func (p *BatchTransferLeaderProcedure) Priority() procedure.Priority {
	return p.batch[0].Priority()
}

func (p *BatchTransferLeaderProcedure) updateStateWithLock(state procedure.State) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.state = state
}
