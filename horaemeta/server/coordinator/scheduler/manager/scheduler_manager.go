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

package manager

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/incubator-horaedb-meta/pkg/log"
	"github.com/apache/incubator-horaedb-meta/server/cluster/metadata"
	"github.com/apache/incubator-horaedb-meta/server/coordinator"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/procedure"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/scheduler"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/scheduler/nodepicker"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/scheduler/rebalanced"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/scheduler/reopen"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/scheduler/static"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/watch"
	"github.com/apache/incubator-horaedb-meta/server/storage"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	schedulerInterval = time.Second * 5
)

// SchedulerManager used to manage schedulers, it will register all schedulers when it starts.
//
// Each registered scheduler will generate procedures if the cluster topology matches the scheduling condition.
type SchedulerManager interface {
	ListScheduler() []scheduler.Scheduler

	Start(ctx context.Context) error

	Stop(ctx context.Context) error

	// UpdateEnableSchedule can only be used in dynamic mode, it will throw error when topology type is static.
	// when enableSchedule is true, shard topology will not be updated, it is usually used in scenarios such as cluster deploy.
	UpdateEnableSchedule(ctx context.Context, enable bool) error

	// GetEnableSchedule can only be used in dynamic mode, it will throw error when topology type is static.
	GetEnableSchedule(ctx context.Context) (bool, error)

	// AddShardAffinityRule adds a shard affinity rule to the manager, and then apply it to the underlying schedulers.
	AddShardAffinityRule(ctx context.Context, rule scheduler.ShardAffinityRule) error

	// Remove the shard rules applied to some specific rule.
	RemoveShardAffinityRule(ctx context.Context, shardID storage.ShardID) error

	// ListShardAffinityRules lists all the rules about shard affinity of all the registered schedulers.
	ListShardAffinityRules(ctx context.Context) (map[string]scheduler.ShardAffinityRule, error)

	// Scheduler will be called when received new heartbeat, every scheduler registered in schedulerManager will be called to generate procedures.
	// Scheduler cloud be schedule with fix time interval or heartbeat.
	Scheduler(ctx context.Context, clusterSnapshot metadata.Snapshot) []scheduler.ScheduleResult
}

type schedulerManagerImpl struct {
	logger           *zap.Logger
	procedureManager procedure.Manager
	factory          *coordinator.Factory
	nodePicker       nodepicker.NodePicker
	client           *clientv3.Client
	clusterMetadata  *metadata.ClusterMetadata
	rootPath         string

	// This lock is used to protect the following field.
	lock                        sync.RWMutex
	registerSchedulers          []scheduler.Scheduler
	shardWatch                  watch.ShardWatch
	isRunning                   atomic.Bool
	topologyType                storage.TopologyType
	procedureExecutingBatchSize uint32
	enableSchedule              bool
	shardAffinities             map[storage.ShardID]scheduler.ShardAffinityRule
}

func NewManager(logger *zap.Logger, procedureManager procedure.Manager, factory *coordinator.Factory, clusterMetadata *metadata.ClusterMetadata, client *clientv3.Client, rootPath string, topologyType storage.TopologyType, procedureExecutingBatchSize uint32) SchedulerManager {
	var shardWatch watch.ShardWatch
	switch topologyType {
	case storage.TopologyTypeDynamic:
		shardWatch = watch.NewEtcdShardWatch(logger, clusterMetadata.Name(), rootPath, client)
		shardWatch.RegisteringEventCallback(&schedulerWatchCallback{c: clusterMetadata})
	case storage.TopologyTypeStatic:
		shardWatch = watch.NewNoopShardWatch()
	}

	return &schedulerManagerImpl{
		logger:                      logger,
		procedureManager:            procedureManager,
		factory:                     factory,
		nodePicker:                  nodepicker.NewConsistentUniformHashNodePicker(logger),
		client:                      client,
		clusterMetadata:             clusterMetadata,
		rootPath:                    rootPath,
		lock:                        sync.RWMutex{},
		registerSchedulers:          []scheduler.Scheduler{},
		shardWatch:                  shardWatch,
		isRunning:                   atomic.Bool{},
		topologyType:                topologyType,
		procedureExecutingBatchSize: procedureExecutingBatchSize,
		enableSchedule:              false,
		shardAffinities:             make(map[storage.ShardID]scheduler.ShardAffinityRule),
	}
}

func (m *schedulerManagerImpl) Stop(ctx context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.isRunning.Load() {
		m.registerSchedulers = m.registerSchedulers[:0]
		m.isRunning.Store(false)
		if err := m.shardWatch.Stop(ctx); err != nil {
			return errors.WithMessage(err, "stop shard watch failed")
		}
	}

	return nil
}

func (m *schedulerManagerImpl) Start(ctx context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.isRunning.Load() {
		return nil
	}

	m.initRegister()

	if err := m.shardWatch.Start(ctx); err != nil {
		return errors.WithMessage(err, "start shard watch failed")
	}

	go func() {
		m.isRunning.Store(true)
		for {
			if !m.isRunning.Load() {
				m.logger.Info("scheduler manager is canceled")
				return
			}

			time.Sleep(schedulerInterval)
			// Get latest cluster snapshot.
			clusterSnapshot := m.clusterMetadata.GetClusterSnapshot()
			m.logger.Debug("scheduler manager invoke", zap.String("clusterSnapshot", fmt.Sprintf("%v", clusterSnapshot)))

			if clusterSnapshot.Topology.IsPrepareFinished() {
				m.logger.Info("try to update cluster state to stable")
				if err := m.clusterMetadata.UpdateClusterView(ctx, storage.ClusterStateStable, clusterSnapshot.Topology.ClusterView.ShardNodes); err != nil {
					m.logger.Error("update cluster view failed", zap.Error(err))
				}
				continue
			}

			results := m.Scheduler(ctx, clusterSnapshot)
			for _, result := range results {
				if result.Procedure != nil {
					m.logger.Info("scheduler submit new procedure", zap.Uint64("ProcedureID", result.Procedure.ID()), zap.String("Reason", result.Reason))
					if err := m.procedureManager.Submit(ctx, result.Procedure); err != nil {
						m.logger.Error("scheduler submit new procedure failed", zap.Uint64("ProcedureID", result.Procedure.ID()), zap.Error(err))
					}
				}
			}
		}
	}()

	return nil
}

type schedulerWatchCallback struct {
	c *metadata.ClusterMetadata
}

func (callback *schedulerWatchCallback) OnShardRegistered(_ context.Context, _ watch.ShardRegisterEvent) error {
	return nil
}

func (callback *schedulerWatchCallback) OnShardExpired(ctx context.Context, event watch.ShardExpireEvent) error {
	oldLeader := event.OldLeaderNode
	shardID := event.ShardID
	return callback.c.DropShardNodes(ctx, []storage.ShardNode{
		{
			ID:        shardID,
			ShardRole: storage.ShardRoleLeader,
			NodeName:  oldLeader,
		},
	})
}

// Schedulers should to be initialized and registered here.
func (m *schedulerManagerImpl) initRegister() {
	var schedulers []scheduler.Scheduler
	switch m.topologyType {
	case storage.TopologyTypeDynamic:
		schedulers = m.createDynamicTopologySchedulers()
	case storage.TopologyTypeStatic:
		schedulers = m.createStaticTopologySchedulers()
	}
	for i := 0; i < len(schedulers); i++ {
		m.registerScheduler(schedulers[i])
	}
}

func (m *schedulerManagerImpl) createStaticTopologySchedulers() []scheduler.Scheduler {
	staticTopologyShardScheduler := static.NewShardScheduler(m.factory, m.nodePicker, m.procedureExecutingBatchSize)
	reopenShardScheduler := reopen.NewShardScheduler(m.factory, m.procedureExecutingBatchSize)
	return []scheduler.Scheduler{staticTopologyShardScheduler, reopenShardScheduler}
}

func (m *schedulerManagerImpl) createDynamicTopologySchedulers() []scheduler.Scheduler {
	rebalancedShardScheduler := rebalanced.NewShardScheduler(m.logger, m.factory, m.nodePicker, m.procedureExecutingBatchSize)
	reopenShardScheduler := reopen.NewShardScheduler(m.factory, m.procedureExecutingBatchSize)
	return []scheduler.Scheduler{rebalancedShardScheduler, reopenShardScheduler}
}

func (m *schedulerManagerImpl) registerScheduler(scheduler scheduler.Scheduler) {
	m.logger.Info("register new scheduler", zap.String("schedulerName", reflect.TypeOf(scheduler).String()), zap.Int("totalSchedulerLen", len(m.registerSchedulers)))
	m.registerSchedulers = append(m.registerSchedulers, scheduler)
}

func (m *schedulerManagerImpl) ListScheduler() []scheduler.Scheduler {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.registerSchedulers
}

func (m *schedulerManagerImpl) Scheduler(ctx context.Context, clusterSnapshot metadata.Snapshot) []scheduler.ScheduleResult {
	// TODO: Every scheduler should run in an independent goroutine.
	results := make([]scheduler.ScheduleResult, 0, len(m.registerSchedulers))
	for _, scheduler := range m.registerSchedulers {
		result, err := scheduler.Schedule(ctx, clusterSnapshot)
		if err != nil {
			m.logger.Error("scheduler failed", zap.Error(err))
			continue
		}
		results = append(results, result)
	}
	return results
}

func (m *schedulerManagerImpl) UpdateEnableSchedule(ctx context.Context, enable bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.topologyType != storage.TopologyTypeDynamic {
		return ErrInvalidTopologyType.WithMessagef("deploy mode could only update when topology type is dynamic")
	}

	m.enableSchedule = enable
	for _, scheduler := range m.registerSchedulers {
		scheduler.UpdateEnableSchedule(ctx, enable)
	}

	return nil
}

func (m *schedulerManagerImpl) GetEnableSchedule(_ context.Context) (bool, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if m.topologyType != storage.TopologyTypeDynamic {
		return false, ErrInvalidTopologyType.WithMessagef("deploy mode could only get when topology type is dynamic")
	}

	return m.enableSchedule, nil
}

func (m *schedulerManagerImpl) AddShardAffinityRule(ctx context.Context, rule scheduler.ShardAffinityRule) error {
	var lastErr error
	for _, scheduler := range m.registerSchedulers {
		if err := scheduler.AddShardAffinityRule(ctx, rule); err != nil {
			log.Error("failed to add shard affinity rule of a scheduler", zap.String("scheduler", scheduler.Name()), zap.Error(err))
			lastErr = err
		}
	}

	return lastErr
}

func (m *schedulerManagerImpl) RemoveShardAffinityRule(ctx context.Context, shardID storage.ShardID) error {
	var lastErr error
	for _, scheduler := range m.registerSchedulers {
		if err := scheduler.RemoveShardAffinityRule(ctx, shardID); err != nil {
			log.Error("failed to remove shard affinity rule of a scheduler", zap.String("scheduler", scheduler.Name()), zap.Error(err))
			lastErr = err
		}
	}

	return lastErr
}

func (m *schedulerManagerImpl) ListShardAffinityRules(ctx context.Context) (map[string]scheduler.ShardAffinityRule, error) {
	rules := make(map[string]scheduler.ShardAffinityRule, len(m.registerSchedulers))
	var lastErr error

	for _, scheduler := range m.registerSchedulers {
		rule, err := scheduler.ListShardAffinityRule(ctx)
		if err != nil {
			log.Error("failed to list shard affinity rule of a scheduler", zap.String("scheduler", scheduler.Name()), zap.Error(err))
			lastErr = err
		}

		rules[scheduler.Name()] = rule
	}

	return rules, lastErr
}
