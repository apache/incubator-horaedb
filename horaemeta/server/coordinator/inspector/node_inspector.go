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

package inspector

import (
	"context"
	"sync"
	"time"

	"github.com/apache/incubator-horaedb-meta/pkg/coderr"
	"github.com/apache/incubator-horaedb-meta/pkg/log"
	"github.com/apache/incubator-horaedb-meta/server/cluster/metadata"
	"github.com/apache/incubator-horaedb-meta/server/storage"
	"go.uber.org/zap"
)

var ErrStartAgain = coderr.NewCodeErrorDef(coderr.Internal, "try to start NodeInspector again")
var ErrStopNotStart = coderr.NewCodeErrorDef(coderr.Internal, "try to stop a not-started NodeInspector")

const defaultInspectInterval = time.Second * 5

// NodeInspector will inspect node status and remove expired data.
type NodeInspector struct {
	logger          *zap.Logger
	clusterMetadata ClusterMetaDataManipulator
	interval        time.Duration

	starter sync.Once
	// After `Start` is called, the following fields will be initialized
	stopCtx     context.Context
	bgJobCancel context.CancelFunc
}

// ClusterMetaDataManipulator provides the snapshot for NodeInspector to check and utilities of drop expired shard nodes.
type ClusterMetaDataManipulator interface {
	GetClusterSnapshot() metadata.Snapshot
	DropShardNodes(context.Context, []storage.ShardNode) error
}

func NewNodeInspectorWithInterval(logger *zap.Logger, clusterMetadata ClusterMetaDataManipulator, inspectInterval time.Duration) *NodeInspector {
	return &NodeInspector{
		logger:          logger,
		clusterMetadata: clusterMetadata,
		interval:        inspectInterval,
		starter:         sync.Once{},
		stopCtx:         nil,
		bgJobCancel:     nil,
	}
}

func NewNodeInspector(logger *zap.Logger, clusterMetadata ClusterMetaDataManipulator) *NodeInspector {
	return NewNodeInspectorWithInterval(logger, clusterMetadata, defaultInspectInterval)
}

func (ni *NodeInspector) Start(ctx context.Context) error {
	started := false
	ni.starter.Do(func() {
		log.Info("node inspector start")
		started = true
		ni.stopCtx, ni.bgJobCancel = context.WithCancel(ctx)
		go func() {
			for {
				t := time.NewTimer(ni.interval)
				select {
				case <-ni.stopCtx.Done():
					ni.logger.Info("node inspector is stopped, cancel the bg inspecting")
					if !t.Stop() {
						<-t.C
					}
					return
				case <-t.C:
				}

				ni.inspect(ctx)
			}
		}()
	})

	if !started {
		return ErrStartAgain.WithMessagef("")
	}

	return nil
}

func (ni *NodeInspector) Stop(_ context.Context) error {
	if ni.bgJobCancel != nil {
		ni.bgJobCancel()
		return nil
	}

	return ErrStopNotStart.WithMessagef("")
}

func (ni *NodeInspector) inspect(ctx context.Context) {
	// Get latest cluster snapshot.
	snapshot := ni.clusterMetadata.GetClusterSnapshot()
	expiredShardNodes := findExpiredShardNodes(snapshot)
	if len(expiredShardNodes) == 0 {
		return
	}

	// Try to remove useless data if it exists.
	if err := ni.clusterMetadata.DropShardNodes(ctx, expiredShardNodes); err != nil {
		log.Error("drop shard node failed", zap.Error(err))
	}
}

func findExpiredShardNodes(snapshot metadata.Snapshot) []storage.ShardNode {
	// In most cases, there is no expired shard nodes so don't pre-allocate the memory here.
	expiredNodes := make(map[string]struct{}, 0)
	// Check node status.
	now := time.Now()
	for i := range snapshot.RegisteredNodes {
		node := &snapshot.RegisteredNodes[i]
		if node.IsExpired(now) {
			expiredNodes[node.Node.Name] = struct{}{}
		}
	}

	expiredShardNodes := make([]storage.ShardNode, 0, len(expiredNodes))
	for _, shardNode := range snapshot.Topology.ClusterView.ShardNodes {
		_, ok := expiredNodes[shardNode.NodeName]
		if ok {
			expiredShardNodes = append(expiredShardNodes, shardNode)
		}
	}

	return expiredShardNodes
}
