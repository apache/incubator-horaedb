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

package static

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/apache/incubator-horaedb-meta/server/cluster/metadata"
	"github.com/apache/incubator-horaedb-meta/server/coordinator"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/procedure"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/scheduler"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/scheduler/nodepicker"
	"github.com/apache/incubator-horaedb-meta/server/storage"
)

type schedulerImpl struct {
	factory                     *coordinator.Factory
	nodePicker                  nodepicker.NodePicker
	procedureExecutingBatchSize uint32
}

func NewShardScheduler(factory *coordinator.Factory, nodePicker nodepicker.NodePicker, procedureExecutingBatchSize uint32) scheduler.Scheduler {
	return schedulerImpl{factory: factory, nodePicker: nodePicker, procedureExecutingBatchSize: procedureExecutingBatchSize}
}

func (s schedulerImpl) Name() string {
	return "static_scheduler"
}

func (s schedulerImpl) UpdateEnableSchedule(_ context.Context, _ bool) {
	// StaticTopologyShardScheduler do not need EnableSchedule.
}

func (s schedulerImpl) AddShardAffinityRule(_ context.Context, _ scheduler.ShardAffinityRule) error {
	return ErrNotImplemented.WithMessagef("static topology scheduler doesn't support shard affinity")
}

func (s schedulerImpl) RemoveShardAffinityRule(_ context.Context, _ storage.ShardID) error {
	return ErrNotImplemented.WithMessagef("static topology scheduler doesn't support shard affinity")
}

func (s schedulerImpl) ListShardAffinityRule(_ context.Context) (scheduler.ShardAffinityRule, error) {
	var emptyRule scheduler.ShardAffinityRule
	return emptyRule, ErrNotImplemented.WithMessagef("static topology scheduler doesn't support shard affinity")
}

func (s schedulerImpl) Schedule(ctx context.Context, clusterSnapshot metadata.Snapshot) (scheduler.ScheduleResult, error) {
	var procedures []procedure.Procedure
	var reasons strings.Builder
	var emptyScheduleRes scheduler.ScheduleResult

	switch clusterSnapshot.Topology.ClusterView.State {
	case storage.ClusterStateEmpty:
		return emptyScheduleRes, nil
	case storage.ClusterStatePrepare:
		unassignedShardIds := make([]storage.ShardID, 0, len(clusterSnapshot.Topology.ShardViewsMapping))
		for _, shardView := range clusterSnapshot.Topology.ShardViewsMapping {
			_, exists := findNodeByShard(shardView.ShardID, clusterSnapshot.Topology.ClusterView.ShardNodes)
			if exists {
				continue
			}
			unassignedShardIds = append(unassignedShardIds, shardView.ShardID)
		}
		pickConfig := nodepicker.Config{
			NumTotalShards:    uint32(len(clusterSnapshot.Topology.ShardViewsMapping)),
			ShardAffinityRule: map[storage.ShardID]scheduler.ShardAffinity{},
		}
		// Assign shards
		shardNodeMapping, err := s.nodePicker.PickNode(ctx, pickConfig, unassignedShardIds, clusterSnapshot.RegisteredNodes)
		if err != nil {
			return emptyScheduleRes, err
		}
		for shardID, node := range shardNodeMapping {
			// Shard exists and ShardNode not exists.
			p, err := s.factory.CreateTransferLeaderProcedure(ctx, coordinator.TransferLeaderRequest{
				Snapshot:          clusterSnapshot,
				ShardID:           shardID,
				OldLeaderNodeName: "",
				NewLeaderNodeName: node.Node.Name,
			})
			if err != nil {
				return emptyScheduleRes, err
			}
			procedures = append(procedures, p)
			reasons.WriteString(fmt.Sprintf("Cluster initialization, assign shard to node, shardID:%d, nodeName:%s. ", shardID, node.Node.Name))
			if len(procedures) >= int(s.procedureExecutingBatchSize) {
				break
			}
		}
	case storage.ClusterStateStable:
		for i := 0; i < len(clusterSnapshot.Topology.ClusterView.ShardNodes); i++ {
			shardNode := clusterSnapshot.Topology.ClusterView.ShardNodes[i]
			node, err := findOnlineNodeByName(shardNode.NodeName, clusterSnapshot.RegisteredNodes)
			if err != nil {
				continue
			}
			if !containsShard(node.ShardInfos, shardNode.ID) {
				// Shard need to be reopened
				p, err := s.factory.CreateTransferLeaderProcedure(ctx, coordinator.TransferLeaderRequest{
					Snapshot:          clusterSnapshot,
					ShardID:           shardNode.ID,
					OldLeaderNodeName: "",
					NewLeaderNodeName: node.Node.Name,
				})
				if err != nil {
					return emptyScheduleRes, err
				}
				procedures = append(procedures, p)
				reasons.WriteString(fmt.Sprintf("Cluster recover, assign shard to node, shardID:%d, nodeName:%s. ", shardNode.ID, node.Node.Name))
				if len(procedures) >= int(s.procedureExecutingBatchSize) {
					break
				}
			}
		}
	}

	if len(procedures) == 0 {
		return emptyScheduleRes, nil
	}

	batchProcedure, err := s.factory.CreateBatchTransferLeaderProcedure(ctx, coordinator.BatchRequest{
		Batch:     procedures,
		BatchType: procedure.TransferLeader,
	})
	if err != nil {
		return emptyScheduleRes, err
	}

	return scheduler.ScheduleResult{Procedure: batchProcedure, Reason: reasons.String()}, nil
}

func findOnlineNodeByName(nodeName string, nodes []metadata.RegisteredNode) (metadata.RegisteredNode, error) {
	now := time.Now()
	for i := 0; i < len(nodes); i++ {
		node := nodes[i]
		if node.IsExpired(now) {
			continue
		}
		if node.Node.Name == nodeName {
			return node, nil
		}
	}

	var node metadata.RegisteredNode
	return node, metadata.ErrNodeNotFound.WithMessagef("node:%s not found in topology", nodeName)
}

func containsShard(shardInfos []metadata.ShardInfo, shardID storage.ShardID) bool {
	for i := 0; i < len(shardInfos); i++ {
		if shardInfos[i].ID == shardID {
			return true
		}
	}
	return false
}

func findNodeByShard(shardID storage.ShardID, shardNodes []storage.ShardNode) (storage.ShardNode, bool) {
	n, found := slices.BinarySearchFunc(shardNodes, shardID, func(node storage.ShardNode, id storage.ShardID) int {
		return cmp.Compare(node.ID, id)
	})
	if !found {
		var emptyShardNode storage.ShardNode
		return emptyShardNode, false
	}
	return shardNodes[n], true
}
