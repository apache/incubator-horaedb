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

package coordinator

import (
	"context"
	"sort"

	"github.com/apache/incubator-horaedb-meta/pkg/assert"
	"github.com/apache/incubator-horaedb-meta/server/cluster/metadata"
	"github.com/apache/incubator-horaedb-meta/server/storage"
)

// ShardPicker is used to pick up the shards suitable for scheduling in the cluster.
// If expectShardNum bigger than cluster node number, the result depends on enableDuplicateNode:
// TODO: Consider refactor this interface, abstracts the parameters of PickShards as PickStrategy.
type ShardPicker interface {
	PickShards(ctx context.Context, snapshot metadata.Snapshot, expectShardNum int) ([]storage.ShardNode, error)
}

// LeastTableShardPicker selects shards based on the number of tables on the current shard,
// and always selects the shard with the smallest number of current tables.
type leastTableShardPicker struct{}

func NewLeastTableShardPicker() ShardPicker {
	return &leastTableShardPicker{}
}

func (l leastTableShardPicker) PickShards(_ context.Context, snapshot metadata.Snapshot, expectShardNum int) ([]storage.ShardNode, error) {
	if len(snapshot.Topology.ClusterView.ShardNodes) == 0 {
		return nil, ErrNodeNumberNotEnough.WithMessagef("no shard is assigned")
	}

	shardNodeMapping := make(map[storage.ShardID]storage.ShardNode, len(snapshot.Topology.ShardViewsMapping))
	sortedShardsByTableCount := make([]storage.ShardID, 0, len(snapshot.Topology.ShardViewsMapping))
	for _, shardNode := range snapshot.Topology.ClusterView.ShardNodes {
		shardNodeMapping[shardNode.ID] = shardNode
		// Only collect the shards witch has been allocated to a node.
		sortedShardsByTableCount = append(sortedShardsByTableCount, shardNode.ID)
	}

	// Sort shard by table number,
	// the shard with the smallest number of tables is at the front of the array.
	sort.SliceStable(sortedShardsByTableCount, func(i, j int) bool {
		shardView1 := snapshot.Topology.ShardViewsMapping[sortedShardsByTableCount[i]]
		shardView2 := snapshot.Topology.ShardViewsMapping[sortedShardsByTableCount[j]]
		// When the number of tables is the same, sort according to the size of ShardID.
		if len(shardView1.TableIDs) == len(shardView2.TableIDs) {
			return shardView1.ShardID < shardView2.ShardID
		}
		return len(shardView1.TableIDs) < len(shardView2.TableIDs)
	})

	result := make([]storage.ShardNode, 0, expectShardNum)

	for i := 0; i < expectShardNum; i++ {
		selectShardID := sortedShardsByTableCount[i%len(sortedShardsByTableCount)]
		shardNode, ok := shardNodeMapping[selectShardID]
		assert.Assert(ok)
		result = append(result, shardNode)
	}

	return result, nil
}
