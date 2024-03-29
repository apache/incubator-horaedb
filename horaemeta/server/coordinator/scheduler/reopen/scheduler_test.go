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

package reopen_test

import (
	"context"
	"testing"

	"github.com/apache/incubator-horaedb-meta/server/cluster/metadata"
	"github.com/apache/incubator-horaedb-meta/server/coordinator"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/procedure/test"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/scheduler/reopen"
	"github.com/apache/incubator-horaedb-meta/server/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestReopenShardScheduler(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	emptyCluster := test.InitEmptyCluster(ctx, t)

	procedureFactory := coordinator.NewFactory(zap.NewNop(), test.MockIDAllocator{}, test.MockDispatch{}, test.NewTestStorage(t), emptyCluster.GetMetadata())

	s := reopen.NewShardScheduler(procedureFactory, 1)

	// ReopenShardScheduler should not schedule when cluster is not stable.
	result, err := s.Schedule(ctx, emptyCluster.GetMetadata().GetClusterSnapshot())
	re.NoError(err)
	re.Nil(result.Procedure)

	stableCluster := test.InitStableCluster(ctx, t)
	snapshot := stableCluster.GetMetadata().GetClusterSnapshot()

	// Add shard with ready status.
	snapshot.RegisteredNodes[0].ShardInfos = append(snapshot.RegisteredNodes[0].ShardInfos, metadata.ShardInfo{
		ID:      0,
		Role:    storage.ShardRoleLeader,
		Version: 0,
		Status:  storage.ShardStatusReady,
	})
	re.NoError(err)
	re.Nil(result.Procedure)

	// Add shard with partitionOpen status.
	snapshot.RegisteredNodes[0].ShardInfos = append(snapshot.RegisteredNodes[0].ShardInfos, metadata.ShardInfo{
		ID:      1,
		Role:    storage.ShardRoleLeader,
		Version: 0,
		Status:  storage.ShardStatusPartialOpen,
	})
	result, err = s.Schedule(ctx, snapshot)
	re.NoError(err)
	re.NotNil(result.Procedure)
}
