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

package http

import "github.com/apache/incubator-horaedb-meta/pkg/coderr"

// FIXME: the error definitions needs updates.
var (
	ErrParseRequest                  = coderr.NewCodeErrorDef(coderr.BadRequest, "parse request params")
	ErrInvalidParamsForCreateCluster = coderr.NewCodeErrorDef(coderr.BadRequest, "invalid params to create cluster")
	ErrInvalidClusterName            = coderr.NewCodeErrorDef(coderr.BadRequest, "get cluster")
	ErrTable                         = coderr.NewCodeErrorDef(coderr.Internal, "table")
	ErrRoute                         = coderr.NewCodeErrorDef(coderr.Internal, "route table")
	ErrGetNodeShards                 = coderr.NewCodeErrorDef(coderr.Internal, "get node shards")
	ErrDropNodeShards                = coderr.NewCodeErrorDef(coderr.Internal, "drop node shards")
	ErrCreateProcedure               = coderr.NewCodeErrorDef(coderr.Internal, "create procedure")
	ErrSubmitProcedure               = coderr.NewCodeErrorDef(coderr.Internal, "submit procedure")
	ErrGetCluster                    = coderr.NewCodeErrorDef(coderr.Internal, "get cluster")
	ErrAlreadyExistsCluster          = coderr.NewCodeErrorDef(coderr.Internal, "the cluster to create exists already")
	ErrAllocShardID                  = coderr.NewCodeErrorDef(coderr.Internal, "alloc shard id")
	ErrForwardToLeader               = coderr.NewCodeErrorDef(coderr.Internal, "forward to leader")
	ErrParseLeaderAddr               = coderr.NewCodeErrorDef(coderr.Internal, "parse leader addr")
	ErrHealthCheck                   = coderr.NewCodeErrorDef(coderr.Internal, "server health check")
	ErrParseTopology                 = coderr.NewCodeErrorDef(coderr.Internal, "parse topology type")
	ErrUpdateFlowLimiter             = coderr.NewCodeErrorDef(coderr.Internal, "update flow limiter")
	ErrGetEnableSchedule             = coderr.NewCodeErrorDef(coderr.Internal, "get enableSchedule")
	ErrUpdateEnableSchedule          = coderr.NewCodeErrorDef(coderr.Internal, "update enableSchedule")
	ErrAddLearner                    = coderr.NewCodeErrorDef(coderr.Internal, "add member as learner")
	ErrListMembers                   = coderr.NewCodeErrorDef(coderr.Internal, "get member list")
	ErrRemoveMembers                 = coderr.NewCodeErrorDef(coderr.Internal, "remove member")
	ErrGetMember                     = coderr.NewCodeErrorDef(coderr.Internal, "get member")
	ErrListAffinityRules             = coderr.NewCodeErrorDef(coderr.Internal, "list affinity rules")
	ErrAddAffinityRule               = coderr.NewCodeErrorDef(coderr.Internal, "add affinity rule")
	ErrRemoveAffinityRule            = coderr.NewCodeErrorDef(coderr.Internal, "remove affinity rule")
)
