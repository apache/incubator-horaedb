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

package procedure

import "github.com/apache/incubator-horaedb-meta/pkg/coderr"

var (
	ErrShardLeaderNotFound     = coderr.NewCodeErrorDef(coderr.Internal, "shard leader not found")
	ErrShardNotMatch           = coderr.NewCodeErrorDef(coderr.Internal, "target shard not match to persis data")
	ErrProcedureNotFound       = coderr.NewCodeErrorDef(coderr.Internal, "procedure not found")
	ErrClusterConfigChanged    = coderr.NewCodeErrorDef(coderr.Internal, "cluster config changed")
	ErrTableNotExists          = coderr.NewCodeErrorDef(coderr.Internal, "table not exists")
	ErrTableAlreadyExists      = coderr.NewCodeErrorDef(coderr.Internal, "table already exists")
	ErrListRunningProcedure    = coderr.NewCodeErrorDef(coderr.Internal, "procedure type not match")
	ErrListProcedure           = coderr.NewCodeErrorDef(coderr.Internal, "list running procedure")
	ErrDecodeRawData           = coderr.NewCodeErrorDef(coderr.Internal, "decode raw data")
	ErrEncodeRawData           = coderr.NewCodeErrorDef(coderr.Internal, "encode raw data")
	ErrGetRequest              = coderr.NewCodeErrorDef(coderr.Internal, "get request from event")
	ErrNodeNumberNotEnough     = coderr.NewCodeErrorDef(coderr.Internal, "node number not enough")
	ErrEmptyPartitionNames     = coderr.NewCodeErrorDef(coderr.Internal, "partition names is empty")
	ErrDropTableResult         = coderr.NewCodeErrorDef(coderr.Internal, "length of shard not correct")
	ErrPickShard               = coderr.NewCodeErrorDef(coderr.Internal, "pick shard failed")
	ErrSubmitProcedure         = coderr.NewCodeErrorDef(coderr.Internal, "submit new procedure")
	ErrQueueFull               = coderr.NewCodeErrorDef(coderr.Internal, "queue is full, unable to offer more data")
	ErrPushDuplicatedProcedure = coderr.NewCodeErrorDef(coderr.Internal, "try to push duplicated procedure")
	ErrShardNumberNotEnough    = coderr.NewCodeErrorDef(coderr.Internal, "shard number not enough")
	ErrEmptyBatchProcedure     = coderr.NewCodeErrorDef(coderr.Internal, "procedure batch is empty")
	ErrMergeBatchProcedure     = coderr.NewCodeErrorDef(coderr.Internal, "failed to merge procedures batch")
)
