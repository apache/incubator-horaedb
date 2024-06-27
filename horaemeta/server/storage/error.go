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

package storage

import "github.com/apache/incubator-horaedb-meta/pkg/coderr"

var (
	ErrEncode = coderr.NewCodeErrorDef(coderr.Internal, "storage encode")
	ErrDecode = coderr.NewCodeErrorDef(coderr.Internal, "storage decode")

	ErrCreateSchemaAgain         = coderr.NewCodeErrorDef(coderr.Internal, "storage create schemas")
	ErrCreateClusterAgain        = coderr.NewCodeErrorDef(coderr.Internal, "storage create cluster")
	ErrUpdateCluster             = coderr.NewCodeErrorDef(coderr.Internal, "storage update cluster")
	ErrCreateClusterViewAgain    = coderr.NewCodeErrorDef(coderr.Internal, "storage create cluster view")
	ErrUpdateClusterViewConflict = coderr.NewCodeErrorDef(coderr.Internal, "storage update cluster view")
	ErrCreateTableAgain          = coderr.NewCodeErrorDef(coderr.Internal, "storage create tables")
	ErrDeleteTableAgain          = coderr.NewCodeErrorDef(coderr.Internal, "storage delete table")
	ErrCreateShardViewAgain      = coderr.NewCodeErrorDef(coderr.Internal, "storage create shard view")
	ErrUpdateShardViewConflict   = coderr.NewCodeErrorDef(coderr.Internal, "storage update shard view")
)
