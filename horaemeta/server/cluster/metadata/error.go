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

package metadata

import "github.com/apache/incubator-horaedb-meta/pkg/coderr"

var (
	ErrCreateCluster         = coderr.NewCodeErrorDef(coderr.BadRequest, "create cluster")
	ErrUpdateCluster         = coderr.NewCodeErrorDef(coderr.Internal, "update cluster")
	ErrStartCluster          = coderr.NewCodeErrorDef(coderr.Internal, "start cluster")
	ErrClusterAlreadyExists  = coderr.NewCodeErrorDef(coderr.ClusterAlreadyExists, "cluster already exists")
	ErrClusterNotFound       = coderr.NewCodeErrorDef(coderr.NotFound, "cluster not found")
	ErrClusterStateInvalid   = coderr.NewCodeErrorDef(coderr.Internal, "cluster state invalid")
	ErrSchemaNotFound        = coderr.NewCodeErrorDef(coderr.NotFound, "schema not found")
	ErrTableNotFound         = coderr.NewCodeErrorDef(coderr.NotFound, "table not found")
	ErrTableMetadataNotFound = coderr.NewCodeErrorDef(coderr.NotFound, "table metadata not found")
	ErrShardNotFound         = coderr.NewCodeErrorDef(coderr.NotFound, "shard not found")
	ErrVersionNotFound       = coderr.NewCodeErrorDef(coderr.NotFound, "version not found")
	ErrNodeNotFound          = coderr.NewCodeErrorDef(coderr.NotFound, "NodeName not found")
	ErrTableAlreadyExists    = coderr.NewCodeErrorDef(coderr.Internal, "table already exists")
	ErrOpenTable             = coderr.NewCodeErrorDef(coderr.Internal, "open table")
	ErrParseTopologyType     = coderr.NewCodeErrorDef(coderr.Internal, "parse topology type")
)
