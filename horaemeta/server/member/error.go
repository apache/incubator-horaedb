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

package member

import "github.com/apache/incubator-horaedb-meta/pkg/coderr"

var (
	ErrResetLeader        = coderr.NewCodeErrorDef(coderr.Internal, "reset leader by deleting leader key")
	ErrGetLeader          = coderr.NewCodeErrorDef(coderr.Internal, "get leader by querying leader key")
	ErrTxnPutLeader       = coderr.NewCodeErrorDef(coderr.Internal, "put leader key in txn")
	ErrMultipleLeader     = coderr.NewCodeErrorDef(coderr.Internal, "multiple leaders found")
	ErrInvalidLeaderValue = coderr.NewCodeErrorDef(coderr.Internal, "invalid leader value")
	ErrMarshalMember      = coderr.NewCodeErrorDef(coderr.Internal, "marshal member information")
	ErrGrantLease         = coderr.NewCodeErrorDef(coderr.Internal, "grant lease")
	ErrRevokeLease        = coderr.NewCodeErrorDef(coderr.Internal, "revoke lease")
	ErrCloseLease         = coderr.NewCodeErrorDef(coderr.Internal, "close lease")
)
