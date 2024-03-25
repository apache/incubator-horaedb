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

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/apache/incubator-horaedb-meta/pkg/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type EtcdAPI struct {
	etcdClient    *clientv3.Client
	forwardClient *ForwardClient
}

type AddMemberRequest struct {
	MemberAddrs []string `json:"memberAddrs"`
}

type UpdateMemberRequest struct {
	OldMemberName string   `json:"oldMemberName"`
	NewMemberAddr []string `json:"newMemberAddr"`
}

type RemoveMemberRequest struct {
	MemberName string `json:"memberName"`
}

type PromoteLearnerRequest struct {
	LearnerName string `json:"learnerName"`
}

type MoveLeaderRequest struct {
	MemberName string `json:"memberName"`
}

func NewEtcdAPI(etcdClient *clientv3.Client, forwardClient *ForwardClient) EtcdAPI {
	return EtcdAPI{
		etcdClient:    etcdClient,
		forwardClient: forwardClient,
	}
}

func (a *EtcdAPI) addMember(req *http.Request) apiFuncResult {
	var addMemberRequest AddMemberRequest
	err := json.NewDecoder(req.Body).Decode(&addMemberRequest)
	if err != nil {
		return errResult(ErrParseRequest.WithCause(err))
	}

	resp, err := a.etcdClient.MemberAdd(req.Context(), addMemberRequest.MemberAddrs)
	if err != nil {
		return errResult(ErrAddLearner.WithCause(err))
	}

	return okResult(resp)
}

func (a *EtcdAPI) getMember(req *http.Request) apiFuncResult {
	resp, err := a.etcdClient.MemberList(req.Context())
	if err != nil {
		return errResult(ErrListMembers.WithCause(err))
	}

	return okResult(resp)
}

func (a *EtcdAPI) updateMember(req *http.Request) apiFuncResult {
	var updateMemberRequest UpdateMemberRequest
	err := json.NewDecoder(req.Body).Decode(&updateMemberRequest)
	if err != nil {
		return errResult(ErrParseTopology.WithCause(err))
	}

	memberListResp, err := a.etcdClient.MemberList(req.Context())
	if err != nil {
		return errResult(ErrListMembers.WithCause(err))
	}

	for _, member := range memberListResp.Members {
		if member.Name == updateMemberRequest.OldMemberName {
			_, err := a.etcdClient.MemberUpdate(req.Context(), member.ID, updateMemberRequest.NewMemberAddr)
			if err != nil {
				return errResult(ErrRemoveMembers.WithCause(err))
			}
			return okResult("ok")
		}
	}

	return errResult(ErrGetMember.WithMessagef("member not found, member name:%s", updateMemberRequest.OldMemberName))
}

func (a *EtcdAPI) removeMember(req *http.Request) apiFuncResult {
	var removeMemberRequest RemoveMemberRequest
	err := json.NewDecoder(req.Body).Decode(&removeMemberRequest)
	if err != nil {
		return errResult(ErrParseRequest.WithCause(err))
	}

	memberListResp, err := a.etcdClient.MemberList(req.Context())
	if err != nil {
		return errResult(ErrListMembers.WithCause(err))
	}

	for _, member := range memberListResp.Members {
		if member.Name == removeMemberRequest.MemberName {
			_, err := a.etcdClient.MemberRemove(req.Context(), member.ID)
			if err != nil {
				return errResult(ErrRemoveMembers.WithCause(err))
			}

			return okResult("ok")
		}
	}

	return errResult(ErrGetMember.WithMessagef("member not found, member name:%s", removeMemberRequest.MemberName))
}

func (a *EtcdAPI) promoteLearner(req *http.Request) apiFuncResult {
	var promoteLearnerRequest PromoteLearnerRequest
	err := json.NewDecoder(req.Body).Decode(&promoteLearnerRequest)
	if err != nil {
		return errResult(ErrParseRequest.WithCause(err))
	}

	memberListResp, err := a.etcdClient.MemberList(req.Context())
	if err != nil {
		return errResult(ErrListMembers.WithCause(err))
	}

	for _, member := range memberListResp.Members {
		if member.Name == promoteLearnerRequest.LearnerName {
			_, err := a.etcdClient.MemberPromote(req.Context(), member.ID)
			if err != nil {
				return errResult(ErrRemoveMembers.WithCause(err))
			}
			return okResult("ok")
		}
	}

	return errResult(ErrGetMember.WithMessagef("learner not found, learner name:%s", promoteLearnerRequest.LearnerName))
}

func (a *EtcdAPI) moveLeader(req *http.Request) apiFuncResult {
	var moveLeaderRequest MoveLeaderRequest
	err := json.NewDecoder(req.Body).Decode(&moveLeaderRequest)
	if err != nil {
		return errResult(ErrParseRequest.WithCause(err))
	}

	memberListResp, err := a.etcdClient.MemberList(req.Context())
	if err != nil {
		return errResult(ErrListMembers.WithCause(err))
	}

	for _, member := range memberListResp.Members {
		if member.Name == moveLeaderRequest.MemberName {
			moveLeaderResp, err := a.etcdClient.MoveLeader(req.Context(), member.ID)
			if err != nil {
				return errResult(ErrRemoveMembers.WithCause(err))
			}
			log.Info("move leader", zap.String("moveLeaderResp", fmt.Sprintf("%v", moveLeaderResp)))
			return okResult("ok")
		}
	}

	return errResult(ErrGetMember.WithMessagef("member not found, member name:%s", moveLeaderRequest.MemberName))
}
