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

package grpc

import (
	"context"

	"github.com/apache/incubator-horaedb-meta/pkg/coderr"
	"github.com/apache/incubator-horaedb-meta/pkg/log"
	"github.com/apache/incubator-horaedb-meta/server/service"
	"github.com/apache/incubator-horaedb-proto/golang/pkg/metaservicepb"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// getForwardedMetaClient get forwarded horaemeta client. When current node is the leader, this func will return (nil,nil).
func (s *Service) getForwardedMetaClient(ctx context.Context) (metaservicepb.MetaRpcServiceClient, error) {
	forwardedAddr, _, err := s.getForwardedAddr(ctx)
	if err != nil {
		return nil, errors.WithMessage(err, "get forwarded horaemeta client")
	}

	if forwardedAddr != "" {
		horaeClient, err := s.getMetaClient(ctx, forwardedAddr)
		if err != nil {
			return nil, coderr.Wrapf(err, "get forwarded horaemeta client, addr:%s", forwardedAddr)
		}
		return horaeClient, nil
	}
	return nil, nil
}

func (s *Service) getMetaClient(ctx context.Context, addr string) (metaservicepb.MetaRpcServiceClient, error) {
	client, err := s.getForwardedGrpcClient(ctx, addr)
	if err != nil {
		return nil, coderr.Wrapf(err, "get horaemeta client, addr:%s", addr)
	}
	return metaservicepb.NewMetaRpcServiceClient(client), nil
}

func (s *Service) getForwardedGrpcClient(ctx context.Context, forwardedAddr string) (*grpc.ClientConn, error) {
	client, ok := s.conns.Load(forwardedAddr)
	if !ok {
		log.Info("try to create horaemeta client", zap.String("addr", forwardedAddr))
		cc, err := service.GetClientConn(ctx, forwardedAddr)
		if err != nil {
			return nil, err
		}
		client = cc
		s.conns.Store(forwardedAddr, cc)
	}
	return client.(*grpc.ClientConn), nil
}

func (s *Service) getForwardedAddr(ctx context.Context) (string, bool, error) {
	resp, err := s.h.GetLeader(ctx)
	if err != nil {
		return "", false, errors.WithMessage(err, "get forwarded addr")
	}
	if resp.IsLocal {
		return "", true, nil
	}
	return resp.LeaderEndpoint, false, nil
}
