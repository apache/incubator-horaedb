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
	"context"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/apache/incubator-horaedb-meta/pkg/log"
	"github.com/apache/incubator-horaedb-meta/server/member"
	"github.com/apache/incubator-horaedb-meta/server/service"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type ForwardClient struct {
	member *member.Member
	client *http.Client
	port   int
}

func NewForwardClient(member *member.Member, port int) *ForwardClient {
	return &ForwardClient{
		member: member,
		client: getForwardedHTTPClient(),
		port:   port,
	}
}

func (s *ForwardClient) GetLeaderAddr(ctx context.Context) (string, error) {
	resp, err := s.member.GetLeaderAddr(ctx)
	if err != nil {
		return "", err
	}

	return resp.LeaderEndpoint, nil
}

func (s *ForwardClient) getForwardedAddr(ctx context.Context) (string, bool, error) {
	resp, err := s.member.GetLeaderAddr(ctx)
	if err != nil {
		return "", false, errors.WithMessage(err, "get forwarded addr")
	}
	if resp.IsLocal {
		return "", true, nil
	}
	// TODO: In the current implementation, if the HTTP port of each node of HoraeMeta is inconsistent, the forwarding address will be wrong
	httpAddr, err := formatHTTPAddr(resp.LeaderEndpoint, s.port)
	if err != nil {
		return "", false, errors.WithMessage(err, "format http addr")
	}
	log.Info("getForwardedAddr", zap.String("leaderAddr", httpAddr), zap.Int("port", s.port))
	return httpAddr, false, nil
}

func (s *ForwardClient) forwardToLeader(req *http.Request) (*http.Response, bool, error) {
	addr, isLeader, err := s.getForwardedAddr(req.Context())
	if err != nil {
		log.Error("get forward addr failed", zap.Error(err))
		return nil, false, err
	}
	if isLeader {
		return nil, true, nil
	}

	// Update remote host
	req.RequestURI = ""
	if req.TLS == nil {
		req.URL.Scheme = "http"
	} else {
		req.URL.Scheme = "https"
	}
	req.URL.Host = addr

	resp, err := s.client.Do(req)
	if err != nil {
		log.Error("forward client send request failed", zap.Error(err))
		return nil, false, err
	}

	return resp, false, nil
}

func getForwardedHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				Deadline:  time.Time{},
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout: 10 * time.Second,
		},
	}
}

// formatHttpAddr convert grpcAddr(http://127.0.0.1:8831) httpPort(5000) to httpAddr(127.0.0.1:5000).
func formatHTTPAddr(grpcAddr string, httpPort int) (string, error) {
	url, err := url.Parse(grpcAddr)
	if err != nil {
		return "", service.ErrParseURL.WithCause(err)
	}
	hostAndPort := strings.Split(url.Host, ":")
	if len(hostAndPort) != 2 {
		return "", ErrParseLeaderAddr.WithMessagef("parse leader addr, grpcAdd:%s", grpcAddr)
	}
	hostAndPort[1] = strconv.Itoa(httpPort)
	httpAddr := strings.Join(hostAndPort, ":")
	return httpAddr, nil
}
