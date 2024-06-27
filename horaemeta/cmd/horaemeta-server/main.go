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

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/apache/incubator-horaedb-meta/pkg/coderr"
	"github.com/apache/incubator-horaedb-meta/pkg/log"
	"github.com/apache/incubator-horaedb-meta/server"
	"github.com/apache/incubator-horaedb-meta/server/config"
	"github.com/pelletier/go-toml/v2"
	"go.uber.org/zap"
)

var (
	buildDate  string
	branchName string
	commitID   string
)

func buildVersion() string {
	return fmt.Sprintf("HoraeMeta Server\nGit commit:%s\nGit branch:%s\nBuild date:%s", commitID, branchName, buildDate)
}

func panicf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	panic(msg)
}

func main() {
	cfgParser, err := config.MakeConfigParser()
	if err != nil {
		panicf("fail to generate config builder, err:%v", err)
	}

	cfg, err := cfgParser.Parse(os.Args[1:])
	if coderr.Is(err, coderr.PrintHelpUsage) {
		return
	}

	if err != nil {
		panicf("fail to parse config from command line params, err:%v", err)
	}

	if cfgParser.NeedPrintVersion() {
		println(buildVersion())
		return
	}

	if err := cfg.ValidateAndAdjust(); err != nil {
		panicf("invalid config, err:%v", err)
	}

	if err := cfgParser.ParseConfigFromToml(); err != nil {
		panicf("fail to parse config from toml file, err:%v", err)
	}

	if err := cfgParser.ParseConfigFromEnv(); err != nil {
		panicf("fail to parse config from environment variable, err:%v", err)
	}

	cfgByte, err := toml.Marshal(cfg)
	if err != nil {
		panicf("fail to marshal server config, err:%v", err)
	}

	if err = os.MkdirAll(cfg.DataDir, os.ModePerm); err != nil {
		panicf("fail to create data dir, data_dir:%v, err:%v", cfg.DataDir, err)
	}

	logger, err := log.InitGlobalLogger(&cfg.Log)
	if err != nil {
		panicf("fail to init global logger, err:%v", err)
	}
	defer logger.Sync() //nolint:errcheck
	log.Info(fmt.Sprintf("server start with version:%s", buildVersion()))
	// TODO: Do adjustment to config for preparing joining existing cluster.
	log.Info("server start with config", zap.String("config", string(cfgByte)))

	srv, err := server.CreateServer(cfg)
	if err != nil {
		log.Error("fail to create server", zap.Error(err))
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	var sig os.Signal
	go func() {
		sig = <-sc
		cancel()
	}()

	if err := srv.Run(ctx); err != nil {
		log.Error("fail to run server", zap.Error(err))
		return
	}

	<-ctx.Done()
	log.Info("got signal to exit", zap.Any("signal", sig))

	srv.Close()
}
