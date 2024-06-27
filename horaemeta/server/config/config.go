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

package config

import (
	"flag"
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	"github.com/apache/incubator-horaedb-meta/pkg/coderr"
	"github.com/apache/incubator-horaedb-meta/pkg/log"
	"github.com/caarlos0/env/v6"
	"github.com/pelletier/go-toml/v2"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

const (
	defaultEnableEmbedEtcd bool = true
	defaultEtcdCaCertPath       = ""
	defaultEtcdKeyPath          = ""
	defaultEtcdCertPath         = ""

	defaultEnableLimiter          bool  = true
	defaultInitialLimiterCapacity int   = 100 * 1000
	defaultInitialLimiterRate     int   = 10 * 1000
	defaultEtcdStartTimeoutMs     int64 = 60 * 1000
	defaultCallTimeoutMs                = 5 * 1000
	defaultEtcdMaxTxnOps                = 128
	defaultEtcdLeaseTTLSec              = 10

	defaultGrpcHandleTimeoutMs int = 60 * 1000
	// GrpcServiceMaxSendMsgSize controls the max size of the sent message(200MB by default).
	defaultGrpcServiceMaxSendMsgSize int = 200 * 1024 * 1024
	// GrpcServiceMaxRecvMsgSize controls the max size of the received message(100MB by default).
	defaultGrpcServiceMaxRecvMsgSize int = 100 * 1024 * 1024
	// GrpcServiceKeepAlivePingMinIntervalSec controls the min interval for one keepalive ping.
	defaultGrpcServiceKeepAlivePingMinIntervalSec int = 20

	defaultNodeNamePrefix          = "horaemeta"
	defaultEndpoint                = "127.0.0.1"
	defaultRootPath                = "/horaedb"
	defaultClientUrls              = "http://0.0.0.0:2379"
	defaultPeerUrls                = "http://0.0.0.0:2380"
	defaultInitialClusterState     = embed.ClusterStateFlagNew
	defaultInitialClusterToken     = "horaemeta-cluster" //#nosec G101
	defaultCompactionMode          = "periodic"
	defaultAutoCompactionRetention = "1h"

	defaultTickIntervalMs    int64 = 500
	defaultElectionTimeoutMs       = 3000
	defaultQuotaBackendBytes       = 8 * 1024 * 1024 * 1024 // 8GB

	defaultMaxRequestBytes uint = 2 * 1024 * 1024 // 2MB

	defaultMaxScanLimit    int  = 100
	defaultMinScanLimit    int  = 20
	defaultMaxOpsPerTxn    int  = 32
	defaultIDAllocatorStep uint = 20

	DefaultClusterName       = "defaultCluster"
	defaultClusterNodeCount  = 2
	defaultClusterShardTotal = 8
	enableSchedule           = true
	// topologyType is used to determine the scheduling cluster strategy of HoraeMeta. It should be determined according to the storage method of HoraeDB. The default is static to support local storage.
	defaultTopologyType                = "static"
	defaultProcedureExecutingBatchSize = math.MaxUint32

	defaultHTTPPort = 8080
	defaultGrpcPort = 2379

	defaultDataDir = "/tmp/horaemeta"

	defaultEtcdDataDir = "/etcd"
	defaultWalDir      = "/wal"

	defaultEtcdLogFile = "/etcd.log"
)

type LimiterConfig struct {
	// Enable is used to control the switch of the limiter.
	Enable bool `toml:"enable" env:"FLOW_LIMITER_ENABLE"`
	// Limit is the updated rate of tokens.
	Limit int `toml:"limit" env:"FLOW_LIMITER_LIMIT"`
	// Burst is the maximum number of tokens.
	Burst int `toml:"burst" env:"FLOW_LIMITER_BURST"`
}

// Config is server start config, it has three input modes:
// 1. toml config file
// 2. env variables
// Their loading has priority, and low priority configurations will be overwritten by high priority configurations.
// The priority from high to low is: env variables > toml config file.
type Config struct {
	Log         log.Config    `toml:"log" env:"LOG"`
	EtcdLog     log.Config    `toml:"etcd-log" env:"ETCD_LOG"`
	FlowLimiter LimiterConfig `toml:"flow-limiter" env:"FLOW_LIMITER"`

	EnableEmbedEtcd bool   `toml:"enable-embed-etcd" env:"ENABLE_EMBED_ETCD"`
	EtcdCaCertPath  string `toml:"etcd-ca-cert-path" env:"ETCD_CA_CERT_PATH"`
	EtcdKeyPath     string `toml:"etcd-key-path" env:"ETCD_KEY_PATH"`
	EtcdCertPath    string `toml:"etcd-cert-path" env:"ETCD_CERT_PATH"`

	EtcdStartTimeoutMs int64 `toml:"etcd-start-timeout-ms" env:"ETCD_START_TIMEOUT_MS"`
	EtcdCallTimeoutMs  int64 `toml:"etcd-call-timeout-ms" env:"ETCD_CALL_TIMEOUT_MS"`
	EtcdMaxTxnOps      int64 `toml:"etcd-max-txn-ops" env:"ETCD_MAX_TXN_OPS"`

	GrpcHandleTimeoutMs                    int `toml:"grpc-handle-timeout-ms" env:"GRPC_HANDLER_TIMEOUT_MS"`
	GrpcServiceMaxSendMsgSize              int `toml:"grpc-service-max-send-msg-size" env:"GRPC_SERVICE_MAX_SEND_MSG_SIZE"`
	GrpcServiceMaxRecvMsgSize              int `toml:"grpc-service-max-recv-msg-size" env:"GRPC_SERVICE_MAX_RECV_MSG_SIZE"`
	GrpcServiceKeepAlivePingMinIntervalSec int `toml:"grpc-service-keep-alive-ping-min-interval-sec" env:"GRPC_SERVICE_KEEP_ALIVE_PING_MIN_INTERVAL_SEC"`

	LeaseTTLSec int64 `toml:"lease-sec" env:"LEASE_SEC"`

	NodeName            string `toml:"node-name" env:"NODE_NAME"`
	Addr                string `toml:"addr" env:"ADDR"`
	DataDir             string `toml:"data-dir" env:"DATA_DIR"`
	StorageRootPath     string `toml:"storage-root-path" env:"STORAGE_ROOT_PATH"`
	InitialCluster      string `toml:"initial-cluster" env:"INITIAL_CLUSTER"`
	InitialClusterState string `toml:"initial-cluster-state" env:"INITIAL_CLUSTER_STATE"`
	InitialClusterToken string `toml:"initial-cluster-token" env:"INITIAL_CLUSTER_TOKEN"`
	// TickInterval is the interval for etcd Raft tick.
	TickIntervalMs    int64 `toml:"tick-interval-ms" env:"TICK_INTERVAL_MS"`
	ElectionTimeoutMs int64 `toml:"election-timeout-ms" env:"ELECTION_TIMEOUT_MS"`
	// QuotaBackendBytes Raise alarms when backend size exceeds the given quota. 0 means use the default quota.
	// the default size is 2GB, the maximum is 8GB.
	QuotaBackendBytes int64 `toml:"quota-backend-bytes" env:"QUOTA_BACKEND_BYTES"`
	// AutoCompactionMode is either 'periodic' or 'revision'. The default value is 'periodic'.
	AutoCompactionMode string `toml:"auto-compaction-mode" env:"AUTO-COMPACTION-MODE"`
	// AutoCompactionRetention is either duration string with time unit
	// (e.g. '5m' for 5-minute), or revision unit (e.g. '5000').
	// If no time unit is provided and compaction mode is 'periodic',
	// the unit defaults to hour. For example, '5' translates into 5-hour.
	// The default retention is 1 hour.
	// Before etcd v3.3.x, the type of retention is int. We add 'v2' suffix to make it backward compatible.
	AutoCompactionRetention string `toml:"auto-compaction-retention" env:"AUTO_COMPACTION_RETENTION"`
	MaxRequestBytes         uint   `toml:"max-request-bytes" env:"MAX_REQUEST_BYTES"`
	MaxScanLimit            int    `toml:"max-scan-limit" env:"MAX_SCAN_LIMIT"`
	MinScanLimit            int    `toml:"min-scan-limit" env:"MIN_SCAN_LIMIT"`
	MaxOpsPerTxn            int    `toml:"max-ops-per-txn" env:"MAX_OPS_PER_TXN"`
	IDAllocatorStep         uint   `toml:"id-allocator-step" env:"ID_ALLOCATOR_STEP"`

	// Following fields are the settings for the default cluster.
	DefaultClusterName       string `toml:"default-cluster-name" env:"DEFAULT_CLUSTER_NAME"`
	DefaultClusterNodeCount  int    `toml:"default-cluster-node-count" env:"DEFAULT_CLUSTER_NODE_COUNT"`
	DefaultClusterShardTotal int    `toml:"default-cluster-shard-total" env:"DEFAULT_CLUSTER_SHARD_TOTAL"`

	// When the EnableSchedule is turned on, the failover scheduling will be turned on, which is used for HoraeDB cluster publishing and using local storage.
	EnableSchedule bool `toml:"enable-schedule" env:"ENABLE_SCHEDULE"`
	// TopologyType indicates the schedule type used by the HoraeDB cluster, it will determine the strategy of HoraeMeta scheduling cluster.
	TopologyType string `toml:"topology-type" env:"TOPOLOGY_TYPE"`
	// ProcedureExecutingBatchSize determines the maximum number of shards in a single batch when opening shards concurrently.
	ProcedureExecutingBatchSize uint32 `toml:"procedure-executing-batch-size" env:"PROCEDURE_EXECUTING_BATCH_SIZE"`

	ClientUrls          string `toml:"client-urls" env:"CLIENT_URLS"`
	PeerUrls            string `toml:"peer-urls" env:"PEER_URLS"`
	AdvertiseClientUrls string `toml:"advertise-client-urls" env:"ADVERTISE_CLIENT_URLS"`
	AdvertisePeerUrls   string `toml:"advertise-peer-urls" env:"ADVERTISE_PEER_URLS"`

	HTTPPort int `toml:"http-port" env:"HTTP_PORT"`
	GrpcPort int `toml:"grpc-port" env:"GRPC_PORT"`
}

func (c *Config) GrpcHandleTimeout() time.Duration {
	return time.Duration(c.GrpcHandleTimeoutMs) * time.Millisecond
}

func (c *Config) EtcdStartTimeout() time.Duration {
	return time.Duration(c.EtcdStartTimeoutMs) * time.Millisecond
}

func (c *Config) EtcdCallTimeout() time.Duration {
	return time.Duration(c.EtcdCallTimeoutMs) * time.Millisecond
}

// ValidateAndAdjust validates the config fields and adjusts some fields which should be adjusted.
// Return error if any field is invalid.
func (c *Config) ValidateAndAdjust() error {
	return nil
}

func (c *Config) GenEtcdConfig() (*embed.Config, error) {
	cfg := embed.NewConfig()

	cfg.Name = c.NodeName
	cfg.Dir = strings.Join([]string{c.DataDir, defaultEtcdDataDir}, "")
	cfg.WalDir = strings.Join([]string{c.DataDir, defaultWalDir}, "")
	cfg.InitialCluster = c.InitialCluster
	cfg.ClusterState = c.InitialClusterState
	cfg.InitialClusterToken = c.InitialClusterToken
	cfg.EnablePprof = true
	cfg.TickMs = uint(c.TickIntervalMs)
	cfg.ElectionMs = uint(c.ElectionTimeoutMs)
	cfg.AutoCompactionMode = c.AutoCompactionMode
	cfg.AutoCompactionRetention = c.AutoCompactionRetention
	cfg.QuotaBackendBytes = c.QuotaBackendBytes
	cfg.MaxRequestBytes = c.MaxRequestBytes
	cfg.MaxTxnOps = uint(c.EtcdMaxTxnOps)

	var err error
	cfg.ListenPeerUrls, err = parseUrls(c.PeerUrls)
	if err != nil {
		return nil, err
	}

	cfg.AdvertisePeerUrls, err = parseUrls(c.AdvertisePeerUrls)
	if err != nil {
		return nil, err
	}

	cfg.ListenClientUrls, err = parseUrls(c.ClientUrls)
	if err != nil {
		return nil, err
	}

	cfg.AdvertiseClientUrls, err = parseUrls(c.AdvertiseClientUrls)
	if err != nil {
		return nil, err
	}

	cfg.Logger = "zap"
	cfg.LogOutputs = []string{strings.Join([]string{c.DataDir, defaultEtcdLogFile}, "")}
	cfg.LogLevel = c.EtcdLog.Level

	return cfg, nil
}

// Parser builds the config from the flags.
type Parser struct {
	flagSet        *flag.FlagSet
	cfg            *Config
	configFilePath string
	version        *bool
}

func (p *Parser) Parse(arguments []string) (*Config, error) {
	if err := p.flagSet.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil, ErrHelpRequested.WithCause(err)
		}
		return nil, ErrInvalidCommandArgs.WithMessagef("fail to parse flag arguments:%v, err:%v", arguments, err)
	}
	return p.cfg, nil
}

func (p *Parser) NeedPrintVersion() bool {
	return *p.version
}

func makeDefaultNodeName() (string, error) {
	host, err := os.Hostname()
	if err != nil {
		return "", ErrRetrieveHostname.WithCause(err)
	}

	return fmt.Sprintf("%s-%s", defaultNodeNamePrefix, host), nil
}

func makeDefaultInitialCluster(nodeName string) string {
	return fmt.Sprintf("%s=%s", nodeName, defaultPeerUrls)
}

func MakeConfigParser() (*Parser, error) {
	defaultNodeName, err := makeDefaultNodeName()
	if err != nil {
		return nil, err
	}
	defaultInitialCluster := makeDefaultInitialCluster(defaultNodeName)

	fs, cfg := flag.NewFlagSet("meta", flag.ContinueOnError), &Config{
		Log: log.Config{
			Level: log.DefaultLogLevel,
			File:  log.DefaultLogFile,
		},
		EtcdLog: log.Config{
			Level: log.DefaultLogLevel,
			File:  log.DefaultLogFile,
		},
		FlowLimiter: LimiterConfig{
			Enable: defaultEnableLimiter,
			Limit:  defaultInitialLimiterRate,
			Burst:  defaultInitialLimiterCapacity,
		},

		EnableEmbedEtcd: defaultEnableEmbedEtcd,
		EtcdCaCertPath:  defaultEtcdCaCertPath,
		EtcdCertPath:    defaultEtcdCertPath,
		EtcdKeyPath:     defaultEtcdKeyPath,

		EtcdStartTimeoutMs: defaultEtcdStartTimeoutMs,
		EtcdCallTimeoutMs:  defaultCallTimeoutMs,
		EtcdMaxTxnOps:      defaultEtcdMaxTxnOps,

		GrpcHandleTimeoutMs:                    defaultGrpcHandleTimeoutMs,
		GrpcServiceMaxSendMsgSize:              defaultGrpcServiceMaxSendMsgSize,
		GrpcServiceMaxRecvMsgSize:              defaultGrpcServiceMaxRecvMsgSize,
		GrpcServiceKeepAlivePingMinIntervalSec: defaultGrpcServiceKeepAlivePingMinIntervalSec,

		LeaseTTLSec: defaultEtcdLeaseTTLSec,

		NodeName:        defaultNodeName,
		Addr:            defaultEndpoint,
		DataDir:         defaultDataDir,
		StorageRootPath: defaultRootPath,

		InitialCluster:      defaultInitialCluster,
		InitialClusterState: defaultInitialClusterState,
		InitialClusterToken: defaultInitialClusterToken,

		ClientUrls:          defaultClientUrls,
		AdvertiseClientUrls: defaultClientUrls,
		PeerUrls:            defaultPeerUrls,
		AdvertisePeerUrls:   defaultPeerUrls,

		TickIntervalMs:    defaultTickIntervalMs,
		ElectionTimeoutMs: defaultElectionTimeoutMs,

		QuotaBackendBytes:       defaultQuotaBackendBytes,
		AutoCompactionMode:      defaultCompactionMode,
		AutoCompactionRetention: defaultAutoCompactionRetention,
		MaxRequestBytes:         defaultMaxRequestBytes,
		MaxScanLimit:            defaultMaxScanLimit,
		MinScanLimit:            defaultMinScanLimit,
		MaxOpsPerTxn:            defaultMaxOpsPerTxn,
		IDAllocatorStep:         defaultIDAllocatorStep,

		DefaultClusterName:          DefaultClusterName,
		DefaultClusterNodeCount:     defaultClusterNodeCount,
		DefaultClusterShardTotal:    defaultClusterShardTotal,
		EnableSchedule:              enableSchedule,
		TopologyType:                defaultTopologyType,
		ProcedureExecutingBatchSize: defaultProcedureExecutingBatchSize,

		HTTPPort: defaultHTTPPort,
		GrpcPort: defaultGrpcPort,
	}

	version := fs.Bool("version", false, "print version information")

	builder := &Parser{
		flagSet:        fs,
		cfg:            cfg,
		version:        version,
		configFilePath: "",
	}

	fs.StringVar(&builder.configFilePath, "config", "", "config file path")

	return builder, nil
}

// ParseConfigFromToml read configuration from the toml file, if the config item already exists, it will be overwritten.
func (p *Parser) ParseConfigFromToml() error {
	if len(p.configFilePath) == 0 {
		log.Info("no config file specified, skip parse config")
		return nil
	}
	log.Info("get config from toml", zap.String("configFile", p.configFilePath))

	file, err := os.ReadFile(p.configFilePath)
	if err != nil {
		log.Error("err", zap.Error(err))
		return errors.WithMessage(err, fmt.Sprintf("read config file, configFile:%s", p.configFilePath))
	}
	log.Info("toml config value", zap.String("config", string(file)))

	err = toml.Unmarshal(file, p.cfg)
	if err != nil {
		log.Error("err", zap.Error(err))
		return coderr.Wrapf(err, "unmarshal toml config, configFile:%s", p.configFilePath)
	}

	return nil
}

func (p *Parser) ParseConfigFromEnv() error {
	err := env.Parse(p.cfg)
	if err != nil {
		return coderr.Wrapf(err, "parse config from env variables")
	}
	return nil
}
