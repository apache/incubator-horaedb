// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Kafka implementation's config

use serde_derive::{Deserialize, Serialize};

/// Generic client config that is used for consumers, producers as well as admin
/// operations (like "create topic").
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub client_config: ClientConfig,
    pub topic_management_config: TopicManagementConfig,
    pub consumer_config: ConsumerConfig,
    // TODO: may need some config options for producer,
    // but it seems nothing needed now.
}

#[derive(Clone, Default, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ClientConfig {
    /// The endpoint of boost broker, must be set and will panic if found it
    /// None.
    pub boost_broker: Option<String>,

    /// Maximum message size in bytes.
    ///
    /// Defaults to `None` (rskafka default).
    pub max_message_size: Option<usize>,

    /// Optional SOCKS5 proxy to use for connecting to the brokers.
    ///
    /// Defaults to `None`.
    pub socks5_proxy: Option<String>,
}

/// Config for topic creation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct TopicManagementConfig {
    /// Replication factor.
    ///
    /// Extracted from `replication_factor` option. Defaults to `1`.
    pub create_replication_factor: i16,

    /// The maximum amount of time to wait while creating topic.
    ///
    /// Defaults to `5_000`.
    pub create_max_wait_ms: i32,

    /// The maximum amount of time to wait while deleting records in topic.
    ///
    /// Defaults to `5_000`.
    pub delete_max_wait_ms: i32,
}

impl Default for TopicManagementConfig {
    fn default() -> Self {
        Self {
            create_replication_factor: 1,
            create_max_wait_ms: 5000,
            delete_max_wait_ms: 5000,
        }
    }
}

/// Config for consumers.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ConsumerConfig {
    /// The maximum amount of time to wait for data before returning.
    ///
    /// Defaults to `None` (rskafka default).
    pub max_wait_ms: Option<i32>,

    /// The maximum amount of data for the consumer to fetch in a single batch.
    ///
    /// Defaults to `None` (rskafka default).
    pub min_batch_size: Option<i32>,

    /// Will wait for at least `min_batch_size` bytes of data.
    ///
    /// Defaults to `None` (rskafka default).
    pub max_batch_size: Option<i32>,
}
