// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Context for instance

use std::{fmt, sync::Arc};

use table_engine::engine::EngineRuntimes;

use crate::{sst::meta_data::cache::MetaCacheRef, Config};

/// Context for instance open
pub struct OpenContext {
    /// Engine config
    pub config: Config,

    /// Background job runtime
    pub runtimes: Arc<EngineRuntimes>,

    /// Sst meta data cache.
    pub meta_cache: Option<MetaCacheRef>,
}

impl fmt::Debug for OpenContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenContext")
            .field("config", &self.config)
            .finish()
    }
}
