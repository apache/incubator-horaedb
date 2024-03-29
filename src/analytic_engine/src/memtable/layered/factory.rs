// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Skiplist memtable factory

use std::sync::Arc;

use crate::memtable::{
    factory::{Factory, FactoryRef, Options},
    layered::LayeredMemTable,
    MemTableRef, Result,
};

/// Factory to create memtable
#[derive(Debug)]
pub struct LayeredMemtableFactory {
    inner_memtable_factory: FactoryRef,
    mutable_switch_threshold: usize,
}

impl LayeredMemtableFactory {
    pub fn new(inner_memtable_factory: FactoryRef, mutable_switch_threshold: usize) -> Self {
        Self {
            inner_memtable_factory,
            mutable_switch_threshold,
        }
    }
}

impl Factory for LayeredMemtableFactory {
    fn create_memtable(&self, opts: Options) -> Result<MemTableRef> {
        let memtable = LayeredMemTable::new(
            &opts,
            self.inner_memtable_factory.clone(),
            self.mutable_switch_threshold,
        )?;

        Ok(Arc::new(memtable))
    }
}
