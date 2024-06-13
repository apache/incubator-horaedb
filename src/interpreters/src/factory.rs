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

//! Interpreter factory

use catalog::manager::ManagerRef;
use query_engine::{executor::ExecutorRef, physical_planner::PhysicalPlannerRef};
use query_frontend::plan::Plan;
use runtime::PriorityRuntime;
use table_engine::engine::TableEngineRef;

use crate::{
    alter_table::AlterTableInterpreter,
    context::Context,
    create::CreateInterpreter,
    describe::DescribeInterpreter,
    drop::DropInterpreter,
    exists::ExistsInterpreter,
    insert::InsertInterpreter,
    interpreter::{InterpreterPtr, Result},
    select::SelectInterpreter,
    show::ShowInterpreter,
    table_manipulator::TableManipulatorRef,
    validator::{ValidateContext, Validator},
};

/// A factory to create interpreters
pub struct Factory {
    query_executor: ExecutorRef,
    query_runtime: PriorityRuntime,
    physical_planner: PhysicalPlannerRef,
    catalog_manager: ManagerRef,
    table_engine: TableEngineRef,
    table_manipulator: TableManipulatorRef,
}

impl Factory {
    pub fn new(
        query_executor: ExecutorRef,
        physical_planner: PhysicalPlannerRef,
        catalog_manager: ManagerRef,
        table_engine: TableEngineRef,
        table_manipulator: TableManipulatorRef,
        query_runtime: PriorityRuntime,
    ) -> Self {
        Self {
            query_executor,
            query_runtime,
            physical_planner,
            catalog_manager,
            table_engine,
            table_manipulator,
        }
    }

    pub fn create(self, ctx: Context, plan: Plan) -> Result<InterpreterPtr> {
        let validate_ctx = ValidateContext {
            enable_partition_table_access: ctx.enable_partition_table_access(),
        };
        let validator = Validator::new(validate_ctx);
        validator.validate(&plan)?;

        let interpreter = match plan {
            Plan::Query(p) => SelectInterpreter::create(
                ctx,
                p,
                self.query_executor,
                self.physical_planner,
                self.query_runtime,
            ),
            Plan::Insert(p) => {
                InsertInterpreter::create(ctx, p, self.query_executor, self.physical_planner)
            }
            Plan::Create(p) => {
                CreateInterpreter::create(ctx, p, self.table_engine, self.table_manipulator)
            }
            Plan::Drop(p) => {
                DropInterpreter::create(ctx, p, self.table_engine, self.table_manipulator)
            }
            Plan::Describe(p) => DescribeInterpreter::create(p),
            Plan::AlterTable(p) => AlterTableInterpreter::create(p),
            Plan::Show(p) => ShowInterpreter::create(ctx, p, self.catalog_manager),
            Plan::Exists(p) => ExistsInterpreter::create(p),
        };

        Ok(interpreter)
    }
}
