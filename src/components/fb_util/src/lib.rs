pub mod common;
#[allow(clippy::all)]
#[rustfmt::skip]
#[allow(non_snake_case)]
#[allow(unused_imports)]
#[path = "../target/flatbuffers/remote_engine_generated.rs"]
pub mod remote_engine_generated;
include!("remote_engine.RemoteEngineFbService.rs");
