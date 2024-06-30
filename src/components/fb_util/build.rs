use std::{io::Result, path::Path};

fn main() -> Result<()> {
    if let Ok(flatc_bin) = std::env::var("FLATC") {
        std::env::set_var(
            "PATH",
            std::env::var("PATH").unwrap() + format!(":{}", flatc_bin.as_str()).as_str(),
        );
    } else {
        println!("FLATC enviroment variable not found!");
    }

    println!("cargo:rerun-if-changed=src/remote_engine.fbs");
    flatc_rust::run(flatc_rust::Args {
        inputs: &[Path::new("src/remote_engine.fbs")],
        out_dir: Path::new("target/flatbuffers/"),
        ..Default::default()
    })
    .expect("flatc");

    let greeter_service = tonic_build::manual::Service::builder()
        .name("RemoteEngineFbService")
        .package("remote_engine")
        .method(
            tonic_build::manual::Method::builder()
                .name("write")
                .route_name("Write")
                .input_type("crate::common::FlatBufferBytes")
                .output_type("crate::common::FlatBufferBytes")
                .codec_path("crate::common::FlatBufferCodec")
                .build(),
        )
        .method(
            tonic_build::manual::Method::builder()
                .name("write_batch")
                .route_name("WriteBatch")
                .input_type("crate::common::FlatBufferBytes")
                .output_type("crate::common::FlatBufferBytes")
                .codec_path("crate::common::FlatBufferCodec")
                .build(),
        )
        .build();

    tonic_build::manual::Builder::new()
        .out_dir("src")
        .compile(&[greeter_service]);
    Ok(())
}
