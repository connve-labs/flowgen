fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = proConfig::new();
    config.protoc_arg("--experimental_allow_proto3_optional");

    // Google gRPC APIs.
    // https://github.com/googleapis/googleapis
    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .out_dir("src/google")
        .compile_with_config(
            config,
            &["proto/googleapis/google/storage/v2/storage.proto"],
            &["proto/googleapis"],
        )?;

    // Salesforce PubSub.
    // https://github.com/forcedotcom/pub-sub-api
    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .out_dir("src/salesforce")
        .compile_with_config(
            config,
            &["proto/salesforce/pubsub/pubsub.proto"],
            &["proto/salesforce/pubsub"],
        )?;
    Ok(())
}
