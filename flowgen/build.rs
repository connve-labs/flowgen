fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Google gRPC APIs.
    // https://github.com/googleapis/googleapis
    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .out_dir("src/google")
        .compile(
            &["proto/googleapis/google/storage/v2/storage.proto"],
            &["proto/googleapis"],
        )?;

    // Salesforce PubSub.
    // https://github.com/forcedotcom/pub-sub-api
    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .out_dir("src/salesforce")
        .compile(
            &["proto/salesforce/pubsub/pubsub.proto"],
            &["proto/salesforce/pubsub"],
        )?;
    Ok(())
}
