fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Google gRPC APIs.
    // https://github.com/googleapis/googleapis
    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .out_dir("src")
        .compile(
            &["proto/googleapis/google/storage/v2/storage.proto"],
            &["proto/googleapis"],
        )?;
    Ok(())
}
