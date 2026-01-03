fn main() {
    tonic_build::compile_protos("proto/barq.proto")
        .expect("Failed to compile barq.proto");
    tonic_build::compile_protos("proto/client.proto")
        .expect("Failed to compile client.proto");
}
