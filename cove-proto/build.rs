fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = &[
        "proto/cove/common.proto",
        "proto/cove/auth.proto",
        "proto/cove/user.proto",
        "proto/cove/profile.proto",
        "proto/cove/follow.proto",
        "proto/cove/post.proto",
        "proto/cove/feed.proto",
        "proto/cove/comment.proto",
        "proto/cove/like.proto",
        "proto/cove/share.proto",
        "proto/cove/search.proto",
        "proto/cove/notification.proto",
        "proto/cove/media.proto",
        "proto/cove/admin.proto",
    ];

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(protos, &["proto"])?;

    for proto in protos {
        println!("cargo:rerun-if-changed={}", proto);
    }

    Ok(())
}
