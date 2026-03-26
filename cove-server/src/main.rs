//! CoveServer binary entry point.
//! Bootstraps all infrastructure, registers gRPC services, and starts serving.

use cove_server::admin::AdminServiceImpl;
use cove_server::auth::AuthServiceImpl;
use cove_server::comments::CommentServiceImpl;
use cove_server::config::CoveConfig;
use cove_server::crypto::{EncryptionService, PasswordHasher, TokenService};
use cove_server::feed::FeedServiceImpl;
use cove_server::likes::LikeServiceImpl;
use cove_server::media::MediaServiceImpl;
use cove_server::notifications::NotificationServiceImpl;
use cove_server::posts::PostServiceImpl;
use cove_server::profiles::ProfileServiceImpl;
use cove_server::push::PushService;
use cove_server::search::SearchServiceImpl;
use cove_server::sharing::ShareServiceImpl;
use cove_server::social_graph::FollowServiceImpl;
use cove_server::users::UserServiceImpl;

use cove_proto::cove::admin::admin_service_server::AdminServiceServer;
use cove_proto::cove::auth::auth_service_server::AuthServiceServer;
use cove_proto::cove::comment::comment_service_server::CommentServiceServer;
use cove_proto::cove::feed::feed_service_server::FeedServiceServer;
use cove_proto::cove::follow::follow_service_server::FollowServiceServer;
use cove_proto::cove::like::like_service_server::LikeServiceServer;
use cove_proto::cove::media::media_service_server::MediaServiceServer;
use cove_proto::cove::notification::notification_service_server::NotificationServiceServer;
use cove_proto::cove::post::post_service_server::PostServiceServer;
use cove_proto::cove::profile::profile_service_server::ProfileServiceServer;
use cove_proto::cove::search::search_service_server::SearchServiceServer;
use cove_proto::cove::share::share_service_server::ShareServiceServer;
use cove_proto::cove::user::user_service_server::UserServiceServer;

use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use tonic::transport::Server;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --- Observability ---
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "cove_server=info".into()))
        .with(tracing_subscriber::fmt::layer().json())
        .init();

    tracing::info!("starting CoveServer");

    // --- Configuration ---
    let config = CoveConfig::load().expect("failed to load configuration");
    let config = Arc::new(config);

    // --- Database ---
    let pool = PgPoolOptions::new()
        .max_connections(config.database.max_connections)
        .min_connections(config.database.min_connections)
        .connect(&config.database.url)
        .await
        .expect("failed to connect to PostgreSQL");

    tracing::info!("connected to PostgreSQL");

    sqlx::migrate!("../migrations")
        .run(&pool)
        .await
        .expect("failed to run database migrations");

    tracing::info!("database migrations applied");

    // --- Supabase Storage ---
    let storage = cove_server::storage::object_store::SupabaseStorageService::new(
        config.storage.endpoint.clone(),
        config.storage.bucket.clone(),
        config.storage.api_key.clone(),
    )
    .expect("failed to configure Supabase storage");

    storage
        .ensure_bucket_exists()
        .await
        .expect("failed to ensure Supabase storage bucket exists");

    storage
        .health_check()
        .await
        .expect("failed to connect to Supabase storage");

    tracing::info!(
        endpoint = %storage.endpoint(),
        bucket = %storage.bucket(),
        "supabase storage initialized"
    );

    // --- Crypto Services ---
    let password_hasher = Arc::new(PasswordHasher::new());
    let token_service = TokenService::new(
        &config.auth.jwt_secret,
        config.auth.access_token_ttl_secs,
        config.auth.refresh_token_ttl_secs,
    );
    let _encryption_service = if config.crypto.master_key_path.exists() {
        Some(
            EncryptionService::from_file(&config.crypto.master_key_path)
                .expect("failed to load master key"),
        )
    } else {
        tracing::warn!("master key not found, media encryption disabled");
        None
    };

    // --- Prometheus Metrics ---
    let metrics_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .expect("failed to install metrics recorder");

    // --- gRPC Service Impls ---
    let jwt_secret = config.auth.jwt_secret.clone();
    let push_service = Arc::new(PushService::new(pool.clone(), &config.push)?);

    let auth_svc = AuthServiceImpl::new(
        pool.clone(),
        token_service.clone(),
        password_hasher.clone(),
        config.clone(),
    );

    let user_svc = UserServiceImpl::new(pool.clone(), jwt_secret.clone(), password_hasher.clone());
    let profile_svc = ProfileServiceImpl::new(pool.clone(), jwt_secret.clone());
    let follow_svc = FollowServiceImpl::new(pool.clone(), jwt_secret.clone(), push_service.clone());
    let post_svc = PostServiceImpl::new(pool.clone(), jwt_secret.clone());
    let feed_svc = FeedServiceImpl::new(pool.clone(), jwt_secret.clone());
    let comment_svc = CommentServiceImpl::new(pool.clone(), jwt_secret.clone());
    let like_svc = LikeServiceImpl::new(pool.clone(), jwt_secret.clone());
    let share_svc = ShareServiceImpl::new(pool.clone(), jwt_secret.clone());
    let search_svc = SearchServiceImpl::new(pool.clone(), jwt_secret.clone());
    let notification_svc =
        NotificationServiceImpl::new(pool.clone(), jwt_secret.clone(), push_service.clone());
    let media_svc = MediaServiceImpl::new(pool.clone(), storage.clone(), jwt_secret.clone());
    let admin_svc = AdminServiceImpl::new(
        pool.clone(),
        jwt_secret.clone(),
        storage.clone(),
    );

    // --- Health / Metrics Endpoint (HTTP on port 9090) ---
    let metrics_handle_for_health = metrics_handle;
    tokio::spawn(async move {
        tracing::info!("metrics server listening on 0.0.0.0:9090");
        let listener = tokio::net::TcpListener::bind("0.0.0.0:9090").await.unwrap();
        loop {
            if let Ok((stream, _)) = listener.accept().await {
                let handle = metrics_handle_for_health.clone();
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut stream = stream;
                    let mut buf = [0u8; 1024];
                    let _ = stream.read(&mut buf).await;
                    let body = handle.render();
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                });
            }
        }
    });

    // --- Start gRPC Server ---
    let addr = format!("{}:{}", config.server.host, config.server.port)
        .parse()
        .expect("invalid server address");

    tracing::info!(address = %addr, "gRPC server starting");

    Server::builder()
        .add_service(AuthServiceServer::new(auth_svc))
        .add_service(UserServiceServer::new(user_svc))
        .add_service(ProfileServiceServer::new(profile_svc))
        .add_service(FollowServiceServer::new(follow_svc))
        .add_service(PostServiceServer::new(post_svc))
        .add_service(FeedServiceServer::new(feed_svc))
        .add_service(CommentServiceServer::new(comment_svc))
        .add_service(LikeServiceServer::new(like_svc))
        .add_service(ShareServiceServer::new(share_svc))
        .add_service(SearchServiceServer::new(search_svc))
        .add_service(NotificationServiceServer::new(notification_svc))
        .add_service(MediaServiceServer::new(media_svc))
        .add_service(AdminServiceServer::new(admin_svc))
        .serve(addr)
        .await?;

    Ok(())
}
