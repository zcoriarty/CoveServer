//! Server configuration loaded from environment variables and optional config file.
//!
//! Environment variables use the `COVE_` prefix. Nested keys use double underscore,
//! e.g. `COVE_SERVER__HOST`, `COVE_DATABASE__URL`.

use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize)]
pub struct CoveConfig {
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    pub redis: RedisConfig,
    pub storage: StorageConfig,
    pub auth: AuthConfig,
    pub crypto: CryptoConfig,
    pub media: MediaConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    pub url: String,
    pub max_connections: u32,
    pub min_connections: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RedisConfig {
    pub url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuthConfig {
    pub access_token_ttl_secs: u64,
    pub refresh_token_ttl_secs: u64,
    pub jwt_secret: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CryptoConfig {
    pub master_key_path: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MediaConfig {
    pub max_upload_bytes: u64,
    pub max_video_duration_secs: u32,
    pub allowed_image_types: Vec<String>,
    pub allowed_video_types: Vec<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 50051,
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: "postgres://localhost/cove".to_string(),
            max_connections: 20,
            min_connections: 5,
        }
    }
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            url: "redis://localhost:6379".to_string(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:9000".to_string(),
            bucket: "cove-media".to_string(),
            region: "us-east-1".to_string(),
            access_key: "".to_string(),
            secret_key: "".to_string(),
        }
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            access_token_ttl_secs: 900,   // 15 minutes
            refresh_token_ttl_secs: 604_800, // 7 days
            jwt_secret: "change-me-in-production".to_string(),
        }
    }
}

impl Default for CryptoConfig {
    fn default() -> Self {
        Self {
            master_key_path: PathBuf::from("/run/cove/master.key"),
        }
    }
}

impl Default for MediaConfig {
    fn default() -> Self {
        Self {
            max_upload_bytes: 50 * 1024 * 1024, // 50 MiB
            max_video_duration_secs: 60,
            allowed_image_types: vec![
                "image/jpeg".to_string(),
                "image/png".to_string(),
                "image/webp".to_string(),
            ],
            allowed_video_types: vec![
                "video/mp4".to_string(),
                "video/quicktime".to_string(),
            ],
        }
    }
}

impl CoveConfig {
    /// Load configuration from environment variables (COVE_ prefix) with optional
    /// config file. Environment overrides file. Uses sensible defaults for missing values.
    pub fn load() -> Result<Self, config::ConfigError> {
        let default_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://localhost/cove".to_string());
        let default_redis = std::env::var("REDIS_URL")
            .unwrap_or_else(|_| "redis://localhost:6379".to_string());

        let cfg = config::Config::builder()
            .set_default("server.host", "0.0.0.0")?
            .set_default("server.port", 50051i64)?
            .set_default("database.url", default_url)?
            .set_default("database.max_connections", 20i64)?
            .set_default("database.min_connections", 5i64)?
            .set_default("redis.url", default_redis)?
            .set_default("storage.endpoint", "http://localhost:9000")?
            .set_default("storage.bucket", "cove-media")?
            .set_default("storage.region", "us-east-1")?
            .set_default("storage.access_key", "")?
            .set_default("storage.secret_key", "")?
            .set_default("auth.access_token_ttl_secs", 900i64)?
            .set_default("auth.refresh_token_ttl_secs", 604_800i64)?
            .set_default("auth.jwt_secret", "change-me-in-production")?
            .set_default("crypto.master_key_path", "/run/cove/master.key")?
            .set_default("media.max_upload_bytes", 52_428_800i64)? // 50 MiB
            .set_default("media.max_video_duration_secs", 60i64)?
            .set_default(
                "media.allowed_image_types",
                vec!["image/jpeg", "image/png", "image/webp"],
            )?
            .set_default(
                "media.allowed_video_types",
                vec!["video/mp4", "video/quicktime"],
            )?
            .add_source(
                config::File::with_name("config").required(false),
            )
            .add_source(
                config::Environment::with_prefix("COVE")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()?;

        cfg.try_deserialize()
    }
}
