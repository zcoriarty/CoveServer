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
    pub storage: StorageConfig,
    pub auth: AuthConfig,
    pub crypto: CryptoConfig,
    pub media: MediaConfig,
    pub push: PushConfig,
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
pub struct StorageConfig {
    pub endpoint: String,
    pub bucket: String,
    pub api_key: String,
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

#[derive(Debug, Clone, Deserialize)]
pub struct PushConfig {
    pub enabled: bool,
    pub apns_key_id: String,
    pub apns_team_id: String,
    pub apns_bundle_id: String,
    pub apns_private_key: String,
    pub apns_private_key_path: PathBuf,
    pub default_environment: String,
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
            url: String::new(),
            max_connections: 5,
            min_connections: 0,
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            bucket: String::new(),
            api_key: String::new(),
        }
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            access_token_ttl_secs: 900,      // 15 minutes
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
            allowed_video_types: vec!["video/mp4".to_string(), "video/quicktime".to_string()],
        }
    }
}

impl Default for PushConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            apns_key_id: String::new(),
            apns_team_id: String::new(),
            apns_bundle_id: String::new(),
            apns_private_key: String::new(),
            apns_private_key_path: PathBuf::from("./secrets/AuthKey.p8"),
            default_environment: "development".to_string(),
        }
    }
}

impl CoveConfig {
    /// Load configuration from environment variables (COVE_ prefix) with optional
    /// config file. Environment overrides file.
    pub fn load() -> Result<Self, config::ConfigError> {
        let default_port = std::env::var("PORT")
            .ok()
            .and_then(|p| p.parse::<i64>().ok())
            .unwrap_or(50051);

        let default_database_url = first_non_empty_env(&["DATABASE_URL"]).unwrap_or_default();
        let default_jwt_secret = first_non_empty_env(&["JWT_SECRET"])
            .unwrap_or_else(|| "change-me-in-production".to_string());

        let default_storage_endpoint =
            first_non_empty_env(&["SUPABASE_STORAGE_ENDPOINT"]).unwrap_or_default();
        let default_storage_bucket =
            first_non_empty_env(&["SUPABASE_STORAGE_BUCKET"]).unwrap_or_else(|| "media".to_string());
        let default_storage_api_key =
            first_non_empty_env(&["SUPABASE_SECRET_KEY"]).unwrap_or_default();

        let cfg = config::Config::builder()
            .set_default("server.host", "0.0.0.0")?
            .set_default("server.port", default_port)?
            .set_default("database.url", default_database_url)?
            .set_default("database.max_connections", 5i64)?
            .set_default("database.min_connections", 0i64)?
            .set_default("storage.endpoint", default_storage_endpoint)?
            .set_default("storage.bucket", default_storage_bucket)?
            .set_default("storage.api_key", default_storage_api_key)?
            .set_default("auth.access_token_ttl_secs", 900i64)?
            .set_default("auth.refresh_token_ttl_secs", 604_800i64)?
            .set_default("auth.jwt_secret", default_jwt_secret)?
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
            .set_default("push.enabled", false)?
            .set_default("push.apns_key_id", "")?
            .set_default("push.apns_team_id", "")?
            .set_default("push.apns_bundle_id", "")?
            .set_default("push.apns_private_key", "")?
            .set_default("push.apns_private_key_path", "./secrets/AuthKey.p8")?
            .set_default("push.default_environment", "development")?
            .add_source(config::File::with_name("config/cove").required(false))
            .add_source(
                config::Environment::with_prefix("COVE")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()?;

        let mut parsed: Self = cfg.try_deserialize()?;
        if let Ok(raw_port) = std::env::var("PORT") {
            if let Ok(platform_port) = raw_port.parse::<u16>() {
                if parsed.server.port != platform_port {
                    tracing::warn!(
                        configured_port = parsed.server.port,
                        platform_port,
                        "overriding server.port with PORT environment value"
                    );
                    parsed.server.port = platform_port;
                }
            }
        }

        if let Some(raw_enabled) =
            first_non_empty_env(&["COVE_PUSH__ENABLED", "COVE__PUSH__ENABLED"])
        {
            if let Some(enabled) = parse_bool_env(&raw_enabled) {
                parsed.push.enabled = enabled;
            } else {
                tracing::warn!(
                    raw_value = %raw_enabled,
                    "invalid value for push.enabled env override; expected true/false"
                );
            }
        }

        if let Some(value) = first_non_empty_env(&["COVE_PUSH__APNS_KEY_ID", "COVE__PUSH__APNS_KEY_ID"]) {
            parsed.push.apns_key_id = value;
        }
        if let Some(value) = first_non_empty_env(&["COVE_PUSH__APNS_TEAM_ID", "COVE__PUSH__APNS_TEAM_ID"]) {
            parsed.push.apns_team_id = value;
        }
        if let Some(value) =
            first_non_empty_env(&["COVE_PUSH__APNS_BUNDLE_ID", "COVE__PUSH__APNS_BUNDLE_ID"])
        {
            parsed.push.apns_bundle_id = value;
        }
        if let Some(value) =
            first_non_empty_env(&["COVE_PUSH__APNS_PRIVATE_KEY", "COVE__PUSH__APNS_PRIVATE_KEY"])
        {
            parsed.push.apns_private_key = value;
        }
        if let Some(value) = first_non_empty_env(&[
            "COVE_PUSH__APNS_PRIVATE_KEY_PATH",
            "COVE__PUSH__APNS_PRIVATE_KEY_PATH",
        ]) {
            parsed.push.apns_private_key_path = PathBuf::from(value);
        }
        if let Some(value) = first_non_empty_env(&[
            "COVE_PUSH__DEFAULT_ENVIRONMENT",
            "COVE__PUSH__DEFAULT_ENVIRONMENT",
        ]) {
            parsed.push.default_environment = value;
        }

        parsed.validate()?;
        Ok(parsed)
    }

    fn validate(&self) -> Result<(), config::ConfigError> {
        if self.database.url.trim().is_empty() {
            return Err(config::ConfigError::Message(
                "database.url is required (set DATABASE_URL)".to_string(),
            ));
        }

        if self.database.url.contains("[YOUR-PASSWORD]") {
            return Err(config::ConfigError::Message(
                "database.url still contains [YOUR-PASSWORD]; provide DATABASE_URL with the real password".to_string(),
            ));
        }

        if self.database.max_connections == 0 {
            return Err(config::ConfigError::Message(
                "database.max_connections must be >= 1".to_string(),
            ));
        }

        if self.database.min_connections > self.database.max_connections {
            return Err(config::ConfigError::Message(
                "database.min_connections must be <= database.max_connections".to_string(),
            ));
        }

        if self.storage.endpoint.trim().is_empty() {
            return Err(config::ConfigError::Message(
                "storage.endpoint is required (set SUPABASE_STORAGE_ENDPOINT)".to_string(),
            ));
        }

        if self.storage.bucket.trim().is_empty() {
            return Err(config::ConfigError::Message(
                "storage.bucket is required (set SUPABASE_STORAGE_BUCKET)".to_string(),
            ));
        }

        if self.storage.api_key.trim().is_empty() {
            return Err(config::ConfigError::Message(
                "storage.api_key is required (set SUPABASE_SECRET_KEY)".to_string(),
            ));
        }

        Ok(())
    }
}

fn first_non_empty_env(keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| match std::env::var(key) {
        Ok(value) if !value.trim().is_empty() => Some(value),
        _ => None,
    })
}

fn parse_bool_env(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}
