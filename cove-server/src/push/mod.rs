//! APNs token registration and push delivery helpers.

use crate::config::PushConfig;
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use cove_common::auth_context::AuthContext;
use cove_common::id::UserId;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use reqwest::{Client, StatusCode};
use serde::Serialize;
use serde_json::json;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::metadata::MetadataMap;

const HEADER_PUSH_ACTION: &str = "x-cove-push-action";
const HEADER_PUSH_TOKEN: &str = "x-cove-push-token";
const HEADER_PUSH_ENVIRONMENT: &str = "x-cove-push-env";

const ACTION_REGISTER: &str = "register";
const ACTION_UNREGISTER: &str = "unregister";

/// Service that stores push tokens and sends APNs pushes.
#[derive(Clone)]
pub struct PushService {
    pool: PgPool,
    apns: Option<Arc<ApnsClient>>,
    default_environment: PushEnvironment,
}

impl PushService {
    pub fn new(pool: PgPool, config: &PushConfig) -> Self {
        let default_environment = PushEnvironment::parse(&config.default_environment)
            .unwrap_or(PushEnvironment::Development);

        let apns = if config.enabled {
            match ApnsClient::from_config(config) {
                Ok(client) => Some(Arc::new(client)),
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        "push delivery disabled: failed to initialize APNs client"
                    );
                    None
                }
            }
        } else {
            None
        };

        Self {
            pool,
            apns,
            default_environment,
        }
    }

    /// Reads push registration hints from gRPC metadata and upserts/revokes tokens.
    pub async fn sync_token_from_metadata(
        &self,
        auth: &AuthContext,
        metadata: &MetadataMap,
    ) -> Result<()> {
        let Some(action) = metadata_value(metadata, HEADER_PUSH_ACTION) else {
            return Ok(());
        };
        let Some(raw_token) = metadata_value(metadata, HEADER_PUSH_TOKEN) else {
            return Ok(());
        };
        let Some(token) = normalize_device_token(&raw_token) else {
            tracing::warn!("ignored malformed push token from metadata");
            return Ok(());
        };

        match action.as_str() {
            ACTION_REGISTER => {
                let environment = metadata_value(metadata, HEADER_PUSH_ENVIRONMENT)
                    .and_then(|value| PushEnvironment::parse(&value))
                    .unwrap_or(self.default_environment)
                    .as_str();

                sqlx::query(
                    r#"
                    INSERT INTO push_tokens (token, user_id, session_id, platform, environment, created_at, updated_at, revoked_at)
                    VALUES ($1, $2, $3, 'ios', $4, NOW(), NOW(), NULL)
                    ON CONFLICT (token)
                    DO UPDATE SET
                        user_id = EXCLUDED.user_id,
                        session_id = EXCLUDED.session_id,
                        platform = EXCLUDED.platform,
                        environment = EXCLUDED.environment,
                        updated_at = NOW(),
                        revoked_at = NULL
                    "#,
                )
                .bind(&token)
                .bind(auth.user_id.as_uuid())
                .bind(auth.session_id.as_uuid())
                .bind(environment)
                .execute(&self.pool)
                .await
                .context("failed to upsert push token")?;
            }
            ACTION_UNREGISTER => {
                sqlx::query(
                    r#"
                    UPDATE push_tokens
                    SET revoked_at = COALESCE(revoked_at, NOW()), updated_at = NOW()
                    WHERE token = $1 AND user_id = $2
                    "#,
                )
                .bind(&token)
                .bind(auth.user_id.as_uuid())
                .execute(&self.pool)
                .await
                .context("failed to revoke push token")?;
            }
            _ => {}
        }

        Ok(())
    }

    pub async fn send_follow_request_push(
        &self,
        recipient_id: UserId,
        actor_id: UserId,
    ) -> Result<()> {
        self.send_follow_push(
            recipient_id,
            actor_id,
            "New follow request",
            "{actor} requested to follow you",
            "follow_request",
        )
        .await
    }

    pub async fn send_follow_accepted_push(
        &self,
        recipient_id: UserId,
        actor_id: UserId,
    ) -> Result<()> {
        self.send_follow_push(
            recipient_id,
            actor_id,
            "Follow request accepted",
            "{actor} accepted your follow request",
            "follow_accepted",
        )
        .await
    }

    async fn send_follow_push(
        &self,
        recipient_id: UserId,
        actor_id: UserId,
        title: &str,
        body_template: &str,
        event_type: &str,
    ) -> Result<()> {
        let Some(apns) = self.apns.as_ref() else {
            return Ok(());
        };

        let actor_name = sqlx::query_scalar::<_, String>(
            "SELECT COALESCE(NULLIF(display_name, ''), username) FROM users WHERE id = $1",
        )
        .bind(actor_id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .context("failed to load actor name for push")?
        .unwrap_or_else(|| "Someone".to_string());

        let body = body_template.replace("{actor}", &actor_name);

        let tokens = sqlx::query_as::<_, (String, String)>(
            r#"
            SELECT token, environment
            FROM push_tokens
            WHERE user_id = $1
              AND platform = 'ios'
              AND revoked_at IS NULL
            "#,
        )
        .bind(recipient_id.as_uuid())
        .fetch_all(&self.pool)
        .await
        .context("failed to load recipient push tokens")?;

        for (token, environment_str) in tokens {
            let environment =
                PushEnvironment::parse(&environment_str).unwrap_or(self.default_environment);

            match apns
                .send_alert(&token, environment, title, &body, event_type)
                .await
            {
                Ok(DeliveryResult::Sent) => {}
                Ok(DeliveryResult::InvalidToken(reason)) => {
                    tracing::warn!(
                        token = %token,
                        reason = %reason,
                        "revoking invalid APNs token"
                    );
                    self.revoke_token(&token).await?;
                }
                Err(error) => {
                    tracing::warn!(
                        token = %token,
                        error = %error,
                        "failed sending APNs push"
                    );
                }
            }
        }

        Ok(())
    }

    async fn revoke_token(&self, token: &str) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE push_tokens
            SET revoked_at = COALESCE(revoked_at, NOW()), updated_at = NOW()
            WHERE token = $1
              AND revoked_at IS NULL
            "#,
        )
        .bind(token)
        .execute(&self.pool)
        .await
        .context("failed to revoke invalid token")?;

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PushEnvironment {
    Development,
    Production,
}

impl PushEnvironment {
    fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "development" | "sandbox" => Some(Self::Development),
            "production" | "prod" => Some(Self::Production),
            _ => None,
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::Development => "development",
            Self::Production => "production",
        }
    }

    fn apns_host(&self) -> &'static str {
        match self {
            Self::Development => "api.sandbox.push.apple.com",
            Self::Production => "api.push.apple.com",
        }
    }
}

struct ApnsClient {
    client: Client,
    key_id: String,
    team_id: String,
    topic: String,
    encoding_key: EncodingKey,
    jwt_cache: Mutex<Option<CachedApnsJwt>>,
}

impl ApnsClient {
    fn from_config(config: &PushConfig) -> Result<Self> {
        if config.apns_key_id.trim().is_empty() {
            return Err(anyhow!("push.apns_key_id is required when push is enabled"));
        }
        if config.apns_team_id.trim().is_empty() {
            return Err(anyhow!(
                "push.apns_team_id is required when push is enabled"
            ));
        }
        if config.apns_bundle_id.trim().is_empty() {
            return Err(anyhow!(
                "push.apns_bundle_id is required when push is enabled"
            ));
        }

        let private_key_bytes =
            std::fs::read(&config.apns_private_key_path).with_context(|| {
                format!(
                    "failed to read APNs private key: {}",
                    config.apns_private_key_path.display()
                )
            })?;
        let encoding_key = EncodingKey::from_ec_pem(&private_key_bytes)
            .context("failed to parse APNs private key (expected .p8 EC key)")?;

        let client = Client::builder()
            .use_rustls_tls()
            .http2_adaptive_window(true)
            .build()
            .context("failed to build APNs HTTP client")?;

        Ok(Self {
            client,
            key_id: config.apns_key_id.clone(),
            team_id: config.apns_team_id.clone(),
            topic: config.apns_bundle_id.clone(),
            encoding_key,
            jwt_cache: Mutex::new(None),
        })
    }

    async fn send_alert(
        &self,
        token: &str,
        environment: PushEnvironment,
        title: &str,
        body: &str,
        event_type: &str,
    ) -> Result<DeliveryResult> {
        let jwt = self.provider_token().await?;
        let endpoint = format!("https://{}/3/device/{}", environment.apns_host(), token);

        let payload = json!({
            "aps": {
                "alert": {
                    "title": title,
                    "body": body,
                },
                "sound": "default",
                "badge": 1,
            },
            "type": event_type
        });

        let response = self
            .client
            .post(endpoint)
            .header("authorization", format!("bearer {}", jwt))
            .header("apns-topic", &self.topic)
            .header("apns-push-type", "alert")
            .header("apns-priority", "10")
            .json(&payload)
            .send()
            .await
            .context("APNs request failed")?;

        if response.status().is_success() {
            return Ok(DeliveryResult::Sent);
        }

        let status = response.status();
        let response_body = response.text().await.unwrap_or_default();
        let reason = apns_reason(&response_body).unwrap_or_else(|| response_body.clone());

        if status == StatusCode::BAD_REQUEST || status == StatusCode::GONE {
            if matches!(
                reason.as_str(),
                "BadDeviceToken" | "Unregistered" | "DeviceTokenNotForTopic"
            ) {
                return Ok(DeliveryResult::InvalidToken(reason));
            }
        }

        Err(anyhow!(
            "APNs rejected notification: status={} reason={}",
            status,
            reason
        ))
    }

    async fn provider_token(&self) -> Result<String> {
        let now = Utc::now();
        let mut cache = self.jwt_cache.lock().await;

        if let Some(cached) = cache.as_ref() {
            if cached.expires_at > now + Duration::seconds(30) {
                return Ok(cached.token.clone());
            }
        }

        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(self.key_id.clone());

        let claims = ApnsClaims {
            iss: self.team_id.clone(),
            iat: now.timestamp() as usize,
        };

        let token = encode(&header, &claims, &self.encoding_key)
            .context("failed to encode APNs provider token")?;

        *cache = Some(CachedApnsJwt {
            token: token.clone(),
            expires_at: now + Duration::minutes(50),
        });

        Ok(token)
    }
}

struct CachedApnsJwt {
    token: String,
    expires_at: DateTime<Utc>,
}

#[derive(Serialize)]
struct ApnsClaims {
    iss: String,
    iat: usize,
}

enum DeliveryResult {
    Sent,
    InvalidToken(String),
}

fn metadata_value(metadata: &MetadataMap, key: &str) -> Option<String> {
    metadata
        .get(key)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn normalize_device_token(raw: &str) -> Option<String> {
    let token = raw
        .trim()
        .trim_start_matches('<')
        .trim_end_matches('>')
        .replace(' ', "")
        .to_ascii_lowercase();

    if token.len() != 64 || !token.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return None;
    }

    Some(token)
}

fn apns_reason(raw: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(raw).ok()?;
    value
        .get("reason")
        .and_then(|reason| reason.as_str())
        .map(ToString::to_string)
}
