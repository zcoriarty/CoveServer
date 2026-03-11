//! Redis caching layer for feed pages, profile summaries, and rate limiting.

use cove_common::error::{CoveError, CoveResult};
use cove_common::id::UserId;
use redis::AsyncCommands;

#[derive(Clone)]
pub struct CacheService {
    conn: redis::aio::ConnectionManager,
}

impl CacheService {
    pub fn new(conn: redis::aio::ConnectionManager) -> Self {
        Self { conn }
    }

    pub async fn get_feed_page(&self, user_id: &UserId, page: u32) -> CoveResult<Option<Vec<u8>>> {
        let key = format!("feed:{}:{}", user_id, page);
        let mut conn = self.conn.clone();
        let result: Option<Vec<u8>> = conn
            .get(&key)
            .await
            .map_err(|e| CoveError::Internal(format!("redis get: {}", e)))?;
        Ok(result)
    }

    pub async fn set_feed_page(
        &self,
        user_id: &UserId,
        page: u32,
        data: &[u8],
        ttl_secs: u64,
    ) -> CoveResult<()> {
        let key = format!("feed:{}:{}", user_id, page);
        let mut conn = self.conn.clone();
        let _: () = conn.set_ex(&key, data, ttl_secs)
            .await
            .map_err(|e| CoveError::Internal(format!("redis set: {}", e)))?;
        Ok(())
    }

    pub async fn invalidate_feed(&self, user_id: &UserId) -> CoveResult<()> {
        let pattern = format!("feed:{}:*", user_id);
        let mut conn = self.conn.clone();
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&pattern)
            .query_async(&mut conn)
            .await
            .map_err(|e| CoveError::Internal(format!("redis keys: {}", e)))?;

        if !keys.is_empty() {
            let _: () = redis::cmd("DEL")
                .arg(&keys)
                .query_async(&mut conn)
                .await
                .map_err(|e| CoveError::Internal(format!("redis del: {}", e)))?;
        }
        Ok(())
    }

    pub async fn get_profile_summary(&self, user_id: &UserId) -> CoveResult<Option<Vec<u8>>> {
        let key = format!("profile:{}", user_id);
        let mut conn = self.conn.clone();
        let result: Option<Vec<u8>> = conn
            .get(&key)
            .await
            .map_err(|e| CoveError::Internal(format!("redis get: {}", e)))?;
        Ok(result)
    }

    pub async fn set_profile_summary(
        &self,
        user_id: &UserId,
        data: &[u8],
        ttl_secs: u64,
    ) -> CoveResult<()> {
        let key = format!("profile:{}", user_id);
        let mut conn = self.conn.clone();
        let _: () = conn.set_ex(&key, data, ttl_secs)
            .await
            .map_err(|e| CoveError::Internal(format!("redis set: {}", e)))?;
        Ok(())
    }

    pub async fn invalidate_profile(&self, user_id: &UserId) -> CoveResult<()> {
        let key = format!("profile:{}", user_id);
        let mut conn = self.conn.clone();
        let _: () = conn
            .del(&key)
            .await
            .map_err(|e| CoveError::Internal(format!("redis del: {}", e)))?;
        Ok(())
    }

    pub async fn check_rate_limit(
        &self,
        key_prefix: &str,
        identifier: &str,
        max_requests: u64,
        window_secs: u64,
    ) -> CoveResult<bool> {
        let key = format!("rl:{}:{}", key_prefix, identifier);
        let mut conn = self.conn.clone();

        let count: u64 = conn
            .incr(&key, 1u64)
            .await
            .map_err(|e| CoveError::Internal(format!("redis incr: {}", e)))?;

        if count == 1 {
            let _: () = conn
                .expire(&key, window_secs as i64)
                .await
                .map_err(|e| CoveError::Internal(format!("redis expire: {}", e)))?;
        }

        Ok(count <= max_requests)
    }

    pub async fn health_check(&self) -> CoveResult<()> {
        let mut conn = self.conn.clone();
        let _: String = redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .map_err(|e| CoveError::Unavailable(format!("redis: {}", e)))?;
        Ok(())
    }
}
