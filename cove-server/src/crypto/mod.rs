//! Cryptography module: password hashing, JWT tokens, and AES-256-GCM encryption.

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::Aes256Gcm;
use argon2::{
    password_hash::{
        rand_core::OsRng, PasswordHash, PasswordHasher as Argon2Hasher, PasswordVerifier,
        SaltString,
    },
    Argon2,
};
use cove_common::error::{CoveError, CoveResult};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// =============================================================================
// PasswordHasher
// =============================================================================

#[derive(Clone)]
pub struct PasswordHasher {
    argon2: Argon2<'static>,
}

impl PasswordHasher {
    pub fn new() -> Self {
        Self {
            argon2: Argon2::default(),
        }
    }

    pub fn hash(&self, password: &str) -> CoveResult<String> {
        let salt = SaltString::generate(&mut OsRng);
        self.argon2
            .hash_password(password.as_bytes(), &salt)
            .map(|h| h.to_string())
            .map_err(|e| CoveError::Crypto(format!("password hash failed: {}", e)))
    }

    pub fn verify(&self, password: &str, hash: &str) -> CoveResult<bool> {
        let parsed = PasswordHash::new(hash)
            .map_err(|e| CoveError::Crypto(format!("invalid password hash: {}", e)))?;
        Ok(self
            .argon2
            .verify_password(password.as_bytes(), &parsed)
            .is_ok())
    }
}

impl Default for PasswordHasher {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// TokenService
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessTokenClaims {
    pub user_id: String,
    pub session_id: String,
    pub is_admin: bool,
    pub exp: u64,
    pub iat: u64,
}

#[derive(Clone)]
pub struct TokenService {
    jwt_secret: Vec<u8>,
    access_token_ttl_secs: u64,
    refresh_token_ttl_secs: u64,
}

impl TokenService {
    pub fn new(jwt_secret: &str, access_token_ttl_secs: u64, refresh_token_ttl_secs: u64) -> Self {
        Self {
            jwt_secret: jwt_secret.as_bytes().to_vec(),
            access_token_ttl_secs,
            refresh_token_ttl_secs,
        }
    }

    pub fn create_access_token(
        &self,
        user_id: &str,
        session_id: &str,
        is_admin: bool,
    ) -> CoveResult<String> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| CoveError::Internal(e.to_string()))?
            .as_secs();
        let exp = now + self.access_token_ttl_secs;

        let claims = AccessTokenClaims {
            user_id: user_id.to_string(),
            session_id: session_id.to_string(),
            is_admin,
            exp,
            iat: now,
        };

        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(&self.jwt_secret),
        )
        .map_err(|e| CoveError::Crypto(format!("JWT encode failed: {}", e)))
    }

    pub fn validate_access_token(&self, token: &str) -> CoveResult<AccessTokenClaims> {
        let mut validation = Validation::default();
        validation.validate_exp = true;

        let token_data = decode::<AccessTokenClaims>(
            token,
            &DecodingKey::from_secret(&self.jwt_secret),
            &validation,
        )
        .map_err(|e| CoveError::Unauthorized(format!("invalid token: {}", e)))?;

        Ok(token_data.claims)
    }

    pub fn generate_refresh_token(&self) -> String {
        let mut bytes = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut bytes);
        bytes_to_hex(&bytes)
    }

    pub fn hash_refresh_token(&self, token: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(token.as_bytes());
        bytes_to_hex(&hasher.finalize())
    }

    pub fn refresh_token_ttl_secs(&self) -> u64 {
        self.refresh_token_ttl_secs
    }
}

// =============================================================================
// EncryptionService
// =============================================================================

#[derive(Clone)]
pub struct EncryptionService {
    master_key: [u8; 32],
}

impl EncryptionService {
    pub fn new(master_key: [u8; 32]) -> Self {
        Self { master_key }
    }

    pub fn from_file(path: &std::path::Path) -> CoveResult<Self> {
        let raw = std::fs::read(path)
            .map_err(|e| CoveError::Crypto(format!("failed to read master key: {}", e)))?;

        // Support both 32 raw bytes and 64 hex-char files
        let key = if raw.len() == 32 {
            let mut k = [0u8; 32];
            k.copy_from_slice(&raw);
            k
        } else {
            let hex_str: String = raw
                .iter()
                .filter(|b| !b.is_ascii_whitespace())
                .map(|&b| b as char)
                .collect();
            let hex_str = hex_str.trim();
            if hex_str.len() != 64 || !hex_str.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(CoveError::Crypto(
                    "master key must be 32 raw bytes or 64 hex characters".into(),
                ));
            }
            let decoded = hex_decode(hex_str)?;
            let mut k = [0u8; 32];
            k.copy_from_slice(&decoded);
            k
        };

        Ok(Self::new(key))
    }

    pub fn generate_dek(&self) -> Vec<u8> {
        let mut dek = vec![0u8; 32];
        rand::thread_rng().fill_bytes(&mut dek);
        dek
    }

    pub fn wrap_dek(&self, dek: &[u8]) -> CoveResult<Vec<u8>> {
        let cipher = Aes256Gcm::new_from_slice(&self.master_key)
            .map_err(|e| CoveError::Crypto(e.to_string()))?;

        let mut nonce = [0u8; 12];
        rand::thread_rng()
            .try_fill_bytes(&mut nonce)
            .map_err(|e| CoveError::Crypto(e.to_string()))?;

        let ciphertext = cipher
            .encrypt((&nonce).into(), dek)
            .map_err(|e| CoveError::Crypto(format!("DEK wrap failed: {}", e)))?;

        let mut out = nonce.to_vec();
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    pub fn unwrap_dek(&self, wrapped: &[u8]) -> CoveResult<Vec<u8>> {
        if wrapped.len() < 13 {
            return Err(CoveError::Crypto("wrapped DEK too short".into()));
        }
        let (nonce_bytes, ciphertext) = wrapped.split_at(12);
        let cipher = Aes256Gcm::new_from_slice(&self.master_key)
            .map_err(|e| CoveError::Crypto(e.to_string()))?;
        let nonce: &[u8; 12] = nonce_bytes
            .try_into()
            .map_err(|_| CoveError::Crypto("invalid nonce".into()))?;
        cipher
            .decrypt(nonce.into(), ciphertext)
            .map_err(|e| CoveError::Crypto(format!("DEK unwrap failed: {}", e)))
    }

    pub fn encrypt(&self, plaintext: &[u8]) -> CoveResult<(Vec<u8>, Vec<u8>)> {
        let cipher = Aes256Gcm::new_from_slice(&self.master_key)
            .map_err(|e| CoveError::Crypto(e.to_string()))?;

        let mut nonce = [0u8; 12];
        rand::thread_rng()
            .try_fill_bytes(&mut nonce)
            .map_err(|e| CoveError::Crypto(e.to_string()))?;

        let ciphertext = cipher
            .encrypt((&nonce).into(), plaintext)
            .map_err(|e| CoveError::Crypto(e.to_string()))?;

        Ok((ciphertext, nonce.to_vec()))
    }

    pub fn decrypt(&self, ciphertext: &[u8], nonce: &[u8]) -> CoveResult<Vec<u8>> {
        if nonce.len() != 12 {
            return Err(CoveError::Crypto("nonce must be 12 bytes".into()));
        }
        let cipher = Aes256Gcm::new_from_slice(&self.master_key)
            .map_err(|e| CoveError::Crypto(e.to_string()))?;
        let nonce_arr: [u8; 12] = nonce
            .try_into()
            .map_err(|_| CoveError::Crypto("invalid nonce length".into()))?;
        cipher
            .decrypt((&nonce_arr).into(), ciphertext)
            .map_err(|e| CoveError::Crypto(e.to_string()))
    }
}

// =============================================================================
// Hex helpers (avoids adding `hex` crate dependency)
// =============================================================================

fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn hex_decode(hex: &str) -> CoveResult<Vec<u8>> {
    if hex.len() % 2 != 0 {
        return Err(CoveError::Crypto("odd-length hex string".into()));
    }
    hex.as_bytes()
        .chunks(2)
        .map(|chunk| {
            let s =
                std::str::from_utf8(chunk).map_err(|_| CoveError::Crypto("invalid hex".into()))?;
            u8::from_str_radix(s, 16).map_err(|_| CoveError::Crypto("invalid hex digit".into()))
        })
        .collect()
}
