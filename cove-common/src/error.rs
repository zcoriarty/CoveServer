use thiserror::Error;
use tonic::Status;

#[derive(Debug, Error)]
pub enum CoveError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("unauthorized: {0}")]
    Unauthorized(String),

    #[error("forbidden: {0}")]
    Forbidden(String),

    #[error("invalid input: {0}")]
    InvalidInput(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("rate limited")]
    RateLimited,

    #[error("internal error: {0}")]
    Internal(String),

    #[error("database error: {0}")]
    Database(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("crypto error: {0}")]
    Crypto(String),

    #[error("media processing error: {0}")]
    MediaProcessing(String),

    #[error("service unavailable: {0}")]
    Unavailable(String),
}

impl From<CoveError> for Status {
    fn from(err: CoveError) -> Self {
        match &err {
            CoveError::NotFound(msg) => Status::not_found(msg),
            CoveError::Unauthorized(msg) => Status::unauthenticated(msg),
            CoveError::Forbidden(msg) => Status::permission_denied(msg),
            CoveError::InvalidInput(msg) => Status::invalid_argument(msg),
            CoveError::Conflict(msg) => Status::already_exists(msg),
            CoveError::RateLimited => Status::resource_exhausted("rate limited"),
            CoveError::Internal(msg) => {
                tracing::error!(error = %msg, "internal error");
                Status::internal("internal error")
            }
            CoveError::Database(msg) => {
                tracing::error!(error = %msg, "database error");
                Status::internal("internal error")
            }
            CoveError::Storage(msg) => {
                tracing::error!(error = %msg, "storage error");
                Status::internal("internal error")
            }
            CoveError::Crypto(msg) => {
                tracing::error!(error = %msg, "crypto error");
                Status::internal("internal error")
            }
            CoveError::MediaProcessing(msg) => {
                tracing::error!(error = %msg, "media processing error");
                Status::internal("internal error")
            }
            CoveError::Unavailable(msg) => Status::unavailable(msg),
        }
    }
}

pub type CoveResult<T> = Result<T, CoveError>;
