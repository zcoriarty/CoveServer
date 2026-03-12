//! Local filesystem storage for media files.

use cove_common::error::{CoveError, CoveResult};
use std::path::{Path, PathBuf};
use tokio::fs;

#[derive(Clone)]
pub struct LocalStorageService {
    base_path: PathBuf,
}

impl LocalStorageService {
    pub fn new(base_path: impl Into<PathBuf>) -> Self {
        Self {
            base_path: base_path.into(),
        }
    }

    /// Ensure the base directory exists (called once at startup).
    pub async fn init(&self) -> CoveResult<()> {
        fs::create_dir_all(&self.base_path)
            .await
            .map_err(|e| CoveError::Storage(format!("failed to create storage dir: {}", e)))?;
        Ok(())
    }

    fn resolve(&self, key: &str) -> PathBuf {
        self.base_path.join(key)
    }

    /// Write data to a file, creating parent directories as needed.
    /// Uses write-to-tmp + rename for atomicity.
    pub async fn write_file(&self, key: &str, data: &[u8]) -> CoveResult<()> {
        let path = self.resolve(key);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| CoveError::Storage(format!("mkdir failed: {}", e)))?;
        }

        let tmp_path = path.with_extension("tmp");
        fs::write(&tmp_path, data)
            .await
            .map_err(|e| CoveError::Storage(format!("write failed: {}", e)))?;

        fs::rename(&tmp_path, &path)
            .await
            .map_err(|e| CoveError::Storage(format!("rename failed: {}", e)))?;

        Ok(())
    }

    pub async fn read_file(&self, key: &str) -> CoveResult<Vec<u8>> {
        let path = self.resolve(key);
        fs::read(&path)
            .await
            .map_err(|e| CoveError::Storage(format!("read {}: {}", path.display(), e)))
    }

    pub async fn delete_file(&self, key: &str) -> CoveResult<()> {
        let path = self.resolve(key);
        match fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(CoveError::Storage(format!("delete failed: {}", e))),
        }
    }

    pub async fn exists(&self, key: &str) -> CoveResult<bool> {
        let path = self.resolve(key);
        Ok(path.exists())
    }

    pub async fn file_size(&self, key: &str) -> CoveResult<Option<u64>> {
        let path = self.resolve(key);
        match fs::metadata(&path).await {
            Ok(meta) => Ok(Some(meta.len())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(CoveError::Storage(format!("stat failed: {}", e))),
        }
    }

    pub async fn health_check(&self) -> CoveResult<()> {
        let probe = self.base_path.join(".health_check");
        fs::write(&probe, b"ok")
            .await
            .map_err(|e| CoveError::Unavailable(format!("storage: {}", e)))?;
        fs::remove_file(&probe)
            .await
            .map_err(|e| CoveError::Unavailable(format!("storage: {}", e)))?;
        Ok(())
    }

    pub fn base_path(&self) -> &Path {
        &self.base_path
    }
}
