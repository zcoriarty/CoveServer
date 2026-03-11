use crate::id::{SessionId, UserId};

#[derive(Debug, Clone)]
pub struct AuthContext {
    pub user_id: UserId,
    pub session_id: SessionId,
    pub is_admin: bool,
}

impl AuthContext {
    pub fn require_admin(&self) -> Result<(), crate::error::CoveError> {
        if !self.is_admin {
            return Err(crate::error::CoveError::Forbidden(
                "admin access required".into(),
            ));
        }
        Ok(())
    }

    pub fn require_owner(&self, resource_owner: &UserId) -> Result<(), crate::error::CoveError> {
        if self.user_id != *resource_owner {
            return Err(crate::error::CoveError::Forbidden(
                "not the resource owner".into(),
            ));
        }
        Ok(())
    }
}
