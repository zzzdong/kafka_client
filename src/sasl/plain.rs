use super::{SaslCredentials, SaslMechanism};
use crate::error::SaslError;
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};

pub struct PlainMechanism {
    complete: bool,
    success: bool,
}

impl PlainMechanism {
    pub fn new() -> Self {
        Self {
            complete: false,
            success: false,
        }
    }
}

impl Default for PlainMechanism {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SaslMechanism for PlainMechanism {
    fn name(&self) -> &'static str {
        "PLAIN"
    }

    fn is_client_first(&self) -> bool {
        true
    }

    async fn initial_response(
        &mut self,
        credentials: &SaslCredentials,
    ) -> Result<Option<Bytes>, SaslError> {
        // 格式: authzid\0authcid\0passwd
        let authzid = credentials.authzid.as_deref().unwrap_or("");
        let mut buf = BytesMut::new();
        buf.extend_from_slice(authzid.as_bytes());
        buf.put_u8(0);
        buf.extend_from_slice(credentials.username.as_bytes());
        buf.put_u8(0);
        buf.extend_from_slice(credentials.password.as_bytes());

        self.complete = true;
        self.success = true;
        Ok(Some(buf.freeze()))
    }

    async fn challenge(&mut self, _challenge: &[u8]) -> Result<Option<Bytes>, SaslError> {
        Err(SaslError::ProtocolError(
            "PLAIN should not receive challenge".to_string(),
        ))
    }

    fn is_complete(&self) -> bool {
        self.complete
    }

    fn is_success(&self) -> bool {
        self.success
    }

    fn reset(&mut self) {
        self.complete = false;
        self.success = false;
    }
}
