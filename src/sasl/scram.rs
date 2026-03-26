use bytes::Bytes;
use base64::{Engine as _, engine::general_purpose};
use sha2::{Sha256, Sha512, Digest};
use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2_hmac;
use super::{SaslCredentials, SaslMechanismType};
use crate::error::SaslError;

/// SCRAM 状态
#[derive(Debug, PartialEq, Clone, Copy)]
enum ScramState {
    Initial,
    WaitingServerFirst,
    WaitingServerFinal,
    Complete,
}

/// SCRAM 机制实现（同步版本，用于连接认证）
pub struct ScramMechanism {
    mechanism_type: SaslMechanismType,
    state: ScramState,
    username: String,
    password: String,
    client_nonce: String,
    server_nonce: Option<String>,
    salt: Option<Vec<u8>>,
    iterations: Option<u32>,
    auth_message: Option<String>,
    success: bool,
    salted_password: Option<Vec<u8>>,
}

impl ScramMechanism {
    pub fn new_sha256() -> Self {
        Self::new(SaslMechanismType::ScramSha256)
    }

    pub fn new_sha512() -> Self {
        Self::new(SaslMechanismType::ScramSha512)
    }

    fn new(mechanism_type: SaslMechanismType) -> Self {
        Self {
            mechanism_type,
            state: ScramState::Initial,
            username: String::new(),
            password: String::new(),
            client_nonce: Self::generate_nonce(),
            server_nonce: None,
            salt: None,
            iterations: None,
            auth_message: None,
            success: false,
            salted_password: None,
        }
    }

    fn generate_nonce() -> String {
        use rand::RngCore;
        let mut bytes = [0u8; 24];
        rand::rng().fill_bytes(&mut bytes);
        general_purpose::STANDARD.encode(&bytes)
    }

    /// 获取机制名称
    pub fn name(&self) -> &'static str {
        match self.mechanism_type {
            SaslMechanismType::ScramSha256 => "SCRAM-SHA-256",
            SaslMechanismType::ScramSha512 => "SCRAM-SHA-512",
            _ => unreachable!(),
        }
    }

    /// 是否为 client-first 机制
    pub fn is_client_first(&self) -> bool {
        true
    }

    /// 认证是否完成
    pub fn is_complete(&self) -> bool {
        matches!(self.state, ScramState::Complete)
    }

    /// 认证是否成功
    pub fn is_success(&self) -> bool {
        self.success
    }

    /// 重置状态（用于重试）
    pub fn reset(&mut self) {
        self.state = ScramState::Initial;
        self.client_nonce = Self::generate_nonce();
        self.server_nonce = None;
        self.salt = None;
        self.iterations = None;
        self.auth_message = None;
        self.success = false;
        self.salted_password = None;
    }

    /// 生成初始响应（client-first）
    pub fn client_first(&mut self, credentials: &SaslCredentials) -> Result<Bytes, SaslError> {
        self.username = credentials.username.clone();
        self.password = credentials.password.clone();

        // 格式: n,,n=username,r=client_nonce
        let msg = format!("n,,n={},r={}", self.username, self.client_nonce);
        self.state = ScramState::WaitingServerFirst;

        Ok(Bytes::from(msg))
    }

    /// 处理服务器第一轮挑战，生成 client-final
    pub fn client_final(&mut self, server_first: &[u8]) -> Result<Bytes, SaslError> {
        if self.state != ScramState::WaitingServerFirst {
            return Err(SaslError::InvalidState);
        }

        let challenge_str = String::from_utf8(server_first.to_vec())?;
        self.parse_server_first(&challenge_str)?;
        let client_final = self.generate_client_final()?;
        self.state = ScramState::WaitingServerFinal;
        
        Ok(Bytes::from(client_final))
    }

    /// 验证服务器最终响应
    pub fn verify_server_final(&mut self, server_final: &[u8]) -> Result<(), SaslError> {
        if self.state != ScramState::WaitingServerFinal {
            return Err(SaslError::InvalidState);
        }

        let challenge_str = String::from_utf8(server_final.to_vec())?;
        
        if let Some(sig) = challenge_str.strip_prefix("v=") {
            self.verify_server_signature(sig)?;
            self.state = ScramState::Complete;
            self.success = true;
            Ok(())
        } else {
            Err(SaslError::InvalidChallenge("Missing server signature".to_string()))
        }
    }

    fn hmac(&self, key: &[u8], data: &[u8]) -> Vec<u8> {
        match self.mechanism_type {
            SaslMechanismType::ScramSha256 => {
                let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
                mac.update(data);
                mac.finalize().into_bytes().to_vec()
            }
            SaslMechanismType::ScramSha512 => {
                let mut mac = Hmac::<Sha512>::new_from_slice(key).unwrap();
                mac.update(data);
                mac.finalize().into_bytes().to_vec()
            }
            _ => unreachable!(),
        }
    }

    fn hash(&self, data: &[u8]) -> Vec<u8> {
        match self.mechanism_type {
            SaslMechanismType::ScramSha256 => {
                let mut hasher = Sha256::new();
                hasher.update(data);
                hasher.finalize().to_vec()
            }
            SaslMechanismType::ScramSha512 => {
                let mut hasher = Sha512::new();
                hasher.update(data);
                hasher.finalize().to_vec()
            }
            _ => unreachable!(),
        }
    }

    fn hi(&self, password: &str, salt: &[u8], iterations: u32) -> Vec<u8> {
        let dk_len = match self.mechanism_type {
            SaslMechanismType::ScramSha256 => 32,
            SaslMechanismType::ScramSha512 => 64,
            _ => unreachable!(),
        };
        let mut output = vec![0u8; dk_len];

        match self.mechanism_type {
            SaslMechanismType::ScramSha256 => {
                pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut output);
            }
            SaslMechanismType::ScramSha512 => {
                pbkdf2_hmac::<Sha512>(password.as_bytes(), salt, iterations, &mut output);
            }
            _ => unreachable!(),
        }

        output
    }

    fn parse_server_first(&mut self, challenge: &str) -> Result<(), SaslError> {
        let mut nonce = None;
        let mut salt = None;
        let mut iterations = None;

        for part in challenge.split(',') {
            if let Some(value) = part.strip_prefix("r=") {
                nonce = Some(value.to_string());
            } else if let Some(value) = part.strip_prefix("s=") {
                salt = Some(general_purpose::STANDARD.decode(value)
                    .map_err(|e| SaslError::InvalidChallenge(format!("Invalid base64 salt: {}", e)))?);
            } else if let Some(value) = part.strip_prefix("i=") {
                iterations = Some(value.parse()
                    .map_err(|e| SaslError::InvalidChallenge(format!("Invalid iterations: {}", e)))?);
            }
        }

        let nonce = nonce.ok_or_else(|| SaslError::InvalidChallenge("Missing nonce".to_string()))?;
        let salt = salt.ok_or_else(|| SaslError::InvalidChallenge("Missing salt".to_string()))?;
        let iterations = iterations.ok_or_else(|| SaslError::InvalidChallenge("Missing iterations".to_string()))?;

        // Verify that server nonce starts with client nonce
        if !nonce.starts_with(&self.client_nonce) {
            return Err(SaslError::InvalidChallenge("Server nonce does not start with client nonce".to_string()));
        }

        self.server_nonce = Some(nonce);
        self.salt = Some(salt);
        self.iterations = Some(iterations);

        Ok(())
    }

    fn generate_client_final(&mut self) -> Result<String, SaslError> {
        let server_nonce = self.server_nonce.as_ref()
            .ok_or_else(|| SaslError::InvalidState)?;
        let salt = self.salt.as_ref()
            .ok_or_else(|| SaslError::InvalidState)?;
        let iterations = self.iterations
            .ok_or_else(|| SaslError::InvalidState)?;

        // Calculate SaltedPassword
        let salted_password = self.hi(&self.password, salt, iterations);
        self.salted_password = Some(salted_password.clone());

        // ClientKey = HMAC(SaltedPassword, "Client Key")
        let client_key = self.hmac(&salted_password, b"Client Key");

        // StoredKey = H(ClientKey)
        let stored_key = self.hash(&client_key);

        // ClientFirstMessageBare = "n=", username, ",r=", client_nonce
        let client_first_message_bare = format!("n={},r={}", self.username, self.client_nonce);

        // ServerFirstMessage = "r=", server_nonce, ",s=", base64(salt), ",i=", iterations
        let server_first_message = format!("r={},s={},i={}",
            server_nonce,
            general_purpose::STANDARD.encode(salt),
            iterations);

        // ClientFinalMessageWithoutProof = "c=biws,r=", server_nonce
        let client_final_message_without_proof = format!("c=biws,r={}", server_nonce);

        // AuthMessage = ClientFirstMessageBare + "," + ServerFirstMessage + "," + ClientFinalMessageWithoutProof
        let auth_message = format!("{},{},{}",
            client_first_message_bare,
            server_first_message,
            client_final_message_without_proof);
        self.auth_message = Some(auth_message.clone());

        // ClientSignature = HMAC(StoredKey, AuthMessage)
        let client_signature = self.hmac(&stored_key, auth_message.as_bytes());

        // ClientProof = ClientKey XOR ClientSignature
        let client_proof: Vec<u8> = client_key.iter()
            .zip(client_signature.iter())
            .map(|(k, s)| k ^ s)
            .collect();

        Ok(format!("{},p={}", client_final_message_without_proof, general_purpose::STANDARD.encode(&client_proof)))
    }

    fn verify_server_signature(&self, signature: &str) -> Result<(), SaslError> {
        let salted_password = self.salted_password.as_ref()
            .ok_or_else(|| SaslError::InvalidState)?;
        let auth_message = self.auth_message.as_ref()
            .ok_or_else(|| SaslError::InvalidState)?;

        // ServerKey = HMAC(SaltedPassword, "Server Key")
        let server_key = self.hmac(salted_password, b"Server Key");

        // ServerSignature = HMAC(ServerKey, AuthMessage)
        let expected_signature = self.hmac(&server_key, auth_message.as_bytes());
        let expected_signature_b64 = general_purpose::STANDARD.encode(&expected_signature);

        if signature != expected_signature_b64 {
            return Err(SaslError::AuthenticationFailed("Server signature verification failed".to_string()));
        }

        Ok(())
    }
}

// ============================================================================
// 异步适配器（用于需要 async trait 的场景）
// ============================================================================

use async_trait::async_trait;
use super::SaslMechanism;

pub struct AsyncScramMechanism {
    inner: ScramMechanism,
}

impl AsyncScramMechanism {
    pub fn new_sha256() -> Self {
        Self {
            inner: ScramMechanism::new_sha256(),
        }
    }

    pub fn new_sha512() -> Self {
        Self {
            inner: ScramMechanism::new_sha512(),
        }
    }
}

#[async_trait]
impl SaslMechanism for AsyncScramMechanism {
    fn name(&self) -> &'static str {
        self.inner.name()
    }

    fn is_client_first(&self) -> bool {
        self.inner.is_client_first()
    }

    async fn initial_response(&mut self, credentials: &SaslCredentials)
        -> Result<Option<Bytes>, SaslError> {
        self.inner.client_first(credentials).map(Some)
    }

    async fn challenge(&mut self, challenge: &[u8])
        -> Result<Option<Bytes>, SaslError> {
        if self.inner.state == ScramState::WaitingServerFirst {
            self.inner.client_final(challenge).map(Some)
        } else if self.inner.state == ScramState::WaitingServerFinal {
            self.inner.verify_server_final(challenge).map(|_| None)
        } else {
            Err(SaslError::ProtocolError("Invalid SCRAM state".to_string()))
        }
    }

    fn is_complete(&self) -> bool {
        self.inner.is_complete()
    }

    fn is_success(&self) -> bool {
        self.inner.is_success()
    }

    fn reset(&mut self) {
        self.inner.reset();
    }
}
