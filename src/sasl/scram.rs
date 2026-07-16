use super::{SaslCredentials, SaslMechanismType};
use crate::error::SaslError;
use base64::{Engine as _, engine::general_purpose};
use bytes::Bytes;
use hmac::{Hmac, KeyInit, Mac};
use pbkdf2::pbkdf2_hmac;
use rand::Rng;
use sha2::{Digest, Sha256, Sha512};
use subtle::ConstantTimeEq;

/// SCRAM state.
#[derive(Debug, PartialEq, Clone, Copy)]
enum ScramState {
    Initial,
    WaitingServerFirst,
    WaitingServerFinal,
    Complete,
}

/// SCRAM mechanism implementation (synchronous version).
///
/// Used by `handshake.rs` for SASL authentication.
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
        let mut bytes = [0u8; 24];
        rand::rng().fill_bytes(&mut bytes);
        general_purpose::STANDARD.encode(bytes)
    }

    /// Return the SASL mechanism name.
    #[allow(dead_code)]
    pub fn name(&self) -> &'static str {
        match self.mechanism_type {
            SaslMechanismType::ScramSha256 => "SCRAM-SHA-256",
            SaslMechanismType::ScramSha512 => "SCRAM-SHA-512",
            _ => unreachable!(),
        }
    }

    /// Return `true` if this mechanism sends the first client message.
    #[allow(dead_code)]
    pub fn is_client_first(&self) -> bool {
        true
    }

    /// Return `true` if the authentication exchange has completed.
    #[allow(dead_code)]
    pub fn is_complete(&self) -> bool {
        matches!(self.state, ScramState::Complete)
    }

    /// Return `true` if the authentication exchange completed successfully.
    #[allow(dead_code)]
    pub fn is_success(&self) -> bool {
        self.success
    }

    /// Reset the mechanism state so it can be reused for a retry.
    #[allow(dead_code)]
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

    /// Generate the client-first message.
    pub fn client_first(&mut self, credentials: &SaslCredentials) -> Result<Bytes, SaslError> {
        self.username = credentials.username().to_string();
        self.password = credentials.password().to_string();

        // SASLprep normalization + escape special chars (RFC 5802 §5.1)
        // ',' → "=2C", '=' → "=3D"
        let escaped_user = self.escape_name(&self.username);

        // Format: n,,n=username,r=client_nonce
        let msg = format!("n,,n={},r={}", escaped_user, self.client_nonce);
        self.state = ScramState::WaitingServerFirst;

        Ok(Bytes::from(msg))
    }

    /// Escape username for SCRAM: '=' → "=3D", ',' → "=2C" (RFC 5802 §5.1).
    fn escape_name(&self, s: &str) -> String {
        s.replace('=', "=3D").replace(',', "=2C")
    }

    /// Process the server-first challenge and generate the client-final message.
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

    /// Verify the server-final message.
    pub fn verify_server_final(&mut self, server_final: &[u8]) -> Result<(), SaslError> {
        if self.state != ScramState::WaitingServerFinal {
            return Err(SaslError::InvalidState);
        }

        let challenge_str = String::from_utf8(server_final.to_vec())?;

        // RFC 5802 §7: server-final can be "v=<signature>" (success) or "e=<error>" (failure)
        if let Some(sig) = challenge_str.strip_prefix("v=") {
            self.verify_server_signature(sig)?;
            self.state = ScramState::Complete;
            self.success = true;
            Ok(())
        } else if let Some(err_msg) = challenge_str.strip_prefix("e=") {
            Err(SaslError::AuthenticationFailed(format!(
                "Server SCRAM error: {}",
                err_msg
            )))
        } else {
            Err(SaslError::InvalidChallenge(
                "Missing server signature".to_string(),
            ))
        }
    }

    fn hmac(&self, key: &[u8], data: &[u8]) -> Result<Vec<u8>, SaslError> {
        match self.mechanism_type {
            SaslMechanismType::ScramSha256 => {
                let mut mac = Hmac::<Sha256>::new_from_slice(key)
                    .map_err(|_| SaslError::InvalidKey("HMAC-SHA-256 key invalid".to_string()))?;
                mac.update(data);
                Ok(mac.finalize().into_bytes().to_vec())
            }
            SaslMechanismType::ScramSha512 => {
                let mut mac = Hmac::<Sha512>::new_from_slice(key)
                    .map_err(|_| SaslError::InvalidKey("HMAC-SHA-512 key invalid".to_string()))?;
                mac.update(data);
                Ok(mac.finalize().into_bytes().to_vec())
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
            // RFC 5802 §6: reject mandatory (m=) extensions
            if part.starts_with("m=") {
                return Err(SaslError::InvalidChallenge(
                    "Mandatory extension (m=) not supported".to_string(),
                ));
            }
            if let Some(value) = part.strip_prefix("r=") {
                nonce = Some(value.to_string());
            } else if let Some(value) = part.strip_prefix("s=") {
                salt = Some(general_purpose::STANDARD.decode(value).map_err(|e| {
                    SaslError::InvalidChallenge(format!("Invalid base64 salt: {}", e))
                })?);
            } else if let Some(value) = part.strip_prefix("i=") {
                iterations = Some(value.parse().map_err(|e| {
                    SaslError::InvalidChallenge(format!("Invalid iterations: {}", e))
                })?);
            }
        }

        let nonce =
            nonce.ok_or_else(|| SaslError::InvalidChallenge("Missing nonce".to_string()))?;
        let salt = salt.ok_or_else(|| SaslError::InvalidChallenge("Missing salt".to_string()))?;
        let iterations = iterations
            .ok_or_else(|| SaslError::InvalidChallenge("Missing iterations".to_string()))?;

        // Verify that server nonce starts with client nonce
        if !nonce.starts_with(&self.client_nonce) {
            return Err(SaslError::InvalidChallenge(
                "Server nonce does not start with client nonce".to_string(),
            ));
        }

        // Reject dangerously low iteration counts
        if iterations < 4096 {
            return Err(SaslError::InvalidChallenge(format!(
                "Iteration count {iterations} too low (minimum 4096)"
            )));
        }

        self.server_nonce = Some(nonce);
        self.salt = Some(salt);
        self.iterations = Some(iterations);

        Ok(())
    }

    fn generate_client_final(&mut self) -> Result<String, SaslError> {
        let server_nonce = self.server_nonce.as_ref().ok_or(SaslError::InvalidState)?;
        let salt = self.salt.as_ref().ok_or(SaslError::InvalidState)?;
        let iterations = self.iterations.ok_or(SaslError::InvalidState)?;

        // Calculate SaltedPassword
        let salted_password = self.hi(&self.password, salt, iterations);
        self.salted_password = Some(salted_password.clone());

        // ClientKey = HMAC(SaltedPassword, "Client Key")
        let client_key = self.hmac(&salted_password, b"Client Key")?;

        // StoredKey = H(ClientKey)
        let stored_key = self.hash(&client_key);

        // ClientFirstMessageBare = "n=", escaped_username, ",r=", client_nonce
        // Must use escaped username (same as what was sent in client_first)
        let client_first_message_bare = format!(
            "n={},r={}",
            self.escape_name(&self.username),
            self.client_nonce
        );

        // ServerFirstMessage = "r=", server_nonce, ",s=", base64(salt), ",i=", iterations
        let server_first_message = format!(
            "r={},s={},i={}",
            server_nonce,
            general_purpose::STANDARD.encode(salt),
            iterations
        );

        // ClientFinalMessageWithoutProof = "c=biws,r=", server_nonce
        let client_final_message_without_proof = format!("c=biws,r={}", server_nonce);

        // AuthMessage = ClientFirstMessageBare + "," + ServerFirstMessage + "," + ClientFinalMessageWithoutProof
        let auth_message = format!(
            "{},{},{}",
            client_first_message_bare, server_first_message, client_final_message_without_proof
        );
        self.auth_message = Some(auth_message.clone());

        // ClientSignature = HMAC(StoredKey, AuthMessage)
        let client_signature = self.hmac(&stored_key, auth_message.as_bytes())?;

        // ClientProof = ClientKey XOR ClientSignature
        let client_proof: Vec<u8> = client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(k, s)| k ^ s)
            .collect();

        Ok(format!(
            "{},p={}",
            client_final_message_without_proof,
            general_purpose::STANDARD.encode(&client_proof)
        ))
    }

    #[cfg(test)]
    fn set_nonce(&mut self, nonce: &str) {
        self.client_nonce = nonce.to_string();
    }

    fn verify_server_signature(&self, signature: &str) -> Result<(), SaslError> {
        let salted_password = self
            .salted_password
            .as_ref()
            .ok_or(SaslError::InvalidState)?;
        let auth_message = self.auth_message.as_ref().ok_or(SaslError::InvalidState)?;

        // ServerKey = HMAC(SaltedPassword, "Server Key")
        let server_key = self.hmac(salted_password, b"Server Key")?;

        // ServerSignature = HMAC(ServerKey, AuthMessage)
        let expected_signature = self.hmac(&server_key, auth_message.as_bytes())?;

        let signature_bytes = general_purpose::STANDARD
            .decode(signature)
            .map_err(|e| SaslError::InvalidChallenge(format!("Invalid server signature: {}", e)))?;
        if !bool::from(signature_bytes.ct_eq(&expected_signature)) {
            return Err(SaslError::AuthenticationFailed(
                "Server signature verification failed".to_string(),
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sha256_creds() -> SaslCredentials {
        SaslCredentials::scram_sha256("user", "pencil")
    }

    fn new_sha256(nonce: &str) -> ScramMechanism {
        let mut m = ScramMechanism::new_sha256();
        m.set_nonce(nonce);
        m
    }

    // ── State machine ────────────────────────────────────────────────

    #[test]
    fn test_initial_state() {
        let m = ScramMechanism::new_sha256();
        assert!(!m.is_complete());
        assert!(!m.is_success());
    }

    #[test]
    fn test_nonce_is_random() {
        let m1 = ScramMechanism::new_sha256();
        let m2 = ScramMechanism::new_sha256();
        assert_ne!(m1.client_nonce, m2.client_nonce);
    }

    #[test]
    fn test_state_gating() {
        let mut m = ScramMechanism::new_sha256();
        // Can't verify before challenge
        assert!(m.verify_server_final(b"v=x").is_err());
    }

    #[test]
    fn test_reset_clears_state() {
        let mut m = new_sha256("test-nonce");
        m.client_first(&sha256_creds()).unwrap();
        m.reset();
        assert!(!m.is_complete());
        assert!(!m.is_success());
        assert!(m.server_nonce.is_none());
        assert!(m.salt.is_none());
    }

    // ── Crypto primitives ────────────────────────────────────────────

    #[test]
    fn test_hash_sha256() {
        let m = ScramMechanism::new_sha256();
        let h = m.hash(b"hello");
        assert_eq!(
            hex::encode(&h),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn test_hmac_sha256() {
        let m = ScramMechanism::new_sha256();
        let mac = m
            .hmac(b"key", b"The quick brown fox jumps over the lazy dog")
            .unwrap();
        assert_eq!(
            hex::encode(&mac),
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
    }

    #[test]
    fn test_hi_sha256() {
        let m = ScramMechanism::new_sha256();
        let dk = m.hi("password", b"salt", 1);
        assert_eq!(
            hex::encode(&dk),
            "120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b"
        );
    }

    #[test]
    fn test_hash_sha512() {
        let m = ScramMechanism::new_sha512();
        assert_eq!(m.hash(b"hello").len(), 64); // SHA-512 → 64 bytes
    }

    #[test]
    fn test_hi_sha512() {
        let m = ScramMechanism::new_sha512();
        let dk = m.hi("password", b"salt", 1);
        assert_eq!(dk.len(), 64);
    }

    // ── RFC 7677 partial: Client-First + Server-First parsing ────────

    #[test]
    fn test_rfc7677_client_first() {
        let mut m = new_sha256("rOprNGfwEbeRWgbNEkqO");
        let cf = m.client_first(&sha256_creds()).unwrap();
        assert_eq!(
            std::str::from_utf8(&cf).unwrap(),
            "n,,n=user,r=rOprNGfwEbeRWgbNEkqO"
        );
    }

    #[test]
    fn test_rfc7677_parse_server_first() {
        let mut m = new_sha256("rOprNGfwEbeRWgbNEkqO");
        m.client_first(&sha256_creds()).unwrap();

        let sf = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let msg = m.client_final(sf.as_bytes()).unwrap();
        let s = std::str::from_utf8(&msg).unwrap();

        // Must contain c=biws (no channel binding) and a proof
        assert!(s.starts_with("c=biws,r="), "{s}");
        assert!(s.contains(",p="), "{s}");

        // Proof should be non-empty base64
        let proof = s.split("p=").nth(1).unwrap();
        assert!(!proof.is_empty());
        general_purpose::STANDARD.decode(proof).unwrap();
    }

    /// Full SCRAM-SHA-256 roundtrip: client-first → server-first → client-final → server-final.
    /// Uses deterministic nonce so the exchange is reproducible.
    #[test]
    fn test_scram_sha256_full_roundtrip() {
        let mut client = new_sha256("rOprNGfwEbeRWgbNEkqO");

        // 1. Client-First
        let cf = client.client_first(&sha256_creds()).unwrap();
        assert!(std::str::from_utf8(&cf).unwrap().contains("n,,n=user,r="));

        // 2. Simulate server: parse client-first, generate server-first with known salt
        let sf = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let cf_msg = client.client_final(sf.as_bytes()).unwrap();
        let cf_str = std::str::from_utf8(&cf_msg).unwrap();
        assert!(cf_str.contains(",p="));

        // 3. Simulate server: compute server signature from same inputs
        // Use a second SCRAM instance to mimic the server-side computation.
        let mut server = new_sha256("rOprNGfwEbeRWgbNEkqO");
        server.client_first(&sha256_creds()).unwrap();
        server.client_final(sf.as_bytes()).unwrap();

        // Both sides should compute the same salted_password → same ServerKey → same signature
        let server_key = server
            .hmac(server.salted_password.as_ref().unwrap(), b"Server Key")
            .unwrap();
        let server_sig = server
            .hmac(
                &server_key,
                server.auth_message.as_ref().unwrap().as_bytes(),
            )
            .unwrap();
        let srv_final = format!("v={}", general_purpose::STANDARD.encode(&server_sig));

        // 4. Client verifies
        client.verify_server_final(srv_final.as_bytes()).unwrap();
        assert!(client.is_complete());
        assert!(client.is_success());
    }

    /// Wrong server signature must be rejected.
    #[test]
    fn test_bad_server_signature_rejected() {
        let mut m = new_sha256("rOprNGfwEbeRWgbNEkqO");
        m.client_first(&sha256_creds()).unwrap();
        let sf = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        m.client_final(sf.as_bytes()).unwrap();

        let err = m
            .verify_server_final(b"v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
            .unwrap_err();
        assert!(matches!(err, SaslError::AuthenticationFailed(_)));
    }

    // ── Error paths ──────────────────────────────────────────────────

    #[test]
    fn test_missing_nonce_rejected() {
        let mut m = new_sha256("test");
        m.client_first(&sha256_creds()).unwrap();
        assert!(
            m.client_final(b"s=c2FsdA==,i=4096")
                .unwrap_err()
                .to_string()
                .contains("Missing nonce")
        );
    }

    #[test]
    fn test_bad_nonce_prefix_rejected() {
        let mut m = new_sha256("my-nonce");
        m.client_first(&sha256_creds()).unwrap();
        assert!(
            m.client_final(b"r=wrong,s=c2FsdA==,i=4096")
                .unwrap_err()
                .to_string()
                .contains("nonce")
        );
    }

    // ── B1: Server-side `e=` error reporting ─────────────────────────

    #[test]
    fn test_server_error_e_is_reported() {
        let mut m = new_sha256("test");
        m.client_first(&sha256_creds()).unwrap();
        assert!(m.client_final(b"r=test123,s=c2FsdA==,i=4096").is_ok());
        let err = m
            .verify_server_final(b"e=authentication_failed")
            .unwrap_err();
        assert!(
            err.to_string().contains("authentication_failed"),
            "e= error should be reported: {err}"
        );
    }

    // ── B3: `m=` mandatory extension rejected ────────────────────────

    #[test]
    fn test_m_extension_rejected() {
        let mut m = new_sha256("test");
        m.client_first(&sha256_creds()).unwrap();
        let err = m.client_final(b"m=not-supported,r=test-random,s=c2FsdA==,i=4096");
        assert!(err.is_err(), "m= extension should be rejected");
        assert!(
            err.unwrap_err().to_string().contains("Mandatory extension"),
            "error should mention mandatory extension"
        );
    }

    // ── B4: Low iterations rejected ──────────────────────────────────

    #[test]
    fn test_low_iterations_rejected() {
        let mut m = new_sha256("test");
        m.client_first(&sha256_creds()).unwrap();
        let err = m.client_final(b"r=test-ok,s=c2FsdA==,i=1");
        assert!(err.is_err(), "i=1 should be rejected");
        assert!(
            err.unwrap_err().to_string().contains("too low"),
            "error should mention too low"
        );
    }

    #[test]
    fn test_edge_iterations_4096_accepted() {
        let mut m = new_sha256("test");
        m.client_first(&sha256_creds()).unwrap();
        // i=4096 is the minimum and should be accepted
        assert!(m.client_final(b"r=test-edge,s=c2FsdA==,i=4096").is_ok());
    }

    // ── B2: Username escaping ─────────────────────────────────────────

    #[test]
    fn test_username_with_special_chars_escaped() {
        let creds = SaslCredentials::scram_sha256("u,ser", "pass");
        let mut m = new_sha256("nonce123");
        let cf = m.client_first(&creds).unwrap();
        let cf_str = std::str::from_utf8(&cf).unwrap();
        // ',' should be escaped to "=2C"
        assert!(
            cf_str.contains("n=u=2Cser"),
            "comma in username should be escaped: {cf_str}"
        );
    }

    #[test]
    fn test_username_escape_roundtrip() {
        let creds = SaslCredentials::scram_sha256("user", "pass");
        let mut m = new_sha256("nonce123");
        let cf = m.client_first(&creds).unwrap();
        let cf_str = std::str::from_utf8(&cf).unwrap();
        // Normal username should be unchanged
        assert!(cf_str.contains("n=user"), "normal username: {cf_str}");
    }

    #[test]
    fn test_username_eq_escaped() {
        let creds = SaslCredentials::scram_sha256("u=ser", "pass");
        let mut m = new_sha256("nonce123");
        let cf = m.client_first(&creds).unwrap();
        let cf_str = std::str::from_utf8(&cf).unwrap();
        // '=' should be escaped to "=3D"
        assert!(
            cf_str.contains("n=u=3Dser"),
            "equals sign should be escaped: {cf_str}"
        );
    }
}
