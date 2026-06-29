//! SASL authentication types — credentials and mechanism identifiers.
//!
//! Actual authentication logic is in [`scram::ScramMechanism`] (SCRAM)
//! and inline in the connection handshake (PLAIN).

pub mod scram;

// ===========================================================================
// SaslCredentials
// ===========================================================================

/// SASL credentials.
///
/// Construct via the named constructors:
/// - [`SaslCredentials::plain`] — PLAIN (SASL/PLAIN)
/// - [`SaslCredentials::scram_sha256`] — SCRAM-SHA-256
/// - [`SaslCredentials::scram_sha512`] — SCRAM-SHA-512
/// - [`SaslCredentials::new`] — custom mechanism
#[derive(Debug, Clone)]
pub struct SaslCredentials {
    mechanism: SaslMechanismType,
    username: String,
    password: String,
    /// Authorisation identity (used by PLAIN).
    authzid: Option<String>,
}

impl SaslCredentials {
    /// Create credentials with an arbitrary mechanism.
    pub fn new(
        mechanism: SaslMechanismType,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            mechanism,
            username: username.into(),
            password: password.into(),
            authzid: None,
        }
    }

    /// PLAIN authentication.
    pub fn plain(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self::new(SaslMechanismType::Plain, username, password)
    }

    /// SCRAM-SHA-256 authentication.
    pub fn scram_sha256(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self::new(SaslMechanismType::ScramSha256, username, password)
    }

    /// SCRAM-SHA-512 authentication.
    pub fn scram_sha512(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self::new(SaslMechanismType::ScramSha512, username, password)
    }

    /// The SASL mechanism (PLAIN, SCRAM-SHA-256, etc.).
    pub fn mechanism(&self) -> SaslMechanismType {
        self.mechanism
    }

    /// The username.
    pub fn username(&self) -> &str {
        &self.username
    }

    /// The password.
    pub fn password(&self) -> &str {
        &self.password
    }

    /// Authorisation identity (None = same as username).
    pub fn authzid(&self) -> Option<&str> {
        self.authzid.as_deref()
    }
}

// ===========================================================================
// SaslMechanismType
// ===========================================================================

/// SASL mechanism type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaslMechanismType {
    Plain,
    ScramSha256,
    ScramSha512,
}

impl SaslMechanismType {
    /// Protocol name sent on the wire (e.g. "PLAIN", "SCRAM-SHA-256").
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha512 => "SCRAM-SHA-512",
        }
    }
}
