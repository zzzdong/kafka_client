use thiserror::Error;

/// Errors returned by Kerberos / GSS-API operations.
#[derive(Debug, Error)]
pub enum KerberosError {
    #[error("invalid credential: {0}")]
    InvalidCredential(String),

    #[error("keytab error: {0}")]
    Keytab(String),

    #[error("ASN.1 decode error: {0}")]
    Asn1(String),

    #[error("crypto error: {0}")]
    Crypto(String),

    #[error("GSS context error: {0}")]
    Gss(String),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("unsupported: {0}")]
    Unsupported(String),
}

pub type Result<T> = std::result::Result<T, KerberosError>;
