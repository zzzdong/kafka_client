use std::io;
use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncWrite};

pub mod tcp;
pub mod tls;

pub use tcp::TcpNetworkStream;
pub use tls::{TlsConfig, TlsNetworkStream};

/// Abstract network stream trait.
///
/// Unifies TCP and TLS connections so that upper layers do not need to know
/// whether the underlying transport is encrypted.
pub trait NetworkStream: AsyncRead + AsyncWrite + Send + Sync + Unpin {
    /// Return the remote peer address.
    fn peer_addr(&self) -> io::Result<SocketAddr>;

    /// Return the local address.
    fn local_addr(&self) -> io::Result<SocketAddr>;

    /// Return `true` if the stream is encrypted with TLS.
    fn is_secure(&self) -> bool;
}

/// Security protocol type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecurityProtocol {
    /// Plaintext TCP.
    Plaintext,
    /// TLS/SSL.
    Ssl(TlsConfig),
    /// SASL over plaintext (requires SASL authentication).
    SaslPlaintext,
    /// SASL over TLS.
    SaslSsl(TlsConfig),
}

impl SecurityProtocol {
    pub fn uses_tls(&self) -> bool {
        matches!(
            self,
            SecurityProtocol::Ssl(_) | SecurityProtocol::SaslSsl(_)
        )
    }

    pub fn uses_sasl(&self) -> bool {
        matches!(
            self,
            SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl(_)
        )
    }
}

/// Transport layer connector.
pub struct TransportConnector;

impl TransportConnector {
    /// Establish a network connection with the given security protocol.
    pub async fn connect(
        addr: SocketAddr,
        protocol: &SecurityProtocol,
    ) -> io::Result<Box<dyn NetworkStream>> {
        match protocol {
            SecurityProtocol::Plaintext | SecurityProtocol::SaslPlaintext => {
                let stream = TcpNetworkStream::connect(addr).await?;
                Ok(Box::new(stream) as Box<dyn NetworkStream>)
            }
            SecurityProtocol::Ssl(config) | SecurityProtocol::SaslSsl(config) => {
                let stream = TlsNetworkStream::connect(addr, config.clone()).await?;
                Ok(Box::new(stream) as Box<dyn NetworkStream>)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn security_protocol_flags() {
        assert!(!SecurityProtocol::Plaintext.uses_tls());
        assert!(!SecurityProtocol::Plaintext.uses_sasl());
        assert!(SecurityProtocol::SaslPlaintext.uses_sasl());
        assert!(!SecurityProtocol::SaslPlaintext.uses_tls());

        let tls = SecurityProtocol::Ssl(TlsConfig::default());
        assert!(tls.uses_tls());
        assert!(!tls.uses_sasl());

        let sasl_tls = SecurityProtocol::SaslSsl(TlsConfig::default());
        assert!(sasl_tls.uses_tls());
        assert!(sasl_tls.uses_sasl());
    }

    #[test]
    fn tls_config_default_has_no_custom_paths() {
        let config = TlsConfig::default();
        assert!(config.domain.is_empty());
        assert!(config.ca_cert_path.is_none());
        assert!(config.client_cert_path.is_none());
        assert!(config.client_key_path.is_none());
    }
}
