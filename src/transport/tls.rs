use super::NetworkStream;
use rustls::pki_types::ServerName;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::{self, ClientConfig};

/// TLS configuration.
///
/// Certificate verification is always enabled. The `NoCertificateVerification`
/// path in rustls 0.23 is incompatible with the Kafka TLS stack (it causes an
/// `AlertReceived` error), so this library does not provide a way to skip
/// certificate verification.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TlsConfig {
    /// Server domain name used for SNI and certificate verification.
    pub domain: String,
    /// Path to a CA certificate file. If not set, the system certificate store
    /// is used.
    pub ca_cert_path: Option<String>,
    /// Path to the client certificate (used for mTLS).
    pub client_cert_path: Option<String>,
    /// Path to the client private key (used for mTLS).
    pub client_key_path: Option<String>,
}

/// TLS network stream.
pub struct TlsNetworkStream {
    inner: tokio_rustls::client::TlsStream<TcpStream>,
}

impl TlsNetworkStream {
    /// Establish a TLS connection (TCP connection + TLS handshake).
    pub async fn connect(addr: SocketAddr, config: TlsConfig) -> io::Result<Self> {
        let tcp = TcpStream::connect(addr).await?;
        Self::from_stream(tcp, config).await
    }

    /// Perform the TLS handshake over an existing TCP stream.
    ///
    /// Splitting TCP connection and TLS handshake into two separate steps makes
    /// it easier for callers to distinguish between:
    /// - TCP-level failures (unreachable address, port not listening, etc.)
    /// - TLS-level failures (certificate errors, hostname mismatch, etc.)
    pub async fn from_stream(tcp: TcpStream, config: TlsConfig) -> io::Result<Self> {
        let domain = config.domain.clone();
        let tls_config = Self::build_config(&config)?;

        let connector = TlsConnector::from(tls_config);
        let server_name = ServerName::try_from(domain)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let tls_stream = connector.connect(server_name, tcp).await?;

        Ok(Self { inner: tls_stream })
    }

    /// Build a TLS client configuration.
    ///
    /// Always uses the standard certificate verification path. The
    /// `NoCertificateVerification` path in rustls 0.23 is incompatible with
    /// the Kafka TLS stack (it causes an `AlertReceived` error).
    fn build_config(config: &TlsConfig) -> io::Result<Arc<ClientConfig>> {
        let mut root_certs = rustls::RootCertStore::empty();

        if let Some(ca_path) = &config.ca_cert_path {
            let ca_cert = std::fs::read(ca_path)?;
            for result in rustls_pemfile::certs(&mut ca_cert.as_slice()) {
                let cert = result.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                root_certs
                    .add(cert)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            }
        } else {
            let native_certs = rustls_native_certs::load_native_certs();
            for cert in native_certs.certs {
                root_certs
                    .add(cert)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            }
        }

        let config_builder = ClientConfig::builder().with_root_certificates(root_certs);

        let client_config = if let (Some(cert_path), Some(key_path)) =
            (&config.client_cert_path, &config.client_key_path)
        {
            let cert_file = std::fs::read(cert_path)?;
            let key_file = std::fs::read(key_path)?;

            let certs: Result<Vec<_>, _> =
                rustls_pemfile::certs(&mut cert_file.as_slice()).collect();
            let certs = certs.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            let key = rustls_pemfile::private_key(&mut key_file.as_slice())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "No private key found")
                })?;

            config_builder
                .with_client_auth_cert(certs, key)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
        } else {
            config_builder.with_no_client_auth()
        };

        Ok(Arc::new(client_config))
    }
}

// AsyncRead implementation
impl AsyncRead for TlsNetworkStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

// AsyncWrite implementation
impl AsyncWrite for TlsNetworkStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

// NetworkStream implementation
impl NetworkStream for TlsNetworkStream {
    fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.inner.get_ref().0.peer_addr()
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.get_ref().0.local_addr()
    }

    fn is_secure(&self) -> bool {
        true
    }
}
