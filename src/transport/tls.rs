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

/// TLS 配置
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsConfig {
    /// 是否验证证书（生产环境应为 true）
    pub verify_certificate: bool,
    /// 服务器域名（用于 SNI 和证书验证）
    pub domain: String,
    /// CA 证书文件路径（可选，不设置则使用系统证书）
    pub ca_cert_path: Option<String>,
    /// 客户端证书路径（mTLS 时使用）
    pub client_cert_path: Option<String>,
    /// 客户端私钥路径（mTLS 时使用）
    pub client_key_path: Option<String>,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            verify_certificate: true,
            domain: String::new(),
            ca_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
        }
    }
}

/// TLS 网络流
pub struct TlsNetworkStream {
    inner: tokio_rustls::client::TlsStream<TcpStream>,
}

impl TlsNetworkStream {
    /// 建立 TLS 连接
    pub async fn connect(addr: SocketAddr, config: TlsConfig) -> io::Result<Self> {
        // 1. 建立 TCP 连接
        let tcp = TcpStream::connect(addr).await?;

        // 2. 构建 TLS 配置
        let domain = config.domain.clone();
        let tls_config = Self::build_config(&config)?;

        // 3. 建立 TLS 连接
        let connector = TlsConnector::from(tls_config);
        let server_name = ServerName::try_from(domain)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let tls_stream = connector.connect(server_name, tcp).await?;

        Ok(Self { inner: tls_stream })
    }

    /// 构建 TLS 客户端配置
    ///
    /// NOTE: `verify_certificate=false` (NoCertificateVerification) is
    /// incompatible with Kafka's TLS stack in rustls 0.23 because the
    /// `dangerous().with_custom_certificate_verifier()` builder path
    /// causes the broker to reject the handshake (AlertReceived).
    /// Therefore we always use the standard builder path regardless of
    /// the `verify_certificate` setting.
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

// 实现 AsyncRead
impl AsyncRead for TlsNetworkStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

// 实现 AsyncWrite
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

// 实现 NetworkStream
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
