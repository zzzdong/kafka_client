use std::io;
use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncWrite};

pub mod tcp;
pub mod tls;

pub use tcp::TcpNetworkStream;
pub use tls::{TlsConfig, TlsNetworkStream};

/// 网络流抽象 trait
///
/// 统一 TCP 和 TLS 连接，使上层无需关心底层加密细节
pub trait NetworkStream: AsyncRead + AsyncWrite + Send + Sync + Unpin {
    /// 获取对端地址
    fn peer_addr(&self) -> io::Result<SocketAddr>;

    /// 获取本地地址
    fn local_addr(&self) -> io::Result<SocketAddr>;

    /// 是否使用 TLS 加密
    fn is_secure(&self) -> bool;
}

/// TLS 配置
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsConfigData {
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

impl Default for TlsConfigData {
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

/// 安全协议类型
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecurityProtocol {
    /// 明文 TCP
    Plaintext,
    /// TLS/SSL
    Ssl(TlsConfigData),
    /// SASL + 明文（需 SASL 认证）
    SaslPlaintext,
    /// SASL + TLS
    SaslSsl(TlsConfigData),
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

/// 传输层连接器
pub struct TransportConnector;

impl TransportConnector {
    /// 建立网络连接
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
                let tls_config = tls::TlsConfig {
                    verify_certificate: config.verify_certificate,
                    domain: config.domain.clone(),
                    ca_cert_path: config.ca_cert_path.clone(),
                    client_cert_path: config.client_cert_path.clone(),
                    client_key_path: config.client_key_path.clone(),
                };
                let stream = TlsNetworkStream::connect(addr, tls_config).await?;
                Ok(Box::new(stream) as Box<dyn NetworkStream>)
            }
        }
    }
}
