// kafka-client/src/connection.rs

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio_util::codec::Framed;
use tracing::debug;

use crate::codec::{KafkaCodec, KafkaFrame};
use crate::error::{KafkaError, Result};
use crate::sasl::{SaslCredentials, SaslMechanismType};
use crate::transport::*;
use kafka_client_protocol::{self as protocol, Request, Response};

// 导入 SCRAM 实现
use crate::sasl::scram::ScramMechanism;

// ============================================================================
// NegotiatedVersions
// ============================================================================

#[derive(Debug, Clone)]
pub struct NegotiatedVersions {
    versions: HashMap<i16, i16>,
}

impl NegotiatedVersions {
    pub fn new() -> Self {
        Self {
            versions: HashMap::new(),
        }
    }

    pub fn set_version(&mut self, api_key: i16, version: i16) {
        self.versions.insert(api_key, version);
    }

    pub fn get_version(&self, api_key: i16) -> Option<i16> {
        self.versions.get(&api_key).copied()
    }
}

impl Default for NegotiatedVersions {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Pipeline 模式连接（正常操作阶段）
// ============================================================================

/// Pipeline 模式的连接
/// 内部使用顺序请求模式（发送后等待响应）
/// 拥有 Framed 的所有权（外部通过 Arc<Mutex<Connection>> 实现线程安全）
pub struct Connection {
    framed: Framed<Box<dyn NetworkStream>, KafkaCodec>,
    negotiated: Arc<NegotiatedVersions>,
    next_correlation_id: std::sync::atomic::AtomicI32,
}

impl Connection {
    fn new(
        framed: Framed<Box<dyn NetworkStream>, KafkaCodec>,
        _client_id: Option<String>,
        negotiated: Arc<NegotiatedVersions>,
    ) -> Self {
        Connection {
            framed,
            negotiated,
            next_correlation_id: std::sync::atomic::AtomicI32::new(rand::random()),
        }
    }

    /// 发送请求并等待响应（顺序模式：发送 → 等待响应）
    pub async fn send_request<Req, Resp>(&mut self, request: &Req) -> Result<Resp>
    where
        Req: Request,
        Resp: Response,
    {
        let api_key = request.api_key();
        let version = self
            .negotiated
            .get_version(api_key)
            .ok_or(KafkaError::UnsupportedApi(api_key))?;

        let correlation_id = self
            .next_correlation_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        debug!(
            api_key = api_key,
            version = version,
            correlation_id = correlation_id,
            "sending request"
        );

        // 编码请求
        let request_data =
            request.encode_frame(version, correlation_id, Some("kafka-client".to_string()))?;

        // 发送到网络
        self.framed.send(KafkaFrame::new(request_data)).await?;

        // 显式 flush
        self.framed.flush().await?;

        // 等待响应
        let frame = self
            .framed
            .next()
            .await
            .ok_or(KafkaError::ConnectionClosed)??;

        debug!(response_len = frame.data.len(), "received response");

        // 解码响应
        let (header, response) = Resp::decode_frame(frame.data, version)?;

        // 验证 correlation_id
        if header.correlation_id() != correlation_id {
            return Err(KafkaError::CorrelationIdMismatch {
                expected: correlation_id,
                actual: header.correlation_id(),
            });
        }

        Ok(response)
    }

    /// 发送请求并在指定版本下等待响应（用于调试/测试）
    pub async fn send_request_at<Req, Resp>(&mut self, request: &Req, version: i16) -> Result<Resp>
    where
        Req: Request,
        Resp: Response,
    {
        let api_key = request.api_key();
        let correlation_id = self
            .next_correlation_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        debug!(
            api_key = api_key,
            version = version,
            "sending request at explicit version"
        );

        // 编码请求
        let request_data =
            request.encode_frame(version, correlation_id, Some("kafka-client".to_string()))?;

        // 发送到网络
        self.framed.send(KafkaFrame::new(request_data)).await?;

        // 显式 flush
        self.framed.flush().await?;

        // 等待响应
        let frame = self
            .framed
            .next()
            .await
            .ok_or(KafkaError::ConnectionClosed)??;

        // 解码响应
        let (header, response) = Resp::decode_frame(frame.data, version)?;

        // 验证 correlation_id
        if header.correlation_id() != correlation_id {
            return Err(KafkaError::CorrelationIdMismatch {
                expected: correlation_id,
                actual: header.correlation_id(),
            });
        }

        Ok(response)
    }

    pub fn negotiated(&self) -> &NegotiatedVersions {
        &self.negotiated
    }

    pub async fn close(mut self) -> Result<()> {
        // 先 flush 所有未写入的数据，再关闭底层连接（发送 TLS close_notify）
        if let Err(e) = self.framed.close().await {
            debug!("Error closing framed connection: {}", e);
        }
        Ok(())
    }
}

// ============================================================================
// 构建器 - 管理连接建立流程
// ============================================================================

pub struct Builder {
    addr: SocketAddr,
    security_protocol: SecurityProtocol,
    client_name: String,
    client_version: String,
    client_id: Option<String>,
    sasl_config: Option<(SaslMechanismType, SaslCredentials)>,
}

impl Builder {
    pub fn new(
        addr: SocketAddr,
        security_protocol: SecurityProtocol,
        client_name: String,
        client_version: String,
    ) -> Self {
        Builder {
            addr,
            security_protocol,
            client_name,
            client_version,
            client_id: Some("kafka-client".to_string()),
            sasl_config: None,
        }
    }

    pub fn with_sasl(mut self, mechanism: SaslMechanismType, credentials: SaslCredentials) -> Self {
        self.sasl_config = Some((mechanism, credentials));
        self
    }

    pub fn with_client_id(mut self, client_id: String) -> Self {
        self.client_id = Some(client_id);
        self
    }

    pub async fn build(self) -> Result<Connection> {
        // 1. 建立底层连接
        let stream = TransportConnector::connect(self.addr, &self.security_protocol).await?;
        let framed = Framed::new(stream, KafkaCodec::new());

        // 2. 创建顺序连接（一问一答模式）
        let mut seq_conn = SequentialConnection::new(framed, self.client_id);

        // 3. 版本协商
        let negotiated =
            Self::handshake(&mut seq_conn, self.client_name, self.client_version).await?;
        seq_conn.set_negotiated(negotiated);

        // 4. SASL 认证（如果需要）
        if let Some((mechanism, credentials)) = self.sasl_config {
            Self::sasl_authenticate(&mut seq_conn, mechanism, credentials).await?;
        }

        // 5. 转换为 Pipeline 连接（析构 SequentialConnection）
        Ok(seq_conn.into_pipeline())
    }

    async fn handshake(
        conn: &mut SequentialConnection,
        client_name: String,
        client_version: String,
    ) -> Result<NegotiatedVersions> {
        let request = protocol::ApiVersionsRequest {
            client_software_name: Some(client_name),
            client_software_version: Some(client_version),
        };

        let response: protocol::ApiVersionsResponse = conn.send_request(&request).await?;

        if response.error_code != 0 {
            return Err(KafkaError::Protocol(format!(
                "ApiVersions failed: error {}",
                response.error_code
            )));
        }

        let mut negotiated = NegotiatedVersions::new();
        for api in response.api_keys {
            if let Some((client_min, client_max)) = protocol::get_version_range(api.api_key) {
                let mut version = api.max_version.min(client_max);
                // 当前测试 broker 对 flexible 响应的支持不稳定，先协商到非 flexible 版本。
                if let Some(flex) = protocol::get_flexible_version(api.api_key) {
                    if version >= flex {
                        version = flex - 1;
                    }
                }
                if version >= api.min_version && version >= client_min {
                    negotiated.set_version(api.api_key, version);
                    debug!(
                        api_key = api.api_key,
                        negotiated_version = version,
                        "negotiated API version"
                    );
                }
            }
        }

        Ok(negotiated)
    }

    async fn sasl_authenticate(
        conn: &mut SequentialConnection,
        mechanism: SaslMechanismType,
        credentials: SaslCredentials,
    ) -> Result<()> {
        // SASL 握手
        let handshake_req = protocol::SaslHandshakeRequest {
            mechanism: mechanism.as_str().to_string(),
        };

        let handshake_resp: protocol::SaslHandshakeResponse =
            conn.send_request(&handshake_req).await?;

        if handshake_resp.error_code != 0 {
            return Err(KafkaError::Protocol(format!(
                "SASL handshake failed: error {}",
                handshake_resp.error_code
            )));
        }

        // 根据机制认证
        match mechanism {
            SaslMechanismType::Plain => Self::authenticate_plain(conn, credentials).await,
            SaslMechanismType::ScramSha256 | SaslMechanismType::ScramSha512 => {
                Self::authenticate_scram(conn, mechanism, credentials).await
            }
        }
    }

    async fn authenticate_plain(
        conn: &mut SequentialConnection,
        credentials: SaslCredentials,
    ) -> Result<()> {
        let mut auth_bytes = Vec::new();
        auth_bytes.push(0x00);
        auth_bytes.extend_from_slice(credentials.username.as_bytes());
        auth_bytes.push(0x00);
        auth_bytes.extend_from_slice(credentials.password.as_bytes());

        let auth_req = protocol::SaslAuthenticateRequest {
            auth_bytes: Bytes::from(auth_bytes),
        };

        let auth_resp: protocol::SaslAuthenticateResponse = conn.send_request(&auth_req).await?;

        if auth_resp.error_code != 0 {
            return Err(KafkaError::Protocol(format!(
                "PLAIN authentication failed: error {}, message: {:?}",
                auth_resp.error_code, auth_resp.error_message
            )));
        }

        Ok(())
    }

    async fn authenticate_scram(
        conn: &mut SequentialConnection,
        mechanism: SaslMechanismType,
        credentials: SaslCredentials,
    ) -> Result<()> {
        let mut scram = match mechanism {
            SaslMechanismType::ScramSha256 => ScramMechanism::new_sha256(),
            SaslMechanismType::ScramSha512 => ScramMechanism::new_sha512(),
            _ => {
                return Err(KafkaError::MechanismNotSupported(
                    mechanism.as_str().to_string(),
                ));
            }
        };

        // 第一轮
        let auth_bytes = scram
            .client_first(&credentials)
            .map_err(KafkaError::SaslError)?;
        let auth_req = protocol::SaslAuthenticateRequest { auth_bytes };
        let auth_resp: protocol::SaslAuthenticateResponse = conn.send_request(&auth_req).await?;

        if auth_resp.error_code != 0 {
            return Err(KafkaError::Protocol(format!(
                "SCRAM round 1 failed: error {}",
                auth_resp.error_code
            )));
        }

        // 第二轮
        let auth_bytes = scram
            .client_final(&auth_resp.auth_bytes)
            .map_err(KafkaError::SaslError)?;
        let auth_req = protocol::SaslAuthenticateRequest { auth_bytes };
        let auth_resp: protocol::SaslAuthenticateResponse = conn.send_request(&auth_req).await?;

        if auth_resp.error_code != 0 {
            return Err(KafkaError::Protocol(format!(
                "SCRAM round 2 failed: error {}",
                auth_resp.error_code
            )));
        }

        // 验证
        scram
            .verify_server_final(&auth_resp.auth_bytes)
            .map_err(KafkaError::SaslError)?;

        Ok(())
    }
}

// ============================================================================
// 一问一答模式连接（初始化阶段）
// ============================================================================

/// 一问一答模式的连接
/// 用于版本协商和 SASL 认证
/// 直接拥有 Framed 所有权，into_pipeline 时移交所有权给 Connection
pub struct SequentialConnection {
    framed: Framed<Box<dyn NetworkStream>, KafkaCodec>,
    client_id: Option<String>,
    negotiated: NegotiatedVersions,
}

impl SequentialConnection {
    /// 创建新的顺序连接
    pub fn new(
        framed: Framed<Box<dyn NetworkStream>, KafkaCodec>,
        client_id: Option<String>,
    ) -> Self {
        SequentialConnection {
            framed,
            client_id,
            negotiated: NegotiatedVersions::new(),
        }
    }

    /// 发送请求并等待响应（自动处理 correlation_id）
    pub async fn send_request<Req, Resp>(&mut self, request: &Req) -> Result<Resp>
    where
        Req: Request,
        Resp: Response,
    {
        let api_key = request.api_key();
        let version = self.negotiated.get_version(api_key).unwrap_or(0);

        // 每次请求使用新的 correlation_id
        let correlation_id = rand::random();

        // 编码请求
        let request_data = request.encode_frame(version, correlation_id, self.client_id.clone())?;

        debug!(
            api_key = api_key,
            version = version,
            correlation_id = correlation_id,
            "sending sequential request"
        );

        // 发送
        self.framed.send(KafkaFrame::new(request_data)).await?;

        // 显式 flush
        self.framed.flush().await?;

        // 等待响应
        let frame = self
            .framed
            .next()
            .await
            .ok_or(KafkaError::ConnectionClosed)??;

        debug!(
            response_len = frame.data.len(),
            "received sequential response"
        );

        // 解码响应
        let (header, response) = Resp::decode_frame(frame.data, version)?;

        // 验证 correlation_id
        if header.correlation_id() != correlation_id {
            return Err(KafkaError::CorrelationIdMismatch {
                expected: correlation_id,
                actual: header.correlation_id(),
            });
        }

        Ok(response)
    }

    /// 获取当前协商的版本
    pub fn negotiated(&self) -> &NegotiatedVersions {
        &self.negotiated
    }

    /// 设置协商版本（在 ApiVersions 后调用）
    pub fn set_negotiated(&mut self, negotiated: NegotiatedVersions) {
        self.negotiated = negotiated;
    }

    /// 转换为 Pipeline 连接（析构 SequentialConnection，移交 Framed 所有权）
    pub fn into_pipeline(self) -> Connection {
        Connection::new(self.framed, self.client_id, Arc::new(self.negotiated))
    }

    /// 获取底层 framed（用于特殊场景）
    pub fn into_parts(self) -> Framed<Box<dyn NetworkStream>, KafkaCodec> {
        self.framed
    }
}
