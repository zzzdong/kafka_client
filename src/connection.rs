// kafka-client/src/connection.rs

use bytes::Bytes;
use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;
use tracing::{debug, error, warn};

use crate::codec::{KafkaCodec, KafkaFrame};
use crate::error::{KafkaError, Result};
use crate::sasl::{SaslCredentials, SaslMechanismType};
use crate::transport::*;
use kafka_client_protocol::{self as protocol, Request, Response, ResponseHeader};

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
// 一问一答模式连接（初始化阶段）
// ============================================================================

/// 一问一答模式的连接
/// 用于版本协商和 SASL 认证
pub struct SequentialConnection {
    sink: SplitSink<Framed<Box<dyn NetworkStream>, KafkaCodec>, KafkaFrame>,
    stream: futures::stream::SplitStream<Framed<Box<dyn NetworkStream>, KafkaCodec>>,
    client_id: Option<String>,
    negotiated: NegotiatedVersions,
}

impl SequentialConnection {
    /// 创建新的顺序连接
    pub fn new(
        framed: Framed<Box<dyn NetworkStream>, KafkaCodec>,
        client_id: Option<String>,
    ) -> Self {
        let (sink, stream) = framed.split();
        SequentialConnection {
            sink,
            stream,
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

        // 发送
        self.sink
            .send(KafkaFrame::new(request_data))
            .await?;

        // 等待响应
        let frame = self
            .stream
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

    /// 获取当前协商的版本
    pub fn negotiated(&self) -> &NegotiatedVersions {
        &self.negotiated
    }

    /// 设置协商版本（在 ApiVersions 后调用）
    pub fn set_negotiated(&mut self, negotiated: NegotiatedVersions) {
        self.negotiated = negotiated;
    }

    /// 转换为 Pipeline 连接（析构 SequentialConnection）
    pub fn into_pipeline(self) -> PipelineConnection {
        PipelineConnection::new(
            self.sink,
            self.stream,
            self.client_id,
            Arc::new(self.negotiated),
        )
    }

    /// 获取底层 sink（用于特殊场景）
    pub fn into_parts(
        self,
    ) -> (
        SplitSink<Framed<Box<dyn NetworkStream>, KafkaCodec>, KafkaFrame>,
        futures::stream::SplitStream<Framed<Box<dyn NetworkStream>, KafkaCodec>>,
    ) {
        (self.sink, self.stream)
    }
}

// ============================================================================
// Pipeline 模式连接（正常操作阶段）
// ============================================================================

/// Pipeline 模式的连接
/// 支持并发请求，响应乱序
pub struct PipelineConnection {
    sink: SplitSink<Framed<Box<dyn NetworkStream>, KafkaCodec>, KafkaFrame>,
    receiver_task: tokio::task::JoinHandle<()>,
    request_tx: mpsc::UnboundedSender<PipelineRequest>,
    negotiated: Arc<NegotiatedVersions>,
    next_correlation_id: std::sync::atomic::AtomicI32,
}

struct PipelineRequest {
    correlation_id: i32,
    #[allow(dead_code)]
    api_key: i16,
    #[allow(dead_code)]
    version: i16,
    #[allow(dead_code)]
    data: Bytes,
    response_tx: oneshot::Sender<Result<Bytes>>,
}

impl PipelineConnection {
    fn new(
        sink: SplitSink<Framed<Box<dyn NetworkStream>, KafkaCodec>, KafkaFrame>,
        stream: futures::stream::SplitStream<Framed<Box<dyn NetworkStream>, KafkaCodec>>,
        client_id: Option<String>,
        negotiated: Arc<NegotiatedVersions>,
    ) -> Self {
        let (request_tx, request_rx) = mpsc::unbounded_channel();

        let receiver_task = tokio::spawn(Self::run_receiver(stream, request_rx, client_id));

        PipelineConnection {
            sink,
            receiver_task,
            request_tx,
            negotiated,
            next_correlation_id: std::sync::atomic::AtomicI32::new(rand::random()),
        }
    }

    async fn run_receiver(
        mut stream: futures::stream::SplitStream<Framed<Box<dyn NetworkStream>, KafkaCodec>>,
        mut request_rx: mpsc::UnboundedReceiver<PipelineRequest>,
        _client_id: Option<String>,
    ) {
        let mut pending: HashMap<i32, oneshot::Sender<Result<Bytes>>> = HashMap::new();

        loop {
            tokio::select! {
                Some(req) = request_rx.recv() => {
                    debug!("Registered pending request: correlation_id={}", req.correlation_id);
                    pending.insert(req.correlation_id, req.response_tx);
                }

                frame_result = stream.next() => {
                    match frame_result {
                        Some(Ok(KafkaFrame { data })) => {
                            if data.len() < 4 {
                                warn!("Response too short");
                                continue;
                            }

                            let correlation_id = i32::from_be_bytes([
                                data[0], data[1], data[2], data[3]
                            ]);

                            debug!("Received response: correlation_id={}", correlation_id);

                            if let Some(tx) = pending.remove(&correlation_id) {
                                let _ = tx.send(Ok(data));
                            } else {
                                warn!("Unknown correlation_id: {}", correlation_id);
                            }
                        }
                        Some(Err(e)) => {
                            error!("Receive error: {}", e);
                            let kind = e.kind();
                            let msg = e.to_string();
                            for (_, tx) in pending.drain() {
                                let _ = tx.send(Err(io::Error::new(kind.clone(), msg.clone()).into()));
                            }
                            break;
                        }
                        None => {
                            debug!("Connection closed");
                            for (_, tx) in pending.drain() {
                                let _ = tx.send(Err(KafkaError::ConnectionClosed));
                            }
                            break;
                        }
                    }
                }
            }
        }
    }

    /// 发送请求并等待响应
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

        // 编码请求
        let request_data = request.encode_frame(
            version,
            correlation_id,
            None, // Pipeline 模式不需要 client_id
        )?;

        // 创建响应通道
        let (tx, rx) = oneshot::channel();

        // 发送请求到后台任务
        self.request_tx
            .send(PipelineRequest {
                correlation_id,
                api_key,
                version,
                data: request_data.clone(),
                response_tx: tx,
            })
            .map_err(|_| KafkaError::ConnectionClosed)?;

        // 发送到网络
        self.sink
            .send(KafkaFrame::new(request_data))
            .await?;

        // 等待响应
        let response_data = rx.await.map_err(|_| KafkaError::ConnectionClosed)??;

        // 解码响应
        let (header, response) = Resp::decode_frame(response_data, version)?;

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

    pub async fn close(self) -> Result<()> {
        self.receiver_task.await.map_err(|e| {
            KafkaError::Io(format!("Task join error: {}", e))
        })?;
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

    pub async fn build(self) -> Result<PipelineConnection> {
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
                let version = api.max_version.min(client_max);
                if version >= api.min_version && version >= client_min {
                    negotiated.set_version(api.api_key, version);
                    debug!("Negotiated API {} -> version {}", api.api_key, version);
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
        let auth_bytes = scram.client_first(&credentials)
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
        let auth_bytes = scram.client_final(&auth_resp.auth_bytes)
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
        scram.verify_server_final(&auth_resp.auth_bytes)
            .map_err(KafkaError::SaslError)?;

        Ok(())
    }
}

// ============================================================================
// Connection 类型别名（用于 lib.rs 导出）
// ============================================================================

/// Connection 类型别名，指向 PipelineConnection
pub type Connection = PipelineConnection;

// ============================================================================
// 使用示例
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore] // 需要运行 Kafka 服务器
    async fn example_usage() -> Result<()> {
        // 方式1: 使用 Builder（推荐）
        let mut conn = Builder::new(
            "localhost:9092".parse().unwrap(),
            SecurityProtocol::Plaintext,
            "my-client".to_string(),
            "1.0.0".to_string(),
        )
        .with_sasl(
            SaslMechanismType::Plain,
            SaslCredentials {
                mechanism: SaslMechanismType::Plain,
                username: "alice".to_string(),
                password: "secret".to_string(),
                authzid: None,
            },
        )
        .build()
        .await?;

        // 现在可以使用 pipeline 模式发送业务请求
        let response: protocol::MetadataResponse = conn
            .send_request(&protocol::MetadataRequest {
                topics: None,
                allow_auto_topic_creation: false,
                ..Default::default()
            })
            .await?;

        conn.close().await?;

        Ok(())
    }
}
