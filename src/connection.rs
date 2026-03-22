use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use bytes::{Bytes, BytesMut, Buf, BufMut};
use tokio_util::codec::Framed;
use futures::{SinkExt, StreamExt};
use tracing::debug;

use crate::transport::*;
use crate::protocol::*;
use crate::sasl::*;
use crate::error::{Result, KafkaError};

/// 协商的 API 版本信息
#[derive(Debug, Clone)]
pub struct NegotiatedVersions {
    versions: HashMap<i16, i16>,
    client_id: String,
}

impl NegotiatedVersions {
    pub fn new(client_id: String) -> Self {
        Self { versions: HashMap::new(), client_id }
    }

    pub fn set_version(&mut self, api_key: i16, version: i16) {
        self.versions.insert(api_key, version);
    }

    pub fn get_version(&self, api_key: i16) -> Option<i16> {
        self.versions.get(&api_key).copied()
    }
}

/// Kafka 连接
pub struct KafkaConnection {
    /// 业务阶段的 Framed
    framed: Framed<Box<dyn NetworkStream>, KafkaCodec>,
    /// 协商的版本信息
    negotiated: Arc<NegotiatedVersions>,
    /// 待处理的请求
    pending: HashMap<i32, (i16, i16)>,
    /// 下一个 correlation_id
    correlation_id: i32,
    /// 是否已认证
    authenticated: bool,
}

impl KafkaConnection {
    /// 建立连接（自动完成握手和 codec 切换）
    pub async fn connect(
        addr: SocketAddr,
        protocol: SecurityProtocol,
        client_id: String,
    ) -> Result<Self> {
        // 1. 创建网络流
        let stream = TransportConnector::connect(addr, &protocol).await?;

        // 2. 使用握手 Codec
        let mut handshake_framed = Framed::new(stream, ApiVersionsCodec::new());

        // 3. 执行版本协商
        let negotiated = Self::perform_handshake(&mut handshake_framed, client_id).await?;

        // 4. 切换到业务 Codec
        let parts = handshake_framed.into_parts();
        let framed = Framed::new(parts.io, KafkaCodec::new());

        Ok(Self {
            framed,
            negotiated: Arc::new(negotiated),
            pending: HashMap::new(),
            correlation_id: 0,
            authenticated: false,
        })
    }

    /// 执行握手
    async fn perform_handshake(
        framed: &mut Framed<Box<dyn NetworkStream>, ApiVersionsCodec>,
        client_id: String,
    ) -> Result<NegotiatedVersions> {
        // 1. 构建 ApiVersionsRequest (v0)
        let correlation_id = rand::random();
        let request_data = Self::encode_api_versions_request(correlation_id, &client_id);

        // 2. 发送请求
        framed.send(RawFrame {
            size: request_data.len() as i32,
            data: request_data,
        }).await.map_err(KafkaError::Io)?;

        // 3. 接收响应
        let raw_frame = framed.next().await
            .ok_or_else(|| KafkaError::Protocol("No ApiVersions response".to_string()))?
            .map_err(KafkaError::Io)?;

        // 4. 解析响应
        let response = Self::parse_api_versions_response(&raw_frame.data)?;

        if response.error_code != 0 {
            return Err(KafkaError::Protocol(format!(
                "ApiVersions failed: error {}", response.error_code
            )));
        }

        // 5. 计算协商版本
        let mut negotiated = NegotiatedVersions::new(client_id);
        for api in response.api_versions {
            let client_max = Self::client_max_version(api.api_key);
            let version = client_max.min(api.max_version);
            if version >= api.min_version {
                negotiated.set_version(api.api_key, version);
                debug!("Negotiated API {} -> version {}", api.api_key, version);
            }
        }

        Ok(negotiated)
    }

    /// 客户端支持的最大版本
    fn client_max_version(api_key: i16) -> i16 {
        match api_key {
            0 => 12,   // Produce
            1 => 16,   // Fetch
            3 => 12,   // Metadata
            17 => 1,   // SaslHandshake
            36 => 2,   // SaslAuthenticate
            18 => 4,   // ApiVersions
            _ => 0,
        }
    }

    /// 编码 ApiVersions 请求
    fn encode_api_versions_request(correlation_id: i32, client_id: &str) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(18); // api_key
        buf.put_i16(0);  // api_version
        buf.put_i32(correlation_id);
        if client_id.is_empty() {
            buf.put_i16(-1);
        } else {
            buf.put_i16(client_id.len() as i16);
            buf.put_slice(client_id.as_bytes());
        }
        buf.freeze()
    }

    /// 解析 ApiVersions 响应
    fn parse_api_versions_response(data: &Bytes) -> Result<api::ApiVersionsResponse> {
        let mut buf = data.clone();

        // Skip correlation_id
        if buf.remaining() < 4 {
            return Err(KafkaError::ProtocolDecodeError("Missing correlation_id".to_string()));
        }
        buf.get_i32();

        api::ApiVersionsResponse::decode(&mut buf, 0)
    }

    /// 发送请求
    pub async fn send_request<Req, Resp>(
        &mut self,
        api_key: i16,
        request: &Req,
    ) -> Result<Resp>
    where
        Req: VersionedKafkaEncode,
        Resp: VersionedKafkaDecode,
    {
        let version = self.negotiated.get_version(api_key)
            .ok_or_else(|| KafkaError::UnsupportedApi(api_key))?;

        let correlation_id = self.next_correlation_id();

        // 编码请求
        let request_data = self.encode_request(api_key, version, correlation_id, request)?;

        // 记录待处理请求
        self.pending.insert(correlation_id, (api_key, version));

        // 发送
        self.framed.send(KafkaFrame {
            correlation_id,
            data: request_data,
        }).await.map_err(KafkaError::Io)?;

        // 接收响应
        let response_frame = self.framed.next().await
            .ok_or_else(|| KafkaError::Protocol("No response".to_string()))?
            .map_err(KafkaError::Io)?;

        // 解码响应
        self.decode_response::<Resp>(response_frame)
    }

    fn next_correlation_id(&mut self) -> i32 {
        self.correlation_id = self.correlation_id.wrapping_add(1);
        self.correlation_id
    }

    fn encode_request<Req: VersionedKafkaEncode>(
        &self,
        api_key: i16,
        version: i16,
        correlation_id: i32,
        request: &Req,
    ) -> Result<Bytes> {
        let use_flexible = version >= 9;
        let mut buf = BytesMut::new();

        if use_flexible {
            // RequestHeader v2
            buf.put_i16(api_key);
            buf.put_i16(version);
            buf.put_i32(correlation_id);
            let client_id = if self.negotiated.client_id.is_empty() {
                None
            } else {
                Some(&self.negotiated.client_id)
            };
            encode_compact_nullable_string(&mut buf, &client_id);
            encode_unsigned_varint(&mut buf, 0); // tagged_fields
        } else {
            // RequestHeader v1
            buf.put_i16(api_key);
            buf.put_i16(version);
            buf.put_i32(correlation_id);
            if self.negotiated.client_id.is_empty() {
                buf.put_i16(-1);
            } else {
                buf.put_i16(self.negotiated.client_id.len() as i16);
                buf.put_slice(self.negotiated.client_id.as_bytes());
            }
        }

        request.encode(&mut buf, version);
        Ok(buf.freeze())
    }

    fn decode_response<Resp: VersionedKafkaDecode>(
        &mut self,
        frame: KafkaFrame,
    ) -> Result<Resp> {
        let (_api_key, version) = self.pending.remove(&frame.correlation_id)
            .ok_or_else(|| KafkaError::Protocol(
                format!("Unknown correlation_id: {}", frame.correlation_id)
            ))?;

        let mut buf = frame.data;
        let _ = buf.get_i32(); // 跳过 correlation_id

        if version >= 9 && buf.has_remaining() {
            skip_tagged_fields(&mut buf).map_err(KafkaError::from)?;
        }

        Resp::decode(&mut buf, version).map_err(KafkaError::from)
    }

    /// SASL 认证
    pub async fn authenticate(&mut self, credentials: &SaslCredentials) -> Result<()> {
        // 1. SaslHandshake
        let handshake_request = api::SaslHandshakeRequest::new(credentials.mechanism.as_str());
        let handshake_response: api::SaslHandshakeResponse =
            self.send_request(17, &handshake_request).await?;

        if handshake_response.error_code != 0 {
            return Err(KafkaError::SaslHandshakeFailed(
                format!("Error code: {}", handshake_response.error_code)
            ));
        }

        // 2. 创建机制实例
        let mut mechanism = create_mechanism(credentials.mechanism);

        // 3. 发送初始响应（如果是 client-first 机制）
        let mut auth_data = mechanism.initial_response(credentials).await
            .map_err(KafkaError::SaslError)?;

        // 4. 循环 SaslAuthenticate 直到完成
        loop {
            let auth_request = api::SaslAuthenticateRequest::new(
                auth_data.unwrap_or_else(Bytes::new)
            );
            let auth_response: api::SaslAuthenticateResponse =
                self.send_request(36, &auth_request).await?;

            if auth_response.error_code != 0 {
                return Err(KafkaError::AuthenticationFailed(
                    auth_response.error_message.unwrap_or_else(||
                        format!("Error code: {}", auth_response.error_code))
                ));
            }

            if mechanism.is_complete() {
                break;
            }

            // 处理服务器挑战
            auth_data = mechanism.challenge(&auth_response.auth_bytes).await
                .map_err(KafkaError::SaslError)?;

            if mechanism.is_complete() {
                break;
            }
        }

        if !mechanism.is_success() {
            return Err(KafkaError::AuthenticationFailed("SASL authentication failed".to_string()));
        }

        self.authenticated = true;
        Ok(())
    }

    /// 关闭连接
    pub async fn close(mut self) -> Result<()> {
        self.framed.close().await.map_err(KafkaError::Io)?;
        Ok(())
    }

    /// 获取协商的版本信息
    pub fn negotiated_versions(&self) -> &NegotiatedVersions {
        &self.negotiated
    }

    /// 检查是否已认证
    pub fn is_authenticated(&self) -> bool {
        self.authenticated
    }
}
