// kafka-client/src/connection.rs

use bytes::Bytes;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc as tokio_mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::codec::Framed;
use tracing::{debug, error, warn};

use crate::codec::{KafkaCodec, KafkaFrame};
use crate::error::{KafkaError, Result};
use crate::transport::*;
use kafka_client_protocol::{self as protocol, Message, Request, Response, ResponseHeader};

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

// ============================================================================
// 内部类型
// ============================================================================

struct PendingRequest {
    version: i16,
    api_key: i16,
    response_tx: oneshot::Sender<Result<Bytes>>,
}

enum Command {
    SendRequest {
        correlation_id: i32,
        api_key: i16,
        version: i16,
        data: Bytes,
        response_tx: oneshot::Sender<Result<Bytes>>,
    },
    Shutdown,
}

// ============================================================================
// Connection
// ============================================================================

pub struct Connection {
    sink: SplitSink<Framed<Box<dyn NetworkStream>, KafkaCodec>, KafkaFrame>,
    receiver_task: JoinHandle<()>,
    cmd_tx: tokio_mpsc::UnboundedSender<Command>,
    negotiated: Arc<NegotiatedVersions>,
    correlation_id: std::sync::atomic::AtomicI32,
}

impl Connection {
    pub async fn connect(
        addr: SocketAddr,
        protocol: SecurityProtocol,
        client_name: String,
        client_version: String,
    ) -> Result<Self> {
        let stream = TransportConnector::connect(addr, &protocol).await?;
        let mut framed = Framed::new(stream, KafkaCodec::new());

        let negotiated = Self::handshake(&mut framed, client_name, client_version).await?;

        // TODO: SASL 认证

        let (sink, stream) = framed.split();
        let (cmd_tx, cmd_rx) = tokio_mpsc::unbounded_channel();

        let receiver_task = tokio::spawn(Self::receive_loop(stream, cmd_rx));

        Ok(Self {
            sink,
            receiver_task,
            cmd_tx,
            negotiated,
            correlation_id: std::sync::atomic::AtomicI32::new(rand::random()),
        })
    }

    async fn handshake(
        framed: &mut Framed<Box<dyn NetworkStream>, KafkaCodec>,
        client_name: String,
        client_version: String,
    ) -> Result<Arc<NegotiatedVersions>> {
        let correlation_id = rand::random();

        let request = protocol::ApiVersionsRequest {
            client_software_name: Some(client_name),
            client_software_version: Some(client_version),
        };

        let request_data =
            request.encode_frame(0, correlation_id, Some("kafka-client".to_string()))?;

        framed
            .send(KafkaFrame::new(request_data))
            .await
            .map_err(KafkaError::Io)?;

        let frame = framed
            .next()
            .await
            .ok_or_else(|| KafkaError::Protocol("No ApiVersions response".to_string()))?
            .map_err(KafkaError::Io)?;

        let (header, response) = protocol::ApiVersionsResponse::decode_frame(frame.data, 0)?;

        if header.correlation_id() != correlation_id {
            return Err(KafkaError::CorrelationIdMismatch {
                expected: correlation_id,
                actual: header.correlation_id(),
            });
        }

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

        Ok(Arc::new(negotiated))
    }

    async fn receive_loop(
        mut stream: SplitStream<Framed<Box<dyn NetworkStream>, KafkaCodec>>,
        mut cmd_rx: tokio_mpsc::UnboundedReceiver<Command>,
    ) {
        let mut pending: HashMap<i32, PendingRequest> = HashMap::new();

        loop {
            tokio::select! {
                Some(cmd) = cmd_rx.recv() => {
                    match cmd {
                        Command::SendRequest { correlation_id, api_key, version, data, response_tx } => {
                            debug!("Registered pending request: correlation_id={}", correlation_id);
                            pending.insert(correlation_id, PendingRequest {
                                version,
                                api_key,
                                response_tx,
                            });
                        }
                        Command::Shutdown => {
                            debug!("Shutting down receiver loop");
                            break;
                        }
                    }
                }

                frame_result = stream.next() => {
                    match frame_result {
                        Some(Ok(frame)) => {
                            if frame.data.len() < 4 {
                                warn!("Response too short: {} bytes", frame.data.len());
                                continue;
                            }

                            let correlation_id = i32::from_be_bytes([
                                frame.data[0], frame.data[1], frame.data[2], frame.data[3]
                            ]);

                            debug!("Received response: correlation_id={}", correlation_id);

                            if let Some(req) = pending.remove(&correlation_id) {
                                let _ = req.response_tx.send(Ok(frame.data));
                            } else {
                                warn!("Received response for unknown correlation_id: {}", correlation_id);
                            }
                        }
                        Some(Err(e)) => {
                            error!("Error reading response: {}", e);
                            let kind = e.kind();
                            let msg = e.to_string();
                            for (_, req) in pending.drain() {
                                let _ = req.response_tx.send(Err(KafkaError::Io(io::Error::new(kind, msg.clone()))));
                            }
                            break;
                        }
                        None => {
                            debug!("Connection closed");
                            for (_, req) in pending.drain() {
                                let _ = req.response_tx.send(Err(KafkaError::ConnectionClosed));
                            }
                            break;
                        }
                    }
                }
            }
        }
    }

    fn next_correlation_id(&self) -> i32 {
        self.correlation_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
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
            .ok_or_else(|| KafkaError::UnsupportedApi(api_key))?;

        let correlation_id = self.next_correlation_id();

        // 1. 编码请求
        let request_data =
            request.encode_frame(version, correlation_id, Some("kafka-client".to_string()))?;

        // 2. 创建 oneshot 通道
        let (tx, rx) = oneshot::channel();

        // 3. 注册 pending
        self.cmd_tx
            .send(Command::SendRequest {
                correlation_id,
                api_key,
                version,
                data: request_data.clone(),
                response_tx: tx,
            })
            .map_err(|_| KafkaError::ConnectionClosed)?;

        // 4. 发送到网络
        self.sink
            .send(KafkaFrame::new(request_data))
            .await
            .map_err(KafkaError::Io)?;

        // 5. 等待响应
        let response_data = rx.await.map_err(|_| KafkaError::ConnectionClosed)??;

        // 6. 解码响应
        let (header, response) = Resp::decode_frame(response_data, version)?;

        // 7. 验证 correlation_id（双重验证）
        if header.correlation_id() != correlation_id {
            return Err(KafkaError::CorrelationIdMismatch {
                expected: correlation_id,
                actual: header.correlation_id(),
            });
        }

        Ok(response)
    }

    pub fn negotiated_versions(&self) -> &NegotiatedVersions {
        &self.negotiated
    }

    pub async fn close(mut self) -> Result<()> {
        let _ = self.cmd_tx.send(Command::Shutdown);
        self.receiver_task.await.map_err(|err| {
            KafkaError::Io(io::Error::new(io::ErrorKind::BrokenPipe, err.to_string()))
        })?;
        Ok(())
    }
}
