//! Connection layer - TCP/TLS connection management with Kafka protocol

mod handshake;
mod versions;

pub use handshake::Handshake;
pub use versions::NegotiatedVersions;

use std::sync::Arc;
use std::time::Duration;
use std::sync::atomic::{AtomicI32, Ordering};

use futures::{SinkExt, StreamExt};
use tokio_util::codec::Framed;
use tracing::debug;

use crate::wire::{KafkaCodec, KafkaFrame};
use crate::error::{KafkaError, Result};
use crate::transport::NetworkStream;
use kafka_client_protocol::{Request, Response};

/// Pipeline mode connection for normal operations
///
/// Uses sequential request-response pattern internally.
/// Thread-safe via external `Arc<Mutex<Connection>>`.
pub struct Connection {
    framed: Framed<Box<dyn NetworkStream>, KafkaCodec>,
    negotiated: Arc<NegotiatedVersions>,
    next_correlation_id: AtomicI32,
}

impl Connection {
    pub(crate) fn new(
        framed: Framed<Box<dyn NetworkStream>, KafkaCodec>,
        negotiated: Arc<NegotiatedVersions>,
    ) -> Self {
        Connection {
            framed,
            negotiated,
            next_correlation_id: AtomicI32::new(rand::random()),
        }
    }

    /// Send request and wait for response (sequential mode)
    ///
    /// Default timeout: 30 seconds. Returns `KafkaError::RequestTimeout` on timeout.
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

        let correlation_id = self.next_correlation_id.fetch_add(1, Ordering::SeqCst);

        debug!(
            api_key = api_key,
            version = version,
            correlation_id = correlation_id,
            "sending request"
        );

        // Encode request
        let request_data =
            request.encode_frame(version, correlation_id, Some("kafka-client".to_string()))?;

        // Send to network
        self.framed.send(KafkaFrame::new(request_data)).await?;
        self.framed.flush().await?;

        // Wait for response with 30s timeout
        const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
        let frame = tokio::time::timeout(REQUEST_TIMEOUT, self.framed.next())
            .await
            .map_err(|_| {
                debug!(api_key, "request timed out after 30s");
                KafkaError::RequestTimeout
            })?
            .ok_or(KafkaError::ConnectionClosed)??;

        debug!(response_len = frame.data.len(), "received response");

        // Decode response
        let (header, response) = Resp::decode_frame(frame.data, version)?;

        // Verify correlation_id
        if header.correlation_id() != correlation_id {
            return Err(KafkaError::CorrelationIdMismatch {
                expected: correlation_id,
                actual: header.correlation_id(),
            });
        }

        Ok(response)
    }

    /// Send request at explicit version (for debugging/testing)
    pub async fn send_request_at<Req, Resp>(&mut self, request: &Req, version: i16) -> Result<Resp>
    where
        Req: Request,
        Resp: Response,
    {
        let api_key = request.api_key();
        let correlation_id = self.next_correlation_id.fetch_add(1, Ordering::SeqCst);

        debug!(
            api_key = api_key,
            version = version,
            "sending request at explicit version"
        );

        let request_data =
            request.encode_frame(version, correlation_id, Some("kafka-client".to_string()))?;

        self.framed.send(KafkaFrame::new(request_data)).await?;
        self.framed.flush().await?;

        const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
        let frame = tokio::time::timeout(REQUEST_TIMEOUT, self.framed.next())
            .await
            .map_err(|_| {
                debug!(api_key, version, "request timed out after 30s");
                KafkaError::RequestTimeout
            })?
            .ok_or(KafkaError::ConnectionClosed)??;

        let (header, response) = Resp::decode_frame(frame.data, version)?;

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
        if let Err(e) = self.framed.close().await {
            debug!("Error closing framed connection: {}", e);
        }
        Ok(())
    }
}

/// Connection builder - manages connection establishment flow
pub struct Builder {
    addr: std::net::SocketAddr,
    security_protocol: crate::transport::SecurityProtocol,
    client_name: String,
    client_version: String,
    client_id: Option<String>,
    sasl_config: Option<(crate::sasl::SaslMechanismType, crate::sasl::SaslCredentials)>,
}

impl Builder {
    pub fn new(
        addr: std::net::SocketAddr,
        security_protocol: crate::transport::SecurityProtocol,
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

    pub fn with_sasl(
        mut self,
        mechanism: crate::sasl::SaslMechanismType,
        credentials: crate::sasl::SaslCredentials,
    ) -> Self {
        self.sasl_config = Some((mechanism, credentials));
        self
    }

    pub fn with_client_id(mut self, client_id: String) -> Self {
        self.client_id = Some(client_id);
        self
    }

    pub async fn build(self) -> Result<Connection> {
        use crate::transport::TransportConnector;

        // 1. Establish underlying connection
        let stream = TransportConnector::connect(self.addr, &self.security_protocol).await?;
        let framed = Framed::new(stream, crate::wire::KafkaCodec::new());

        // 2. Create sequential connection for handshake
        let mut seq_conn = SequentialConnection::new(framed, self.client_id);

        // 3. Version negotiation
        let negotiated =
            Handshake::perform(&mut seq_conn, self.client_name, self.client_version).await?;
        seq_conn.set_negotiated(negotiated);

        // 4. SASL authentication (if needed)
        if let Some((mechanism, credentials)) = self.sasl_config {
            Handshake::sasl_authenticate(&mut seq_conn, mechanism, credentials).await?;
        }

        // 5. Convert to Pipeline connection
        Ok(seq_conn.into_pipeline())
    }
}

/// Sequential connection for initialization phase (ApiVersions, SASL)
///
/// One request → one response pattern.
pub struct SequentialConnection {
    framed: Framed<Box<dyn NetworkStream>, KafkaCodec>,
    client_id: Option<String>,
    negotiated: NegotiatedVersions,
}

impl SequentialConnection {
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

    pub async fn send_request<Req, Resp>(&mut self, request: &Req) -> Result<Resp>
    where
        Req: Request,
        Resp: Response,
    {
        let api_key = request.api_key();
        let version = self.negotiated.get_version(api_key).unwrap_or(0);

        let correlation_id = rand::random();

        let request_data = request.encode_frame(version, correlation_id, self.client_id.clone())?;

        debug!(
            api_key = api_key,
            version = version,
            correlation_id = correlation_id,
            "sending sequential request"
        );

        self.framed.send(KafkaFrame::new(request_data)).await?;
        self.framed.flush().await?;

        let frame = self
            .framed
            .next()
            .await
            .ok_or(KafkaError::ConnectionClosed)??;

        debug!(
            response_len = frame.data.len(),
            "received sequential response"
        );

        let (header, response) = Resp::decode_frame(frame.data, version)?;

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

    pub fn set_negotiated(&mut self, negotiated: NegotiatedVersions) {
        self.negotiated = negotiated;
    }

    pub fn into_pipeline(self) -> Connection {
        Connection::new(self.framed, Arc::new(self.negotiated))
    }

    pub fn into_parts(self) -> Framed<Box<dyn NetworkStream>, KafkaCodec> {
        self.framed
    }
}