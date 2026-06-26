//! Connection layer — TCP/TLS connection management with Kafka protocol
//!
//! Architecture:
//!
//! ```text
//! ConnectionHandle (Clone, no lock)
//!   │ cmd_tx: mpsc::UnboundedSender
//!   │
//!   └─→ ConnectionReactor (spawned task)
//!          │ framed.send / framed.next
//!          │ pending: HashMap<cid, oneshot::Sender>
//!          │
//!          └─→ TCP
//! ```
//!
//! Multiple [`ConnectionHandle`]s to the same broker share one reactor.
//! Each handle can dispatch requests concurrently — the reactor matches
//! responses to callers via correlation_id, allowing true pipelining.

mod handshake;
mod versions;

pub use handshake::Handshake;
pub use versions::NegotiatedVersions;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;
use tracing::debug;

use crate::error::{KafkaError, Result};
use crate::transport::NetworkStream;
use crate::wire::{KafkaCodec, KafkaFrame};
use kafka_client_protocol::{Request, Response};

// ---------------------------------------------------------------------------
// Commands sent from ConnectionHandle to ConnectionReactor
// ---------------------------------------------------------------------------

enum Command {
    SendRequest {
        data: Bytes,
        correlation_id: i32,
        response_tx: oneshot::Sender<Result<Bytes>>,
    },
    Shutdown {
        done_tx: oneshot::Sender<()>,
    },
}

// ---------------------------------------------------------------------------
// ConnectionHandle — cloneable, no-Mutex handle
// ---------------------------------------------------------------------------

/// A cloneable handle to a Kafka broker connection.
///
/// Internally dispatches requests through an mpsc channel to a background
/// [`ConnectionReactor`] that manages the actual TCP socket.
/// Multiple handles can send requests concurrently — the reactor correlates
/// responses via correlation IDs, enabling true request pipelining.
#[derive(Clone)]
pub struct ConnectionHandle {
    cmd_tx: mpsc::UnboundedSender<Command>,
    negotiated: Arc<NegotiatedVersions>,
    next_correlation_id: Arc<AtomicI32>,
    client_id: Option<String>,
}

impl ConnectionHandle {
    fn new(
        cmd_tx: mpsc::UnboundedSender<Command>,
        negotiated: Arc<NegotiatedVersions>,
        client_id: Option<String>,
    ) -> Self {
        Self {
            cmd_tx,
            negotiated,
            next_correlation_id: Arc::new(AtomicI32::new(rand::random())),
            client_id,
        }
    }

    /// Send a request and wait for the response.
    ///
    /// The request is serialised, sent to the reactor, and the caller awaits
    /// the response via a oneshot channel. Multiple concurrent callers sharing
    /// the same handle are supported.
    pub async fn send_request<Req, Resp>(&self, request: &Req) -> Result<Resp>
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

        let encoded = request.encode_frame(version, correlation_id, self.client_id.clone())?;

        let (response_tx, response_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::SendRequest {
                data: encoded,
                correlation_id,
                response_tx,
            })
            .map_err(|_| KafkaError::ConnectionClosed)?;

        let response_data = response_rx
            .await
            .map_err(|_| KafkaError::ConnectionClosed)??;

        let (header, response) = Resp::decode_frame(response_data, version)?;
        if header.correlation_id() != correlation_id {
            return Err(KafkaError::CorrelationIdMismatch {
                expected: correlation_id,
                actual: header.correlation_id(),
            });
        }

        Ok(response)
    }

    /// Access the negotiated API versions for this connection.
    pub fn negotiated(&self) -> &NegotiatedVersions {
        &self.negotiated
    }

    /// Gracefully close the connection.
    ///
    /// Sends a shutdown signal to the reactor and waits for it to complete.
    pub async fn close(self) {
        let (done_tx, done_rx) = oneshot::channel();
        if self.cmd_tx.send(Command::Shutdown { done_tx }).is_ok() {
            let _ = done_rx.await;
        }
    }
}

// ---------------------------------------------------------------------------
// ConnectionReactor — background task driving the TCP socket
// ---------------------------------------------------------------------------

struct ConnectionReactor {
    framed: Framed<Box<dyn NetworkStream>, KafkaCodec>,
    cmd_rx: mpsc::UnboundedReceiver<Command>,
    pending: HashMap<i32, oneshot::Sender<Result<Bytes>>>,
}

impl ConnectionReactor {
    fn new(
        framed: Framed<Box<dyn NetworkStream>, KafkaCodec>,
        cmd_rx: mpsc::UnboundedReceiver<Command>,
    ) -> Self {
        Self {
            framed,
            cmd_rx,
            pending: HashMap::new(),
        }
    }

    /// Run the reactor loop until shutdown or connection error.
    async fn run(&mut self) {
        loop {
            tokio::select! {
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(Command::SendRequest { data, correlation_id, response_tx }) => {
                            match self.framed.send(KafkaFrame::new(data)).await {
                                Ok(()) => {
                            if let Err(e) = self.framed.flush().await {
                                self.fail_pending(KafkaError::Io(e.to_string()));
                                return;
                            }
                                    self.pending.insert(correlation_id, response_tx);
                                }
                                Err(e) => {
                                    let _ = response_tx.send(Err(KafkaError::Io(e.to_string())));
                                    self.fail_pending(KafkaError::ConnectionClosed);
                                    return;
                                }
                            }
                        }
                        Some(Command::Shutdown { done_tx }) => {
                            debug!("ConnectionReactor received shutdown signal");
                            self.fail_pending(KafkaError::ConnectionClosed);
                            let _ = done_tx.send(());
                            return;
                        }
                        None => {
                            // All senders dropped — exit
                            self.fail_pending(KafkaError::ConnectionClosed);
                            return;
                        }
                    }
                }
                frame = self.framed.next() => {
                    match frame {
                        Some(Ok(KafkaFrame { data })) => {
                            let corr_id = extract_correlation_id(&data);
                            if let Some(tx) = self.pending.remove(&corr_id) {
                                let _ = tx.send(Ok(data));
                            } else {
                                debug!(
                                    "Dropping unmatched response corr_id={}, pending_count={}, data_len={}",
                                    corr_id,
                                    self.pending.len(),
                                    data.len(),
                                );
                            }
                        }
                        Some(Err(e)) => {
                            self.fail_pending(KafkaError::Io(e.to_string()));
                            return;
                        }
                        None => {
                            self.fail_pending(KafkaError::ConnectionClosed);
                            return;
                        }
                    }
                }
            }
        }
    }

    fn fail_pending(&mut self, err: KafkaError) {
        for (_, tx) in self.pending.drain() {
            let _ = tx.send(Err(err.clone()));
        }
    }
}

/// Spawn a reactor and return a handle to it.
fn spawn_reactor(
    framed: Framed<Box<dyn NetworkStream>, KafkaCodec>,
    negotiated: Arc<NegotiatedVersions>,
    client_id: Option<String>,
) -> ConnectionHandle {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let mut reactor = ConnectionReactor::new(framed, cmd_rx);
    tokio::spawn(async move {
        reactor.run().await;
    });
    ConnectionHandle::new(cmd_tx, negotiated, client_id)
}

// ---------------------------------------------------------------------------
// SequentialConnection — used during handshake (ApiVersions, SASL)
// ---------------------------------------------------------------------------

/// Sequential connection for the initialisation phase.
///
/// One request → one response. Used only during connection establishment
/// (version negotiation, SASL authentication). Not shared between tasks.
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

    /// Convert to a pipelining handle backed by a reactor task.
    pub fn into_pipeline(self) -> ConnectionHandle {
        spawn_reactor(self.framed, Arc::new(self.negotiated), self.client_id)
    }
}

// ---------------------------------------------------------------------------
// Builder — constructs a ConnectionHandle for a broker
// ---------------------------------------------------------------------------

/// Connection builder.
///
/// Handles the full connection lifecycle:
/// 1. Establish TCP/TLS socket
/// 2. Perform version negotiation (ApiVersions)
/// 3. SASL authentication (if configured)
/// 4. Return a [`ConnectionHandle`] backed by a reactor task.
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

    /// Build the connection: TCP/TLS → handshake → SASL → reactor handle.
    pub async fn build(self) -> Result<ConnectionHandle> {
        use crate::transport::TransportConnector;

        // 1. Establish underlying connection
        let stream = TransportConnector::connect(self.addr, &self.security_protocol).await?;
        let framed = Framed::new(stream, KafkaCodec::new());

        // 2. Sequential handshake
        let mut seq_conn = SequentialConnection::new(framed, self.client_id);

        let negotiated =
            Handshake::perform(&mut seq_conn, self.client_name, self.client_version).await?;
        seq_conn.set_negotiated(negotiated);

        // 3. SASL authentication
        if let Some((mechanism, credentials)) = self.sasl_config {
            Handshake::sasl_authenticate(&mut seq_conn, mechanism, credentials).await?;
        }

        // 4. Convert to pipelining handle
        Ok(seq_conn.into_pipeline())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract the correlation_id from a Kafka response frame.
///
/// The correlation_id is the first 4 bytes of the response data
/// (the frame length prefix has already been stripped by KafkaCodec).
fn extract_correlation_id(data: &Bytes) -> i32 {
    use bytes::Buf;
    (&data[..]).get_i32()
}
