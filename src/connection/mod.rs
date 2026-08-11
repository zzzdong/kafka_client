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
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;
use tracing::{debug, warn};

use crate::error::{KafkaError, Result};
use crate::wire::{KafkaCodec, KafkaFramed};
use kafka_client_protocol::{self as protocol, Request, Response};

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
/// `ConnectionReactor` that manages the actual TCP socket.
/// Multiple handles can send requests concurrently — the reactor correlates
/// responses via correlation IDs, enabling true request pipelining.
#[derive(Clone)]
pub struct ConnectionHandle {
    cmd_tx: mpsc::UnboundedSender<Command>,
    negotiated: Arc<NegotiatedVersions>,
    next_correlation_id: Arc<AtomicI32>,
    client_id: Option<String>,
    request_timeout: Duration,
}

impl ConnectionHandle {
    fn new(
        cmd_tx: mpsc::UnboundedSender<Command>,
        negotiated: Arc<NegotiatedVersions>,
        client_id: Option<String>,
        request_timeout: Duration,
    ) -> Self {
        Self {
            cmd_tx,
            negotiated,
            next_correlation_id: Arc::new(AtomicI32::new(rand::random())),
            client_id,
            request_timeout,
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

        let response_data = tokio::time::timeout(self.request_timeout, response_rx)
            .await
            .map_err(|_| KafkaError::RequestTimeout)?
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

    /// Send a pre-encoded request frame and return the raw response bytes.
    ///
    /// **Experimental.** This API is new in 0.8.0 and may change in a future
    /// minor release as raw-frame use cases become clearer.
    ///
    /// Bypasses the [`Request`]/[`Response`] traits entirely: `data` is written
    /// verbatim (the codec only adds the length prefix) and the response is
    /// returned undecoded, with its length prefix already stripped.
    ///
    /// This method routes `data` through the shared connection reactor, so it
    /// multiplexes alongside ordinary [`send_request`](Self::send_request) calls
    /// and is subject to the same request timeout. It is the right choice when
    /// you already hold a [`ConnectionHandle`] and only occasionally need to
    /// forward a hand-built frame.
    ///
    /// `data` must contain a complete request header + body with **no** 4-byte
    /// length prefix; the codec prepends that. To build such bytes correctly
    /// (header v1/v2 layout, `client_id` encoding, tagged-fields varint,
    /// response asymmetry), prefer
    /// [`kafka_client_protocol::Request::encode_frame`] over hand-rolling. The
    /// correlation ID at bytes `[4..8]` must be unique
    /// among all in-flight requests on this handle — a collision with an ID
    /// generated by [`send_request`](Self::send_request) silently displaces the
    /// earlier waiter, which then fails with
    /// [`KafkaError::RequestTimeout`](KafkaError#variant.RequestTimeout).
    ///
    /// If you are relaying frames from a downstream client whose correlation
    /// IDs you do not control, prefer
    /// [`Builder::build_framed`] — the returned [`KafkaFramed`] has no reactor
    /// and therefore no shared ID space.
    ///
    /// # Errors
    ///
    /// - [`KafkaError::Protocol`] — `data` is shorter than the 8 bytes needed
    ///   to contain a correlation ID
    /// - [`KafkaError::ConnectionClosed`](KafkaError#variant.ConnectionClosed) —
    ///   the reactor is gone
    /// - [`KafkaError::RequestTimeout`](KafkaError#variant.RequestTimeout) —
    ///   no response within the configured request timeout
    pub async fn send_raw_frame(&self, data: Bytes) -> Result<Bytes> {
        let correlation_id = request_correlation_id(&data).ok_or_else(|| {
            KafkaError::Protocol(format!(
                "raw frame too short ({} bytes; need >= 8 for \
                 api_key + api_version + correlation_id)",
                data.len()
            ))
        })?;

        debug!(
            correlation_id = correlation_id,
            len = data.len(),
            "sending raw frame"
        );

        let (response_tx, response_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::SendRequest {
                data,
                correlation_id,
                response_tx,
            })
            .map_err(|_| KafkaError::ConnectionClosed)?;

        tokio::time::timeout(self.request_timeout, response_rx)
            .await
            .map_err(|_| KafkaError::RequestTimeout)?
            .map_err(|_| KafkaError::ConnectionClosed)?
    }

    /// Send a request built from a **structured header + raw body** and return
    /// the structured response header + raw response body.
    ///
    /// **Experimental.** This API is new in 0.8.0 and may change in a future
    /// minor release as structured raw-frame use cases become clearer.
    ///
    /// This sits between the fully-typed [`send_request`](Self::send_request)
    /// and the fully-raw [`send_raw_frame`](Self::send_raw_frame):
    ///
    /// - the request **header** is a structured [`protocol::RequestHeader`] — the
    ///   caller controls `api_key`, `api_version`, `correlation_id`, `client_id`
    ///   and (for the flexible `V2` variant) tagged fields as real fields, not
    ///   byte offsets;
    /// - the request **body** is a caller-provided [`Bytes`] slice that is
    ///   **appended verbatim** — it is never re-serialised, so it can carry
    ///   payload bytes produced elsewhere (e.g. a frame relayed from a
    ///   downstream client);
    /// - the response is returned as a structured
    ///   [`protocol::ResponseHeader`] (from which the correlation ID and any
    ///   tagged fields can be read) plus the undecoded response body bytes.
    ///
    /// Correlation ID handling is the reactor's job: the ID is read out of the
    /// structured header and used to route the matching response back through
    /// the reactor's `pending` map, exactly like
    /// [`send_request`](Self::send_request). The caller is free to choose the ID
    /// (it must be unique among in-flight requests on this handle) and to inspect
    /// the returned [`protocol::ResponseHeader`] — this method does not impose an
    /// additional equality check, since the reactor has already matched the
    /// frame by that ID.
    ///
    /// The header variant determines response framing: a `V2` (flexible) header
    /// expects a flexible response header with tagged fields; `V1` expects a
    /// traditional one.
    ///
    /// # Errors
    ///
    /// - [`KafkaError::ConnectionClosed`](KafkaError#variant.ConnectionClosed) —
    ///   the reactor is gone
    /// - [`KafkaError::RequestTimeout`](KafkaError#variant.RequestTimeout) — no
    ///   response within the configured request timeout
    pub async fn send_request_frame(
        &self,
        header: protocol::RequestHeader,
        body: Bytes,
    ) -> Result<(protocol::ResponseHeader, Bytes)> {
        let correlation_id = header.correlation_id();
        let use_flexible = matches!(&header, protocol::RequestHeader::V2(_));

        debug!(
            api_key = header.api_key(),
            version = header.api_version(),
            correlation_id = correlation_id,
            body_len = body.len(),
            "sending request frame (header + body)"
        );

        // Encode the structured header, then append the caller's pre-encoded
        // body verbatim — the body is never re-serialised.
        let mut data = BytesMut::with_capacity(64 + body.len());
        header.encode(&mut data);
        data.put_slice(&body);

        let (response_tx, response_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::SendRequest {
                data: data.freeze(),
                correlation_id,
                response_tx,
            })
            .map_err(|_| KafkaError::ConnectionClosed)?;

        let response_data = tokio::time::timeout(self.request_timeout, response_rx)
            .await
            .map_err(|_| KafkaError::RequestTimeout)?
            .map_err(|_| KafkaError::ConnectionClosed)??;

        let mut response_buf = response_data;
        let response_header = protocol::ResponseHeader::decode(&mut response_buf, use_flexible)?;

        Ok((response_header, response_buf))
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
    framed: KafkaFramed,
    cmd_rx: mpsc::UnboundedReceiver<Command>,
    pending: HashMap<i32, oneshot::Sender<Result<Bytes>>>,
}

impl ConnectionReactor {
    fn new(framed: KafkaFramed, cmd_rx: mpsc::UnboundedReceiver<Command>) -> Self {
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
                            match self.framed.send_frame(data).await {
                                Ok(()) => {
                                    self.pending.insert(correlation_id, response_tx);
                                }
                                Err(e) => {
                                    let _ = response_tx.send(Err(e));
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
                frame = self.framed.recv_frame() => {
                    match frame {
                        Ok(data) => {
                            let Some(corr_id) = extract_correlation_id(&data) else {
                                warn!(
                                    "Received response frame too short to contain a correlation id ({} bytes), closing connection",
                                    data.len()
                                );
                                self.fail_pending(KafkaError::Protocol(
                                    "Response frame too short to contain a correlation id".into(),
                                ));
                                return;
                            };
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
                        Err(e) => {
                            self.fail_pending(e);
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
    framed: KafkaFramed,
    negotiated: Arc<NegotiatedVersions>,
    client_id: Option<String>,
    request_timeout: Duration,
) -> ConnectionHandle {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let mut reactor = ConnectionReactor::new(framed, cmd_rx);
    tokio::spawn(async move {
        reactor.run().await;
    });
    ConnectionHandle::new(cmd_tx, negotiated, client_id, request_timeout)
}

// ---------------------------------------------------------------------------
// SequentialConnection — used during handshake (ApiVersions, SASL)
// ---------------------------------------------------------------------------

/// Sequential connection for the initialisation phase.
///
/// One request → one response. Used only during connection establishment
/// (version negotiation, SASL authentication). Not shared between tasks.
pub struct SequentialConnection {
    framed: KafkaFramed,
    client_id: Option<String>,
    negotiated: NegotiatedVersions,
    request_timeout: Duration,
}

impl SequentialConnection {
    pub fn new(framed: KafkaFramed, client_id: Option<String>, request_timeout: Duration) -> Self {
        SequentialConnection {
            framed,
            client_id,
            negotiated: NegotiatedVersions::new(),
            request_timeout,
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

        self.framed.send_frame(request_data).await?;

        let frame = tokio::time::timeout(self.request_timeout, self.framed.recv_frame())
            .await
            .map_err(|_| KafkaError::RequestTimeout)??;

        debug!(response_len = frame.len(), "received sequential response");

        let (header, response) = Resp::decode_frame(frame, version)?;

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
        spawn_reactor(
            self.framed,
            Arc::new(self.negotiated),
            self.client_id,
            self.request_timeout,
        )
    }

    /// Unwrap into the raw framed stream, discarding negotiated versions.
    ///
    /// Prefer [`into_framed_parts`](Self::into_framed_parts) unless the
    /// negotiated API versions are genuinely not needed — recovering them
    /// afterwards requires a second `ApiVersions` round-trip.
    pub fn into_framed(self) -> KafkaFramed {
        self.framed
    }

    /// Unwrap into the raw framed stream plus the negotiated API versions.
    ///
    /// See [`Builder::build_framed`] for the intended proxy/relay use case and
    /// its important caveat about already-completed authentication.
    pub fn into_framed_parts(self) -> (KafkaFramed, NegotiatedVersions) {
        (self.framed, self.negotiated)
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
    kerberos_config: Option<krb5_gss::KerberosCredentials>,
    /// Broker 主机名 (用于构造 Kerberos 服务 principal `service/host`)。None 时回退为 IP。
    broker_hostname: Option<String>,
    /// KDC 主机名 (None 时回退为 realm 域名或 localhost)。
    kdc_host: Option<String>,
    /// KDC 端口。默认 88。
    kdc_port: u16,
    /// 单请求超时。
    request_timeout: Duration,
    /// 单帧最大字节数。默认 100 MiB。
    max_frame_size: usize,
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
            kerberos_config: None,
            broker_hostname: None,
            kdc_host: None,
            kdc_port: 88,
            request_timeout: Duration::from_secs(60),
            max_frame_size: crate::wire::DEFAULT_MAX_FRAME_SIZE,
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

    /// Configure SASL/GSSAPI (Kerberos) authentication.
    ///
    /// Requires the `kerberos` feature. Switches the security protocol to
    /// `SaslPlaintext` (combine with TLS separately if needed).
    pub fn with_kerberos(mut self, credentials: krb5_gss::KerberosCredentials) -> Self {
        self.security_protocol = crate::transport::SecurityProtocol::SaslPlaintext;
        self.kerberos_config = Some(credentials);
        self
    }

    /// 设置 KDC 地址 (主机名 + 端口)。仅当 kerberos 认证启用时生效。
    pub fn with_kdc(mut self, host: impl Into<String>, port: u16) -> Self {
        self.kdc_host = Some(host.into());
        self.kdc_port = port;
        self
    }

    /// 设置 broker 主机名 (用于 Kerberos 服务 principal)。
    /// kerberos 需要 `service/hostname` 格式, 如果未设置则回退为 IP 字符串。
    pub fn with_broker_hostname(mut self, host: impl Into<String>) -> Self {
        self.broker_hostname = Some(host.into());
        self
    }

    /// Set the maximum time a single request may wait for a response.
    ///
    /// Defaults to 60 seconds. Must be larger than any fetch `max_wait`,
    /// since a fetch may legitimately block on the broker for that long.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Set the maximum accepted frame size, in bytes.
    ///
    /// Defaults to 100 MiB. Frames larger than this are rejected by the codec
    /// with [`KafkaError::Protocol`], in both directions. Raise this when
    /// relaying unusually large record batches (see [`Builder::build_framed`]).
    pub fn with_max_frame_size(mut self, max_frame_size: usize) -> Self {
        self.max_frame_size = max_frame_size;
        self
    }

    /// Run the shared connection pipeline: TCP → TLS → ApiVersions → SASL.
    ///
    /// Stops at a [`SequentialConnection`], which each public `build*` method
    /// then converts into its own connection shape. Keeping this in one place
    /// guarantees every entry point performs the exact same handshake and
    /// authentication steps.
    async fn establish(self) -> Result<SequentialConnection> {
        // 1. Establish the transport (TCP, optionally wrapped in TLS).
        //
        // `TransportConnector` returns a plain `io::Error`; map it here so the
        // per-stage error contract documented on `build()` is preserved.
        let uses_tls = self.security_protocol.uses_tls();
        let stream =
            crate::transport::TransportConnector::connect(self.addr, &self.security_protocol)
                .await
                .map_err(|e| {
                    if uses_tls {
                        KafkaError::TlsError {
                            addr: self.addr.to_string(),
                            details: e.to_string(),
                        }
                    } else {
                        KafkaError::Io(format!("Failed to connect to {}: {}", self.addr, e))
                    }
                })?;

        let framed = KafkaFramed::new(Framed::new(
            stream,
            KafkaCodec::new_with_max_frame_size(self.max_frame_size),
        ));

        // 2. Sequential handshake (ApiVersions negotiation)
        let mut seq_conn = SequentialConnection::new(framed, self.client_id, self.request_timeout);

        let negotiated =
            Handshake::perform(&mut seq_conn, self.client_name, self.client_version).await?;
        seq_conn.set_negotiated(negotiated);

        // 2.5. Probe server for SASL requirement (client has no credentials configured)
        let has_kerberos = self.kerberos_config.is_some();
        if !self.sasl_config.is_some() && !has_kerberos {
            let probe_req = protocol::SaslHandshakeRequest {
                mechanism: "PLAIN".to_string(),
            };
            match seq_conn
                .send_request::<_, protocol::SaslHandshakeResponse>(&probe_req)
                .await
            {
                Ok(resp) if !resp.mechanisms.is_empty() => {
                    return Err(KafkaError::AuthenticationFailed(format!(
                        "SASL authentication required, missing credentials. \
                         Server SASL mechanisms: {}. Available client: plain, scram_sha256, scram_sha512.",
                        resp.mechanisms.join(", ")
                    )));
                }
                Ok(_) => {
                    debug!("Server does not require SASL authentication");
                }
                Err(e) => {
                    return Err(KafkaError::AuthenticationFailed(format!(
                        "SASL authentication required, missing credentials. Probe error: {}.",
                        e
                    )));
                }
            }
        }

        // 3. SASL authentication (Kerberos/GSSAPI)
        if let Some(mut creds) = self.kerberos_config.clone() {
            // 每个 broker 连接使用自己的服务 principal:
            // 连接级 hostname (从 metadata 的 advertised host 解析, 见
            // BrokerManager) 优先, 保证多 broker 集群中每台 broker 都用
            // 自己的 `kafka/<host>` principal 认证; 未设置时回退到
            // credentials 中配置的 hostname, 再回退到 IP。
            if let Some(ref h) = self.broker_hostname {
                creds = creds.with_broker_hostname(h.clone());
            }
            Handshake::sasl_authenticate_gssapi(
                &mut seq_conn,
                creds,
                &self.addr.ip().to_string(), // fallback when creds.broker_hostname not set
                self.kdc_host.as_deref(),
                self.kdc_port,
            )
            .await
            .map_err(auth_err)?;
        } else if let Some((mechanism, credentials)) = self.sasl_config {
            Handshake::sasl_authenticate(&mut seq_conn, mechanism, credentials)
                .await
                .map_err(auth_err)?;
        }

        Ok(seq_conn)
    }

    /// Build the connection: TCP → TLS → handshake → SASL → reactor handle.
    ///
    /// This is the standard entry point. The returned [`ConnectionHandle`] is
    /// cloneable and multiplexes concurrent requests over a single socket.
    ///
    /// # Errors
    ///
    /// Returns distinct error types for each stage:
    /// - [`KafkaError::Io`] — TCP connection failure (address unreachable, port not listening, etc.)
    /// - [`KafkaError::TlsError`] — TLS handshake failure (certificate error, domain mismatch, etc.)
    /// - [`KafkaError::Protocol`] — Kafka protocol handshake failure (ApiVersions negotiation)
    /// - [`KafkaError::AuthenticationFailed`] — SASL authentication failure (wrong credentials, etc.)
    pub async fn build(self) -> Result<ConnectionHandle> {
        Ok(self.establish().await?.into_pipeline())
    }

    /// Build the connection but stop at the sequential (one-request-at-a-time) stage.
    ///
    /// **Experimental.** This API is new in 0.8.0 and may change in a future
    /// minor release.
    ///
    /// Performs the identical TCP → TLS → ApiVersions → SASL pipeline as
    /// [`build`](Self::build), but returns the connection *before* a reactor
    /// task is spawned. Useful for simple tools that issue a handful of
    /// requests in strict order and do not need pipelining.
    ///
    /// Call [`SequentialConnection::into_pipeline`] later to upgrade to a
    /// multiplexed [`ConnectionHandle`].
    ///
    /// # Errors
    ///
    /// Same as [`build`](Self::build).
    pub async fn build_sequential(self) -> Result<SequentialConnection> {
        self.establish().await
    }

    /// Build the connection and return the raw framed stream for 1:1 frame relay.
    ///
    /// **Experimental.** This API is new in 0.8.0 and may change in a future
    /// minor release as proxy/relay use cases become clearer.
    ///
    /// Performs the identical TCP → TLS → ApiVersions → SASL pipeline as
    /// [`build`](Self::build), but returns the authenticated [`KafkaFramed`]
    /// directly instead of spawning a reactor. The caller drives the socket and
    /// owns correlation-id handling entirely.
    ///
    /// This is the intended API for proxy / gateway use cases that forward
    /// frames verbatim between a downstream client and a broker: because there
    /// is no reactor, downstream correlation IDs pass through untouched and
    /// cannot collide with internally generated ones (unlike
    /// [`ConnectionHandle::send_raw_frame`]).
    ///
    /// The [`NegotiatedVersions`] from the handshake are returned alongside the
    /// stream so callers can reject APIs the broker does not support without
    /// issuing a second `ApiVersions` request.
    ///
    /// # The connection is already authenticated
    ///
    /// **Do not forward a downstream client's `ApiVersions`, `SaslHandshake`,
    /// or `SaslAuthenticate` frames over this stream.** The broker's connection
    /// state machine has already completed negotiation and authentication;
    /// replaying those frames will be rejected or misinterpreted. A proxy
    /// should answer them locally (using the returned [`NegotiatedVersions`])
    /// and forward only post-authentication frames.
    ///
    /// # Frame size
    ///
    /// The codec's limit applies to relayed frames in both directions; see
    /// [`with_max_frame_size`](Self::with_max_frame_size) if you relay large
    /// record batches.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use futures::{SinkExt, StreamExt};
    /// use kafka_client::wire::KafkaFrame;
    ///
    /// let (framed, negotiated) = builder.build_framed().await?;
    /// // Split into independent read/write halves for full-duplex relaying.
    /// let (mut sink, mut stream) = framed.split();
    ///
    /// sink.send(KafkaFrame::new(request_bytes)).await?;
    /// if let Some(frame) = stream.next().await {
    ///     let response_bytes = frame?.data;
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Same as [`build`](Self::build).
    pub async fn build_framed(self) -> Result<(KafkaFramed, NegotiatedVersions)> {
        Ok(self.establish().await?.into_framed_parts())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Map handshake/SASL errors to [`KafkaError::AuthenticationFailed`].
fn auth_err(e: KafkaError) -> KafkaError {
    match &e {
        KafkaError::Protocol(msg) => KafkaError::AuthenticationFailed(msg.clone()),
        KafkaError::SaslError(sasl_err) => KafkaError::AuthenticationFailed(sasl_err.to_string()),
        _ => e,
    }
}

/// Extract the correlation_id from a Kafka **response** frame.
///
/// The correlation_id is the first 4 bytes of the response data
/// (the frame length prefix has already been stripped by KafkaCodec).
///
/// Note the asymmetry with [`request_correlation_id`]: response headers begin
/// directly with the correlation ID, whereas request headers are prefixed by
/// `api_key` and `api_version`.
fn extract_correlation_id(data: &Bytes) -> Option<i32> {
    use bytes::Buf;
    if data.len() < 4 {
        return None;
    }
    Some((&data[..]).get_i32())
}

/// Extract the correlation_id from a Kafka **request** frame.
///
/// Request header layout (all versions), after the length prefix is stripped:
///
/// ```text
/// api_key: i16 | api_version: i16 | correlation_id: i32 | ...
///  bytes 0..2  |    bytes 2..4    |     bytes 4..8      |
/// ```
///
/// Returns `None` if the frame is too short to contain a correlation ID.
///
/// Used internally by the reactor to key pending responses by correlation ID;
/// it is not part of the public API.
fn request_correlation_id(data: &Bytes) -> Option<i32> {
    use bytes::Buf;
    if data.len() < 8 {
        return None;
    }
    Some((&data[4..8]).get_i32())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::NetworkStream;
    use crate::wire::KafkaFrame;
    use bytes::BytesMut;
    use futures::{SinkExt, StreamExt};
    use std::io;
    use std::net::SocketAddr;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
    use tokio_util::codec::Framed;

    #[test]
    fn request_correlation_id_reads_bytes_4_to_8() {
        // api_key=3 (Metadata), api_version=12, correlation_id=0x0A0B0C0D
        let frame = Bytes::from_static(&[
            0x00, 0x03, // api_key
            0x00, 0x0C, // api_version
            0x0A, 0x0B, 0x0C, 0x0D, // correlation_id
            0xFF, 0xFF, // trailing payload
        ]);
        assert_eq!(request_correlation_id(&frame), Some(0x0A0B0C0D));
    }

    #[test]
    fn request_correlation_id_handles_negative_ids() {
        let frame = Bytes::from_static(&[
            0x00, 0x12, // api_key = 18 (ApiVersions)
            0x00, 0x03, // api_version
            0xFF, 0xFF, 0xFF, 0xFF, // correlation_id = -1
        ]);
        assert_eq!(request_correlation_id(&frame), Some(-1));
    }

    #[test]
    fn request_correlation_id_rejects_short_frames() {
        // 7 bytes: one short of a complete correlation ID.
        assert_eq!(
            request_correlation_id(&Bytes::from_static(&[0, 3, 0, 12, 0, 0, 0])),
            None
        );
        assert_eq!(request_correlation_id(&Bytes::new()), None);
    }

    /// Request and response headers put the correlation ID at different
    /// offsets; a mix-up silently misroutes responses.
    #[test]
    fn request_and_response_offsets_differ() {
        let request = Bytes::from_static(&[
            0x00, 0x03, // api_key
            0x00, 0x0C, // api_version
            0x00, 0x00, 0x00, 0x07, // correlation_id = 7
        ]);
        let response = Bytes::from_static(&[
            0x00, 0x00, 0x00, 0x07, // correlation_id = 7, no api_key/version
        ]);
        assert_eq!(request_correlation_id(&request), Some(7));
        assert_eq!(extract_correlation_id(&response), Some(7));
        // Reading a request with the response helper picks up api_key/version.
        assert_eq!(extract_correlation_id(&request), Some(0x0003_000C));
    }

    #[test]
    fn request_correlation_id_matches_encoded_frame() {
        // Guard against drift from the real encoder.
        use kafka_client_protocol::{MetadataRequest, Request};

        let correlation_id = 0x1234_5678;
        let encoded = MetadataRequest {
            topics: None,
            allow_auto_topic_creation: false,
            include_cluster_authorized_operations: false,
            include_topic_authorized_operations: false,
        }
        .encode_frame(12, correlation_id, Some("test-client".to_string()))
        .expect("encode should succeed");

        assert_eq!(request_correlation_id(&encoded), Some(correlation_id));
    }

    // --- send_request_frame (structured header + raw body) ---

    /// An in-memory [`NetworkStream`] backed by a `tokio` duplex channel.
    struct DuplexNetwork {
        inner: DuplexStream,
    }

    impl AsyncRead for DuplexNetwork {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for DuplexNetwork {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.inner).poll_write(cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_flush(cx)
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    impl NetworkStream for DuplexNetwork {
        fn peer_addr(&self) -> io::Result<SocketAddr> {
            Ok("127.0.0.1:9092".parse().unwrap())
        }

        fn local_addr(&self) -> io::Result<SocketAddr> {
            Ok("127.0.0.1:0".parse().unwrap())
        }

        fn is_secure(&self) -> bool {
            false
        }
    }

    /// Build a `ConnectionHandle` whose reactor is fed from the client end of a
    /// duplex channel; the test drives the server end directly.
    fn duplex_handle() -> (ConnectionHandle, DuplexStream) {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let framed = KafkaFramed::new(Framed::new(
            Box::new(DuplexNetwork { inner: client }),
            KafkaCodec::new(),
        ));
        let handle = spawn_reactor(
            framed,
            Arc::new(NegotiatedVersions::new()),
            None,
            Duration::from_secs(5),
        );
        (handle, server)
    }

    /// Broker-side helper: read one request frame, then write a response built
    /// from the request's correlation ID (bytes `[4..8]`) plus `body`. When
    /// `flexible` is set the response header carries an empty tagged-fields set.
    async fn broker_respond(server: DuplexStream, flexible: bool, body: Bytes) {
        let mut framed = Framed::new(server, KafkaCodec::new());
        let req = framed
            .next()
            .await
            .expect("request frame")
            .expect("no error");
        assert!(req.data.len() >= 8, "request too short for correlation id");
        let corr_id = i32::from_be_bytes([req.data[4], req.data[5], req.data[6], req.data[7]]);
        let mut resp = BytesMut::with_capacity(4 + body.len());
        resp.put_i32(corr_id);
        if flexible {
            resp.put_u8(0); // empty tagged-fields set (varint count = 0)
        }
        resp.extend_from_slice(&body);
        framed
            .send(KafkaFrame::new(resp.freeze()))
            .await
            .expect("send response");
        framed.flush().await.expect("flush response");
    }

    #[tokio::test]
    async fn send_request_frame_traditional_header_roundtrips() {
        let (handle, server) = duplex_handle();
        let body = Bytes::from_static(&[0xAA, 0xBB, 0xCC]);
        let broker = tokio::spawn(broker_respond(server, false, body.clone()));

        let header = kafka_client_protocol::RequestHeader::new_v1(
            3,          // api_key = Metadata
            12,         // api_version
            0x0A0B0C0D, // correlation_id
            Some("test-client".to_string()),
        );

        let (response_header, response_body) = handle
            .send_request_frame(header, body.clone())
            .await
            .expect("send_request_frame should succeed");

        assert_eq!(
            response_header.correlation_id(),
            0x0A0B0C0D,
            "traditional response must carry the request correlation id"
        );
        assert!(
            matches!(
                response_header,
                kafka_client_protocol::ResponseHeader::V0(_)
            ),
            "traditional header must decode to ResponseHeader::V0"
        );
        assert_eq!(response_body, body, "response body must round-trip");

        broker.await.expect("broker task");
    }

    #[tokio::test]
    async fn send_request_frame_flexible_header_roundtrips() {
        let (handle, server) = duplex_handle();
        let body = Bytes::from_static(&[0x01, 0x02, 0x03]);
        let broker = tokio::spawn(broker_respond(server, true, body.clone()));

        let header = kafka_client_protocol::RequestHeader::new_v2(
            3,           // api_key = Metadata
            12,          // api_version
            0x0102_0304, // correlation_id
            Some("test-client".to_string()),
        );

        let (response_header, response_body) = handle
            .send_request_frame(header, body.clone())
            .await
            .expect("send_request_frame should succeed");

        assert_eq!(
            response_header.correlation_id(),
            0x0102_0304,
            "flexible response must carry the request correlation id"
        );
        assert!(
            matches!(
                response_header,
                kafka_client_protocol::ResponseHeader::V1(_)
            ),
            "flexible header must decode to ResponseHeader::V1"
        );
        assert_eq!(response_body, body, "response body must round-trip");

        broker.await.expect("broker task");
    }

    #[tokio::test]
    async fn send_request_frame_concurrent_out_of_order() {
        // Two requests with distinct correlation IDs; the broker replies in
        // reverse order. Because the reactor matches by correlation ID, each
        // caller still receives the response for its own request.
        let (handle, server) = duplex_handle();
        let body = Bytes::from_static(&[0x99, 0x88]);

        let broker = tokio::spawn(async move {
            let mut framed = Framed::new(server, KafkaCodec::new());
            let req1 = framed
                .next()
                .await
                .expect("request frame")
                .expect("no error");
            let corr1 =
                i32::from_be_bytes([req1.data[4], req1.data[5], req1.data[6], req1.data[7]]);
            let req2 = framed
                .next()
                .await
                .expect("request frame")
                .expect("no error");
            let corr2 =
                i32::from_be_bytes([req2.data[4], req2.data[5], req2.data[6], req2.data[7]]);

            // Reply to the second request first (out-of-order).
            let mut r2 = BytesMut::new();
            r2.put_i32(corr2);
            framed
                .send(KafkaFrame::new(r2.freeze()))
                .await
                .expect("send response");
            framed.flush().await.expect("flush response");

            let mut r1 = BytesMut::new();
            r1.put_i32(corr1);
            framed
                .send(KafkaFrame::new(r1.freeze()))
                .await
                .expect("send response");
            framed.flush().await.expect("flush response");
        });

        let h1 = kafka_client_protocol::RequestHeader::new_v1(
            3,
            12,
            100,
            Some("test-client".to_string()),
        );
        let h2 = kafka_client_protocol::RequestHeader::new_v1(
            3,
            12,
            101,
            Some("test-client".to_string()),
        );

        // Poll both requests concurrently so they are dispatched to the broker
        // before either response is awaited; the reactor then routes each
        // response back by correlation ID regardless of arrival order.
        let f1 = handle.send_request_frame(h1, body.clone());
        let f2 = handle.send_request_frame(h2, body.clone());
        let (res1, res2) = tokio::join!(f1, f2);
        let (r1_header, _r1_body) = res1.expect("request 1");
        let (r2_header, _r2_body) = res2.expect("request 2");
        assert_eq!(
            r1_header.correlation_id(),
            100,
            "request 1 must get its own response (corr_id 100)"
        );
        assert_eq!(
            r2_header.correlation_id(),
            101,
            "request 2 must get its own response (corr_id 101)"
        );

        broker.await.expect("broker task");
    }
}
