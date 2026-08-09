//! Wire layer (L2) — Kafka length-prefixed framing.
//!
//! This layer sits between the transport ([`crate::transport`], raw byte
//! streams) and the connection layer ([`crate::connection`], request/response
//! semantics). It understands exactly one thing: Kafka's 4-byte big-endian
//! length prefix.
//!
//! [`KafkaCodec`] strips that prefix on decode and prepends it on encode. It
//! does **not** interpret any byte of the frame payload — no API key, no
//! version, no correlation ID. That makes it suitable for verbatim frame
//! relaying, where the payload must not be re-encoded.
//!
//! Most users never need this module; use [`crate::Client`] or
//! [`crate::connection::ConnectionHandle`] instead. It is public to support
//! proxy and gateway use cases built on
//! [`Builder::build_framed`](crate::connection::Builder::build_framed).

mod codec;

pub use codec::{DEFAULT_MAX_FRAME_SIZE, KafkaCodec, KafkaFrame};

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use tokio_util::codec::Framed;

use crate::error::{KafkaError, Result};
use crate::transport::NetworkStream;
use kafka_client_protocol::{Request, RequestHeaderV1, RequestHeaderV2, Response};

/// An authenticated, length-prefixed Kafka frame stream.
///
/// This is a thin wrapper around [`tokio_util::codec::Framed`]`<`[`NetworkStream`]`,`
/// [`KafkaCodec`]`>`, exposed as a stable, crate-owned type. The underlying
/// codec can evolve (or be swapped) without breaking callers' type signatures.
///
/// Three request helpers cover the spectrum from fully typed to fully raw:
///
/// - [`send_request`](Self::send_request) — fully typed (`Req`/`Resp` traits);
///   the library encodes the header, body and correlation ID.
/// - [`send_frame`](Self::send_frame) — you supply `api_key`, `api_version`,
///   `is_flexible` and a pre-encoded `body`; the library encodes the header and
///   owns the correlation ID.
/// - [`send_raw_frame`](Self::send_raw_frame) — you supply the entire
///   header+body byte string; the library only adds the length prefix and
///   forwards the raw response. Correlation-ID uniqueness is yours to manage.
///
/// Every variant returns through [`recv_response`](Self::recv_response), which
/// strips the response header and yields `(correlation_id, body)`.
///
/// For full-duplex proxy / gateway use cases that forward frames verbatim, use
/// [`into_inner`](Self::into_inner) to reach the raw [`Framed`] and `.split()`
/// it into independent read/write halves.
pub struct KafkaFramed {
    inner: Framed<Box<dyn NetworkStream>, KafkaCodec>,
    request_timeout: Duration,
    next_correlation_id: i32,
}

impl KafkaFramed {
    pub(crate) fn new(
        inner: Framed<Box<dyn NetworkStream>, KafkaCodec>,
        request_timeout: Duration,
    ) -> Self {
        Self {
            inner,
            request_timeout,
            next_correlation_id: rand::random(),
        }
    }

    /// The maximum frame size the underlying codec will accept, in bytes.
    pub fn max_frame_size(&self) -> usize {
        self.inner.codec().max_frame_size()
    }

    /// Send a typed request and wait for the decoded response.
    ///
    /// The request is encoded with the given `api_version` and a correlation ID
    /// drawn from this stream's own counter (initialised at construction, so it
    /// never collides with IDs used by other [`KafkaFramed`]s or handles sharing
    /// the same connection). The response is decoded and its correlation ID is
    /// checked against the one sent.
    ///
    /// Prefer this over hand-rolling bytes unless you are forwarding opaque
    /// frames from a downstream client (use
    /// [`into_inner`](Self::into_inner) + `send_raw_frame` for that).
    pub async fn send_request<Req, Resp>(
        &mut self,
        request: &Req,
        api_version: i16,
        client_id: Option<String>,
    ) -> Result<Resp>
    where
        Req: Request,
        Resp: Response,
    {
        let correlation_id = self.next_correlation_id;
        self.next_correlation_id = self.next_correlation_id.wrapping_add(1);

        let data = request.encode_frame(api_version, correlation_id, client_id)?;
        self.send_raw_frame(data).await?;

        let (_corr_id, body) = self.recv_response().await?;
        let (header, response) = Resp::decode_frame(body, api_version)?;
        if header.correlation_id() != correlation_id {
            return Err(KafkaError::CorrelationIdMismatch {
                expected: correlation_id,
                actual: header.correlation_id(),
            });
        }
        Ok(response)
    }

    /// Send a request by API key/version with a caller-supplied body, returning
    /// the decoded response body.
    ///
    /// Sits between [`send_request`](Self::send_request) (fully typed) and
    /// [`send_raw_frame`](Self::send_raw_frame) (fully pre-encoded bytes):
    ///
    /// - The **correlation ID is allocated by the library** from this stream's
    ///   own counter (never colliding with other [`KafkaFramed`]s or handles),
    ///   so you do not have to manage it in the byte layout.
    /// - The **`api_key`, `api_version` and `body` bytes are supplied by you**;
    ///   the library encodes the request header (picking the v1 or v2 flexible
    ///   layout from `is_flexible`) and appends your body. You get back the
    ///   response body with the response header already stripped.
    ///
    /// This is the right tool when you have already encoded a request body
    /// yourself (or are relaying a body produced elsewhere) but want the
    /// library to own correlation-ID bookkeeping and header encoding. Decode the
    /// returned body using the `api_version`/`is_flexible` you sent.
    ///
    /// **Prefer [`send_request`](Self::send_request) when you can** — if you
    /// have (or can derive) a typed `Request`/`Response` pair, `send_request`
    /// gives you the same owned correlation-ID bookkeeping *and* full decoding
    /// with no manual body/version juggling. Reach for `send_frame` only when a
    /// type implementation is impractical (e.g. relaying a body produced
    /// elsewhere).
    ///
    /// # Errors
    ///
    /// - [`KafkaError::ConnectionClosed`](KafkaError#variant.ConnectionClosed) —
    ///   the stream is gone.
    /// - [`KafkaError::RequestTimeout`](KafkaError#variant.RequestTimeout) —
    ///   no response within the configured request timeout.
    /// - [`KafkaError::CorrelationIdMismatch`](KafkaError#variant.CorrelationIdMismatch)
    ///   — the broker's response correlation ID did not match the one sent.
    pub async fn send_frame(
        &mut self,
        api_key: i16,
        api_version: i16,
        is_flexible: bool,
        client_id: Option<String>,
        body: Bytes,
    ) -> Result<Bytes> {
        let correlation_id = self.next_correlation_id;
        self.next_correlation_id = self.next_correlation_id.wrapping_add(1);

        let mut buf = BytesMut::new();
        if is_flexible {
            RequestHeaderV2 {
                api_key,
                api_version,
                correlation_id,
                client_id,
                tagged_fields: Vec::new(),
            }
            .encode(&mut buf);
        } else {
            RequestHeaderV1 {
                api_key,
                api_version,
                correlation_id,
                client_id,
            }
            .encode(&mut buf);
        }
        buf.extend_from_slice(&body);

        self.inner.send(KafkaFrame::new(buf.freeze())).await?;
        self.inner.flush().await?;

        let (resp_corr_id, resp_body) = self.recv_response().await?;
        if resp_corr_id != correlation_id {
            return Err(KafkaError::CorrelationIdMismatch {
                expected: correlation_id,
                actual: resp_corr_id,
            });
        }
        Ok(resp_body)
    }

    /// Read the next response frame and return its `(correlation_id, body)`.
    ///
    /// The 4-byte length prefix and the response header (correlation ID, and any
    /// flexible tagged fields) are stripped; only the header's correlation ID is
    /// surfaced alongside the remaining body bytes, which the caller must decode
    /// using the API and version it originally sent.
    ///
    /// Note the asymmetry with requests: response headers carry no `api_key` or
    /// `api_version`, so the correlation ID sits at bytes `[0..4]`.
    pub async fn recv_response(&mut self) -> Result<(i32, Bytes)> {
        let frame = tokio::time::timeout(self.request_timeout, self.inner.next())
            .await
            .map_err(|_| KafkaError::RequestTimeout)?
            .ok_or(KafkaError::ConnectionClosed)??;
        let data = frame.data;
        if data.len() < 4 {
            return Err(KafkaError::Protocol(
                "Response frame too short to contain a correlation id".into(),
            ));
        }
        let correlation_id = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        Ok((correlation_id, data.slice(4..)))
    }

    /// Send a fully pre-encoded request frame (header + body, no length prefix)
    /// and return the raw response bytes verbatim (header + body, no length
    /// prefix).
    ///
    /// This is the lowest-level primitive: `data` is written as-is (the codec
    /// only adds the length prefix) and the undecoded response is returned
    /// unchanged. Unlike [`send_frame`](Self::send_frame) and
    /// [`send_request`](Self::send_request), this method performs **no**
    /// correlation-ID bookkeeping and **no** header encoding — the bytes you
    /// pass are exactly what goes on the wire, and the bytes you get back are
    /// exactly what the broker sent. Keeping the correlation ID unique among
    /// in-flight requests on this stream is entirely the caller's
    /// responsibility.
    ///
    /// Reach for this only when you need complete control over the byte layout
    /// (for example forwarding opaque frames produced by a downstream client).
    /// In most cases [`send_frame`](Self::send_frame) — which still lets you
    /// supply the body but owns correlation-ID and header encoding — is the
    /// better fit.
    ///
    /// **Prefer [`send_request`](Self::send_request) (typed) or
    /// [`send_frame`](Self::send_frame) (caller-supplied body) whenever
    /// possible.** `send_request` owns correlation-ID allocation, header
    /// encoding *and* response decoding for you — use it if you have a typed
    /// `Request`/`Response` pair (which you can define by implementing the
    /// [`Request`]/[`Response`] traits or deriving [`Message`]). Only fall back
    /// to `send_raw_frame` when the bytes were produced outside this crate and
    /// cannot go through the typed path.
    ///
    /// # Errors
    ///
    /// - [`KafkaError::ConnectionClosed`](KafkaError#variant.ConnectionClosed) —
    ///   the stream is gone.
    /// - [`KafkaError::RequestTimeout`](KafkaError#variant.RequestTimeout) —
    ///   no response within the configured request timeout.
    pub async fn send_raw_frame(&mut self, data: Bytes) -> Result<Bytes> {
        self.inner.send(KafkaFrame::new(data)).await?;
        self.inner.flush().await?;

        // No correlation-ID matching here: the caller owns the byte layout and
        // is responsible for pairing requests and responses.
        let frame = tokio::time::timeout(self.request_timeout, self.inner.next())
            .await
            .map_err(|_| KafkaError::RequestTimeout)?
            .ok_or(KafkaError::ConnectionClosed)??;
        Ok(frame.data)
    }

    /// Unwrap into the raw [`tokio_util::codec::Framed`] stream.
    ///
    /// Use this for proxy / gateway relaying that needs `.split()` into
    /// independent read/write halves, or any low-level frame handling the
    /// semantic helpers above do not cover. The wrapped stream is identical to
    /// the one this type holds internally.
    pub fn into_inner(self) -> Framed<Box<dyn NetworkStream>, KafkaCodec> {
        self.inner
    }

    /// Mutable access to the wrapped [`Framed`] stream (crate-internal).
    pub(crate) fn inner_mut(&mut self) -> &mut Framed<Box<dyn NetworkStream>, KafkaCodec> {
        &mut self.inner
    }
}
