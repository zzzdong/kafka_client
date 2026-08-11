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
//! **Layer boundary.** This layer is intentionally free of any request/response
//! semantics: it has no correlation-ID bookkeeping, no header encoding, no
//! typed decoding. Those belong to the connection layer
//! ([`crate::connection::SequentialConnection`] for the serial pre-auth phase,
//! [`crate::connection::ConnectionHandle`] for the pipelined, out-of-order
//! business phase). Use this module only when you need to drive a frame stream
//! directly — e.g. a proxy forwarding opaque frames via
//! [`Builder::build_framed`](crate::connection::Builder::build_framed).
//!
//! All methods here are strictly one-frame-at-a-time and serial: a caller that
//! needs to correlate responses to requests must implement that at a higher
//! layer.

mod codec;

pub use codec::{DEFAULT_MAX_FRAME_SIZE, KafkaCodec, KafkaFrame};

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio_util::codec::Framed;

use crate::error::{KafkaError, Result};
use crate::transport::NetworkStream;

/// An authenticated, length-prefixed Kafka frame stream.
///
/// This is a thin wrapper around
/// [`tokio_util::codec::Framed`]`<`[`NetworkStream`]`,` [`KafkaCodec`]`>`,
/// exposed as a stable, crate-owned type. The underlying codec can evolve (or
/// be swapped) without breaking callers' type signatures.
///
/// It is the **transport primitive only**: it sends and receives whole frames,
/// doing nothing more than adding/removing the 4-byte length prefix. It has no
/// notion of correlation IDs, request headers, or typed decoding — those are
/// layered on top in [`crate::connection`].
///
/// Because it is strictly serial (one frame in, one frame out), it is *not*
/// safe to call these methods concurrently on the same stream. For concurrent,
/// out-of-order request/response correlation use
/// [`crate::connection::ConnectionHandle`].
pub struct KafkaFramed {
    inner: Framed<Box<dyn NetworkStream>, KafkaCodec>,
}

impl KafkaFramed {
    pub(crate) fn new(inner: Framed<Box<dyn NetworkStream>, KafkaCodec>) -> Self {
        Self { inner }
    }

    /// The maximum frame size the underlying codec will accept, in bytes.
    pub fn max_frame_size(&self) -> usize {
        self.inner.codec().max_frame_size()
    }

    /// Send a single frame (the 4-byte length prefix is added by the codec).
    ///
    /// This is the **only** way to write to the wire at this layer. It performs
    /// **no** correlation-ID bookkeeping and **no** header encoding: `data` is
    /// written exactly as given. It returns once the frame has been flushed,
    /// without waiting for any response — the caller pairs it with a
    /// subsequent [`recv_frame`](Self::recv_frame).
    ///
    /// # Errors
    ///
    /// - [`KafkaError::Io`] — the underlying write/flush failed.
    pub async fn send_frame(&mut self, data: Bytes) -> Result<()> {
        self.inner.send(KafkaFrame::new(data)).await?;
        self.inner.flush().await?;
        Ok(())
    }

    /// Read the next frame and return its complete payload bytes.
    ///
    /// The 4-byte length prefix is stripped by the codec; the returned `Bytes`
    /// is the raw frame payload — for a response it still carries the full
    /// response **header** (correlation ID and, for flexible versions, its
    /// tagged fields) followed by the body. It is exactly what the peer sent,
    /// so it can be handed to
    /// [`Response::decode_frame`](kafka_client_protocol::Response::decode_frame)
    /// for typed decoding or forwarded verbatim.
    ///
    /// This is a pure I/O primitive with **no timeout**: it waits indefinitely
    /// for the next frame. Applying a request timeout is the caller's job (the
    /// connection layer does this), so that a long-lived read loop can keep
    /// blocking on a single stream without being spuriously cancelled.
    ///
    /// This is the raw building block for the connection layer; most callers
    /// should use [`crate::connection`] instead of driving a `KafkaFramed`
    /// directly.
    ///
    /// # Errors
    ///
    /// - [`KafkaError::ConnectionClosed`](KafkaError#variant.ConnectionClosed) —
    ///   the stream is gone.
    pub async fn recv_frame(&mut self) -> Result<Bytes> {
        let frame = self
            .inner
            .next()
            .await
            .ok_or(KafkaError::ConnectionClosed)??;
        Ok(frame.data)
    }

    /// Unwrap into the raw [`tokio_util::codec::Framed`] stream.
    ///
    /// Use this for proxy / gateway relaying that needs `.split()` into
    /// independent read/write halves, or any low-level frame handling the
    /// methods above do not cover. The wrapped stream is identical to the one
    /// this type holds internally.
    pub fn into_inner(self) -> Framed<Box<dyn NetworkStream>, KafkaCodec> {
        self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;
    use std::net::SocketAddr;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};

    /// An in-memory [`NetworkStream`] backed by a `tokio` duplex channel, so
    /// the `KafkaFramed` helpers can be exercised without a real socket.
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

    /// Build a `KafkaFramed` whose remote end is exposed for the test to drive.
    fn duplex_framed() -> (KafkaFramed, DuplexStream) {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let framed = KafkaFramed::new(Framed::new(
            Box::new(DuplexNetwork { inner: client }),
            KafkaCodec::new(),
        ));
        (framed, server)
    }

    #[tokio::test]
    async fn send_then_recv_roundtrips_verbatim() {
        let (mut framed, server) = duplex_framed();
        let payload = Bytes::from_static(&[0x00, 0x03, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x2A]);
        let payload_expected = payload.clone();

        // Broker echoes the exact bytes it received.
        let broker = tokio::spawn(async move {
            let mut framed = Framed::new(server, KafkaCodec::new());
            let req = framed
                .next()
                .await
                .expect("request frame")
                .expect("no error");
            assert_eq!(req.data, payload, "frame must arrive byte-for-byte");
            framed
                .send(KafkaFrame::new(req.data.clone()))
                .await
                .expect("send response");
            framed.flush().await.expect("flush response");
        });

        // send_frame returns without waiting; recv_frame then reads the echo.
        framed
            .send_frame(payload_expected.clone())
            .await
            .expect("send_frame should succeed");
        let echoed = framed.recv_frame().await.expect("recv_frame");
        assert_eq!(echoed, payload_expected, "echo must round-trip verbatim");

        broker.await.expect("broker task");
    }

    #[tokio::test]
    async fn recv_frame_keeps_full_frame_with_header() {
        let (mut framed, server) = duplex_framed();

        // A response whose correlation ID is at bytes [0..4] followed by body.
        let raw = Bytes::from_static(&[0x00, 0x00, 0x00, 0x07, 0x10, 0x20]);
        let raw_expected = raw.clone();
        let broker = tokio::spawn(async move {
            let mut framed = Framed::new(server, KafkaCodec::new());
            framed
                .send(KafkaFrame::new(raw))
                .await
                .expect("send response");
            framed.flush().await.expect("flush response");
        });

        // No send needed: `recv_frame` picks up the broker's frame directly
        // and keeps the full header (length prefix already stripped).
        let frame = framed.recv_frame().await.expect("recv_frame");
        assert_eq!(frame, raw_expected, "recv_frame must keep the full header");
        assert_eq!(
            i32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]),
            7
        );

        broker.await.expect("broker task");
    }
}
