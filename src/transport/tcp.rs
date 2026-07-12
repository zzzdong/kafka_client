use super::NetworkStream;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

/// TCP network stream.
pub struct TcpNetworkStream {
    inner: TcpStream,
}

impl TcpNetworkStream {
    /// Establish a plain TCP connection to the given address.
    pub async fn connect(addr: SocketAddr) -> io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self { inner: stream })
    }

    /// Create a stream from an existing `TcpStream` (mainly for tests).
    pub fn from_stream(stream: TcpStream) -> Self {
        Self { inner: stream }
    }
}

// AsyncRead implementation
impl AsyncRead for TcpNetworkStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

// AsyncWrite implementation
impl AsyncWrite for TcpNetworkStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

// NetworkStream implementation
impl NetworkStream for TcpNetworkStream {
    fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.inner.peer_addr()
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    fn is_secure(&self) -> bool {
        false
    }
}
