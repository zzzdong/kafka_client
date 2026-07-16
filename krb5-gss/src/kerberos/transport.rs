//! KDC transport contract (implemented by the integrator; the crate does not touch sockets).
//!
//! `krb5-gss` is a Kerberos protocol engine + krb5 GSS-API mechanism: ASN.1 encoding/decoding,
//! cryptography, and the AP-REQ/AP-REP and AS-REQ/TGS-REQ state machines all live inside the crate.
//! Network I/O is injected via this trait. The crate ships a built-in [`TokioKdcTransport`]
//! based on tokio `TcpStream` for out-of-the-box use, but integrators may implement this trait
//! to reuse their own TCP/DNS/proxy stack, or even do KDC forwarding or custom krb5.conf parsing.
//! Both implementations can be fully unit-tested offline (using fake implementations like
//! [`MockKdcTransport`] to drive the state machine).
//!
//! Frame format convention: `exchange` receives `req` as ASN.1 DER-encoded AS-REQ / TGS-REQ bytes,
//! and should return the AS-REP / TGS-REP response body **without** the 4-byte big-endian length
//! prefix. The 4-byte length framing (Kerberos TCP, RFC 4120 §7.2.2) is handled inside `exchange`.

use crate::error::Result;
use std::future::Future;
use std::pin::Pin;

/// Maximum accepted KDC response body size (16 MiB) to guard against an unbounded
/// allocation (and thus a DoS) if a (malicious/faulty) KDC returns a huge length prefix.
const MAX_KDC_RESPONSE_LEN: usize = 16 * 1024 * 1024;

/// Boxed future: allows `KdcTransport` to be used as a `dyn` trait object without extra dependencies.
///
/// The output is the KDC response body (`AS-REP` / `TGS-REP`) without the 4-byte length prefix.
pub type KdcBoxFuture<'a> = Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send + 'a>>;

/// KDC transport contract.
///
/// Integrators implement this trait to provide real KDC network I/O. A typical `exchange`
/// implementation should:
/// 1. Resolve `kdc_realm` to a KDC address (default port 88, via DNS SRV / krb5.conf);
/// 2. Send `req` using the Kerberos TCP frame format (4-byte big-endian length prefix + body),
///    then read the 4-byte length prefix and the corresponding body;
/// 3. Return the response body **without** the 4-byte length prefix.
pub trait KdcTransport: Send + Sync {
    /// Send `req` (AS-REQ / TGS-REQ) and return the corresponding KDC response body (AS-REP / TGS-REP).
    ///
    /// `kdc_realm` is used for addressing; ownership of `req` bytes is not transferred —
    /// implementations that need to persist the data should call `to_vec()`.
    fn exchange<'a>(&'a self, kdc_realm: &'a str, req: &'a [u8]) -> KdcBoxFuture<'a>;
}

// ---------------------------------------------------------------------------
// TokioKdcTransport — built-in KDC transport based on tokio TcpStream
// ---------------------------------------------------------------------------

cfg_if::cfg_if! {
if #[cfg(feature = "tokio-transport")] {

/// KDC transport implementation based on tokio TcpStream (provided by default).
///
/// Connects to the KDC via TCP (host:port), using the Kerberos TCP frame format (RFC 4120 §7.2.2):
/// sends a 4-byte big-endian length prefix + request body, then reads a 4-byte big-endian length
/// prefix + response body.
pub struct TokioKdcTransport {
    host: String,
    port: u16,
}

impl TokioKdcTransport {
    /// Create a new `TokioKdcTransport`.
    pub fn new(host: &str, port: u16) -> Self {
        Self {
            host: host.to_string(),
            port,
        }
    }
}

impl KdcTransport for TokioKdcTransport {
    fn exchange<'a>(&'a self, _kdc_realm: &'a str, req: &'a [u8]) -> KdcBoxFuture<'a> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let host = self.host.clone();
        let port = self.port;
        let req = req.to_vec();

        Box::pin(async move {
            let addr = format!("{}:{}", host, port);
            let mut stream = tokio::net::TcpStream::connect(&addr).await.map_err(|e| {
                crate::error::KerberosError::Protocol(format!("KDC TCP connect to {addr}: {e}"))
            })?;

            let mut framed = (req.len() as u32).to_be_bytes().to_vec();
            framed.extend_from_slice(&req);
            stream.write_all(&framed).await.map_err(|e| {
                crate::error::KerberosError::Protocol(format!(
                    "KDC send failed ({}:{}): {e}",
                    host, port
                ))
            })?;

            let mut len_buf = [0u8; 4];
            stream.read_exact(&mut len_buf).await.map_err(|e| {
                let msg = format!("KDC read length failed: {e} (sent {} bytes)", req.len());
                crate::error::KerberosError::Protocol(msg)
            })?;
            let resp_len = u32::from_be_bytes(len_buf) as usize;
            if resp_len > MAX_KDC_RESPONSE_LEN {
                return Err(crate::error::KerberosError::Protocol(format!(
                    "KDC response too large: {resp_len} bytes (max {MAX_KDC_RESPONSE_LEN})"
                )));
            }

            let mut resp = vec![0u8; resp_len];
            stream.read_exact(&mut resp).await.map_err(|e| {
                crate::error::KerberosError::Protocol(format!("KDC read response failed: {e}"))
            })?;

            Ok(resp)
        })
    }
}

} // cfg(feature = "tokio-transport")
} // cfg_if

#[cfg(test)]
pub(crate) mod test_util {
    //! Fake KDC transport for unit/integration tests: records calls and returns preset responses.
    use super::*;
    use std::sync::Mutex;

    /// Records the last call's realm / req, and returns the preset `response`.
    pub struct MockKdcTransport {
        pub last_realm: Mutex<Option<String>>,
        pub last_req: Mutex<Option<Vec<u8>>>,
        pub response: Vec<u8>,
    }

    impl MockKdcTransport {
        pub fn new(response: Vec<u8>) -> Self {
            Self {
                last_realm: Mutex::new(None),
                last_req: Mutex::new(None),
                response,
            }
        }
    }

    impl KdcTransport for MockKdcTransport {
        fn exchange<'a>(&'a self, kdc_realm: &'a str, req: &'a [u8]) -> KdcBoxFuture<'a> {
            Box::pin(async move {
                *self.last_realm.lock().unwrap() = Some(kdc_realm.to_string());
                *self.last_req.lock().unwrap() = Some(req.to_vec());
                Ok(self.response.clone())
            })
        }
    }

    /// Runtime-free `block_on`: used in tests to drive `KdcTransport`'s async `exchange`.
    /// Integrators should use their own async runtime (e.g. tokio); this is for crate-internal unit tests only.
    pub fn block_on<F: Future>(mut f: F) -> F::Output {
        use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
        fn noop(_: *const ()) {}
        fn clone(_: *const ()) -> RawWaker {
            RawWaker::new(std::ptr::null(), &VTABLE)
        }
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
        let raw = RawWaker::new(std::ptr::null(), &VTABLE);
        let waker = unsafe { Waker::from_raw(raw) };
        let mut cx = Context::from_waker(&waker);
        let mut f = unsafe { std::pin::Pin::new_unchecked(&mut f) };
        loop {
            if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
                return v;
            }
        }
    }
}
