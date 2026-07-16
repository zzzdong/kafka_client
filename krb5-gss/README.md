# krb5-gss

Pure Rust Kerberos protocol engine and krb5 GSS-API mechanism (initiator), zero FFI.

Cross-compiles to any target — no system Kerberos libraries required. All cryptography
is built on RustCrypto primitives (`aes`, `cts`, `sha1`, `sha2`, `hmac`, `pbkdf2`).

## Features

### Supported

| Category | Feature | RFC | Status |
|----------|---------|-----|--------|
| **KDC protocol** | AS-REQ / AS-REP (keytab-based, no PKINIT) | 4120 | ✅ |
| | TGS-REQ / TGS-REP | 4120 | ✅ |
| | Keytab file parsing (krb5 FILE v2 format) | — | ✅ |
| **Encryption** | AES-128-CTS-HMAC-SHA1-96 (etype 17) | 3962 | ✅ |
| | AES-256-CTS-HMAC-SHA1-96 (etype 18) | 3962 | ✅ |
| | AES-128-CTS-HMAC-SHA256-128 (etype 19) | 8009 | ✅ |
| | AES-256-CTS-HMAC-SHA384-192 (etype 20) | 8009 | ✅ |
| **GSS-API** | AP-REQ / AP-REP handshake (mutual auth) | 4121 | ✅ |
| | GSS_Init_sec_context via `step()` loop | 2743 | ✅ |
| | GSS_Wrap (integrity + confidentiality) | 4121 | ✅ |
| | GSS_Unwrap (replay detection, seq numbers) | 4121 | ✅ |
| | GSS_GetMIC / GSS_VerifyMIC (sign-only) | 4121 | ✅ |
| | GSS_Wrap_size_limit | 2743 | ✅ |
| | GSS_Context_time | 2743 | ✅ |
| | Subkey negotiation (per-message key rotation) | 4121 | ✅ |
| | Channel binding checksum (0x8003) | 4121 | ✅ |
| **RFC compliance** | Known-answer tests (HMAC, PBKDF2, nfold, string-to-key) | 2202/6070/3961/3962 | ✅ |
| | AES-CTS test vectors (RFC 3962 Appendix B) | 3962 | ✅ |
| | KDF test vectors (RFC 8009 Appendix A) | 8009 | ✅ |
| | Python-cryptography cross-validation script | — | ✅ |

### Not supported (deprecated per RFC 8429)

| Etype | Algorithm | Reason |
|-------|-----------|--------|
| 1/3 | DES-CBC-* | Single-DES prohibited by NIST |
| 16 | DES3-CBC-SHA1-kd | Triple-DES, deprecated |
| 23 | RC4-HMAC (arcfour-hmac) | RC4 cryptographically broken |

## Quick Start

### Acquire a service ticket from the KDC

```no_run
# async fn run() -> krb5_gss::Result<()> {
use krb5_gss::{KerberosCredentials, Krb5Client};

let creds = KerberosCredentials::new("client@EXAMPLE.COM")
    .with_keytab("/path/krb5.keytab");

let client = Krb5Client::new(&creds, "kdc.example.com", 88)?;
let ticket = client.acquire_service_ticket("kafka/broker.example.com").await?;
# let _ = ticket; Ok(())
# }
```

### GSS-API handshake (SASL/GSSAPI)

```no_run
# async fn run() -> krb5_gss::Result<()> {
use krb5_gss::{KerberosCredentials, GssClient, gss::GssContext};

let creds = KerberosCredentials::new("client@EXAMPLE.COM")
    .with_keytab("/path/krb5.keytab");

let gss = GssClient::new(&creds, "kdc.example.com", 88)?;
let mut ctx = gss.context_for("kafka/broker.example.com").await?;

// Standard GSS init_sec_context loop
let mut out = ctx.step(None)?.unwrap();  // AP-REQ token
// Send `out` to the acceptor, receive `resp`:
// while let Some(tok) = ctx.step(Some(&resp))? { out = tok; /* send again */ }
# let _ = out; Ok(())
# }
```

## Architecture

```
krb5-gss
├── credentials.rs    KerberosCredentials — principal, keytab, service config
├── client.rs         Krb5Client — high-level KDC facade (owns transport)
├── gss.rs            GSS-API mechanism: GssClient, GssContext, WRAP/UNWRAP/MIC
├── error.rs          KerberosError, Result
└── kerberos/
    ├── client.rs     KerberosClient — AS-REQ/TGS-REQ state machine
    ├── transport.rs  KdcTransport trait + TokioKdcTransport (TCP)
    ├── crypto.rs     Etype enum, AES encrypt/decrypt (RFC 3961/3962/8009), KDF
    ├── asn1.rs       DER encode/decode for Kerberos ASN.1 messages
    ├── keytab.rs     Keytab file parser (FILE v2)
    ├── messages.rs   Protocol constants and key usage values
    └── util.rs       Time helpers (GeneralizedTime), timestamps
```

### Key types

| Type | Description |
|------|-------------|
| `KerberosCredentials` | Builder for principal, keytab path, realm, service name |
| `Krb5Client` | High-level KDC client — owns credentials + transport |
| `KerberosClient` | Low-level KDC state machine — inject `&dyn KdcTransport` per call |
| `AcquiredTicket` | Service ticket + session key from the KDC (with endtime) |
| `KdcTransport` | Trait for KDC network I/O (implement for non-tokio runtimes) |
| `TokioKdcTransport` | Built-in TCP KDC transport using tokio `TcpStream` |
| `GssClient` | High-level GSS facade: KDC ticket + GSS context |
| `GssContext` | Trait for standard GSS `init_sec_context` step loop |
| `Etype` | Supported encryption types (17, 18, 19, 20) |

### Custom transport

For non-tokio runtimes or non-TCP KDC access (e.g., UDP, Unix sockets, proxy):

```no_run
use krb5_gss::kerberos::transport::{KdcTransport, KdcBoxFuture};
use krb5_gss::kerberos::client::KerberosClient;
use krb5_gss::KerberosCredentials;

struct MyTransport;

impl KdcTransport for MyTransport {
    fn exchange(&self, realm: &str, req: &[u8]) -> KdcBoxFuture<'_, Vec<u8>> {
        // send `req` to the KDC for `realm`, return the response bytes
        todo!()
    }
}

# async fn run() -> krb5_gss::Result<()> {
let creds = KerberosCredentials::new("client@EXAMPLE.COM").with_keytab("/path/krb5.keytab");
let client = KerberosClient::new(&creds)?;
let transport = MyTransport;
let ticket = client.acquire_service_ticket(&transport, "kafka/broker.example.com").await?;
# let _ = ticket; Ok(())
# }
```

## Test coverage

All cryptographic primitives are verified against RFC known-answer tests:

| Test suite | Vectors | Reference |
|------------|---------|-----------|
| HMAC-SHA1 KAT | RFC 2202 test cases 1, 2, 4 | 2104 |
| PBKDF2-HMAC-SHA1 KAT | RFC 6070 test cases (c=1,2,4096) | 2898 |
| n-fold KAT | RFC 3961 Appendix A (4 vectors) | 3961 |
| string-to-key KAT | RFC 3962 Appendix B (c=1200) | 3962 |
| AES-128-CTS encryption | RFC 3962 Appendix B (17/31/32/64 bytes) | 3962 |
| KDF-HMAC-SHA2 KAT | RFC 8009 Appendix A (Kc/Ke/Ki × 2 etypes) | 8009 |
| GSS roundtrip | AP-REQ, AP-REP, WRAP, MIC | 4121 |
| Sequence replay detection | Strict seq number ordering | 4121 |
| Python cross-validation | Independent verification with `python-cryptography` | — |

91 unit tests, 0 failures.

## License

Apache-2.0 OR MIT
