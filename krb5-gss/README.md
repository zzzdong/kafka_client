# krb5-gss

Pure Rust Kerberos protocol engine and krb5 GSS-API mechanism (initiator), no FFI.

## Overview

This crate provides two independent capabilities for interacting with a Kerberos KDC:

1. **Kerberos KDC protocol engine** — AS-REQ / AS-REP / TGS-REQ / TGS-REP state machine that acquires service tickets directly from a KDC.
2. **krb5 GSS-API mechanism (initiator)** — RFC 1964 / RFC 4121 token construction (AP-REQ, AP-REP, WRAP/UNWRAP) for SASL/GSSAPI authentication with any server-first acceptor (Kafka, LDAP, etc.).

No FFI, no system Kerberos libraries. All cryptography is built on RustCrypto primitives (`aes`, `hmac`, `sha1`, `sha2`), so the crate cross-compiles to any target without native dependencies.

## Supported Features

| Feature | Status |
|---------|--------|
| AS-REQ / AS-REP (PKINIT-free, keytab-based) | ✅ |
| TGS-REQ / TGS-REP | ✅ |
| AES-128-CTS-HMAC-SHA1-96 (etype 17) | ✅ |
| AES-256-CTS-HMAC-SHA1-96 (etype 18) | ✅ |
| AES-128-CTS-HMAC-SHA256-128 (etype 19) | ✅ |
| AES-256-CTS-HMAC-SHA384-192 (etype 20) | ✅ |
| GSS AP-REQ / AP-REP handshake | ✅ |
| RFC 4121 WRAP (integrity + confidentiality) | ✅ |
| Keytab parsing (krb5 format) | ✅ |

## Quick Start

### Acquire a service ticket

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
// send `out` to the acceptor, receive `resp`:
// while let Some(tok) = ctx.step(Some(&resp))? { out = tok; /* send */ }
# let _ = out; Ok(())
# }
```

## Architecture

```
krb5-gss
├── credentials.rs     KerberosCredentials — principal / keytab / service config
├── client.rs          Krb5Client — high-level KDC facade (owns transport)
├── gss.rs             GSS-API mechanism: GssClient, GssContext, WRAP/UNWRAP
├── error.rs           KerberosError / Result
└── kerberos/
    ├── client.rs      KerberosClient — AS-REQ/TGS-REQ state machine
    ├── transport.rs   KdcTransport trait + TokioKdcTransport (TCP)
    ├── crypto.rs      Etype enum + AES encryption/decryption (RFC 3961/3962)
    ├── asn1.rs        DER encode/decode for Kerberos messages
    ├── keytab.rs      Keytab file parser
    ├── messages.rs    Protocol constants and key usage values
    └── util.rs        Time helpers for Kerberos timestamps
```

### Key types

| Type | Description |
|------|-------------|
| `KerberosCredentials` | Builder for principal, keytab path, realm, service name |
| `Krb5Client` | High-level KDC client — owns credentials + transport |
| `KerberosClient` | Low-level protocol state machine — inject `&dyn KdcTransport` per call |
| `KdcTransport` | Trait for KDC network I/O (implement for non-tokio runtimes) |
| `TokioKdcTransport` | Built-in TCP transport using tokio |
| `AcquiredTicket` | Service ticket + session key returned by the KDC |
| `GssClient` | High-level GSS facade — combines KDC ticket acquisition + GSS context |
| `GssContext` | Trait for the standard GSS `init_sec_context` step loop |
| `Etype` | Supported Kerberos encryption types |

### Custom transport

For non-tokio runtimes or non-TCP KDC access (e.g., UDP, proxy), implement `KdcTransport`:

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

## License

Apache-2.0 OR MIT
