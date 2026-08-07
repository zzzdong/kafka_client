//! 复现探针：连接真实 Java Kafka broker（SunJGSS），走完整 SASL/GSSAPI 握手，
//! 捕获 broker 回的 AP-REP，解密其 enc-part，验证 SunJGSS `0x7b 0x24` 前缀
//! 会导致「只从 offset 0 解析」的旧逻辑复现 issue 里的 `0x7b` ASN.1 错误。
//!
//! 前提：tests/docker-compose.kerberos.yml 正在运行（KDC + GSSAPI Kafka broker）。
//!
//! 单独运行：
//!   KAFKA_BOOTSTRAP_KERBEROS=127.0.0.1:9096 \
//!   KERBEROS_KDC_HOST=localhost KERBEROS_KDC_PORT=8888 \
//!     cargo test --test reproduce_0x7b --features integration_tests -- --nocapture

#![cfg(feature = "integration_tests")]

use bytes::{BufMut, Bytes, BytesMut};
use kafka_client::protocol::{Request, Response};
use kafka_client::protocol::{
    SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse,
};
use krb5_gss::gss::ApReqOptions;
use krb5_gss::gss::GssContext;
use krb5_gss::gss::build_ap_req_token;
use krb5_gss::gss::unwrap_gss_token;
use krb5_gss::kerberos::asn1::PrincipalName;
use krb5_gss::kerberos::asn1::{decode_ap_rep, decode_enc_ap_rep_part};
use krb5_gss::kerberos::crypto;
use krb5_gss::kerberos::messages::KEY_USAGE_AP_REP_ENC_PART;
use krb5_gss::{GssClient, KerberosClient, KerberosCredentials, TokioKdcTransport};

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// 读取一个完整 Kafka 帧：4 字节大端长度前缀 + payload。
async fn read_frame(stream: &mut tokio::net::TcpStream) -> std::io::Result<Bytes> {
    use tokio::io::AsyncReadExt;
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    Ok(Bytes::from(payload))
}

/// 写入一个完整 Kafka 帧。
async fn write_frame(stream: &mut tokio::net::TcpStream, payload: Bytes) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut buf = BytesMut::new();
    buf.put_u32(payload.len() as u32);
    buf.put_slice(&payload);
    stream.write_all(&buf).await?;
    stream.flush().await?;
    Ok(())
}

#[tokio::test]
async fn reproduce_0x7b_sunjgss_prefix() {
    let bootstrap = env_or("KAFKA_BOOTSTRAP_KERBEROS", "127.0.0.1:9096");
    let keytab_path = env_or(
        "KERBEROS_KEYTAB",
        "tests/fixtures/kerberos/keytabs/client.keytab",
    );
    let kdc_host = env_or("KERBEROS_KDC_HOST", "localhost");
    let kdc_port: u16 = std::env::var("KERBEROS_KDC_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8888);

    assert!(
        std::path::Path::new(&keytab_path).exists(),
        "keytab not found at {keytab_path}; is the KDC container running?"
    );

    let creds = KerberosCredentials::new("client@EXAMPLE.COM")
        .with_keytab(&keytab_path)
        .with_realm("EXAMPLE.COM");

    // ── 1. 从 KDC 获取服务票据 (kafka/localhost@EXAMPLE.COM) ──
    let transport = TokioKdcTransport::new(&kdc_host, kdc_port);
    let client = KerberosClient::new(&creds).expect("create KerberosClient");
    let ticket = client
        .acquire_service_ticket(&transport, "kafka/localhost")
        .await
        .expect("acquire service ticket");
    eprintln!(
        "[repro] ticket acquired: session_etype={:?}, session_key_len={}",
        ticket.session_etype,
        ticket.session_key.len()
    );

    // ── 2. 连接 broker ──
    let mut stream = tokio::net::TcpStream::connect(&bootstrap)
        .await
        .expect("connect to broker");
    eprintln!("[repro] connected to {bootstrap}");

    let correlation_id = 1001;

    // ── 3. SASL Handshake (GSSAPI), api_key=17, v1 ──
    let hs_req = SaslHandshakeRequest {
        mechanism: "GSSAPI".to_string(),
    };
    let hs_frame = hs_req
        .encode_frame(1, correlation_id, Some("reprobe".to_string()))
        .expect("encode SaslHandshakeRequest");
    write_frame(&mut stream, hs_frame)
        .await
        .expect("send handshake");
    let hs_resp_raw = read_frame(&mut stream).await.expect("read handshake resp");
    let (_hdr, hs_resp) =
        SaslHandshakeResponse::decode_frame(hs_resp_raw, 1).expect("decode SaslHandshakeResponse");
    assert_eq!(hs_resp.error_code, 0, "SaslHandshake should succeed");
    assert!(
        hs_resp.mechanisms.iter().any(|m| m == "GSSAPI"),
        "broker should advertise GSSAPI"
    );
    eprintln!("[repro] SaslHandshake OK (GSSAPI supported)");

    // ── 4. 构造 AP-REQ GSS token 并发送 (SaslAuthenticate, api_key=36, v2) ──
    let ctime = krb5_gss::kerberos::util::utc_now_with_micros();
    let (ctime_str, cusec) = ctime;
    let cname = PrincipalName {
        name_type: 1,
        name_string: vec!["client".to_string()],
    };
    let opts = ApReqOptions {
        cname: &cname,
        crealm: "EXAMPLE.COM",
        ticket_der: &ticket.ticket_der,
        session_etype: ticket.session_etype,
        session_key: &ticket.session_key,
        ctime: &ctime_str,
        cusec,
        seq_number: Some(1),
        subkey: None,
    };
    let ap_req_token = build_ap_req_token(&opts).expect("build AP-REQ");

    let auth_req = SaslAuthenticateRequest {
        auth_bytes: Bytes::from(ap_req_token),
    };
    let auth_frame = auth_req
        .encode_frame(2, correlation_id + 1, Some("reprobe".to_string()))
        .expect("encode SaslAuthenticateRequest");
    write_frame(&mut stream, auth_frame)
        .await
        .expect("send SaslAuthenticate (AP-REQ)");

    let auth_resp_raw = read_frame(&mut stream)
        .await
        .expect("read authenticate resp");
    let (_hdr, auth_resp) = SaslAuthenticateResponse::decode_frame(auth_resp_raw, 2)
        .expect("decode SaslAuthenticateResponse");
    assert_eq!(auth_resp.error_code, 0, "SaslAuthenticate (AP-REQ) failed");
    let ap_rep = auth_resp.auth_bytes.to_vec();
    eprintln!("[repro] broker AP-REP token: {} bytes", ap_rep.len());

    // ── 5. 解包 GSS wrapper → AP-REP → enc-part 密文 ──
    let inner = unwrap_gss_token(&ap_rep).expect("unwrap GSS InitialContextToken");
    let enc_part = decode_ap_rep(&inner).expect("decode AP-REP");
    eprintln!(
        "[repro] AP-REP enc-part etype={}, cipher_len={}",
        enc_part.etype,
        enc_part.cipher.len()
    );

    // 用 usage 12 (SunJGSS) 解密 enc-part
    let plain = crypto::decrypt(
        ticket.session_etype,
        &ticket.session_key,
        KEY_USAGE_AP_REP_ENC_PART,
        &enc_part.cipher,
    )
    .expect("decrypt AP-REP enc-part with usage 12");
    eprintln!(
        "[repro] decrypted enc-part plaintext: {} bytes",
        plain.len()
    );
    eprintln!(
        "[repro] plaintext head hex: {}",
        hex::encode(&plain[..plain.len().min(16)])
    );

    // ── 6. 复现：明文确实以 [APPLICATION 27] tag (0x7b) 开头 ──
    assert_eq!(
        plain[0], 0x7b,
        "broker 明文首字节应为 0x7b ([APPLICATION 27] tag), 实际 0x{:02x}",
        plain[0]
    );
    assert_ne!(plain[0], 0x30, "旧解析器期望 0x30, 实际 0x7b");
    eprintln!(
        "[repro] >>> 明文以 0x7b ([APPLICATION 27]) 开头 —— 旧解析器把它当裸 SEQUENCE 即报错"
    );

    // ── 7. 验证修复：真正递归剥掉 [APPLICATION 27] 外层，对完整明文一次解析 ──
    let part =
        decode_enc_ap_rep_part(&plain).expect("must parse [APPLICATION 27]-wrapped EncAPRepPart");
    eprintln!(
        "[repro] >>> 完整明文解析成功: ctime={}, cusec={}",
        part.ctime, part.cusec
    );
    assert_eq!(part.ctime, ctime_str, "AP-REP ctime should echo client's");
    assert_eq!(part.cusec, cusec, "AP-REP cusec should echo client's");

    eprintln!(
        "[repro] SUCCESS: broker(Java/SunJGSS) 的 AP-REP enc-part 明文以 [APPLICATION 27] (0x7b) \
         开头;\n        decode_enc_ap_rep_part 真正剥掉外层包装后正确解析 ctime/cusec 回显验证通过。"
    );
}

/// 用户场景：直接用 `GssClient` + `step()` 走完整 GSSAPI 握手（而非经 `kafka_client`）。
///
/// 修复后（移除不严格模式 + 真正解析 `[APPLICATION 27]`），验证在真实 SunJGSS broker
/// 上**严格双向认证能通过** —— 证明用户不会因为「禁止不严格」而认证失败。
#[tokio::test]
async fn user_scenario_b_direct_gssclient_passes() {
    let bootstrap = env_or("KAFKA_BOOTSTRAP_KERBEROS", "127.0.0.1:9096");
    let keytab_path = env_or(
        "KERBEROS_KEYTAB",
        "tests/fixtures/kerberos/keytabs/client.keytab",
    );
    let kdc_host = env_or("KERBEROS_KDC_HOST", "localhost");
    let kdc_port: u16 = std::env::var("KERBEROS_KDC_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8888);

    assert!(
        std::path::Path::new(&keytab_path).exists(),
        "keytab not found at {keytab_path}"
    );

    let creds = KerberosCredentials::new("client@EXAMPLE.COM")
        .with_keytab(&keytab_path)
        .with_realm("EXAMPLE.COM");

    // ── 用 GssClient 直接驱动握手（用户场景 B）──
    let gss = GssClient::new(&creds, &kdc_host, kdc_port).expect("create GssClient");
    let mut ctx = gss
        .context_for("kafka/localhost")
        .await
        .expect("context_for");

    let mut stream = tokio::net::TcpStream::connect(&bootstrap)
        .await
        .expect("connect broker");
    let corr = 2001;

    // SASL Handshake (GSSAPI)
    let hs_req = SaslHandshakeRequest {
        mechanism: "GSSAPI".to_string(),
    };
    let hs_frame = hs_req
        .encode_frame(1, corr, Some("scenario_b".to_string()))
        .expect("encode handshake");
    write_frame(&mut stream, hs_frame)
        .await
        .expect("send handshake");
    let hs_raw = read_frame(&mut stream).await.expect("read handshake resp");
    let (_h, hs_resp) =
        SaslHandshakeResponse::decode_frame(hs_raw, 1).expect("decode handshake resp");
    assert_eq!(hs_resp.error_code, 0, "handshake should succeed");

    // ── 第 1 轮: AP-REQ 经 step(None) 获得, 发送后收 AP-REP ──
    let ap_req = ctx
        .step(None)
        .expect("first step")
        .expect("first token (AP-REQ)");
    assert!(!ctx.is_complete(), "not complete after AP-REQ");

    let auth_req = SaslAuthenticateRequest {
        auth_bytes: Bytes::from(ap_req),
    };
    let auth_frame = auth_req
        .encode_frame(2, corr + 1, Some("scenario_b".to_string()))
        .expect("encode authenticate");
    write_frame(&mut stream, auth_frame)
        .await
        .expect("send authenticate (AP-REQ)");
    let auth_raw = read_frame(&mut stream)
        .await
        .expect("read authenticate resp");
    let (_h, auth_resp) =
        SaslAuthenticateResponse::decode_frame(auth_raw, 2).expect("decode authenticate resp");
    assert_eq!(
        auth_resp.error_code, 0,
        "authenticate (AP-REQ) should succeed"
    );
    let ap_rep = auth_resp.auth_bytes.to_vec();

    // ── 第 2 轮: 处理 AP-REP（内部严格验证 [APPLICATION 27] + ctime/cusec 回显）──
    let round2_token = ctx
        .step(Some(&ap_rep))
        .expect("handle AP-REP under strict verification");
    eprintln!(
        "[scenario_b] AP-REP handled OK, complete={}, next_len={}",
        ctx.is_complete(),
        round2_token.as_ref().map(|t| t.len()).unwrap_or(0)
    );
    let round2_req = SaslAuthenticateRequest {
        auth_bytes: round2_token.map(Bytes::from).unwrap_or_default(),
    };
    write_frame(
        &mut stream,
        round2_req
            .encode_frame(2, corr + 2, Some("scenario_b".to_string()))
            .expect("encode round2"),
    )
    .await
    .expect("send round2");
    let round2_raw = read_frame(&mut stream).await.expect("read round2 resp");
    let (_h, round2_resp) =
        SaslAuthenticateResponse::decode_frame(round2_raw, 2).expect("decode round2 resp");
    assert_eq!(round2_resp.error_code, 0, "round2 should succeed");

    // ── 第 3 轮: WRAP exchange ──
    let wrap_challenge = round2_resp.auth_bytes.to_vec();
    if !wrap_challenge.is_empty() {
        let final_token = ctx
            .step(Some(&wrap_challenge))
            .expect("handle WRAP challenge under strict verification");
        let final_req = SaslAuthenticateRequest {
            auth_bytes: final_token.map(Bytes::from).unwrap_or_default(),
        };
        write_frame(
            &mut stream,
            final_req
                .encode_frame(2, corr + 3, Some("scenario_b".to_string()))
                .expect("encode final"),
        )
        .await
        .expect("send final");
        let final_raw = read_frame(&mut stream).await.expect("read final resp");
        let (_h, final_resp) =
            SaslAuthenticateResponse::decode_frame(final_raw, 2).expect("decode final resp");
        assert_eq!(final_resp.error_code, 0, "final round should succeed");
    }

    assert!(
        ctx.is_complete(),
        "context must be established after strict AP-REP verification"
    );
    eprintln!("[scenario_b] SUCCESS: 直接 GssClient 路径在严格验证下通过真实 broker 认证");
}
