//! Kerberos client: KDC ticket acquisition state machine (AS-REQ → TGS-REQ) + long-term key (keytab).
//!
//! This module is a **pure protocol state machine**: it builds AS-REQ / TGS-REQ via ASN.1,
//! parses AS-REP / TGS-REP, decrypts the TGT using the long-term key from the keytab, and
//! decrypts the service ticket using the TGT session key. **All network I/O is delegated to
//! the integrator via [`crate::kerberos::transport::KdcTransport`]**.
use crate::credentials::KerberosCredentials;
use crate::error::{KerberosError, Result};
use crate::kerberos::asn1::{
    Authenticator, EncryptedData, KdcReqBody, PaData, PrincipalName, decode_enc_kdc_rep_part,
    decode_kdc_rep, decode_krb_error, encode_ap_req, encode_authenticator, encode_enc_data,
    encode_kdc_req, encode_kdc_req_body, encode_pa_data,
};
use crate::kerberos::crypto::{self, Etype};
use crate::kerberos::keytab::Keytab;
use crate::kerberos::messages::{
    KEY_USAGE_AS_REP_ENC_PART, KEY_USAGE_PA_ENC_TIMESTAMP, KEY_USAGE_TGS_REP_ENC_PART,
    KEY_USAGE_TGS_REQ_PA_TGS_REQ, MSG_AS_REQ, MSG_TGS_REQ, NT_PRINCIPAL, NT_SRV_HST, NT_SRV_INST,
    PADATA_ENC_TIMESTAMP, PADATA_TGS_REQ, PVNO, TAG_AS_REP, TAG_AS_REQ, TAG_TGS_REP, TAG_TGS_REQ,
};
use crate::kerberos::transport::KdcTransport;

/// PA-PAC-REQUEST padata value: `SEQUENCE { [0] include-pac BOOLEAN TRUE }`.
/// DER encoding: `30 05 a0 03 01 01 ff`. Sent in AS-REQ/TGS-REQ to ask the KDC to
/// include a PAC in the reply (required by Java/Kafka acceptors).
const PAC_REQUEST_VALUE: &[u8] = &[0x30, 0x05, 0xa0, 0x03, 0x01, 0x01, 0xff];

/// Service ticket + session key acquired from the KDC.
#[derive(Clone)]
pub struct AcquiredTicket {
    pub ticket_der: Vec<u8>,
    pub session_etype: Etype,
    pub session_key: Vec<u8>,
    pub cname: PrincipalName,
    pub crealm: String,
    /// End time of the service ticket (GeneralizedTime `YYYYMMDDHHMMSSZ`).
    /// Used by GSS_Context_time to report remaining context lifetime.
    pub endtime: String,
}

/// Temporary holder for the TGT decrypted from AS-REP (input to TGS-REQ).
struct Tgt {
    ticket_der: Vec<u8>,
    session_key: Vec<u8>,
    session_etype: Etype,
    /// TGS service sname (krbtgt/REALM).
    #[allow(dead_code)]
    sname: PrincipalName,
    #[allow(dead_code)]
    crealm: String,
}

pub struct KerberosClient {
    #[allow(dead_code)]
    creds: KerberosCredentials,
    keytab: Keytab,
    #[allow(dead_code)]
    etype: Etype,
    /// Client realm (used for KDC addressing).
    realm: String,
    /// Client principal name (used as cname in AS-REQ / AP-REQ).
    cname: PrincipalName,
}

impl KerberosClient {
    pub fn new(creds: &KerberosCredentials) -> Result<Self> {
        let (name, realm) = creds.split();
        let keytab = match &creds.keytab_path {
            Some(path) => Keytab::parse_file(path)?,
            None => {
                return Err(KerberosError::InvalidCredential(
                    "keytab_path required (ccache not yet supported)".into(),
                ));
            }
        };
        let etype = Etype::Aes256CtsHmacSha196;
        let cname = PrincipalName {
            name_type: NT_PRINCIPAL,
            name_string: vec![name],
        };
        Ok(Self {
            creds: creds.clone(),
            keytab,
            etype,
            realm,
            cname,
        })
    }

    /// Create a `KerberosClient` for testing (no keytab required).
    /// The keytab is set to an empty default, so only operations that do not
    /// require keytab lookups (e.g. constructing AP-REQ with pre-acquired tickets)
    /// will succeed.
    pub fn new_for_test(creds: &KerberosCredentials) -> Result<Self> {
        let (name, realm) = creds.split();
        let cname = PrincipalName {
            name_type: NT_PRINCIPAL,
            name_string: vec![name],
        };
        Ok(Self {
            creds: creds.clone(),
            keytab: Keytab::default(),
            etype: Etype::Aes256CtsHmacSha196,
            realm,
            cname,
        })
    }

    /// Acquire a service ticket + session key from the KDC (AS-REQ + TGS-REQ).
    pub async fn acquire_service_ticket(
        &self,
        transport: &dyn KdcTransport,
        service: &str,
    ) -> Result<AcquiredTicket> {
        let tgt = self.as_req_step(transport).await?;

        let tgs_req = self.build_tgs_req(&tgt, service)?;
        let tgs_rep = transport.exchange(&self.realm, &tgs_req).await?;
        // 检查 KRB-ERROR
        if tgs_rep.first().copied() == Some(0x7E) {
            if let Ok(err) = decode_krb_error(&tgs_rep) {
                return Err(KerberosError::Protocol(format!(
                    "TGS KDC error code={}",
                    err.error_code
                )));
            } else {
                return Err(KerberosError::Protocol(
                    "TGS KRB-ERROR decode failed".into(),
                ));
            }
        }
        self.parse_tgs_rep(&tgs_rep, tgt.session_etype, &tgt.session_key)
    }

    /// AS-REQ: 先无 padata 发送，如果 AS-REP 则成功；
    /// 如果 KRB-ERROR (PREAUTH_REQUIRED) 则回退到带 PA-ENC-TIMESTAMP 的请求
    async fn as_req_step(&self, transport: &dyn KdcTransport) -> Result<Tgt> {
        let entry = self
            .keytab
            .find(&self.cname.name_string.join("/"), &self.realm)
            .ok_or_else(|| {
                KerberosError::Keytab(format!(
                    "no keytab entry for {}@{}",
                    self.cname.name_string.join("/"),
                    self.realm
                ))
            })?;

        // 1. 发送 AS-REQ（带 PAC-REQUEST padata，同 kinit 行为）
        let req1 = self.build_as_req()?;
        let resp = transport.exchange(&self.realm, &req1).await?;
        let fb = resp.first().copied().unwrap_or(0);
        if fb == TAG_AS_REP {
            return self.parse_as_rep_inner(&resp);
        }
        if fb == 0x7E {
            let err = decode_krb_error(&resp)
                .map_err(|_| KerberosError::Protocol("KRB-ERROR parse failed".into()))?;
            if err.error_code != 25 {
                return Err(KerberosError::Protocol(format!(
                    "KDC error code={}",
                    err.error_code
                )));
            }
            let req2 = self.build_as_req_auth(entry.etype, &entry.key, entry.kvno)?;
            let resp2 = transport.exchange(&self.realm, &req2).await?;
            let fb2 = resp2.first().copied().unwrap_or(0);
            if fb2 == TAG_AS_REP {
                return self.parse_as_rep_inner(&resp2);
            }
            if fb2 == 0x7E
                && let Ok(e2) = decode_krb_error(&resp2)
            {
                return Err(KerberosError::Protocol(format!(
                    "KDC error code={}",
                    e2.error_code
                )));
            }
            return Err(KerberosError::Protocol(
                "KRB-ERROR after PA-ENC-TIMESTAMP retry".into(),
            ));
        }
        Err(KerberosError::Protocol(format!(
            "unexpected first response tag 0x{fb:02x}",
        )))
    }

    /// 构建 AS-REQ body (无 preauth / 有 preauth 版本共用 body 部分)。
    fn build_as_req_body(&self) -> Vec<u8> {
        let till = chrono_like_add_days(1);
        let rtime = chrono_like_add_days(7);
        // 使用真正的随机 nonce 避免 KDC 回放缓存
        let nonce: i32 = random_nonce();
        // 同 kinit: forwardable=bit1, renewable=bit8
        let kdc_options = [0x40u8, 0x80, 0x00, 0x00];
        let body = KdcReqBody {
            kdc_options,
            cname: Some(self.cname.clone()),
            realm: self.realm.clone(),
            sname: Some(PrincipalName {
                name_type: NT_SRV_INST,
                name_string: vec!["krbtgt".to_string(), self.realm.clone()],
            }),
            from: None,
            till,
            rtime: Some(rtime),
            nonce,
            etype: vec![20, 19, 18, 17],
            addresses: None,
        };
        encode_kdc_req_body(&body)
    }

    /// 构建 AS-REQ (带 PA-PAC-REQUEST padata，对齐 kinit 行为)。
    fn build_as_req(&self) -> Result<Vec<u8>> {
        let body_der = self.build_as_req_body();
        // PA-PAC-REQUEST (type=128): ask the KDC to include a PAC in the AS-REP (TGT).
        let pa = PaData {
            padata_type: 128,
            padata_value: PAC_REQUEST_VALUE.to_vec(),
        };
        let padasta_der = crate::kerberos::asn1::tlv_seq(&encode_pa_data(&pa));
        Ok(encode_kdc_req(
            PVNO,
            MSG_AS_REQ,
            TAG_AS_REQ,
            Some(&padasta_der),
            &body_der,
        ))
    }

    /// 第二步: 带 PA-ENC-TIMESTAMP 的认证 AS-REQ (收到 preauth 需求后调用)。
    fn build_as_req_auth(&self, etype: Etype, key: &[u8], kvno: u8) -> Result<Vec<u8>> {
        let now_str = chrono_like_now();
        let pa_enc_ts_plain = {
            let mut inner = Vec::new();
            inner.extend(crate::kerberos::asn1::tlv_ctx(
                0,
                &crate::kerberos::asn1::tlv(0x18, now_str.as_bytes()),
            ));
            inner.extend(crate::kerberos::asn1::ctx_int(1, 0));
            crate::kerberos::asn1::tlv_seq(&inner)
        };
        let cipher = crypto::encrypt(etype, key, KEY_USAGE_PA_ENC_TIMESTAMP, &pa_enc_ts_plain)?;
        let enc = EncryptedData {
            etype: etype as i32,
            kvno: Some(kvno as i32),
            cipher,
        };
        let pa = PaData {
            padata_type: PADATA_ENC_TIMESTAMP,
            padata_value: encode_enc_data(&enc),
        };
        let padasta_der = crate::kerberos::asn1::tlv_seq(&encode_pa_data(&pa));
        let body_der = self.build_as_req_body();
        Ok(encode_kdc_req(
            PVNO,
            MSG_AS_REQ,
            TAG_AS_REQ,
            Some(&padasta_der),
            &body_der,
        ))
    }

    /// 解析 AS-REP/KDC-REP 中的 enc-part, 返回 TGT。
    fn parse_as_rep_inner(&self, as_rep: &[u8]) -> Result<Tgt> {
        let kdc_rep = decode_kdc_rep(as_rep, TAG_AS_REP)?;
        let enc_etype = Etype::from_u32(kdc_rep.enc_part.etype as u32).ok_or_else(|| {
            KerberosError::Protocol(format!("unsupported enc etype {}", kdc_rep.enc_part.etype))
        })?;
        // 尝试所有 keytab 条目
        let entries: Vec<_> = self
            .keytab
            .entries
            .iter()
            .filter(|e| e.principal == self.cname.name_string.join("/") && e.realm == self.realm)
            .collect();
        if entries.is_empty() {
            return Err(KerberosError::Keytab(format!(
                "no keytab entry for {}@{}",
                self.cname.name_string.join("/"),
                self.realm
            )));
        }
        let mut last_err = None;
        for entry in &entries {
            match crypto::decrypt(
                enc_etype,
                &entry.key,
                KEY_USAGE_AS_REP_ENC_PART,
                &kdc_rep.enc_part.cipher,
            ) {
                Ok(plain) => {
                    let enc_part = decode_enc_kdc_rep_part(&plain)?;
                    let session_key = enc_part.key.keyvalue;
                    let session_etype =
                        Etype::from_u32(enc_part.key.keytype as u32).ok_or_else(|| {
                            KerberosError::Protocol(format!(
                                "unsupported session etype {}",
                                enc_part.key.keytype
                            ))
                        })?;
                    return Ok(Tgt {
                        ticket_der: kdc_rep.ticket_der,
                        session_key,
                        session_etype,
                        sname: enc_part.sname,
                        crealm: enc_part.srealm,
                    });
                }
                Err(e) => {
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| KerberosError::Crypto("all keys failed".into())))
    }

    /// 构建 TGS-REQ: 用 TGT 向 TGS 服务请求 `service` 的服务票据。
    fn build_tgs_req(&self, tgt: &Tgt, service: &str) -> Result<Vec<u8>> {
        // 1) 先构建 KDC-REQ-BODY (需要 body_der 做 checksum)
        let (sname_type, sname_comps) = parse_service_name(service);
        let till = chrono_like_add_days(1);
        let nonce: i32 = random_nonce();
        let body = KdcReqBody {
            // canonicalize=bit16 (MIT krb5 KDC_OPT_CANONICALIZE=0x00010000).
            // KDC 需要 canonicalize 标志才会返回 PAC 授权数据。
            kdc_options: [0x00, 0x01, 0x00, 0x00],
            cname: Some(self.cname.clone()),
            realm: self.realm.clone(),
            sname: Some(PrincipalName {
                name_type: sname_type,
                name_string: sname_comps,
            }),
            from: None,
            till: till.clone(),
            rtime: None,
            nonce,
            etype: vec![tgt.session_etype as i32, 17, 19, 20],
            addresses: None,
        };
        let body_der = encode_kdc_req_body(&body);

        // 2) RFC 4120 §5.4.1 + RFC 3961: Authenticator checksum MUST be over KDC-REQ-BODY
        // MIT KDC krb5int_dk_checksum(): Kc = DK(session_key, usage|0x99), usage=6
        // suffix 0x99 = checksum key (RFC 3961 §5.3, not 0x55 which is integrity key)
        let cksum_key = crypto::dk_for(
            tgt.session_etype,
            &tgt.session_key,
            &crypto::usage_constant(6, 0x99),
        );
        let cksum_full = crypto::hmac_for(tgt.session_etype, &cksum_key, &body_der);
        let cksum_value = cksum_full[..12].to_vec(); // Truncate to 96 bits (output_size=12)
        let cksumtype_val: i32 = match tgt.session_etype {
            crypto::Etype::Aes256CtsHmacSha196 => 0x0010,
            crypto::Etype::Aes128CtsHmacSha196 => 0x000F,
            crypto::Etype::Aes128CtsHmacSha256128 => 0x0013,
            crypto::Etype::Aes256CtsHmacSha384192 => 0x0014,
        };
        let cksum = {
            // Checksum ::= SEQUENCE { cksumtype [0] INTEGER, checksum [1] OCTET STRING }
            let mut inner = Vec::new();
            inner.extend(crate::kerberos::asn1::ctx_int(0, cksumtype_val));
            inner.extend(crate::kerberos::asn1::tlv_ctx(
                1,
                &crate::kerberos::asn1::tlv(0x04, &cksum_value),
            ));
            crate::kerberos::asn1::tlv_seq(&inner)
        };

        // 3) 构造 Authenticator (包含 checksum)
        let ctime = chrono_like_now();
        let cusec = 0;
        let auth = Authenticator {
            authenticator_vno: 5,
            crealm: self.realm.clone(),
            cname: self.cname.clone(),
            cksum: Some(cksum),
            cusec,
            ctime: ctime.clone(),
            subkey: None,
            seq_number: Some(1),
        };
        let auth_der = encode_authenticator(&auth);
        let enc_auth_cipher = crypto::encrypt(
            tgt.session_etype,
            &tgt.session_key,
            KEY_USAGE_TGS_REQ_PA_TGS_REQ,
            &auth_der,
        )?;
        let enc_auth = EncryptedData {
            etype: tgt.session_etype as i32,
            kvno: None,
            cipher: enc_auth_cipher,
        };

        // 4) 构建 PA-PAC-REQUEST + PA-TGS-REQ
        //    PA-PAC-REQUEST (type=128) 告诉 KDC 需包含 PAC 授权数据，
        //    否则 Java broker 收到无 PAC 的票据后可能直接拒绝认证。
        let pa_pac = PaData {
            padata_type: 128,
            padata_value: PAC_REQUEST_VALUE.to_vec(),
        };
        let ap_req_der = encode_ap_req(&tgt.ticket_der, &enc_auth);
        let pa_tgs = PaData {
            padata_type: PADATA_TGS_REQ,
            padata_value: ap_req_der,
        };
        let pa_encoded = {
            let mut seq = Vec::new();
            seq.extend(encode_pa_data(&pa_pac));
            seq.extend(encode_pa_data(&pa_tgs));
            crate::kerberos::asn1::tlv_seq(&seq)
        };

        // 5) 构建完整 KDC-REQ
        let req_der = encode_kdc_req(PVNO, MSG_TGS_REQ, TAG_TGS_REQ, Some(&pa_encoded), &body_der);

        Ok(req_der)
    }

    /// SHA-1 摘要 (供 Authenticator checksum 使用)。
    /// 解析 TGS-REP: 用 TGS 会话密钥解密 enc-part, 取出服务票据与会话密钥。
    fn parse_tgs_rep(
        &self,
        tgs_rep: &[u8],
        session_etype: Etype,
        session_key: &[u8],
    ) -> Result<AcquiredTicket> {
        let kdc_rep = decode_kdc_rep(tgs_rep, TAG_TGS_REP)?;
        let plain = crypto::decrypt(
            session_etype,
            session_key,
            KEY_USAGE_TGS_REP_ENC_PART,
            &kdc_rep.enc_part.cipher,
        )?;
        let enc_part = decode_enc_kdc_rep_part(&plain)?;
        let s_etype = Etype::from_u32(enc_part.key.keytype as u32).ok_or_else(|| {
            KerberosError::Protocol(format!("unsupported etype {}", enc_part.key.keytype))
        })?;
        // 使用 KDC 返回的原始 Ticket DER, 避免 re-encode 改变字节
        let ticket_der = kdc_rep.ticket_der;
        Ok(AcquiredTicket {
            ticket_der,
            session_etype: s_etype,
            session_key: enc_part.key.keyvalue,
            cname: self.cname.clone(),
            crealm: self.realm.clone(),
            endtime: enc_part.endtime,
        })
    }
}

// ----------------------------- 辅助函数 -----------------------------

/// 获取当前 UTC 时间的 GeneralizedTime 格式 (`YYYYMMDDHHMMSSZ`)。
fn chrono_like_now() -> String {
    crate::kerberos::util::utc_now_generalized()
}

/// 获取 `n` 天后的 UTC 时间的 GeneralizedTime 格式。
fn chrono_like_add_days(n: u64) -> String {
    crate::kerberos::util::utc_add_days(n)
}

/// 解析服务名 `service` (如 `kafka/broker.example.com`) 为 PrincipalName 组件。
/// 单组件 -> NT_SRV_INST, 多组件 -> NT_SRV_HST (第一个是服务名, 后续是 host)。
fn parse_service_name(service: &str) -> (i32, Vec<String>) {
    let parts: Vec<String> = service.split('/').map(|s| s.to_string()).collect();
    if parts.len() >= 2 {
        (NT_SRV_HST, parts)
    } else {
        (NT_SRV_INST, parts)
    }
}

/// 生成密码学安全的随机 32-bit nonce，用于 KDC 请求。
fn random_nonce() -> i32 {
    let mut buf = [0u8; 4];
    if getrandom::fill(&mut buf).is_ok() {
        i32::from_be_bytes(buf).abs().max(1)
    } else {
        let n = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i32)
            .unwrap_or(0);
        n.abs().max(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kerberos::asn1::{
        EncKdcRepPart, EncryptionKey, PrincipalName, encode_principal_name,
    };
    use crate::kerberos::transport::test_util::{MockKdcTransport, block_on};

    #[test]
    fn kdc_transport_object_roundtrip() {
        let mock = MockKdcTransport::new(vec![0xAA, 0xBB]);
        let out = block_on(mock.exchange("EXAMPLE.COM", &[0x01, 0x02])).unwrap();
        assert_eq!(out, vec![0xAA, 0xBB]);
        assert_eq!(
            mock.last_realm.lock().unwrap().as_deref(),
            Some("EXAMPLE.COM")
        );
        assert_eq!(
            mock.last_req.lock().unwrap().as_deref(),
            Some(&[0x01, 0x02][..])
        );
    }

    /// 验证 AS-REQ body 构建产生有效的 DER.
    #[test]
    fn build_as_req_body_produces_valid_der() {
        let client =
            KerberosClient::new_for_test(&KerberosCredentials::new("client@EXAMPLE.COM")).unwrap();
        let body_der = client.build_as_req_body();
        assert!(body_der.starts_with(&[0x30]));
        // 验证 body_der 长度合理
        assert!(body_der.len() > 40);
    }

    /// 验证 parse_service_name 正确分解。
    #[test]
    fn service_name_parsing() {
        let (t, comps) = parse_service_name("kafka/broker.example.com");
        assert_eq!(t, NT_SRV_HST);
        assert_eq!(comps, vec!["kafka", "broker.example.com"]);

        let (t, comps) = parse_service_name("krbtgt");
        assert_eq!(t, NT_SRV_INST);
        assert_eq!(comps, vec!["krbtgt"]);
    }

    /// 验证 acquire 在 KDC 返回空响应时的错误路径。
    #[test]
    fn acquire_fails_on_empty_kdc_response() {
        let client =
            KerberosClient::new_for_test(&KerberosCredentials::new("client@EXAMPLE.COM")).unwrap();
        let mock = MockKdcTransport::new(vec![]);
        let err = block_on(client.acquire_service_ticket(&mock, "kafka/broker.example.com"));
        // KDC 返回空 → 应出错
        assert!(err.is_err());
    }

    /// PA-ENC-TIMESTAMP 构造 + 加解密 roundtrip: 验证 encrypt/decrypt 经 Ke 派生后仍然自洽。
    #[test]
    fn pa_enc_timestamp_crypto_roundtrip() {
        let key = vec![0x42u8; 32];
        let etype = Etype::Aes256CtsHmacSha196;
        let now = "20260713101112Z";
        let mut inner = Vec::new();
        inner.extend(crate::kerberos::asn1::tlv_ctx(
            0,
            &crate::kerberos::asn1::tlv(0x18, now.as_bytes()),
        ));
        inner.extend(crate::kerberos::asn1::tlv_ctx(
            1,
            &crate::kerberos::asn1::encode_int(0),
        ));
        let plain = crate::kerberos::asn1::tlv_seq(&inner);
        let ct = crypto::encrypt(etype, &key, KEY_USAGE_PA_ENC_TIMESTAMP, &plain).unwrap();
        let dec = crypto::decrypt(etype, &key, KEY_USAGE_PA_ENC_TIMESTAMP, &ct).unwrap();
        assert_eq!(dec, plain);
    }

    /// 验证 EncKdcRepPart 的编解码 roundtrip。
    #[test]
    fn enc_kdc_rep_part_roundtrip() {
        use crate::kerberos::messages::TAG_ENC_AS_REP_PART;
        let part = EncKdcRepPart {
            key: EncryptionKey {
                keytype: 18,
                keyvalue: vec![0xAA; 32],
            },
            nonce: 12345,
            flags: 0x12345678,
            authtime: "20260713101112Z".to_string(),
            starttime: Some("20260713101112Z".to_string()),
            endtime: "20260714101112Z".to_string(),
            srealm: "EXAMPLE.COM".to_string(),
            sname: PrincipalName {
                name_type: 2,
                name_string: vec!["krbtgt".to_string(), "EXAMPLE.COM".to_string()],
            },
        };
        let der = crate::kerberos::asn1::encode_enc_kdc_rep_part(&part, TAG_ENC_AS_REP_PART);
        let decoded = decode_enc_kdc_rep_part(&der).unwrap();
        assert_eq!(decoded.key.keytype, 18);
        assert_eq!(decoded.key.keyvalue, vec![0xAA; 32]);
        assert_eq!(decoded.nonce, 12345);
        assert_eq!(decoded.srealm, "EXAMPLE.COM");
    }

    /// 验证 KDC-REQ 的编解码 roundtrip。
    #[test]
    fn kdc_req_rep_roundtrip() {
        let body = KdcReqBody {
            kdc_options: [0u8; 4],
            cname: Some(PrincipalName {
                name_type: 1,
                name_string: vec!["client".to_string()],
            }),
            realm: "EXAMPLE.COM".to_string(),
            sname: Some(PrincipalName {
                name_type: 2,
                name_string: vec!["krbtgt".to_string(), "EXAMPLE.COM".to_string()],
            }),
            from: None,
            till: "20260714101112Z".to_string(),
            rtime: None,
            nonce: 42,
            etype: vec![18, 17, 20],
            addresses: None,
        };
        let body_der = encode_kdc_req_body(&body);
        // 验证 body_der 以 SEQUENCE 开头
        assert!(body_der.starts_with(&[0x30]));
        // 验证 AS-REQ 以 APPLICATION 10 开头
        let req_der = encode_kdc_req(PVNO, MSG_AS_REQ, TAG_AS_REQ, None, &body_der);
        assert!(req_der.starts_with(&[TAG_AS_REQ]));
    }

    /// 验证 KDC-REP + EncKdcRepPart 的完整 roundtrip。
    #[test]
    fn as_rep_enc_roundtrip() {
        use crate::kerberos::asn1::{PrincipalName, encode_enc_key, tlv_app, tlv_ctx};
        // 构造 KDC-REP 的 enc-part 明文 (EncASRepPart)
        let session_key = EncryptionKey {
            keytype: 18,
            keyvalue: vec![0x42; 32],
        };
        let mut inner = Vec::new();
        inner.extend(tlv_ctx(0, &encode_enc_key(&session_key)));
        inner.extend(crate::kerberos::asn1::ctx_int(2, 42));
        inner.extend(crate::kerberos::asn1::ctx_int(4, 0));
        inner.extend(tlv_ctx(
            5,
            &crate::kerberos::asn1::tlv(0x18, b"20260713101112Z"),
        ));
        inner.extend(tlv_ctx(
            7,
            &crate::kerberos::asn1::tlv(0x18, b"20260714101112Z"),
        ));
        inner.extend(tlv_ctx(
            9,
            &crate::kerberos::asn1::tlv(0x1B, b"EXAMPLE.COM"),
        ));
        inner.extend(tlv_ctx(
            10,
            &encode_principal_name(&PrincipalName {
                name_type: 2,
                name_string: vec!["krbtgt".to_string(), "EXAMPLE.COM".to_string()],
            }),
        ));
        let enc_part_plain = tlv_app(25, &crate::kerberos::asn1::tlv_seq(&inner)); // [APPLICATION 25] SEQUENCE

        // 用测试密钥加密
        let client_key = vec![0x11u8; 32];
        let etype = Etype::Aes256CtsHmacSha196;
        let ct = crypto::encrypt(
            etype,
            &client_key,
            KEY_USAGE_AS_REP_ENC_PART,
            &enc_part_plain,
        )
        .unwrap();

        // 解密并解析
        let dec = crypto::decrypt(etype, &client_key, KEY_USAGE_AS_REP_ENC_PART, &ct).unwrap();
        let part = decode_enc_kdc_rep_part(&dec).unwrap();
        assert_eq!(part.key.keytype, 18);
        assert_eq!(part.key.keyvalue, vec![0x42; 32]);
        assert_eq!(part.nonce, 42);
        assert_eq!(part.srealm, "EXAMPLE.COM");
    }
}
