//! Connection handshake - ApiVersions negotiation and SASL authentication

use bytes::Bytes;
use krb5_gss::gss::NativeGssContext;
use tracing::debug;

use super::SequentialConnection;
use super::versions::NegotiatedVersions;
use crate::error::{KafkaError, Result};
use crate::sasl::scram::ScramMechanism;
use crate::sasl::{SaslCredentials, SaslMechanismType};
use kafka_client_protocol as protocol;
use krb5_gss::gss::GssContext;
use krb5_gss::{GssClient, KerberosCredentials};

/// Handshake logic for connection establishment
pub struct Handshake;

impl Handshake {
    /// Perform ApiVersions handshake
    pub async fn perform(
        conn: &mut SequentialConnection,
        client_name: String,
        client_version: String,
    ) -> Result<NegotiatedVersions> {
        let request = protocol::ApiVersionsRequest {
            client_software_name: client_name,
            client_software_version: client_version,
        };

        let response: protocol::ApiVersionsResponse = conn.send_request(&request).await?;

        if response.error_code != 0 {
            return Err(KafkaError::Protocol(format!(
                "ApiVersions failed: error {}",
                response.error_code
            )));
        }

        let mut negotiated = NegotiatedVersions::new();
        for api in response.api_keys {
            if let Some((client_min, client_max)) = protocol::get_version_range(api.api_key) {
                let version = api.max_version.min(client_max);

                if version >= api.min_version && version >= client_min {
                    negotiated.set_version(api.api_key, version);
                    debug!(
                        api_key = api.api_key,
                        negotiated_version = version,
                        "negotiated API version"
                    );
                }
            }
        }

        Ok(negotiated)
    }

    /// Perform SASL authentication
    pub async fn sasl_authenticate(
        conn: &mut SequentialConnection,
        mechanism: SaslMechanismType,
        credentials: SaslCredentials,
    ) -> Result<()> {
        // SASL handshake
        let handshake_req = protocol::SaslHandshakeRequest {
            mechanism: mechanism.as_str().to_string(),
        };

        let handshake_resp: protocol::SaslHandshakeResponse =
            conn.send_request(&handshake_req).await?;

        if handshake_resp.error_code != 0 {
            return Err(KafkaError::AuthenticationFailed(format!(
                "SASL handshake failed: error {}",
                handshake_resp.error_code
            )));
        }

        // Authenticate based on mechanism
        match mechanism {
            SaslMechanismType::Plain => Self::authenticate_plain(conn, credentials).await,
            SaslMechanismType::ScramSha256 | SaslMechanismType::ScramSha512 => {
                Self::authenticate_scram(conn, mechanism, credentials).await
            }
            SaslMechanismType::Gssapi => {
                // GSSAPI 应走 sasl_authenticate_gssapi 路径, 不应到达此处
                Err(KafkaError::MechanismNotSupported(
                    "GSSAPI must be routed via the kerberos connection path".into(),
                ))
            }
        }
    }

    async fn authenticate_plain(
        conn: &mut SequentialConnection,
        credentials: SaslCredentials,
    ) -> Result<()> {
        let mut auth_bytes = Vec::new();
        auth_bytes.push(0x00);
        auth_bytes.extend_from_slice(credentials.username().as_bytes());
        auth_bytes.push(0x00);
        auth_bytes.extend_from_slice(credentials.password().as_bytes());

        let auth_req = protocol::SaslAuthenticateRequest {
            auth_bytes: Bytes::from(auth_bytes),
        };

        let auth_resp: protocol::SaslAuthenticateResponse = conn.send_request(&auth_req).await?;

        if auth_resp.error_code != 0 {
            return Err(KafkaError::AuthenticationFailed(format!(
                "PLAIN authentication failed: error {}, message: {:?}",
                auth_resp.error_code, auth_resp.error_message
            )));
        }

        Ok(())
    }

    async fn authenticate_scram(
        conn: &mut SequentialConnection,
        mechanism: SaslMechanismType,
        credentials: SaslCredentials,
    ) -> Result<()> {
        let mut scram = match mechanism {
            SaslMechanismType::ScramSha256 => ScramMechanism::new_sha256(),
            SaslMechanismType::ScramSha512 => ScramMechanism::new_sha512(),
            _ => {
                return Err(KafkaError::MechanismNotSupported(
                    mechanism.as_str().to_string(),
                ));
            }
        };

        // Round 1
        let auth_bytes = scram
            .client_first(&credentials)
            .map_err(KafkaError::SaslError)?;
        let auth_req = protocol::SaslAuthenticateRequest { auth_bytes };
        let auth_resp: protocol::SaslAuthenticateResponse = conn.send_request(&auth_req).await?;

        if auth_resp.error_code != 0 {
            return Err(KafkaError::AuthenticationFailed(format!(
                "SCRAM round 1 failed: error {}",
                auth_resp.error_code
            )));
        }

        // Round 2
        let auth_bytes = scram
            .client_final(&auth_resp.auth_bytes)
            .map_err(KafkaError::SaslError)?;
        let auth_req = protocol::SaslAuthenticateRequest { auth_bytes };
        let auth_resp: protocol::SaslAuthenticateResponse = conn.send_request(&auth_req).await?;

        if auth_resp.error_code != 0 {
            return Err(KafkaError::AuthenticationFailed(format!(
                "SCRAM round 2 failed: error {}",
                auth_resp.error_code
            )));
        }

        // Verify server final
        scram
            .verify_server_final(&auth_resp.auth_bytes)
            .map_err(KafkaError::SaslError)?;

        Ok(())
    }

    /// SASL/GSSAPI (Kerberos) 认证: 先经 KDC 获取服务票据, 再与 broker 完成 server-first 多轮握手。
    ///
    /// 流程:
    /// 1. 验证 broker 支持 SaslHandshake v1+ (KIP-152), GSSAPI 需要独立 SaslAuthenticate API
    /// 2. 经 `GssClient` 从 KDC 获取服务票据 → `context_for` 得到 GSS 上下文
    /// 3. `SaslHandshake(mechanism="GSSAPI")` → 确认机制受支持
    /// 4. 发送 AP-REQ token 给 broker → 接收 AP-REP token
    /// 5. 验证 AP-REP → 发送空 auth_bytes 收尾
    pub(crate) async fn sasl_authenticate_gssapi(
        conn: &mut SequentialConnection,
        credentials: KerberosCredentials,
        broker_host: &str,
        kdc_host: Option<&str>,
        kdc_port: u16,
    ) -> Result<()> {
        // 1. 验证 broker 支持 SaslHandshake v1+ (KIP-152)
        Self::validate_sasl_handshake_version(conn)?;

        // 2. 构建 GSS 客户端并获取服务票据上下文
        let mut ctx =
            Self::build_gss_client_and_context(&credentials, broker_host, kdc_host, kdc_port)
                .await?;

        // 3. SASL handshake 声明 GSSAPI 机制
        Self::sasl_handshake_gssapi(conn).await?;

        // 4. 执行 3 轮 GSSAPI 握手
        Self::perform_gss_handshake(conn, &mut ctx).await
    }

    /// 验证 broker 支持 SaslHandshake v1+ (KIP-152)
    fn validate_sasl_handshake_version(conn: &SequentialConnection) -> Result<()> {
        let sasl_hs_api_key: i16 = 17; // SaslHandshake
        match conn.negotiated().get_version(sasl_hs_api_key) {
            Some(v) if v >= 1 => Ok(()),
            Some(v) => Err(KafkaError::AuthenticationFailed(format!(
                "GSSAPI requires SaslHandshake v1+ (KIP-152), \
                 but broker only supports v{v}. Use Kafka 1.0.0+ broker."
            ))),
            None => Err(KafkaError::AuthenticationFailed(
                "Broker does not support SaslHandshake API".into(),
            )),
        }
    }

    /// 构建 GSS 客户端并获取服务票据上下文
    async fn build_gss_client_and_context(
        credentials: &KerberosCredentials,
        broker_host: &str,
        kdc_host: Option<&str>,
        kdc_port: u16,
    ) -> Result<NativeGssContext> {
        let kdc_host_owned = match kdc_host {
            Some(h) => h.to_string(),
            None => credentials
                .realm()
                .unwrap_or_else(|| "localhost".to_string()),
        };

        // 构造服务 principal `service/hostname`
        let service_host = credentials
            .broker_hostname
            .as_deref()
            .unwrap_or(broker_host);
        let service_sname = format!("{}/{}", credentials.service_name, service_host);

        let gss = GssClient::new(credentials, &kdc_host_owned, kdc_port).map_err(|e| {
            KafkaError::AuthenticationFailed(format!("failed to create GSS client: {e}"))
        })?;

        gss.context_for(&service_sname).await.map_err(|e| {
            KafkaError::AuthenticationFailed(format!("failed to acquire GSS ticket: {e}"))
        })
    }

    /// SASL handshake 声明 GSSAPI 机制
    async fn sasl_handshake_gssapi(conn: &mut SequentialConnection) -> Result<()> {
        let handshake_req = protocol::SaslHandshakeRequest {
            mechanism: "GSSAPI".to_string(),
        };
        let handshake_resp: protocol::SaslHandshakeResponse =
            conn.send_request(&handshake_req).await?;
        if handshake_resp.error_code != 0 {
            return Err(KafkaError::AuthenticationFailed(format!(
                "SASL handshake (GSSAPI) failed: error {}",
                handshake_resp.error_code
            )));
        }
        Ok(())
    }

    /// 执行 3 轮 GSSAPI 握手 (RFC 4752)
    async fn perform_gss_handshake(
        conn: &mut SequentialConnection,
        ctx: &mut NativeGssContext,
    ) -> Result<()> {
        // ── 第 1 轮: AP-REQ → AP-REP ──
        let ap_req_token = ctx.initial_token().map_err(|e| {
            KafkaError::AuthenticationFailed(format!("GSS initial_token failed: {e}"))
        })?;

        let req = protocol::SaslAuthenticateRequest {
            auth_bytes: Bytes::from(ap_req_token),
        };
        let resp: protocol::SaslAuthenticateResponse = conn.send_request(&req).await?;
        if resp.error_code != 0 {
            return Err(KafkaError::AuthenticationFailed(format!(
                "GSSAPI authentication failed: error {}, message: {:?}",
                resp.error_code, resp.error_message
            )));
        }

        let server_challenge = resp.auth_bytes.to_vec();

        // ── 第 2 轮: 处理 AP-REP → 返回空 token ──
        let next_token = ctx.handle_challenge(&server_challenge).map_err(|e| {
            KafkaError::AuthenticationFailed(format!("GSS AP-REP challenge failed: {e}"))
        })?;

        let round2_auth = next_token.map(Bytes::from).unwrap_or_default();
        let round2_req = protocol::SaslAuthenticateRequest {
            auth_bytes: round2_auth,
        };
        let round2_resp: protocol::SaslAuthenticateResponse =
            conn.send_request(&round2_req).await?;

        if round2_resp.error_code != 0 {
            return Err(KafkaError::AuthenticationFailed(format!(
                "GSSAPI round 2 (WRAP negotiation) failed: error {}, message: {:?}",
                round2_resp.error_code, round2_resp.error_message
            )));
        }

        // ── 第 3 轮: WRAP exchange ──
        let wrap_challenge = round2_resp.auth_bytes.to_vec();
        if !wrap_challenge.is_empty() {
            let client_wrap = ctx.handle_challenge(&wrap_challenge).map_err(|e| {
                KafkaError::AuthenticationFailed(format!("GSS WRAP challenge failed: {e}"))
            })?;

            let final_auth = client_wrap.map(Bytes::from).unwrap_or_default();
            let final_req = protocol::SaslAuthenticateRequest {
                auth_bytes: final_auth,
            };
            let final_resp: protocol::SaslAuthenticateResponse =
                conn.send_request(&final_req).await?;
            if final_resp.error_code != 0 {
                return Err(KafkaError::AuthenticationFailed(format!(
                    "GSSAPI final handshake failed: error {}, message: {:?}",
                    final_resp.error_code, final_resp.error_message
                )));
            }
        }

        Ok(())
    }
}
