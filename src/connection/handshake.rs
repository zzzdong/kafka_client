//! Connection handshake - ApiVersions negotiation and SASL authentication

use bytes::Bytes;
use tracing::debug;

use super::SequentialConnection;
use super::versions::NegotiatedVersions;
use crate::error::{KafkaError, Result};
use crate::sasl::scram::ScramMechanism;
use crate::sasl::{SaslCredentials, SaslMechanismType};
use kafka_client_protocol as protocol;

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
            client_software_name: Some(client_name),
            client_software_version: Some(client_version),
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
                let mut version = api.max_version.min(client_max);
                // Avoid flexible versions for stability
                if let Some(flex) = protocol::get_flexible_version(api.api_key) {
                    if version >= flex {
                        version = flex - 1;
                    }
                }
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
            return Err(KafkaError::Protocol(format!(
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
        }
    }

    async fn authenticate_plain(
        conn: &mut SequentialConnection,
        credentials: SaslCredentials,
    ) -> Result<()> {
        let mut auth_bytes = Vec::new();
        auth_bytes.push(0x00);
        auth_bytes.extend_from_slice(credentials.username.as_bytes());
        auth_bytes.push(0x00);
        auth_bytes.extend_from_slice(credentials.password.as_bytes());

        let auth_req = protocol::SaslAuthenticateRequest {
            auth_bytes: Bytes::from(auth_bytes),
        };

        let auth_resp: protocol::SaslAuthenticateResponse = conn.send_request(&auth_req).await?;

        if auth_resp.error_code != 0 {
            return Err(KafkaError::Protocol(format!(
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
            return Err(KafkaError::Protocol(format!(
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
            return Err(KafkaError::Protocol(format!(
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
}
