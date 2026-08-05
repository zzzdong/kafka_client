//! Regression tests for DescribeConfigs v4 (flexible) encode/decode.
//!
//! These guard against field/array encoding regressions in the derive macros
//! for the configs (nested compact arrays + nullable strings) structure.

use bytes::BytesMut;
use kafka_client_protocol::Message;
use kafka_client_protocol::describe_configs_response::{
    DescribeConfigsResourceResult, DescribeConfigsResponse, DescribeConfigsResult,
    DescribeConfigsSynonym,
};

#[test]
fn describe_configs_response_v4_roundtrip() {
    let resp = DescribeConfigsResponse {
        throttle_time_ms: 0,
        results: vec![DescribeConfigsResult {
            error_code: 0,
            error_message: None,
            resource_type: 4,
            resource_name: "".to_string(),
            configs: vec![
                DescribeConfigsResourceResult {
                    name: "max.message.bytes".to_string(),
                    value: Some("1048588".to_string()),
                    read_only: false,
                    config_source: 4,
                    is_sensitive: false,
                    synonyms: vec![DescribeConfigsSynonym {
                        name: "max.message.bytes".to_string(),
                        value: Some("1048588".to_string()),
                        source: 4,
                    }],
                    config_type: 2,
                    documentation: Some("largest batch".to_string()),
                },
                DescribeConfigsResourceResult {
                    name: "retention.ms".to_string(),
                    value: Some("604800000".to_string()),
                    read_only: false,
                    config_source: 5,
                    is_sensitive: false,
                    synonyms: vec![],
                    config_type: 2,
                    documentation: None,
                },
            ],
        }],
    };

    let mut buf = BytesMut::new();
    resp.flexible_encode(&mut buf, 4).unwrap();
    let mut bytes = buf.freeze();
    let decoded = DescribeConfigsResponse::flexible_decode(&mut bytes, 4).unwrap();

    assert_eq!(resp, decoded, "v4 flexible roundtrip must preserve all fields");
    assert_eq!(decoded.results.len(), 1);
    assert_eq!(
        decoded.results[0].configs.len(),
        2,
        "both config entries must survive the roundtrip"
    );
    assert_eq!(decoded.results[0].configs[0].name, "max.message.bytes");
    assert_eq!(
        decoded.results[0].configs[1].value.as_deref(),
        Some("604800000")
    );
}

#[test]
fn describe_configs_request_v4_roundtrip() {
    use kafka_client_protocol::describe_configs_request::{
        DescribeConfigsRequest, DescribeConfigsResource,
    };
    let req = DescribeConfigsRequest {
        resources: vec![DescribeConfigsResource {
            resource_type: 4,
            resource_name: String::new(),
            configuration_keys: Some(vec!["max.message.bytes".to_string()]),
        }],
        include_synonyms: false,
        include_documentation: false,
    };
    let mut buf = BytesMut::new();
    req.flexible_encode(&mut buf, 4).unwrap();
    let mut bytes = buf.freeze();
    let decoded = DescribeConfigsRequest::flexible_decode(&mut bytes, 4).unwrap();
    assert_eq!(req, decoded);
}
