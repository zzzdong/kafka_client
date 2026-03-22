//! Kafka 协议 JSON 解析器
//!
//! 解析 Kafka 官方协议定义 JSON 格式

use serde::Deserialize;

/// 消息定义（API 级别）
#[derive(Debug, Clone, Deserialize)]
pub struct MessageDefinition {
    /// API 名称（如 ApiVersions, Metadata, Produce 等）
    pub name: String,
    /// API Key
    #[serde(rename = "apiKey")]
    pub api_key: i16,
    /// 支持的版本范围
    #[serde(rename = "minVersion")]
    pub min_version: i16,
    #[serde(rename = "maxVersion")]
    pub max_version: i16,
    /// 请求定义
    pub request: RequestResponseDef,
    /// 响应定义
    pub response: RequestResponseDef,
}

/// 请求或响应定义
#[derive(Debug, Clone, Deserialize)]
pub struct RequestResponseDef {
    /// 版本范围
    #[serde(rename = "minVersion")]
    pub min_version: i16,
    #[serde(rename = "maxVersion")]
    pub max_version: i16,
    /// 字段列表
    pub fields: Vec<FieldDef>,
}

/// 字段定义
#[derive(Debug, Clone, Deserialize)]
pub struct FieldDef {
    /// 字段名称
    pub name: String,
    /// 字段类型
    #[serde(rename = "type")]
    pub ty: String,
    /// 版本范围（可选，如 "0+", "1-3", "2+"）
    pub versions: Option<String>,
    /// 是否为 nullable
    pub nullable: Option<bool>,
    /// 默认值
    pub default: Option<String>,
    /// 字段文档
    pub doc: Option<String>,
    /// 是否为数组
    #[serde(rename = "isArray")]
    pub is_array: Option<bool>,
    /// 数组元素类型
    #[serde(rename = "itemType")]
    pub item_type: Option<String>,
    /// 是否为 map
    #[serde(rename = "isMap")]
    pub is_map: Option<bool>,
    /// map 的 key 类型
    #[serde(rename = "mapKeyType")]
    pub map_key_type: Option<String>,
    /// map 的 value 类型
    #[serde(rename = "mapValueType")]
    pub map_value_type: Option<String>,
    /// 是否为 flexible 版本字段
    pub flexible: Option<bool>,
    /// 是否为 tagged field（用于 flexible 格式）
    pub tagged: Option<bool>,
    /// tag 编号
    pub tag: Option<i32>,
}

impl FieldDef {
    /// 获取字段的 Rust 类型
    pub fn rust_type(&self) -> String {
        if self.is_array == Some(true) {
            let item = self.item_type.as_deref().unwrap_or("i32");
            format!("Vec<{}>", map_kafka_type_to_rust(item))
        } else if self.is_map == Some(true) {
            let key = self.map_key_type.as_deref().unwrap_or("string");
            let value = self.map_value_type.as_deref().unwrap_or("bytes");
            format!(
                "std::collections::HashMap<{}, {}>",
                map_kafka_type_to_rust(key),
                map_kafka_type_to_rust(value)
            )
        } else {
            map_kafka_type_to_rust(&self.ty)
        }
    }

    /// 解析版本范围
    pub fn version_range(&self) -> (Option<i16>, Option<i16>) {
        match &self.versions {
            None => (None, None),
            Some(v) => {
                if v.contains('+') {
                    let min = v.trim_end_matches('+').parse().ok();
                    (min, None)
                } else if v.contains('-') {
                    let parts: Vec<_> = v.split('-').collect();
                    let min = parts[0].parse().ok();
                    let max = parts.get(1).and_then(|s| s.parse().ok());
                    (min, max)
                } else {
                    (v.parse().ok(), None)
                }
            }
        }
    }

    /// 获取最小版本
    pub fn min_version(&self) -> Option<i16> {
        self.version_range().0
    }

    /// 是否需要版本检查
    pub fn needs_version_check(&self) -> bool {
        self.min_version().is_some()
    }

    /// 是否为 complex 类型（需要单独定义结构体）
    pub fn is_complex_type(&self) -> bool {
        let primitive_types = ["bool", "i8", "i16", "i32", "i64", "string", "bytes", "records"];
        if self.is_array == Some(true) {
            let item = self.item_type.as_deref().unwrap_or("");
            !primitive_types.contains(&item)
        } else {
            !primitive_types.contains(&self.ty.as_str())
        }
    }
}

/// 将 Kafka 类型映射为 Rust 类型
fn map_kafka_type_to_rust(kafka_type: &str) -> String {
    match kafka_type {
        "bool" => "bool".to_string(),
        "i8" | "int8" => "i8".to_string(),
        "i16" | "int16" => "i16".to_string(),
        "i32" | "int32" | "uint32" => "i32".to_string(),
        "i64" | "int64" | "uint64" => "i64".to_string(),
        "string" => "String".to_string(),
        "bytes" => "bytes::Bytes".to_string(),
        "records" => "Vec<RecordBatch>".to_string(),
        // Complex types - 保持原样，作为结构体名称
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_field_versions() {
        let field = FieldDef {
            name: "test".to_string(),
            ty: "i32".to_string(),
            versions: Some("1+".to_string()),
            nullable: None,
            default: None,
            doc: None,
            is_array: None,
            item_type: None,
            is_map: None,
            map_key_type: None,
            map_value_type: None,
            flexible: None,
            tagged: None,
            tag: None,
        };

        assert_eq!(field.min_version(), Some(1));
        assert!(field.needs_version_check());
    }
}
