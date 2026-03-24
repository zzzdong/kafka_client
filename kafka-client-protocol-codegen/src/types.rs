// src/types.rs
use serde::{Deserialize, Deserializer};
use serde_json::Value;

/// 消息定义（有 apiKey 或 type = request/response）
#[derive(Debug, Clone, Deserialize)]
pub struct MessageDef {
    #[serde(default, rename = "apiKey")]
    pub api_key: Option<u16>,

    #[serde(rename = "type")]
    pub msg_type: String,

    pub name: String,

    #[serde(default = "default_versions", rename = "validVersions")]
    pub valid_versions: String,

    #[serde(default, rename = "flexibleVersions")]
    pub flexible_versions: Option<String>,

    #[serde(default)]
    pub fields: Vec<FieldDef>,

    /// 公共结构体定义（如 AddPartitionsToTxnTopic）
    #[serde(default, rename = "commonStructs")]
    pub common_structs: Vec<StructDef>,

    #[serde(default)]
    pub about: Option<String>,
}

/// 共享结构体定义
#[derive(Debug, Clone, Deserialize)]
pub struct StructDef {
    #[serde(rename = "type")]
    #[serde(default)]
    pub struct_type: Option<String>,

    pub name: String,

    #[serde(default = "default_versions", rename = "validVersions")]
    pub valid_versions: String,

    #[serde(default, rename = "flexibleVersions")]
    pub flexible_versions: Option<String>,

    #[serde(default)]
    pub fields: Vec<FieldDef>,

    #[serde(default)]
    pub about: Option<String>,
}

/// 字段定义
#[derive(Debug, Clone, Deserialize)]
pub struct FieldDef {
    pub name: String,

    #[serde(rename = "type")]
    pub field_type: String,

    #[serde(default = "default_versions")]
    pub versions: String,

    #[serde(default, rename = "nullableVersions")]
    pub nullable_versions: Option<String>,

    #[serde(default)]
    pub tag: Option<u32>,

    #[serde(default, rename = "taggedVersions")]
    pub tagged_versions: Option<String>,

    #[serde(default, deserialize_with = "deserialize_default_value")]
    pub default: Option<DefaultValue>,

    /// 是否可忽略（支持布尔值或字符串 "true"/"false"）
    #[serde(default, deserialize_with = "deserialize_bool_or_string")]
    pub ignorable: bool,

    #[serde(default, rename = "mapKey")]
    pub map_key: bool,

    #[serde(default, rename = "entityType")]
    pub entity_type: Option<String>,

    #[serde(default)]
    pub about: Option<String>,

    /// 内联字段（如果字段是结构体类型）
    #[serde(default)]
    pub fields: Option<Vec<FieldDef>>,
}

/// 解析布尔值或字符串 "true"/"false"
fn deserialize_bool_or_string<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;
    let value: Value = Deserialize::deserialize(deserializer)?;

    match value {
        Value::Bool(b) => Ok(b),
        Value::String(s) => match s.as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err(Error::custom(format!("invalid boolean string: {}", s))),
        },
        _ => Ok(false),
    }
}

/// 解析默认值
fn deserialize_default_value<'de, D>(deserializer: D) -> Result<Option<DefaultValue>, D::Error>
where
    D: Deserializer<'de>,
{
    let value: Option<DefaultValue> = Option::deserialize(deserializer)?;
    Ok(value)
}

/// 默认版本
fn default_versions() -> String {
    "0+".to_string()
}

/// 默认值类型
#[derive(Debug, Clone)]
pub enum DefaultValue {
    Bool(bool),
    Int(i64),
    Str(String),
    Null,
}

impl<'de> Deserialize<'de> for DefaultValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;
        let value: Value = Deserialize::deserialize(deserializer)?;

        match value {
            Value::Null => Ok(DefaultValue::Null),
            Value::Bool(b) => Ok(DefaultValue::Bool(b)),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(DefaultValue::Int(i))
                } else {
                    Err(Error::custom("invalid number"))
                }
            }
            Value::String(s) => Ok(DefaultValue::Str(s)),
            _ => Err(Error::custom("invalid default value type")),
        }
    }
}

/// 解析后的字段（用于代码生成）
#[derive(Debug, Clone)]
pub struct ParsedField {
    pub name: String,
    pub rust_name: String,
    pub rust_type: String,
    pub versions: String,
    pub nullable_versions: Option<String>,
    pub tag: Option<u32>,
    pub tagged_versions: Option<String>,
    pub default: Option<String>,
    pub ignorable: bool,
    pub map_key: bool,
    pub about: Option<String>,
}

/// 解析后的共享结构体
#[derive(Debug, Clone)]
pub struct ParsedStruct {
    pub name: String,
    pub struct_name: String,
    pub msg_type: String,
    pub valid_versions: String,
    pub flexible_versions: Option<String>,
    pub fields: Vec<ParsedField>,
    pub about: Option<String>,
}

/// 解析后的消息
#[derive(Debug, Clone)]
pub struct ParsedMessage {
    pub name: String,
    pub struct_name: String,
    pub msg_type: String,
    pub api_key: Option<i16>,
    pub valid_versions: String,
    pub flexible_versions: Option<String>,
    pub fields: Vec<ParsedField>,
    pub inline_structs: Vec<InlineStructInfo>,
    pub common_structs: Vec<StructDef>,
    pub about: Option<String>,
}

/// 内联结构体信息
#[derive(Debug, Clone)]
pub struct InlineStructInfo {
    pub name: String,
    pub struct_name: String,
    pub fields: Vec<ParsedField>,
    pub is_common: bool,
    pub has_tag: bool,
    pub tag: Option<u32>,
}
