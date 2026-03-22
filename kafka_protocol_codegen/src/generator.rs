//! 代码生成器
//!
//! 生成 Rust 结构体和 KafkaMessage trait 实现

use crate::error::{Error, Result};
use crate::parser::{FieldDef, MessageDefinition, RequestResponseDef};
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::path::Path;

pub struct CodeGenerator<'a> {
    out_dir: &'a Path,
}

impl<'a> CodeGenerator<'a> {
    pub fn new(out_dir: &'a Path) -> Self {
        // 确保输出目录存在
        std::fs::create_dir_all(out_dir).ok();
        Self { out_dir }
    }

    pub fn generate(&self, messages: &[MessageDefinition]) -> Result<()> {
        // 1. 收集所有复杂类型（需要生成结构体的）
        let complex_types = self.collect_complex_types(messages);

        // 2. 生成 mod.rs
        self.generate_mod_rs(messages)?;

        // 3. 为每个 API 生成代码
        for message in messages {
            self.generate_api(message)?;
        }

        // 4. 生成复杂类型定义
        self.generate_complex_types(&complex_types)?;

        // 5. 生成 KafkaMessage trait
        self.generate_kafka_message_trait()?;

        Ok(())
    }

    fn collect_complex_types(&self, messages: &[MessageDefinition]) -> HashSet<String> {
        let mut types = HashSet::new();

        for msg in messages {
            self.collect_types_from_def(&msg.request, &mut types);
            self.collect_types_from_def(&msg.response, &mut types);
        }

        types
    }

    fn collect_types_from_def(&self, def: &RequestResponseDef, types: &mut HashSet<String>) {
        for field in &def.fields {
            if field.is_complex_type() {
                if field.is_array == Some(true) {
                    if let Some(ref item) = field.item_type {
                        types.insert(item.clone());
                    }
                } else {
                    types.insert(field.ty.clone());
                }
            }
        }
    }

    fn generate_mod_rs(&self, messages: &[MessageDefinition]) -> Result<()> {
        let mut content = String::new();
        content.push_str("// 自动生成的 Kafka 协议代码\n");
        content.push_str("// 请勿手动修改\n\n");

        // 导入 KafkaMessage trait
        content.push_str("pub use crate::KafkaMessage;\n");
        content.push_str("use bytes::{Bytes, BytesMut};\n");
        content.push_str("use crate::error::Result;\n\n");

        // 为每个 API 生成模块声明
        for msg in messages {
            let module_name = snake_case(&msg.name);
            content.push_str(&format!("pub mod {};\n", module_name));
        }

        content.push_str("\n// 复杂类型定义\n");
        content.push_str("pub mod types;\n");

        let mod_path = self.out_dir.join("mod.rs");
        let mut file = File::create(&mod_path)?;
        file.write_all(content.as_bytes())?;

        Ok(())
    }

    fn generate_api(&self, message: &MessageDefinition) -> Result<()> {
        let module_name = snake_case(&message.name);
        let file_path = self.out_dir.join(format!("{}.rs", module_name));
        let mut file = File::create(&file_path)?;

        let mut content = String::new();

        // 生成请求结构体
        content.push_str(&format!(
            "/// {} 请求 (API Key = {})\n",
            message.name, message.api_key
        ));
        content.push_str("#[derive(Debug, Clone, Default, PartialEq)]\n");
        content.push_str(&format!(
            "pub struct {}Request {{\n",
            message.name
        ));
        for field in &message.request.fields {
            content.push_str(&self.generate_field(field));
        }
        content.push_str("}\n\n");

        // 生成请求的实现
        content.push_str(&self.generate_request_impl(message));
        content.push_str("\n");

        // 生成响应结构体
        content.push_str(&format!(
            "/// {} 响应\n",
            message.name
        ));
        content.push_str("#[derive(Debug, Clone, Default, PartialEq)]\n");
        content.push_str(&format!(
            "pub struct {}Response {{\n",
            message.name
        ));
        for field in &message.response.fields {
            content.push_str(&self.generate_field(field));
        }
        content.push_str("}\n\n");

        // 生成响应的实现
        content.push_str(&self.generate_response_impl(message));

        file.write_all(content.as_bytes())?;

        Ok(())
    }

    fn generate_field(&self, field: &FieldDef) -> String {
        let mut lines = String::new();

        // 文档注释
        if let Some(ref doc) = field.doc {
            lines.push_str(&format!("    /// {}\n", doc));
        }

        // 版本属性
        if let Some(ref versions) = field.versions {
            lines.push_str(&format!("    /// 版本: {}\n", versions));
        }

        // 字段定义
        let rust_type = field.rust_type();
        let field_name = snake_case(&field.name);

        // 处理可选字段
        let is_optional = field.nullable == Some(true) || field.needs_version_check();

        if is_optional {
            lines.push_str(&format!("    pub {}: Option<{}>,\n", field_name, rust_type));
        } else {
            lines.push_str(&format!("    pub {}: {},\n", field_name, rust_type));
        }

        lines
    }

    fn generate_request_impl(&self, message: &MessageDefinition) -> String {
        let mut impl_code = String::new();

        impl_code.push_str(&format!(
            "impl KafkaMessage for {}Request {{\n",
            message.name
        ));

        // encode 方法
        impl_code.push_str("    fn encode(&self, buf: &mut BytesMut, version: i16) {\n");
        impl_code.push_str(&self.generate_encode_body(&message.request, true));
        impl_code.push_str("    }\n\n");

        // decode 方法
        impl_code.push_str("    fn decode(buf: &mut Bytes, version: i16) -> crate::error::Result<Self> {\n");
        impl_code.push_str(&self.generate_decode_body(&message.request, true));
        impl_code.push_str("    }\n");

        impl_code.push_str("}\n");

        impl_code
    }

    fn generate_response_impl(&self, message: &MessageDefinition) -> String {
        let mut impl_code = String::new();

        impl_code.push_str(&format!(
            "impl KafkaMessage for {}Response {{\n",
            message.name
        ));

        // encode 方法
        impl_code.push_str("    fn encode(&self, buf: &mut BytesMut, version: i16) {\n");
        impl_code.push_str(&self.generate_encode_body(&message.response, false));
        impl_code.push_str("    }\n\n");

        // decode 方法
        impl_code.push_str("    fn decode(buf: &mut Bytes, version: i16) -> crate::error::Result<Self> {\n");
        impl_code.push_str(&self.generate_decode_body(&message.response, false));
        impl_code.push_str("    }\n");

        impl_code.push_str("}\n");

        impl_code
    }

    fn generate_encode_body(&self, def: &RequestResponseDef, _is_request: bool) -> String {
        let mut body = String::new();

        body.push_str("        // TODO: 实现 encode 逻辑\n");
        body.push_str("        let _ = (buf, version);\n");

        for field in &def.fields {
            let field_name = snake_case(&field.name);
            body.push_str(&format!(
                "        // encode {}\n",
                field_name
            ));
        }

        body
    }

    fn generate_decode_body(&self, def: &RequestResponseDef, _is_request: bool) -> String {
        let mut body = String::new();

        body.push_str("        // TODO: 实现 decode 逻辑\n");
        body.push_str("        let _ = (buf, version);\n");
        body.push_str("        Ok(Self::default())\n");

        body
    }

    fn generate_complex_types(&self, types: &HashSet<String>) -> Result<()> {
        let file_path = self.out_dir.join("types.rs");
        let mut file = File::create(&file_path)?;

        let mut content = String::new();
        content.push_str("//! 复杂类型定义\n\n");

        for type_name in types {
            content.push_str(&format!(
                "/// {}\n#[derive(Debug, Clone, Default, PartialEq)]\npub struct {} {{\n    // TODO: 定义字段\n}}\n\n",
                type_name, type_name
            ));

            // 为复杂类型实现 KafkaMessage
            content.push_str(&format!(
                "impl KafkaMessage for {} {{\n",
                type_name
            ));
            content.push_str("    fn encode(&self, _buf: &mut BytesMut, _version: i16) {\n");
            content.push_str("        // TODO\n");
            content.push_str("    }\n\n");
            content.push_str("    fn decode(_buf: &mut Bytes, _version: i16) -> crate::error::Result<Self> {\n");
            content.push_str("        Ok(Self::default())\n");
            content.push_str("    }\n");
            content.push_str("}\n\n");
        }

        file.write_all(content.as_bytes())?;
        Ok(())
    }

    fn generate_kafka_message_trait(&self) -> Result<()> {
        let file_path = self.out_dir.join("kafka_message.rs");
        let mut file = File::create(&file_path)?;

        let content = r#"//! KafkaMessage trait 定义

use bytes::{Bytes, BytesMut};
use crate::error::Result;

/// Kafka 消息的 trait
///
/// 所有 Kafka 协议消息都需要实现这个 trait
pub trait KafkaMessage: Sized + Default {
    /// 将消息编码到 buffer
    fn encode(&self, buf: &mut BytesMut, version: i16);

    /// 从 buffer 解码消息
    fn decode(buf: &mut Bytes, version: i16) -> Result<Self>;

    /// 获取消息的 API Key
    fn api_key() -> i16 where Self: Sized {
        -1 // 默认实现，需要覆盖
    }

    /// 获取消息的最小支持版本
    fn min_version() -> i16 {
        0
    }

    /// 获取消息的最大支持版本
    fn max_version() -> i16 {
        i16::MAX
    }
}
"#;

        file.write_all(content.as_bytes())?;
        Ok(())
    }
}

/// 将 CamelCase 转换为 snake_case
fn snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() && i > 0 {
            result.push('_');
        }
        result.push(c.to_lowercase().next().unwrap());
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snake_case() {
        assert_eq!(snake_case("ApiVersions"), "api_versions");
        assert_eq!(snake_case("Metadata"), "metadata");
        assert_eq!(snake_case("ProduceRequest"), "produce_request");
    }
}
