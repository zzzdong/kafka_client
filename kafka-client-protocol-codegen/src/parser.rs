// src/parser.rs
use crate::utils::{map_kafka_type_to_rust, to_pascal_case, to_snake_case};
use crate::{to_field_name, types::*};
use anyhow::Context;
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use walkdir::WalkDir;

/// 解析目录中的所有 JSON 文件
pub fn parse_directory(
    dir: &Path,
) -> Result<(Vec<ParsedMessage>, Vec<ParsedStruct>), anyhow::Error> {
    let mut messages = Vec::new();
    let mut structs = Vec::new();

    for entry in WalkDir::new(dir)
        .follow_links(true)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("json") {
            match parse_file(path) {
                Ok((msg_opt, struct_opt)) => {
                    if let Some(msg) = msg_opt {
                        messages.push(msg);
                    }
                    if let Some(s) = struct_opt {
                        structs.push(s);
                    }
                }
                Err(e) => {
                    eprintln!("Warning: Failed to parse {}: {}", path.display(), e);
                }
            }
        }
    }

    Ok((messages, structs))
}

/// 解析单个 JSON 文件
fn parse_file(path: &Path) -> anyhow::Result<(Option<ParsedMessage>, Option<ParsedStruct>)> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("Failed to read file: {}", path.display()))?;

    let value: serde_json::Value = json5::from_str(&content)
        .with_context(|| format!("Failed to parse JSON: {}", path.display()))?;

    // 检查是否是共享结构体（type = "struct"）
    if let Some(type_val) = value.get("type")
        && type_val.as_str() == Some("struct")
    {
        let def: StructDef = json5::from_str(&content)?;
        return Ok((None, Some(parse_struct(def))));
    }

    // 否则是普通消息
    let def: MessageDef = json5::from_str(&content)?;
    Ok((Some(parse_message(def)), None))
}

/// 解析消息
fn parse_message(def: MessageDef) -> ParsedMessage {
    let struct_name = to_pascal_case(&def.name);
    let api_key = def.api_key.map(|k| k as i16);

    // 收集所有内联结构体
    let mut inline_structs = Vec::new();
    let mut visited = HashSet::new();

    // 处理 commonStructs - 这些结构体需要被定义
    for cs in &def.common_structs {
        let cs_fields = parse_fields_simple_list(&cs.fields, &mut inline_structs, &mut visited);

        inline_structs.push(InlineStructInfo {
            name: cs.name.clone(),
            struct_name: to_pascal_case(&cs.name),
            fields: cs_fields,
            is_common: true,
            has_tag: false,
            tag: None,
        });
        visited.insert(cs.name.clone());
    }

    // 处理顶层字段中的内联结构体
    let fields = parse_fields_recursive(&def.fields, &def.name, &mut inline_structs, &mut visited);

    ParsedMessage {
        name: def.name,
        struct_name,
        msg_type: def.msg_type,
        api_key,
        valid_versions: def.valid_versions,
        flexible_versions: def.flexible_versions,
        fields,
        inline_structs,
        common_structs: def.common_structs,
        about: def.about,
    }
}

/// 解析共享结构体
fn parse_struct(def: StructDef) -> ParsedStruct {
    let struct_name = to_pascal_case(&def.name);

    let fields = def.fields.iter().map(parse_field_simple).collect();

    ParsedStruct {
        name: def.name,
        struct_name,
        msg_type: def
            .struct_type
            .clone()
            .unwrap_or_else(|| "struct".to_string()),
        valid_versions: def.valid_versions,
        flexible_versions: def.flexible_versions,
        fields,
        about: def.about,
    }
}

/// 递归解析字段列表，收集所有内联结构体
fn parse_fields_recursive(
    fields: &[FieldDef],
    parent_name: &str,
    inline_structs: &mut Vec<InlineStructInfo>,
    visited: &mut HashSet<String>,
) -> Vec<ParsedField> {
    fields
        .iter()
        .map(|f| parse_field_recursive(f, parent_name, inline_structs, visited))
        .collect()
}

/// 解析简单字段列表（不处理字段级别的内联结构体）
fn parse_fields_simple_list(
    fields: &[FieldDef],
    _inline_structs: &mut Vec<InlineStructInfo>,
    _visited: &mut HashSet<String>,
) -> Vec<ParsedField> {
    fields.iter().map(parse_field_simple).collect()
}

/// 递归解析字段（支持嵌套内联结构体）
fn parse_field_recursive(
    field: &FieldDef,
    _parent_name: &str,
    inline_structs: &mut Vec<InlineStructInfo>,
    visited: &mut HashSet<String>,
) -> ParsedField {
    let is_nullable = field.nullable_versions.is_some();
    let default_is_null = field.default.as_ref().is_some_and(|d| match d {
        DefaultValue::Null => true,
        DefaultValue::Str(s) => s == "null",
        _ => false,
    });

    // 只有引用类型（string, bytes, records, uuid, [] 数组, 或自定义结构体类型）才能为 null
    // ignorable 只表示该字段可以跳过，不等于 null。不能将 ignorable 作为 nullable 的条件。
    // 否则 int32 等标量类型会被错误地标记为 Option<i32>
    let can_be_nullable = matches!(
        field.field_type.as_str(),
        "string" | "bytes" | "records" | "uuid"
    ) || field.field_type.starts_with("[]")
        || field.fields.is_some();

    let nullable_versions = if can_be_nullable {
        // 只有引用类型才使用 nullable_versions
        field.nullable_versions.clone()
    } else {
        None
    };

    // 只有引用类型才考虑 is_nullable
    let is_nullable = if can_be_nullable { is_nullable } else { false };

    // 检查是否有内联字段（嵌套结构体）
    let rust_type = if let Some(inner_fields) = &field.fields {
        let struct_name = extract_struct_name(&field.field_type);
        let is_array_type = field.field_type.starts_with("[]");

        if !visited.contains(&struct_name) {
            visited.insert(struct_name.clone());

            let parsed_inner_fields =
                parse_fields_recursive(inner_fields, &struct_name, inline_structs, visited);

            inline_structs.push(InlineStructInfo {
                name: struct_name.clone(),
                struct_name: struct_name.clone(),
                fields: parsed_inner_fields,
                is_common: false,
                has_tag: field.tag.is_some(),
                tag: field.tag,
            });
        }

        if is_array_type {
            if is_nullable || default_is_null {
                format!("Option<Vec<{}>>", struct_name)
            } else {
                format!("Vec<{}>", struct_name)
            }
        } else {
            if is_nullable || default_is_null {
                format!("Option<{}>", struct_name)
            } else {
                struct_name
            }
        }
    } else {
        map_kafka_type_to_rust(&field.field_type, is_nullable, default_is_null)
    };

    let default = field.default.as_ref().map(|d| match d {
        DefaultValue::Bool(b) => b.to_string(),
        DefaultValue::Int(i) => i.to_string(),
        DefaultValue::Str(s) => s.clone(),
        DefaultValue::Null => "null".to_string(),
    });

    ParsedField {
        name: field.name.clone(),
        rust_name: to_field_name(&field.name), // 使用 to_field_name 而不是 to_snake_case
        rust_type,
        versions: field.versions.clone(),
        nullable_versions,
        tag: field.tag,
        tagged_versions: field.tagged_versions.clone(),
        default,
        ignorable: field.ignorable,
        map_key: field.map_key,
        about: field.about.clone(),
    }
}

/// 简单解析字段（用于 commonStructs）
/// 简单解析字段（用于共享结构体，无内联嵌套）
fn parse_field_simple(field: &FieldDef) -> ParsedField {
    let is_nullable = field.nullable_versions.is_some();
    let default_is_null = field.default.as_ref().is_some_and(|d| match d {
        DefaultValue::Null => true,
        DefaultValue::Str(s) => s == "null",
        _ => false,
    });

    // 只有引用类型（string, bytes, records, uuid, [] 数组, 或自定义结构体类型）才能为 null
    // ignorable 只表示该字段可以跳过，不等于 null。不能将 ignorable 作为 nullable 的条件。
    let can_be_nullable = matches!(
        field.field_type.as_str(),
        "string" | "bytes" | "records" | "uuid"
    ) || field.field_type.starts_with("[]")
        || field.fields.is_some();

    let nullable_versions = if can_be_nullable {
        field.nullable_versions.clone()
    } else {
        None
    };

    let is_nullable = if can_be_nullable { is_nullable } else { false };

    let rust_type = map_kafka_type_to_rust(&field.field_type, is_nullable, default_is_null);

    let default = field.default.as_ref().map(|d| match d {
        DefaultValue::Bool(b) => b.to_string(),
        DefaultValue::Int(i) => i.to_string(),
        DefaultValue::Str(s) => s.clone(),
        DefaultValue::Null => "null".to_string(),
    });

    ParsedField {
        name: field.name.clone(),
        rust_name: to_snake_case(&field.name),
        rust_type,
        versions: field.versions.clone(),
        nullable_versions,
        tag: field.tag,
        tagged_versions: field.tagged_versions.clone(),
        default,
        ignorable: field.ignorable,
        map_key: field.map_key,
        about: field.about.clone(),
    }
}

/// 从类型字符串中提取结构体名称
fn extract_struct_name(type_str: &str) -> String {
    let name = if let Some(stripped) = type_str.strip_prefix("[]") {
        stripped
    } else {
        type_str
    };
    name.to_string()
}
