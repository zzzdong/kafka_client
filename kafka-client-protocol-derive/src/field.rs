// src/field.rs
use crate::version_range::VersionRange;
#[allow(unused_imports)]
use proc_macro2::Span;
#[allow(unused_imports)]
use quote::ToTokens;
#[allow(unused_imports)]
use quote::format_ident;
use syn::{Field, GenericArgument, Ident, PathArguments, Type};

/// 字段信息
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct FieldInfo {
    /// 字段名
    pub name: Ident,
    /// Rust 类型
    pub ty: Type,
    /// 版本范围
    pub versions: VersionRange,
    /// 可空版本范围
    pub nullable_versions: Option<VersionRange>,
    /// 标签字段的标签号
    pub tag: Option<u32>,
    /// 标签字段适用的版本范围
    pub tagged_versions: Option<VersionRange>,
    /// 是否可忽略
    pub ignorable: bool,
    /// 自定义默认值
    pub default: Option<String>,
    /// 是否作为 map key
    pub map_key: bool,
    /// 字段说明
    pub about: Option<String>,
    /// 字段在线格式中是 bytes 类型但映射为结构体（如 records: Option<RecordBatch>）
    pub encoded_as_bytes: bool,
}

impl FieldInfo {
    /// 从 syn::Field 解析
    pub fn from_field(field: &Field) -> Self {
        let name = field.ident.clone().unwrap();
        let ty = field.ty.clone();

        let mut state = FieldParseState::default();

        for attr in &field.attrs {
            if attr.path().is_ident("kafka") {
                parse_kafka_attr(attr, &mut state);
            }
        }

        Self {
            name,
            ty,
            versions: state.versions,
            nullable_versions: state.nullable_versions,
            tag: state.tag,
            tagged_versions: state.tagged_versions,
            ignorable: state.ignorable,
            default: state.default,
            map_key: state.map_key,
            about: state.about,
            encoded_as_bytes: state.encoded_as_bytes,
        }
    }

    /// 获取类型的字符串表示
    pub fn type_str(&self) -> String {
        let ty = &self.ty;
        quote::quote! { #ty }.to_string()
    }

    /// 获取内部类型（如果是 Option<T>，返回 T 的类型字符串）
    #[allow(dead_code)]
    pub fn inner_type_str(&self) -> String {
        if let Some(inner) = Self::extract_option_inner(&self.ty) {
            quote::quote! { #inner }.to_string()
        } else {
            self.type_str()
        }
    }

    /// 检查类型是否是 Option<T>
    pub fn is_option(&self) -> bool {
        Self::is_option_type(&self.ty)
    }

    /// 检查类型是否是 Vec<T>
    pub fn is_vec(&self) -> bool {
        Self::is_vec_type(&self.ty)
    }

    /// 检查类型是否是 String
    pub fn is_string(&self) -> bool {
        self.type_str() == "String"
    }

    /// 检查类型是否是 Bytes
    pub fn is_bytes(&self) -> bool {
        self.type_str() == "Bytes"
    }

    /// 检查类型是否是 bool
    pub fn is_bool(&self) -> bool {
        self.type_str() == "bool"
    }

    /// 检查类型是否是整数类型
    pub fn is_integer(&self) -> bool {
        let s = self.type_str();
        matches!(
            s.as_str(),
            "i8" | "i16" | "i32" | "i64" | "u8" | "u16" | "u32" | "u64"
        )
    }

    /// 检查类型是否是浮点类型
    pub fn is_float(&self) -> bool {
        self.type_str() == "f64"
    }

    /// 检查类型是否是数字类型
    #[allow(dead_code)]
    pub fn is_numeric(&self) -> bool {
        self.is_integer() || self.is_float()
    }

    /// 检查类型是否是 UUID
    pub fn is_uuid(&self) -> bool {
        self.type_str() == "Uuid"
    }

    /// 检查字段是否在指定版本存在
    #[allow(dead_code)]
    pub fn is_available(&self, version: i16) -> bool {
        self.versions.contains(version)
    }

    /// 检查字段在指定版本是否可空
    #[allow(dead_code)]
    pub fn is_nullable(&self, version: i16) -> bool {
        self.nullable_versions
            .as_ref()
            .map(|v| v.contains(version))
            .unwrap_or(false)
    }

    /// 检查是否为标签字段
    #[allow(dead_code)]
    pub fn is_tagged(&self, version: i16) -> bool {
        self.tagged_versions
            .as_ref()
            .map(|v| v.contains(version))
            .unwrap_or(false)
    }

    /// 获取编码方法名
    #[allow(dead_code)]
    pub fn encode_method(&self, use_flexible: bool) -> String {
        #[allow(unused_variables)]
        let ty_str = self.type_str();

        if self.is_option() {
            return if use_flexible {
                "encode_compact_nullable".to_string()
            } else {
                "encode_nullable".to_string()
            };
        }

        if self.is_string() {
            return if use_flexible {
                "encode_compact_string".to_string()
            } else {
                "encode_string".to_string()
            };
        }

        if self.is_bytes() {
            return if use_flexible {
                "encode_compact_bytes".to_string()
            } else {
                "encode_bytes".to_string()
            };
        }

        if self.is_vec() {
            return if use_flexible {
                "encode_compact_array".to_string()
            } else {
                "encode_array".to_string()
            };
        }

        "encode_primitive".to_string()
    }

    /// 获取解码方法名
    #[allow(dead_code)]
    pub fn decode_method(&self, use_flexible: bool) -> String {
        #[allow(unused_variables)]
        let ty_str = self.type_str();

        if self.is_option() {
            return if use_flexible {
                "decode_compact_nullable".to_string()
            } else {
                "decode_nullable".to_string()
            };
        }

        if self.is_string() {
            return if use_flexible {
                "decode_compact_string".to_string()
            } else {
                "decode_string".to_string()
            };
        }

        if self.is_bytes() {
            return if use_flexible {
                "decode_compact_bytes".to_string()
            } else {
                "decode_bytes".to_string()
            };
        }

        if self.is_vec() {
            return if use_flexible {
                "decode_compact_array".to_string()
            } else {
                "decode_array".to_string()
            };
        }

        "decode_primitive".to_string()
    }

    // ============ 静态辅助方法 ============

    /// 检查类型是否是 Option<T>
    pub fn is_option_type(ty: &Type) -> bool {
        if let Type::Path(type_path) = ty {
            if let Some(segment) = type_path.path.segments.first() {
                return segment.ident == "Option";
            }
        }
        false
    }

    /// 检查类型是否是 Vec<T>
    pub fn is_vec_type(ty: &Type) -> bool {
        if let Type::Path(type_path) = ty {
            if let Some(segment) = type_path.path.segments.first() {
                return segment.ident == "Vec";
            }
        }
        false
    }

    /// 从 Option<T> 中提取内部类型
    pub fn extract_option_inner(ty: &Type) -> Option<Type> {
        if let Type::Path(type_path) = ty {
            if let Some(segment) = type_path.path.segments.first() {
                if segment.ident == "Option" {
                    if let PathArguments::AngleBracketed(args) = &segment.arguments {
                        if let Some(GenericArgument::Type(inner)) = args.args.first() {
                            return Some(inner.clone());
                        }
                    }
                }
            }
        }
        None
    }

    /// 从 Vec<T> 中提取内部类型
    pub fn extract_vec_inner(ty: &Type) -> Option<Type> {
        if let Type::Path(type_path) = ty {
            if let Some(segment) = type_path.path.segments.first() {
                if segment.ident == "Vec" {
                    if let PathArguments::AngleBracketed(args) = &segment.arguments {
                        if let Some(GenericArgument::Type(inner)) = args.args.first() {
                            return Some(inner.clone());
                        }
                    }
                }
            }
        }
        None
    }
}

/// 字段解析状态
struct FieldParseState {
    versions: VersionRange,
    nullable_versions: Option<VersionRange>,
    tag: Option<u32>,
    tagged_versions: Option<VersionRange>,
    ignorable: bool,
    default: Option<String>,
    map_key: bool,
    about: Option<String>,
    encoded_as_bytes: bool,
}

impl Default for FieldParseState {
    fn default() -> Self {
        Self {
            versions: VersionRange::All,
            nullable_versions: None,
            tag: None,
            tagged_versions: None,
            ignorable: false,
            default: None,
            map_key: false,
            about: None,
            encoded_as_bytes: false,
        }
    }
}

/// 解析 kafka 属性
fn parse_kafka_attr(attr: &syn::Attribute, state: &mut FieldParseState) {
    attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("versions") {
            let value: syn::LitStr = meta.value()?.parse()?;
            state.versions = VersionRange::parse(&value.value());
        } else if meta.path.is_ident("nullable_versions") {
            let value: syn::LitStr = meta.value()?.parse()?;
            state.nullable_versions = Some(VersionRange::parse(&value.value()));
        } else if meta.path.is_ident("tag") {
            let value: syn::LitInt = meta.value()?.parse()?;
            state.tag = Some(value.base10_parse()?);
        } else if meta.path.is_ident("tagged_versions") {
            let value: syn::LitStr = meta.value()?.parse()?;
            state.tagged_versions = Some(VersionRange::parse(&value.value()));
        } else if meta.path.is_ident("ignorable") {
            state.ignorable = true;
        } else if meta.path.is_ident("default") {
            let value: syn::LitStr = meta.value()?.parse()?;
            state.default = Some(value.value());
        } else if meta.path.is_ident("map_key") {
            state.map_key = true;
        } else if meta.path.is_ident("about") {
            let value: syn::LitStr = meta.value()?.parse()?;
            state.about = Some(value.value());
        } else if meta.path.is_ident("encoded_as_bytes") {
            state.encoded_as_bytes = true;
        }
        Ok(())
    })
    .ok();
}
