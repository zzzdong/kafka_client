// src/size.rs
use quote::quote;
use proc_macro2::TokenStream;
use crate::field::FieldInfo;
use crate::version_range::VersionRange;

/// 判断字段是否应该使用 flexible format
fn should_use_flexible(field: &FieldInfo, flexible_version: Option<i16>) -> bool {
    if field.tagged_versions.is_some() {
        return true;
    }
    match flexible_version {
        Some(v) => field.versions.min_version() >= v,
        None => false,
    }
}

/// 生成 size 方法
pub fn generate_size(fields: &[FieldInfo], flexible_version: Option<i16>) -> TokenStream {
    let size_fields: Vec<TokenStream> = fields
        .iter()
        .map(|field| generate_size_field(field, flexible_version))
        .collect();
    
    quote! {
        fn size(&self, version: i16) -> usize {
            use kafka_client_protocol_core::codec::*;
            
            let mut total = 0;
            #(#size_fields)*
            total
        }
    }
}

/// 生成单个字段的大小计算
fn generate_size_field(field: &FieldInfo, flexible_version: Option<i16>) -> TokenStream {
    let field_name = &field.name;
    let condition = field.versions.as_check_expr();
    let use_flexible = should_use_flexible(field, flexible_version);
    
    // 标签字段
    if field.tagged_versions.is_some() {
        return quote! {
            if #condition {
                if !self.#field_name.is_default() {
                    total += varint_len(self.#field_name.size(version) as u32 + 1);
                    total += self.#field_name.size(version);
                }
            }
        };
    }
    
    // 可空字段 - 只有当类型是 Option 时才生成 is_none 检查
    if let Some(nullable_versions) = &field.nullable_versions {
        let null_condition = nullable_versions.as_check_expr();
        
        if field.is_option() {
            // Option 类型：检查 is_none()
            return quote! {
                if #condition {
                    if #null_condition && self.#field_name.is_none() {
                        if #use_flexible {
                            total += 1; // varint(0)
                        } else {
                            total += 4; // -1
                        }
                    } else {
                        total += self.#field_name.size(version);
                    }
                }
            };
        } else {
            // 非 Option 类型（如 int 使用 default 值表示空）
            // 不需要 is_none 检查，直接计算大小
            return quote! {
                if #condition {
                    total += self.#field_name.size(version);
                }
            };
        }
    }
    
    // 普通字段
    if field.versions == VersionRange::All {
        quote! { total += self.#field_name.size(version); }
    } else {
        quote! {
            if #condition {
                total += self.#field_name.size(version);
            }
        }
    }
}