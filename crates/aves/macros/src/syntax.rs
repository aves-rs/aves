use proc_macro2::TokenStream as TokenStream2;
use quote::quote;

pub(super) fn request_base_name(request_type: &str) -> String {
    let type_name = request_type
        .split_whitespace()
        .last()
        .unwrap_or(request_type)
        .trim_start_matches("::");
    type_name
        .strip_suffix("RequestPayload")
        .unwrap_or(type_name)
        .to_string()
}

pub(super) fn to_upper_snake(name: &str) -> String {
    let mut rendered = String::new();
    for (index, character) in name.chars().enumerate() {
        if character.is_ascii_uppercase() && index > 0 {
            rendered.push('_');
        }
        rendered.push(character.to_ascii_uppercase());
    }
    rendered
}

pub(super) fn to_snake_case(name: &str) -> String {
    let mut rendered = String::new();
    for (index, character) in name.chars().enumerate() {
        if character.is_ascii_uppercase() && index > 0 {
            rendered.push('_');
        }
        rendered.push(character.to_ascii_lowercase());
    }
    rendered
}

pub(super) fn lower_camel_case(name: &str) -> String {
    let mut characters = name.chars();
    let Some(first) = characters.next() else {
        return String::new();
    };

    format!("{}{}", first.to_ascii_lowercase(), characters.collect::<String>())
}

pub(super) fn compile_error(message: String) -> TokenStream2 {
    quote! { compile_error!(#message); }
}
