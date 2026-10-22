mod domain;
mod error_code;
mod registry;
mod request;
mod response;
mod routing;
mod syntax;

use std::str::FromStr;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;

use domain::SudoServices;
use error_code::render_error_codes;
use registry::{RegisteredRequest, register_request, register_services};
use request::{RequestEnum, strip_request_metadata_attributes};
use response::ResponseEnum;
use routing::explicit_transport_error_conversions;
use syntax::compile_error;

/// Generates the typed, channel-backed sudo services for one domain.
///
/// The declaration owns the service names, request variants, response variants,
/// and service registry metadata. The optional `marker` names the empty domain
/// marker that owns the provider registry for lifecycle domains. Multiple
/// service blocks may be declared for one request/response pair.
#[proc_macro]
pub fn domain(input: TokenStream) -> TokenStream {
    match SudoServices::parse(TokenStream2::from(input)) {
        Ok(services) => {
            let mapped = services
                .services
                .iter()
                .flat_map(|service| service.mappings.iter())
                .map(|mapping| mapping.request_variant.clone())
                .collect::<Vec<_>>();
            match register_services(&services.request, &services.target_domain, mapped) {
                Ok(()) => {
                    let generated = explicit_transport_error_conversions(
                        services.render(),
                        &services.error,
                        &services.transport_error_path,
                    );
                    match TokenStream2::from_str(&generated) {
                        Ok(tokens) => tokens.into(),
                        Err(error) => compile_error(format!(
                            "domain generated invalid Rust tokens: {error}; excerpt: {}",
                            generated.chars().take(4000).collect::<String>()
                        ))
                        .into(),
                    }
                }
                Err(error) => compile_error(error).into(),
            }
        }
        Err(error) => compile_error(error).into(),
    }
}

/// Marks a canonical bus request enum for generated RPC metadata.
///
/// Preserves the annotated item unchanged.
#[proc_macro_attribute]
pub fn bus_requests(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr = TokenStream2::from(attr);
    let item = TokenStream2::from(item);
    let generated = match RequestEnum::parse(attr, item.clone()) {
        Ok(request_enum) => {
            let privileged = request_enum
                .variants
                .iter()
                .filter(|variant| variant.privileged)
                .map(|variant| variant.name.clone())
                .collect::<Vec<_>>();
            match register_request(RegisteredRequest {
                name: request_enum.name.clone(),
                domain: request_enum.domain.clone(),
                privileged,
            }) {
                Ok(()) => request_enum.render(),
                Err(error) => compile_error(error).to_string(),
            }
        }
        Err(error) => compile_error(error).to_string(),
    };
    let item_without_metadata = strip_request_metadata_attributes(item);

    TokenStream::from_str(&format!("{item_without_metadata}\n{generated}"))
        .unwrap_or_else(|error| compile_error(format!("bus_requests generated invalid Rust tokens: {error}")).into())
}

/// Marks a canonical response enum for generated RPC extractors.
///
/// Preserves the annotated item unchanged.
#[proc_macro_attribute]
pub fn bus_responses(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr = TokenStream2::from(attr);
    let item = TokenStream2::from(item);
    let item_string = item.to_string();
    let generated = if let Ok(response_enum) = ResponseEnum::parse(attr, item.clone()) {
        response_enum.render()
    } else {
        compile_error("bus_responses failed to parse response enum".to_string()).to_string()
    };

    TokenStream::from_str(&format!("{item_string}\n{generated}"))
        .unwrap_or_else(|error| compile_error(format!("bus_responses generated invalid Rust tokens: {error}")).into())
}

/// Derives stable RPC codes from error variant names, with explicit compatibility aliases.
#[proc_macro_derive(ErrorCode, attributes(error_code))]
pub fn derive_error_code(item: TokenStream) -> TokenStream {
    match render_error_codes(item.into()) {
        Ok(generated) => generated.into(),
        Err(error) => compile_error(error).into(),
    }
}

#[cfg(test)]
mod tests;
