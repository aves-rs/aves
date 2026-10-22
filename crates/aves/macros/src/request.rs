use proc_macro2::{Delimiter, Group, TokenStream as TokenStream2, TokenTree};
use quote::ToTokens;
use syn::parse::Parser;

use super::syntax::{compile_error, to_snake_case, to_upper_snake};

#[derive(Debug)]
pub(super) struct RequestEnum {
    pub(super) name: String,
    pub(super) domain: String,
    pub(super) marker: Option<String>,
    pub(super) context_type: String,
    pub(super) root_error: String,
    pub(super) domain_error: String,
    pub(super) variants: Vec<RequestVariant>,
}

#[derive(Debug)]
pub(super) struct RequestVariant {
    pub(super) name: String,
    pub(super) fields: RequestFields,
    pub(super) privileged: bool,
    pub(super) operation: Option<RequestOperation>,
}

#[derive(Debug)]
pub(super) struct RequestOperation {
    pub(super) name: String,
    pub(super) kind: OperationKind,
    pub(super) response: String,
    pub(super) response_variant: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum OperationKind {
    Query,
    Mutation,
}

impl OperationKind {
    pub(super) fn parse(value: &str) -> Result<Self, String> {
        match value {
            "query" => Ok(Self::Query),
            "mutation" => Ok(Self::Mutation),
            other => Err(format!(
                "operation metadata kind must be \"query\" or \"mutation\", found `{other}`"
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Mutation => "mutation",
        }
    }
}

#[derive(Debug)]
pub(super) enum RequestFields {
    Unit,
    Named(Vec<RequestField>),
    Tuple(Vec<String>),
}

#[derive(Debug)]
pub(super) struct RequestField {
    pub(super) name: String,
    pub(super) ty: String,
    pub(super) attrs: String,
}

impl RequestField {
    fn public_ty(&self) -> String {
        public_projection_type(&self.ty)
    }

    fn make_canonical_expression(&self, privileged: bool) -> String {
        if privileged || self.public_ty() == self.ty {
            self.name.clone()
        } else {
            format!("{}.into()", self.name)
        }
    }
}

impl RequestEnum {
    pub(super) fn parse(attr: TokenStream2, item: TokenStream2) -> Result<Self, String> {
        let (name, body) = parse_enum_name_and_body(item, "bus_requests")?;
        let domain = parse_domain_attr(attr.clone())?.unwrap_or_else(|| infer_request_domain_name(&name));
        let marker = parse_type_attr(&attr, "marker")?;
        let context_type = parse_type_attr(&attr, "context")?
            .ok_or_else(|| "bus_requests requires context = <context type path>".to_string())?;
        let domain_error = parse_type_attr(&attr, "domain_error")?
            .ok_or_else(|| "bus_requests requires domain_error = <domain error type path>".to_string())?;
        let root_error = parse_type_attr(&attr, "root_error")?.unwrap_or_else(|| domain_error.clone());
        let variants = parse_request_variants(body)?;
        if marker.is_none()
            && variants
                .iter()
                .any(|variant| !variant.privileged && variant.operation.is_some())
        {
            return Err("bus_requests requires marker = <domain marker type path>".to_string());
        }
        reject_duplicate_operations(&domain, &variants)?;

        Ok(Self {
            name,
            domain,
            marker,
            context_type,
            root_error,
            domain_error,
            variants,
        })
    }

    pub(super) fn render(&self) -> String {
        let mut rendered = String::new();
        let request_base = self
            .name
            .strip_suffix("RequestPayload")
            .unwrap_or(&self.name);
        let mapping_trait = format!("{request_base}SudoServicePrivilegeMapping");

        rendered.push_str(&format!(
            "#[doc = \"Marker trait implemented by the aggregate sudo-service registry. Every privileged request variant must implement it at least once; multiple service memberships remain valid. Knowledge request enums with no privileged variants intentionally require no registry.\"]\npub trait {mapping_trait} {{}}\n"
        ));

        for variant in &self.variants {
            if variant.privileged || variant.operation.is_some() {
                rendered.push_str(&variant.render_input_struct());
                rendered.push_str(&variant.render_from_impl(&self.name));
            }
            if variant.privileged {
                let marker = format!("{request_base}Privileged{}", variant.name);
                let assertion = format!("assert_sudo_service_mapping_for_{marker}");
                rendered.push_str(&format!(
                    "#[doc = \"Compile-time marker for privileged request variant {variant_name}.\"]\npub struct {marker};\n#[allow(dead_code)]\nfn {assertion}() {{\n    fn assert_sudo_service_mapping<T: {mapping_trait}>() {{}}\n    assert_sudo_service_mapping::<{marker}>();\n}}\n",
                    variant_name = variant.name,
                ));
            }
        }

        rendered.push_str(&self.render_descriptors());
        rendered.push_str(&self.render_rpc_router());
        rendered.push_str(&self.render_public_bus_clients());
        rendered
    }

    fn response_enum_name(&self) -> String {
        format!(
            "{}Response",
            self.name
                .strip_suffix("RequestPayload")
                .unwrap_or(&self.name)
        )
    }

    pub(super) fn render_rpc_router(&self) -> String {
        let procedures = self
            .variants
            .iter()
            .filter_map(|variant| {
                if variant.privileged {
                    return None;
                }
                variant
                    .operation
                    .as_ref()
                    .map(|_| variant.render_rpc_procedure(&self.context_type, &self.root_error))
            })
            .collect::<Vec<_>>()
            .join("\n");

        format!(
            "#[doc = \"Generated typed RPC router for this bus domain.\"]\npub fn rpc_router() -> rspc::Router<{context_type}> {{\n    rspc::Router::<{context_type}>::new()\n{procedures}\n}}\n",
            context_type = self.context_type,
        )
    }

    pub(super) fn render_descriptors(&self) -> String {
        let const_name = format!("{}_OPERATIONS", to_upper_snake(&self.name));
        let root_error_descriptor_function = format!(
            "{}_operation_descriptors_with_root_error",
            to_snake_case(
                self.name
                    .strip_suffix("RequestPayload")
                    .unwrap_or(&self.name)
            )
        );
        let descriptors = self
            .variants
            .iter()
            .filter_map(|variant| {
                if variant.privileged {
                    return None;
                }
                variant
                    .operation
                    .as_ref()
                    .map(|operation| self.render_operation_descriptor(variant, operation, &self.root_error, false))
            })
            .collect::<Vec<_>>()
            .join(",\n");
        let root_error_dispatchers = self
            .variants
            .iter()
            .filter_map(|variant| {
                if variant.privileged {
                    return None;
                }
                variant.operation.as_ref().map(|operation| {
                    let dispatcher = format!("dispatch_{}_json", to_snake_case(&operation.name));
                    let adapter = format!("{}_with_root_error", dispatcher);
                    format!(
                        "fn {adapter}<RootError>(ctx: {context_type}, arguments: ::serde_json::Value) -> ::aves::OperationDispatchFuture<RootError>\nwhere\n    RootError: ::core::convert::From<{source_error}> + Send + 'static,\n{{\n    Box::pin(async move {{ {dispatcher}(ctx, arguments).await.map_err(::core::convert::From::from) }})\n}}",
                        context_type = self.context_type,
                        source_error = self.root_error,
                    )
                })
            })
            .collect::<Vec<_>>()
            .join("\n\n");
        let root_error_descriptors = self
            .variants
            .iter()
            .filter_map(|variant| {
                if variant.privileged {
                    return None;
                }
                variant
                    .operation
                    .as_ref()
                    .map(|operation| self.render_operation_descriptor(variant, operation, "RootError", true))
            })
            .collect::<Vec<_>>()
            .join(",\n");
        let assertions = self
            .variants
            .iter()
            .filter_map(|variant| {
                if variant.privileged {
                    return None;
                }
                variant.operation.as_ref().map(|operation| {
                    let response_type = &operation.response;
                    format!(
                        "const _: () = {{ fn assert_response_link<T: ::specta::Type>() {{}} let _ = assert_response_link::<{response_type}>; }};"
                    )
                })
            })
            .collect::<Vec<_>>()
            .join("\n");

        format!(
            "#[doc = \"Generated operation metadata for canonical request variants.\"]\npub const {const_name}: &[::aves::RequestOperationDescriptor<{context_type}, {root_error}>] = &[\n{descriptors}\n];\n\n{root_error_dispatchers}\n\n#[doc = \"Generated operation metadata adapted to an embedding root error.\"]\npub fn {root_error_descriptor_function}<RootError>() -> ::std::vec::Vec<::aves::RequestOperationDescriptor<{context_type}, RootError>>\nwhere\n    RootError: ::core::convert::From<{root_error}> + Send + 'static,\n{{\n    vec![\n{root_error_descriptors}\n    ]\n}}\n{assertions}\n",
            context_type = self.context_type,
            root_error = self.root_error,
        )
    }

    fn render_operation_descriptor(
        &self,
        variant: &RequestVariant,
        operation: &RequestOperation,
        error_type: &str,
        generic_error: bool,
    ) -> String {
        let input = variant.input_name();
        let dispatch = if generic_error {
            format!(
                "dispatch_{}_json_with_root_error::<RootError>",
                to_snake_case(&operation.name)
            )
        } else {
            format!("dispatch_{}_json", to_snake_case(&operation.name))
        };
        format!(
            "::aves::RequestOperationDescriptor::<{}, {}> {{ domain: \"{}\", operation: \"{}\", qualified_operation: \"{}.{}\", canonical_variant: \"{}::{}\", kind: ::aves::RequestOperationKind::{}, mcp_eligible: true, input_type: \"{}\", response_type: \"{}\", response_variant: \"{}\", input_schema: || ::schemars::schema_for!({}), output_schema: || ::schemars::schema_for!({}), dispatch: {} }}",
            self.context_type,
            error_type,
            self.domain,
            operation.name,
            self.domain,
            operation.name,
            self.name,
            variant.name,
            match operation.kind {
                OperationKind::Query => "Query",
                OperationKind::Mutation => "Mutation",
            },
            input,
            operation.response,
            operation.response_variant,
            input,
            operation.response,
            dispatch,
        )
    }

    fn render_public_operation(&self, variant: &RequestVariant, operation: &RequestOperation) -> String {
        format!(
            "{}\n\n{}\n\n{}\n",
            self.render_typed_dispatcher(variant, operation),
            self.render_json_dispatcher(variant, operation),
            self.render_operation_sender(variant, operation),
        )
    }

    fn render_typed_dispatcher(&self, variant: &RequestVariant, operation: &RequestOperation) -> String {
        let operation_name = &operation.name;
        let dispatcher = format!("dispatch_{}", to_snake_case(operation_name));
        let broad_name = format!("send_{}_request_internal", to_snake_case(&self.domain));
        let input_name = variant.input_name();
        let output_name = &operation.response;
        let response_enum = self.response_enum_name();
        let root_error = &self.root_error;
        let domain_error = &self.domain_error;
        format!(
            "#[doc = \"Canonical typed dispatcher for the {operation_name} operation.\"]\npub async fn {dispatcher}(context: &::aves::BusRpcContext, input: {input_name}) -> ::core::result::Result<{output_name}, {root_error}>\nwhere\n    {root_error}: ::core::convert::From<::aves::GeneratedCallError<{domain_error}>>,\n{{\n    let jwt = context.jwt()?;\n    let sender = context.bus_sender()?;\n    let response = {broad_name}(jwt, &sender, input).await?;\n    <{output_name} as ::core::convert::TryFrom<{response_enum}>>::try_from(response).map_err(::aves::GeneratedCallError::<{domain_error}>::from).map_err(<{root_error} as ::core::convert::From<::aves::GeneratedCallError<{domain_error}>>>::from)\n}}"
        )
    }

    fn render_json_dispatcher(&self, variant: &RequestVariant, operation: &RequestOperation) -> String {
        let operation_name = &operation.name;
        let dispatcher = format!("dispatch_{}", to_snake_case(operation_name));
        let json_dispatcher = format!("{dispatcher}_json");
        let input_name = variant.input_name();
        let root_error = &self.root_error;
        format!(
            "#[doc = \"JSON adapter for the canonical typed {operation_name} dispatcher.\"]\npub(crate) fn {json_dispatcher}(ctx: {context_type}, arguments: ::serde_json::Value) -> ::aves::OperationDispatchFuture<{root_error}> {{\n    Box::pin(async move {{\n        ctx.admit()?;\n        let input = ::serde_json::from_value::<{input_name}>(arguments)?;\n        let output = {dispatcher}(&ctx, input).await?;\n        Ok(::serde_json::to_value(output)?)\n    }})\n}}",
            context_type = self.context_type
        )
    }

    fn render_operation_sender(&self, variant: &RequestVariant, operation: &RequestOperation) -> String {
        let operation_name = &operation.name;
        let function_name = format!("send_{}", to_snake_case(operation_name));
        let broad_name = format!("send_{}_request_internal", to_snake_case(&self.domain));
        let input_name = variant.input_name();
        let output_name = &operation.response;
        let response_enum = self.response_enum_name();
        let domain_error = &self.domain_error;
        let sender = "&::tokio::sync::mpsc::Sender<::aves::OpaqueRequestEnvelope>";
        format!(
            "#[doc = \"Sends the typed {operation_name} request through the marker-keyed bus.\"]\npub async fn {function_name}(jwt: ::aves::Jwt, sender: {sender}, input: {input_name}) -> ::core::result::Result<{output_name}, ::aves::GeneratedCallError<{domain_error}>> {{\n    let response = {broad_name}(jwt, sender, input).await?;\n    <{output_name} as ::core::convert::TryFrom<{response_enum}>>::try_from(response).map_err(::aves::GeneratedCallError::<{domain_error}>::from)\n}}"
        )
    }

    pub(super) fn render_public_bus_clients(&self) -> String {
        let response_enum = self.response_enum_name();
        let domain_error = &self.domain_error;
        let request_payload = &self.name;
        let request_base = self
            .name
            .strip_suffix("RequestPayload")
            .unwrap_or(&self.name);
        let request_wrapper = self
            .name
            .strip_suffix("Payload")
            .map_or_else(|| format!("{}Request", self.name), |name| name.to_string());
        let Some(marker) = self.marker.as_deref() else {
            return String::new();
        };
        let sender = "&::tokio::sync::mpsc::Sender<::aves::OpaqueRequestEnvelope>";
        let broad_name = format!("send_{}_request_internal", to_snake_case(&self.domain));

        let operation_functions = self
            .variants
            .iter()
            .filter(|variant| !variant.privileged)
            .filter_map(|variant| {
                variant
                    .operation
                    .as_ref()
                    .map(|operation| self.render_public_operation(variant, operation))
            })
            .collect::<Vec<_>>()
            .join("\n");

        let public_inputs = self
            .variants
            .iter()
            .filter(|variant| !variant.privileged && variant.operation.is_some())
            .map(|variant| format!("impl __{request_base}PublicRequest for {input_name} {{\n    fn into_payload(self) -> {request_payload} {{ {request_payload}::from(self) }}\n}}\n", input_name = variant.input_name(), request_base = request_base, request_payload = self.name))
            .collect::<Vec<_>>()
            .join("\n");

        if operation_functions.is_empty() {
            return String::new();
        }

        format!(
            "#[doc = \"Internal typed request capability for non-privileged bus operations.\"]\ntrait __{request_base}PublicRequest {{\n    fn into_payload(self) -> {request_payload};\n}}\n{public_inputs}\n#[doc = \"Sends a non-privileged domain request through the marker-keyed bus.\"]\npub(crate) async fn {broad_name}<RequestInput>(jwt: ::aves::Jwt, sender: {sender}, input: RequestInput) -> ::core::result::Result<{response_enum}, ::aves::GeneratedCallError<{domain_error}>>\nwhere\n    RequestInput: __{request_base}PublicRequest,\n{{\n    let payload = input.into_payload();\n    let (reply_to, mut reply) = ::tokio::sync::oneshot::channel();\n    let (admission_error, admission_error_receiver) = ::tokio::sync::oneshot::channel();\n    let request = {request_wrapper} {{ payload, reply_to }};\n    sender\n        .clone()\n        .send(::aves::OpaqueRequestEnvelope::new_with_rejection::<{marker}, _>(jwt, request, move |error| {{\n            if let Ok(error) = error.downcast::<::aves::AvesAuthError>() {{\n                let _ = admission_error.send(*error);\n            }}\n        }}))\n        .await\n        .map_err(|_| ::aves::GeneratedCallError::InputClosed)?;\n    ::tokio::select! {{\n        biased;\n        result = admission_error_receiver => match result {{\n            Ok(error) => Err(::aves::GeneratedCallError::Authentication(error)),\n            Err(_) => reply\n                .await\n                .map_err(|_| ::aves::GeneratedCallError::ReplyDropped)?\n                .map_err(::aves::GeneratedCallError::DomainError),\n        }},\n        result = &mut reply => result\n            .map_err(|_| ::aves::GeneratedCallError::ReplyDropped)?\n            .map_err(::aves::GeneratedCallError::DomainError),\n    }}\n}}\n{operation_functions}"
        )
    }
}

impl RequestVariant {
    fn input_name(&self) -> String {
        format!("{}Input", self.name)
    }

    pub(super) fn render_input_struct(&self) -> String {
        let input_name = self.input_name();
        if self.privileged {
            return self.render_internal_input_struct(&input_name);
        }

        match &self.fields {
            RequestFields::Unit => format!(
                "#[doc = \"Generated operation-specific input projection for this request variant.\"]\n#[derive(Clone, ::serde::Deserialize, ::serde::Serialize, ::specta::Type, ::schemars::JsonSchema)]\n#[serde(rename_all = \"camelCase\")]\npub struct {input_name} {{}}\nimpl ::core::fmt::Debug for {input_name} {{ fn fmt(&self, formatter: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {{ formatter.write_str(\"{input_name}\") }} }}\n"
            ),
            RequestFields::Named(fields) => {
                let declarations = fields
                    .iter()
                    .map(|field| {
                        let attrs = if field.attrs.is_empty() {
                            String::new()
                        } else {
                            format!("{}\n", field.attrs)
                        };
                        let public_ty = field.public_ty();
                        format!("{attrs}pub {}: {},", field.name, public_ty)
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                format!(
                    "#[doc = \"Generated operation-specific input projection for this request variant.\"]\n#[derive(Clone, ::serde::Deserialize, ::serde::Serialize, ::specta::Type, ::schemars::JsonSchema)]\n#[serde(rename_all = \"camelCase\")]\npub struct {input_name} {{\n{declarations}\n}}\n{}\n",
                    render_named_debug_impl(&input_name, fields)
                )
            }
            RequestFields::Tuple(fields) => {
                let declarations = fields
                    .iter()
                    .map(|field| {
                        let public_ty = public_projection_type(field);
                        format!("pub {public_ty}")
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "#[doc = \"Generated operation-specific input projection for this request variant.\"]\n#[derive(Clone, ::serde::Deserialize, ::serde::Serialize, ::specta::Type, ::schemars::JsonSchema)]\n#[serde(rename_all = \"camelCase\")]\npub struct {input_name}({declarations});\nimpl ::core::fmt::Debug for {input_name} {{ fn fmt(&self, formatter: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {{ formatter.write_str(\"{input_name}(<redacted>)\") }} }}\n"
                )
            }
        }
    }

    fn render_internal_input_struct(&self, input_name: &str) -> String {
        match &self.fields {
            RequestFields::Unit => format!("#[doc(hidden)]\npub struct {input_name} {{}}\n"),
            RequestFields::Named(fields) => {
                let declarations = fields
                    .iter()
                    .map(|field| {
                        // Keep privileged inputs as a private projection of the
                        // original variant, rather than feeding the public
                        // type projection back through the renderer.  The
                        // explicit field declaration also keeps the generated
                        // struct syntax independent from the source field's
                        // attributes.
                        format!("pub {}: {},", field.name, normalize_type(&field.ty))
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("#[doc(hidden)]\npub struct {input_name} {{\n{declarations}\n}}\n")
            }
            RequestFields::Tuple(fields) => {
                let declarations = fields
                    .iter()
                    .map(|field| format!("pub {field}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("#[doc(hidden)]\npub struct {input_name}({declarations});\n")
            }
        }
    }

    fn render_from_impl(&self, enum_name: &str) -> String {
        let input_name = self.input_name();
        let variant_name = &self.name;
        let construction = match &self.fields {
            RequestFields::Unit => format!("{enum_name}::{variant_name}"),
            RequestFields::Named(fields) => {
                let names = fields
                    .iter()
                    .map(|field| field.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                let assignments = fields
                    .iter()
                    .map(|field| format!("{}: {}", field.name, field.make_canonical_expression(self.privileged)))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "let {input_name} {{ {names} }} = input;\n        {enum_name}::{variant_name} {{ {assignments} }}"
                )
            }
            RequestFields::Tuple(fields) => {
                let names = (0..fields.len())
                    .map(|index| format!("field_{index}"))
                    .collect::<Vec<_>>();
                let pattern = names.join(", ");
                let args = fields
                    .iter()
                    .enumerate()
                    .map(|(index, field)| {
                        let name = format!("field_{index}");
                        if self.privileged || public_projection_type(field) == *field {
                            name
                        } else {
                            format!("{name}.into()")
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("let {input_name}({pattern}) = input;\n        {enum_name}::{variant_name}({args})")
            }
        };

        format!(
            "impl ::core::convert::From<{input_name}> for {enum_name} {{\n    fn from(input: {input_name}) -> Self {{\n        {construction}\n    }}\n}}\n"
        )
    }

    fn render_rpc_procedure(&self, context_type: &str, root_error: &str) -> String {
        let Some(operation_metadata) = self.operation.as_ref() else {
            return compile_error(format!(
                "request variant `{}` has no operation metadata and cannot render an RPC procedure",
                self.name
            ))
            .to_string();
        };
        let operation = &operation_metadata.name;
        let procedure_kind = operation_metadata.kind.as_str();
        let input_name = self.input_name();
        let output_name = &operation_metadata.response;
        let dispatcher = format!("dispatch_{}", to_snake_case(operation));

        format!(
            "        .procedure(\n            \"{operation}\",\n            rspc::Procedure::<{context_type}, {input_name}, {output_name}>::builder::<{root_error}>().{procedure_kind}(\n                |ctx, input| async move {{\n                    ctx.admit()?;\n                    {dispatcher}(&ctx, input).await\n                }},\n            ),\n        )",
            context_type = context_type,
            root_error = root_error,
            dispatcher = dispatcher,
            operation = operation,
            input_name = input_name,
            output_name = output_name,
            procedure_kind = procedure_kind,
        )
    }
}

fn parse_domain_attr(attr: TokenStream2) -> Result<Option<String>, String> {
    let tokens = attr.into_iter().collect::<Vec<_>>();
    let mut index = 0;
    while index < tokens.len() {
        if matches!(tokens.get(index), Some(TokenTree::Ident(ident)) if ident == "domain") {
            let value_index = index + 2;
            return match tokens.get(value_index) {
                Some(TokenTree::Literal(literal)) => Ok(Some(unquote_literal(&literal.to_string()))),
                _ => Err("bus_requests domain must be a string literal".to_string()),
            };
        }
        index += 1;
    }

    Ok(None)
}

fn parse_enum_name_and_body(item: TokenStream2, macro_name: &str) -> Result<(String, Group), String> {
    let mut enum_name = None;
    let mut body = None;
    let mut saw_enum = false;
    for token in item {
        match token {
            TokenTree::Ident(ident) if ident == "enum" => saw_enum = true,
            TokenTree::Ident(ident) if saw_enum && enum_name.is_none() => {
                enum_name = Some(ident.to_string());
            }
            TokenTree::Group(group) if group.delimiter() == Delimiter::Brace && enum_name.is_some() => {
                body = Some(group);
                break;
            }
            _ => {}
        }
    }
    let name = enum_name.ok_or_else(|| format!("{macro_name} must be applied to an enum"))?;
    let body = body.ok_or_else(|| format!("{macro_name} could not read body for enum {name}"))?;
    Ok((name, body))
}

pub(super) fn parse_type_attr(attr: &TokenStream2, key: &str) -> Result<Option<String>, String> {
    let tokens = attr.clone().into_iter().collect::<Vec<_>>();
    let Some(key_index) = tokens
        .iter()
        .position(|token| matches!(token, TokenTree::Ident(ident) if ident == key))
    else {
        return Ok(None);
    };
    if !matches!(tokens.get(key_index + 1), Some(TokenTree::Punct(punct)) if punct.as_char() == '=') {
        return Err(format!("bus_requests {key} must use `= <type path>`"));
    }

    let value = tokens
        .iter()
        .skip(key_index + 2)
        .take_while(|token| !matches!(token, TokenTree::Punct(punct) if punct.as_char() == ','))
        .map(TokenTree::to_string)
        .collect::<Vec<_>>()
        .join(" ");
    if value.is_empty() {
        return Err(format!("bus_requests {key} must name a type path"));
    }
    Ok(Some(normalize_type(&value)))
}

fn parse_request_variants(body: Group) -> Result<Vec<RequestVariant>, String> {
    let parsed = split_top_level(body.stream(), ',')
        .into_iter()
        .filter(|tokens| !tokens.is_empty())
        .map(parse_request_variant)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(parsed.into_iter().flatten().collect())
}

fn parse_tuple_fields(group: Group) -> Vec<String> {
    split_top_level(group.stream(), ',')
        .into_iter()
        .filter(|tokens| !tokens.is_empty())
        .map(|tokens| {
            let field = strip_leading_attributes(tokens)
                .iter()
                .map(TokenTree::to_string)
                .collect::<Vec<_>>()
                .join(" ");
            normalize_type(&field)
        })
        .collect()
}

fn parse_request_variant(tokens: Vec<TokenTree>) -> Result<Option<RequestVariant>, String> {
    let mut index = 0;
    let name = loop {
        match tokens.get(index) {
            Some(TokenTree::Punct(punct)) if punct.as_char() == '#' => {
                index += 2;
            }
            Some(TokenTree::Ident(ident)) => break ident.to_string(),
            Some(_) => index += 1,
            None => return Err("bus_requests found a variant without a name".to_string()),
        }
    };

    let attrs = &tokens[..index];
    let privileged_reason = parse_privileged_reason(attrs)?;
    let skip_generation = has_serde_skip(attrs);
    let fields = match tokens.get(index + 1) {
        Some(TokenTree::Group(group)) if group.delimiter() == Delimiter::Brace => {
            RequestFields::Named(parse_request_named_fields(group.clone())?)
        }
        Some(TokenTree::Group(group)) if group.delimiter() == Delimiter::Parenthesis => {
            RequestFields::Tuple(parse_tuple_fields(group.clone()))
        }
        _ => RequestFields::Unit,
    };

    if let Some(reason) = privileged_reason {
        if reason.trim().is_empty() {
            return Err(format!("bus_requests variant {name} has privileged without a reason"));
        }
        return Ok(Some(RequestVariant {
            name,
            fields,
            privileged: true,
            operation: None,
        }));
    }

    let operation = parse_request_operation(attrs)?;
    if operation.is_none() && !skip_generation {
        return Err(format!(
            "bus_requests variant {name} requires #[operation(name = \"...\", kind = \"query\" | \"mutation\", response = ..., response_variant = \"...\")] or #[privileged(reason = \"...\")]"
        ));
    }

    Ok(Some(RequestVariant {
        name,
        fields,
        privileged: false,
        operation,
    }))
}

fn parse_request_operation(tokens: &[TokenTree]) -> Result<Option<RequestOperation>, String> {
    let mut operation = None;
    for attribute in attribute_strings(tokens) {
        if !attribute.starts_with("operation") {
            continue;
        }
        if operation.is_some() {
            return Err("bus_requests found duplicate operation attributes on one variant".to_string());
        }
        let name = parse_string_attribute_value(&attribute, "name")
            .ok_or_else(|| "operation metadata requires name = \"...\"".to_string())?;
        let response = parse_path_attribute_value(&attribute, "response")
            .ok_or_else(|| "operation metadata requires response = ...".to_string())?;
        let response_variant = parse_string_attribute_value(&attribute, "response_variant")
            .ok_or_else(|| "operation metadata requires response_variant = \"...\"".to_string())?;
        let kind = parse_operation_kind(&attribute)?;
        validate_response_linkage(&response, &response_variant)?;
        operation = Some(RequestOperation {
            name,
            kind,
            response,
            response_variant,
        });
    }

    Ok(operation)
}

pub(super) fn parse_operation_kind(attribute: &str) -> Result<OperationKind, String> {
    let values = attribute_token_values(attribute, "kind")
        .ok_or_else(|| "operation metadata requires kind = \"query\" or kind = \"mutation\"".to_string())?;
    if values.len() != 1 {
        return Err(format!(
            "operation metadata kind must be a single string literal, found `{}`",
            values.join(" ")
        ));
    }

    let value = values
        .first()
        .ok_or_else(|| "operation metadata requires kind = \"query\" or kind = \"mutation\"".to_string())?;
    if !value.starts_with('"') || !value.ends_with('"') {
        return Err(format!(
            "operation metadata kind must be a string literal, found `{value}`; use kind = \"query\" or kind = \"mutation\""
        ));
    }

    OperationKind::parse(&unquote_literal(value))
}

fn parse_privileged_reason(tokens: &[TokenTree]) -> Result<Option<String>, String> {
    parse_privileged_reason_attributes(&attribute_strings(tokens))
}

pub(super) fn parse_privileged_reason_attributes(attributes: &[String]) -> Result<Option<String>, String> {
    let mut reason = None;
    for attribute in attributes {
        if attribute.starts_with("rpc_skip") {
            return Err("bus_requests no longer accepts #[rpc_skip]; use #[privileged(reason = \"...\")]".to_string());
        }
        if !attribute.starts_with("privileged") {
            continue;
        }
        if reason.is_some() {
            return Err("bus_requests found duplicate privileged attributes on one variant".to_string());
        }
        reason = Some(
            parse_string_attribute_value(attribute, "reason")
                .ok_or_else(|| "privileged metadata requires reason = \"...\"".to_string())?,
        );
    }

    Ok(reason)
}

fn parse_request_named_fields(group: Group) -> Result<Vec<RequestField>, String> {
    split_top_level(group.stream(), ',')
        .into_iter()
        .filter(|tokens| !tokens.is_empty())
        .map(parse_request_named_field)
        .collect()
}

fn parse_request_named_field(tokens: Vec<TokenTree>) -> Result<RequestField, String> {
    let attrs = leading_attribute_strings(&tokens).join("\n");
    let tokens = strip_leading_attributes(tokens);
    let colon_index = tokens
        .iter()
        .position(|token| matches!(token, TokenTree::Punct(punct) if punct.as_char() == ':'))
        .ok_or_else(|| "bus_requests found a named field without a type".to_string())?;

    let name = tokens[..colon_index]
        .iter()
        .find_map(|token| match token {
            TokenTree::Ident(ident) => Some(ident.to_string()),
            _ => None,
        })
        .ok_or_else(|| "bus_requests found a named field without a name".to_string())?;
    let ty = tokens[colon_index + 1..]
        .iter()
        .map(TokenTree::to_string)
        .collect::<Vec<_>>()
        .join(" ");

    Ok(RequestField { name, ty, attrs })
}

fn parse_string_attribute_value(attribute: &str, key: &str) -> Option<String> {
    let tokens = attribute_token_values(attribute, key)?;
    tokens
        .first()
        .map(|value| unquote_literal(value))
        .filter(|value| !value.is_empty())
}

fn parse_path_attribute_value(attribute: &str, key: &str) -> Option<String> {
    attribute_token_values(attribute, key).map(|tokens| tokens.join(" "))
}

pub(super) fn reject_duplicate_operations(domain: &str, variants: &[RequestVariant]) -> Result<(), String> {
    let mut operations = Vec::<String>::new();

    for variant in variants {
        let Some(operation_metadata) = &variant.operation else {
            continue;
        };
        let operation = operation_metadata.name.clone();
        if operations.iter().any(|existing| existing == &operation) {
            return Err(format!(
                "bus_requests duplicate operation name `{operation}` in domain `{domain}`"
            ));
        }
        operations.push(operation.clone());
    }

    Ok(())
}

fn infer_request_domain_name(enum_name: &str) -> String {
    enum_name
        .strip_suffix("RequestPayload")
        .unwrap_or(enum_name)
        .to_ascii_lowercase()
}

fn has_serde_skip(tokens: &[TokenTree]) -> bool {
    leading_attribute_strings(tokens)
        .into_iter()
        .any(|attribute| {
            let normalized = attribute.replace(' ', "");
            normalized == "#[serde(skip)]" || normalized == "serde(skip)"
        })
}

fn split_top_level(stream: TokenStream2, delimiter: char) -> Vec<Vec<TokenTree>> {
    let mut groups = Vec::new();
    let mut current = Vec::new();
    for token in stream {
        if matches!(&token, TokenTree::Punct(punct) if punct.as_char() == delimiter) {
            groups.push(std::mem::take(&mut current));
        } else {
            current.push(token);
        }
    }
    groups.push(current);
    groups
}

fn strip_leading_attributes(tokens: Vec<TokenTree>) -> Vec<TokenTree> {
    let mut index = 0;
    while matches!(tokens.get(index), Some(TokenTree::Punct(punct)) if punct.as_char() == '#') {
        index += 2;
    }
    tokens.into_iter().skip(index).collect()
}

fn attribute_strings(tokens: &[TokenTree]) -> Vec<String> {
    leading_attribute_strings(tokens)
        .into_iter()
        .map(|attribute| {
            attribute
                .trim_start_matches("# ")
                .trim_start_matches('#')
                .trim()
                .to_string()
        })
        .map(|attribute| {
            attribute
                .trim_start_matches('[')
                .trim_end_matches(']')
                .trim()
                .to_string()
        })
        .collect()
}

fn leading_attribute_strings(tokens: &[TokenTree]) -> Vec<String> {
    let mut attributes = Vec::new();
    let mut index = 0;
    while let (Some(TokenTree::Punct(punct)), Some(TokenTree::Group(group))) =
        (tokens.get(index), tokens.get(index + 1))
    {
        if punct.as_char() != '#' || group.delimiter() != Delimiter::Bracket {
            break;
        }
        attributes.push(format!("#[{}]", group.stream()));
        index += 2;
    }

    attributes
}

fn attribute_token_values(attribute: &str, key: &str) -> Option<Vec<String>> {
    let syn::Meta::List(list) = syn::parse_str::<syn::Meta>(attribute).ok()? else {
        return None;
    };
    let fields = syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated
        .parse2(list.tokens)
        .ok()?;
    let syn::Meta::NameValue(value) = fields.into_iter().find(|meta| meta.path().is_ident(key))? else {
        return None;
    };
    let source = value.value.to_token_stream().to_string();
    let value = if matches!(value.value, syn::Expr::Lit(_)) {
        source
    } else {
        normalize_type(&source).replace(" :: ", "::")
    };
    Some(vec![value])
}

pub(super) fn unquote_literal(value: &str) -> String {
    value.trim().trim_matches('"').to_string()
}

pub(super) fn strip_request_metadata_attributes(stream: TokenStream2) -> String {
    strip_request_metadata_stream(stream)
        .into_iter()
        .map(|token| token.to_string())
        .collect::<Vec<_>>()
        .join(" ")
}

pub(super) fn validate_response_linkage(response: &str, response_variant: &str) -> Result<(), String> {
    let expected = format!("{response_variant}Output");
    if response == expected || response.ends_with(&format!("::{expected}")) {
        Ok(())
    } else {
        Err(format!(
            "operation response {response} does not match response_variant {response_variant}; expected {expected}"
        ))
    }
}

pub(super) fn public_projection_type(ty: &str) -> String {
    match ty.replace(' ', "").as_str() {
        "serde_json::Value" | "Value" => "serde_json::Value".to_string(),
        _ => normalize_type(ty),
    }
}

pub(super) fn normalize_type(ty: &str) -> String {
    let characters = ty.chars().collect::<Vec<_>>();
    let mut normalized = String::with_capacity(ty.len());
    let mut index = 0;

    while index < characters.len() {
        if characters[index] == ':' {
            let mut next = index + 1;
            while characters
                .get(next)
                .is_some_and(|character| character.is_whitespace())
            {
                next += 1;
            }
            if characters.get(next) == Some(&':') {
                normalized.push_str("::");
                index = next + 1;
                continue;
            }
        }

        normalized.push(characters[index]);
        index += 1;
    }

    normalized
}

pub(super) fn render_named_debug_impl(input_name: &str, fields: &[RequestField]) -> String {
    let field_calls = fields
        .iter()
        .map(|field| {
            if is_sensitive_field(&field.name) {
                format!(".field(\"{}\", &\"<redacted>\")", field.name)
            } else {
                format!(".field(\"{}\", &self.{})", field.name, field.name)
            }
        })
        .collect::<Vec<_>>()
        .join("\n            ");
    format!(
        "impl ::core::fmt::Debug for {input_name} {{\n    fn fmt(&self, formatter: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {{\n        formatter.debug_struct(\"{input_name}\")\n            {field_calls}\n            .finish()\n    }}\n}}"
    )
}

pub(super) fn is_sensitive_field(name: &str) -> bool {
    matches!(
        name,
        "password" | "private_key" | "passphrase" | "local_passphrase" | "digest" | "api_key" | "secret" | "token"
    )
}

fn strip_request_metadata_stream(stream: TokenStream2) -> TokenStream2 {
    let mut output = TokenStream2::new();
    let mut iter = stream.into_iter().peekable();

    while let Some(token) = iter.next() {
        if matches!(&token, TokenTree::Punct(punct) if punct.as_char() == '#') {
            if let Some(TokenTree::Group(group)) = iter.peek()
                && group.delimiter() == Delimiter::Bracket
            {
                let attribute = group.stream().to_string();
                if attribute.starts_with("operation") || attribute.starts_with("privileged") {
                    let _ = iter.next();
                    continue;
                }
            }
            output.extend([token]);
            continue;
        }

        match token {
            TokenTree::Group(group) => {
                let mut replacement = Group::new(group.delimiter(), strip_request_metadata_stream(group.stream()));
                replacement.set_span(group.span());
                output.extend([TokenTree::Group(replacement)]);
            }
            other => output.extend([other]),
        }
    }

    output
}
