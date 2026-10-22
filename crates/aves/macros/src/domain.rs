use proc_macro2::{Delimiter, TokenStream as TokenStream2, TokenTree};

use crate::request::unquote_literal;
use crate::routing::{
    normalize_qualified_paths, render_routing_blocks, render_sudo_impl_method, render_sudo_signature,
};
use crate::syntax::{lower_camel_case, request_base_name, to_upper_snake};

type DeclarationHeader = (
    String,
    String,
    String,
    String,
    String,
    Option<String>,
    Option<ResponseTimeout>,
);

type DeclarationBody = (Vec<SudoService>, Vec<RoutingBlock>, Vec<RoutingBlock>);

pub(super) struct SudoServices {
    pub(super) marker: Option<String>,
    pub(super) registry: Option<String>,
    pub(super) request: String,
    pub(super) response: String,
    pub(super) error: String,
    pub(super) transport_error_path: String,
    pub(super) target_domain: String,
    pub(super) authorization_hook: Option<String>,
    pub(super) response_timeout: Option<ResponseTimeout>,
    pub(super) services: Vec<SudoService>,
    pub(super) inbound: Vec<RoutingBlock>,
    pub(super) outbound: Vec<RoutingBlock>,
}

pub(super) struct SudoService {
    pub(super) name: String,
    pub(super) mappings: Vec<SudoMapping>,
}

pub(super) struct SudoMapping {
    pub(super) request_variant: String,
    pub(super) response_variant: String,
    pub(super) operation: String,
}

pub(super) struct RoutingBlock {
    pub(super) source_type: String,
    pub(super) target_type: String,
    pub(super) mappings: Vec<RoutingMapping>,
}

pub(super) struct RoutingMapping {
    pub(super) source_pattern: String,
    pub(super) guard: Option<String>,
    pub(super) target_expression: String,
}

pub(super) struct ResponseTimeout {
    pub(super) side_effecting_predicate: String,
    pub(super) duration: String,
}

impl SudoServices {
    pub(super) fn parse(input: TokenStream2) -> Result<Self, String> {
        let tokens = input.into_iter().map(SudoToken::from_proc_macro).collect();
        Self::parse_tokens(tokens)
    }

    pub(super) fn parse_tokens(tokens: Vec<SudoToken>) -> Result<Self, String> {
        let mut parser = SudoParser { tokens, index: 0 };
        let marker = parse_optional_declaration_value(&mut parser, "marker", "owning domain marker")?;
        let registry = parse_optional_declaration_value(&mut parser, "registry", "service registry")?;
        let (request, response, error, transport_error_path, target_domain, authorization_hook, response_timeout) =
            parse_declaration_header(&mut parser)?;
        let (services, inbound, outbound) = parse_declaration_body(&mut parser)?;

        Ok(Self {
            marker,
            registry,
            request,
            response,
            error,
            transport_error_path,
            target_domain,
            authorization_hook,
            response_timeout,
            services,
            inbound,
            outbound,
        })
    }

    pub(super) fn render(&self) -> String {
        let mut rendered = render_service_type_declarations(self);
        rendered.push_str(&render_privilege_mapping_impls(self));
        let metadata_names = render_service_declarations(self, &mut rendered);
        rendered.push_str(&render_aggregate(self, &metadata_names));
        rendered.push_str(&render_provider_metadata(self));
        rendered.push_str(&render_routing_blocks(self));
        rendered
    }
}

fn parse_optional_declaration_value(
    parser: &mut SudoParser,
    keyword: &str,
    description: &str,
) -> Result<Option<String>, String> {
    if matches!(parser.tokens.get(parser.index), Some(SudoToken::Ident(value)) if value == keyword) {
        parser.index += 1;
        let value = parser.expect_ident_value(description)?;
        parser.expect_punct(';')?;
        Ok(Some(value))
    } else {
        Ok(None)
    }
}

fn parse_declaration_header(parser: &mut SudoParser) -> Result<DeclarationHeader, String> {
    parser.expect_ident("request")?;
    let request = parser.expect_ident_value("request enum")?;
    parser.expect_punct('=')?;
    parser.expect_punct('>')?;
    let response = parser.expect_ident_value("response enum")?;
    parser.expect_ident("error")?;
    parser.expect_punct('=')?;
    let error = parser.expect_ident_value("domain error type")?;
    parser.expect_ident("transport")?;
    parser.expect_punct('=')?;
    let transport = parser.expect_ident_value("transport error type")?;
    let transport_error_path = if matches!(parser.tokens.get(parser.index), Some(SudoToken::Ident(value)) if value == "transport_error_path")
    {
        parser.index += 1;
        parser.expect_punct('=')?;
        parser.expect_path_until("target")?
    } else {
        transport.clone()
    };
    parser.expect_ident("target")?;
    parser.expect_punct('=')?;
    let target_domain = parser.expect_literal_value("target domain")?;
    parser.expect_punct(';')?;

    let authorization_hook = if matches!(parser.tokens.get(parser.index), Some(SudoToken::Ident(value)) if value == "authorization_hook")
    {
        parser.index += 1;
        parser.expect_punct('=')?;
        let hook = parser.expect_path_until_punct(';')?;
        parser.expect_punct(';')?;
        Some(hook)
    } else {
        None
    };

    let response_timeout = if matches!(parser.tokens.get(parser.index), Some(SudoToken::Ident(value)) if value == "response_timeout")
    {
        Some(parse_response_timeout(parser)?)
    } else {
        None
    };

    Ok((
        request,
        response,
        error,
        transport_error_path,
        target_domain,
        authorization_hook,
        response_timeout,
    ))
}

fn parse_declaration_body(parser: &mut SudoParser) -> Result<DeclarationBody, String> {
    let mut services = Vec::new();
    let mut inbound = Vec::new();
    let mut outbound = Vec::new();
    while parser.index < parser.tokens.len() {
        if matches!(parser.tokens.get(parser.index), Some(SudoToken::Ident(value)) if value == "inbound") {
            inbound.push(parse_routing_block(parser, "inbound")?);
            continue;
        }
        if matches!(parser.tokens.get(parser.index), Some(SudoToken::Ident(value)) if value == "outbound") {
            outbound.push(parse_routing_block(parser, "outbound")?);
            continue;
        }
        parser.expect_ident("pub")?;
        parser.expect_ident("service")?;
        let name = parser.expect_ident_value("service name")?;
        let body = parser.expect_group(Delimiter::Brace, "service body")?;
        let mappings = split_sudo_top_level(body, ',')
            .into_iter()
            .filter(|mapping| !mapping.is_empty())
            .map(parse_sudo_mapping)
            .collect::<Result<Vec<_>, _>>()?;
        if mappings.is_empty() {
            return Err(format!("sudo service {name} must map at least one request variant"));
        }
        if services
            .iter()
            .any(|service: &SudoService| service.name == name)
        {
            return Err(format!("sudo_services found duplicate service name `{name}`"));
        }
        let mut mapped_variants = Vec::new();
        for mapping in &mappings {
            if mapped_variants
                .iter()
                .any(|variant: &String| variant == &mapping.request_variant)
            {
                return Err(format!(
                    "sudo service {name} maps request variant `{}` more than once",
                    mapping.request_variant
                ));
            }
            mapped_variants.push(mapping.request_variant.clone());
        }
        services.push(SudoService { name, mappings });
    }

    if services.is_empty() && inbound.is_empty() && outbound.is_empty() {
        return Err("domain declaration must declare a service or routing block".to_string());
    }

    Ok((services, inbound, outbound))
}

fn parse_routing_block(parser: &mut SudoParser, direction: &str) -> Result<RoutingBlock, String> {
    parser.expect_ident(direction)?;
    let source_type = parser.expect_type_until_arrow("routing source type")?;
    parser.expect_punct('=')?;
    parser.expect_punct('>')?;
    let target_type = parser.expect_type_until_group("routing target type")?;
    let body = parser.expect_group(Delimiter::Brace, "routing mapping body")?;
    parser.expect_optional_punct(';');
    let mappings = split_sudo_top_level(body, ',')
        .into_iter()
        .filter(|mapping| !mapping.is_empty())
        .map(parse_routing_mapping)
        .collect::<Result<Vec<_>, _>>()?;
    if mappings.is_empty() {
        return Err(format!("{direction} routing block must map at least one variant"));
    }
    Ok(RoutingBlock {
        source_type,
        target_type,
        mappings,
    })
}

fn parse_response_timeout(parser: &mut SudoParser) -> Result<ResponseTimeout, String> {
    parser.expect_ident("response_timeout")?;
    let body = parser.expect_group(Delimiter::Brace, "response timeout options")?;
    parser.expect_punct(';')?;
    let mut predicate = None;
    let mut duration = None;
    for option in split_sudo_top_level(body, ',')
        .into_iter()
        .filter(|option| !option.is_empty())
    {
        let mut option_parser = SudoParser {
            tokens: option,
            index: 0,
        };
        let name = option_parser.expect_ident_value("response timeout option")?;
        option_parser.expect_punct('=')?;
        let value = normalize_qualified_paths(&join_sudo_tokens(&option_parser.tokens[option_parser.index..]));
        if value.is_empty() {
            return Err(format!("response timeout option `{name}` requires a value"));
        }
        match name.as_str() {
            "predicate" if predicate.is_none() => predicate = Some(value),
            "duration" if duration.is_none() => duration = Some(value),
            "predicate" | "duration" => return Err(format!("response timeout option `{name}` is duplicated")),
            _ => return Err(format!("response timeout has unexpected option `{name}`")),
        }
    }
    Ok(ResponseTimeout {
        side_effecting_predicate: predicate.ok_or_else(|| "response timeout requires `predicate`".to_string())?,
        duration: duration.ok_or_else(|| "response timeout requires `duration`".to_string())?,
    })
}

fn parse_routing_mapping(tokens: Vec<SudoToken>) -> Result<RoutingMapping, String> {
    let (source, target) = split_sudo_arrow(tokens).ok_or_else(|| "routing mapping must contain `=>`".to_string())?;
    if source.is_empty() || target.is_empty() {
        return Err("routing mapping requires both a source pattern and target expression".to_string());
    }
    let (source_pattern, guard) = split_routing_guard(source);
    let source_pattern = join_sudo_tokens(&source_pattern);
    let guard = guard.map(|tokens| join_sudo_tokens(&tokens));
    let target_expression = join_sudo_tokens(&target);
    Ok(RoutingMapping {
        source_pattern,
        guard,
        target_expression,
    })
}

fn find_sudo_arrow(tokens: &[SudoToken]) -> Option<usize> {
    tokens
        .windows(2)
        .position(|window| matches!((&window[0], &window[1]), (SudoToken::Punct('='), SudoToken::Punct('>'))))
}

fn split_routing_guard(tokens: Vec<SudoToken>) -> (Vec<SudoToken>, Option<Vec<SudoToken>>) {
    let Some(index) = tokens
        .iter()
        .position(|token| matches!(token, SudoToken::Ident(value) if value == "if"))
    else {
        return (tokens, None);
    };
    (tokens[..index].to_vec(), Some(tokens[index + 1..].to_vec()))
}

fn split_sudo_arrow(tokens: Vec<SudoToken>) -> Option<(Vec<SudoToken>, Vec<SudoToken>)> {
    let arrow = find_sudo_arrow(&tokens)?;
    Some((tokens[..arrow].to_vec(), tokens[arrow + 2..].to_vec()))
}

fn render_service_type_declarations(config: &SudoServices) -> String {
    let request_base = request_base_name(&config.request);
    let mapping_type = format!("{}SudoServiceMapping", request_base);
    let metadata_type = format!("{}SudoServiceMetadata", request_base);
    format!(
        "#[doc = \"A generated mapping between a sudo request variant and its response variant.\"]\npub struct {mapping_type} {{ pub request_variant: &'static str, pub response_variant: &'static str }}\n#[doc = \"Generated provider metadata for one channel-backed sudo service.\"]\npub struct {metadata_type} {{ pub service_name: &'static str, pub provider_key: &'static str, pub request_type: &'static str, pub response_type: &'static str, pub mappings: &'static [{mapping_type}] }}\n"
    )
}

fn render_privilege_mapping_impls(config: &SudoServices) -> String {
    let base = request_base_name(&config.request);
    let mapping_trait = format!("{base}SudoServicePrivilegeMapping");
    let mut mapped_variants = Vec::new();
    for service in &config.services {
        for mapping in &service.mappings {
            if !mapped_variants
                .iter()
                .any(|variant: &String| variant == &mapping.request_variant)
            {
                mapped_variants.push(mapping.request_variant.clone());
            }
        }
    }
    mapped_variants
        .into_iter()
        .map(|variant| format!("impl {mapping_trait} for {base}Privileged{variant} {{}}\n"))
        .collect()
}

fn render_service_declarations(config: &SudoServices, rendered: &mut String) -> Vec<String> {
    let request_base = request_base_name(&config.request);
    let request_type = config.request.as_str();
    let request_wrapper = config
        .request
        .strip_suffix("Payload")
        .map_or_else(|| format!("{}Request", config.request), str::to_string);
    let mapping_type = format!("{request_base}SudoServiceMapping");
    let metadata_type = format!("{request_base}SudoServiceMetadata");
    let mut metadata = Vec::new();
    for service in &config.services {
        let response_type = config.response.as_str();
        let error_type = config.error.as_str();
        let transport_error_path = config.transport_error_path.as_str();
        let impl_name = format!("{}Impl", service.name);
        let key_name = format!("{}Key", service.name);
        let mapping_name = format!("{}_MAPPINGS", to_upper_snake(&service.name));
        let metadata_name = format!("{}_SUDO_SERVICE_METADATA", to_upper_snake(&service.name));
        let methods = service
            .mappings
            .iter()
            .map(|mapping| render_sudo_signature(mapping, error_type))
            .collect::<Vec<_>>()
            .join("\n");
        let impl_methods = service
            .mappings
            .iter()
            .map(|mapping| {
                render_sudo_impl_method(
                    mapping,
                    request_type,
                    &request_wrapper,
                    error_type,
                    transport_error_path,
                    &config.target_domain,
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        let mapping_entries = service
            .mappings
            .iter()
            .map(|mapping| {
                format!(
                    "{mapping_type} {{ request_variant: \"{}\", response_variant: \"{}\" }}",
                    mapping.request_variant, mapping.response_variant
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        rendered.push_str(&format!(
                 "#[doc = \"Compile-time key for the generated sudo service slot.\"]\npub struct {key_name};\n#[doc = \"Generated async sudo service trait. Callers must obtain it from a filtered kernel context.\"]\n#[async_trait::async_trait]\npub trait {name}: Send + Sync {{\n{methods}\n}}\n#[doc = \"Generated channel-backed sudo service implementation. The implementation wraps a service-specific domain handler channel; callers must obtain it from a filtered kernel context.\"]\npub(crate) struct {impl_name} {{ sender: ::tokio::sync::mpsc::Sender<{request_wrapper}> }}\nimpl {impl_name} {{ pub(crate) fn channel(capacity: usize) -> (::std::sync::Arc<Self>, ::tokio::sync::mpsc::Receiver<{request_wrapper}>) {{ let (sender, receiver) = ::tokio::sync::mpsc::channel(capacity); (::std::sync::Arc::new(Self {{ sender }}), receiver) }} }}\n#[async_trait::async_trait]\nimpl {name} for {impl_name} {{\n{impl_methods}\n}}\npub const {mapping_name}: &[{mapping_type}] = &[{mapping_entries}];\n",
                 name = service.name,
                 key_name = key_name,
                 impl_methods = impl_methods,
             ));
        rendered.push_str(&format!(
                "#[doc = \"Generated provider metadata and static service-slot key.\"]\npub const {metadata_name}: {metadata_type} = {metadata_type} {{ service_name: \"{}\", provider_key: \"{}\", request_type: \"{}\", response_type: \"{}\", mappings: {mapping_name} }};\n",
                 service.name, service.name, request_type, response_type
            ));
        metadata.push(metadata_name);
    }
    metadata
}

fn render_aggregate(config: &SudoServices, metadata: &[String]) -> String {
    let request_base = request_base_name(&config.request);
    let metadata_type = format!("{request_base}SudoServiceMetadata");
    let aggregate_name = config
        .registry
        .clone()
        .unwrap_or_else(|| format!("{}_SUDO_SERVICES", to_upper_snake(&request_base)));
    let aggregate = if metadata.is_empty() {
        "&[]".to_string()
    } else {
        format!("&[{}]", metadata.join(", "))
    };
    format!(
        "#[doc = \"All generated sudo service providers and their variant mappings for this domain.\"]\npub const {aggregate_name}: &[{metadata_type}] = {aggregate};\n"
    )
}

fn render_provider_metadata(config: &SudoServices) -> String {
    let mut rendered = String::new();
    if let Some(marker) = &config.marker {
        let provider_keys_type = format!("{}SudoServiceProviderKeys", marker);
        let (provider_keys, provider_set) = if config.services.is_empty() {
            ("()".to_string(), "()".to_string())
        } else {
            let keys = format!(
                "({},)",
                config
                    .services
                    .iter()
                    .map(|service| format!("{}Key", service.name))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            (keys, provider_keys_type.clone())
        };
        rendered.push_str(&format!(
                "#[doc = \"The compile-time provider set generated from this domain's sudo service registry.\"]\npub type {provider_keys_type} = {provider_keys};\nimpl ::aves::DomainProviderMetadata for {marker} {{ type ProviderSet = {provider_set}; }}\n",
            ));
    }
    rendered
}

struct SudoParser {
    tokens: Vec<SudoToken>,
    index: usize,
}

#[derive(Clone)]
pub(super) enum SudoToken {
    Ident(String),
    Literal(String),
    Punct(char),
    Group(Delimiter, Vec<SudoToken>),
}

impl SudoToken {
    fn from_proc_macro(token: TokenTree) -> Self {
        match token {
            TokenTree::Ident(value) => Self::Ident(value.to_string()),
            TokenTree::Literal(value) => Self::Literal(value.to_string()),
            TokenTree::Punct(value) => Self::Punct(value.as_char()),
            TokenTree::Group(group) => Self::Group(
                group.delimiter(),
                group
                    .stream()
                    .into_iter()
                    .map(Self::from_proc_macro)
                    .collect(),
            ),
        }
    }
}

impl SudoParser {
    fn expect_type_until_arrow(&mut self, description: &str) -> Result<String, String> {
        let start = self.index;
        let relative_arrow = find_sudo_arrow(&self.tokens[start..]);
        let Some(relative_arrow) = relative_arrow else {
            return Err(format!("domain routing expected {description}"));
        };
        self.index = start + relative_arrow;
        Ok(self.tokens[start..self.index]
            .iter()
            .map(SudoToken::to_source)
            .collect::<Vec<_>>()
            .join(" "))
    }

    fn expect_type_until_group(&mut self, description: &str) -> Result<String, String> {
        let start = self.index;
        while self.index < self.tokens.len()
            && !matches!(self.tokens.get(self.index), Some(SudoToken::Group(Delimiter::Brace, _)))
        {
            self.index += 1;
        }
        if self.index == start {
            return Err(format!("domain routing expected {description}"));
        }
        Ok(self.tokens[start..self.index]
            .iter()
            .map(SudoToken::to_source)
            .collect::<Vec<_>>()
            .join(" "))
    }

    fn expect_optional_punct(&mut self, expected: char) {
        if matches!(self.tokens.get(self.index), Some(SudoToken::Punct(value)) if *value == expected) {
            self.index += 1;
        }
    }

    fn expect_path_until(&mut self, next_keyword: &str) -> Result<String, String> {
        let start = self.index;
        while self.index < self.tokens.len() {
            if matches!(self.tokens.get(self.index), Some(SudoToken::Ident(value)) if value == next_keyword) {
                break;
            }
            self.index += 1;
        }
        if self.index == start {
            return Err(format!(
                "sudo_services expected a transport error path before `{next_keyword}`"
            ));
        }
        Ok(self.tokens[start..self.index]
            .iter()
            .map(SudoToken::to_source)
            .collect::<Vec<_>>()
            .join(" "))
    }

    fn expect_path_until_punct(&mut self, delimiter: char) -> Result<String, String> {
        let start = self.index;
        while self.index < self.tokens.len()
            && !matches!(self.tokens.get(self.index), Some(SudoToken::Punct(value)) if *value == delimiter)
        {
            self.index += 1;
        }
        if self.index == start {
            return Err(format!("sudo_services expected a path before `{delimiter}`"));
        }
        Ok(join_sudo_tokens(&self.tokens[start..self.index]))
    }

    fn expect_ident(&mut self, expected: &str) -> Result<(), String> {
        match self.tokens.get(self.index) {
            Some(SudoToken::Ident(value)) if value == expected => {
                self.index += 1;
                Ok(())
            }
            _ => Err(format!("sudo_services expected `{expected}`")),
        }
    }

    fn expect_ident_value(&mut self, description: &str) -> Result<String, String> {
        match self.tokens.get(self.index) {
            Some(SudoToken::Ident(value)) => {
                self.index += 1;
                Ok(value.clone())
            }
            _ => Err(format!("sudo_services expected {description}")),
        }
    }

    fn expect_literal_value(&mut self, description: &str) -> Result<String, String> {
        match self.tokens.get(self.index) {
            Some(SudoToken::Literal(value)) => {
                self.index += 1;
                Ok(unquote_literal(value))
            }
            _ => Err(format!("sudo_services expected {description}")),
        }
    }

    fn expect_punct(&mut self, expected: char) -> Result<(), String> {
        match self.tokens.get(self.index) {
            Some(SudoToken::Punct(value)) if *value == expected => {
                self.index += 1;
                Ok(())
            }
            _ => Err(format!("sudo_services expected `{expected}`")),
        }
    }

    fn expect_group(&mut self, delimiter: Delimiter, description: &str) -> Result<Vec<SudoToken>, String> {
        match self.tokens.get(self.index) {
            Some(SudoToken::Group(group_delimiter, tokens)) if *group_delimiter == delimiter => {
                self.index += 1;
                Ok(tokens.clone())
            }
            _ => Err(format!("sudo_services expected {description}")),
        }
    }
}

impl SudoToken {
    fn to_source(&self) -> String {
        match self {
            Self::Ident(value) | Self::Literal(value) => value.clone(),
            Self::Punct(value) => value.to_string(),
            Self::Group(delimiter, tokens) => {
                let (open, close) = match delimiter {
                    Delimiter::Parenthesis => ('(', ')'),
                    Delimiter::Brace => ('{', '}'),
                    Delimiter::Bracket => ('[', ']'),
                    Delimiter::None => (' ', ' '),
                };
                format!("{open}{}{close}", join_sudo_tokens(tokens))
            }
        }
    }
}

fn split_sudo_top_level(tokens: Vec<SudoToken>, delimiter: char) -> Vec<Vec<SudoToken>> {
    let mut groups = Vec::new();
    let mut current = Vec::new();

    for token in tokens {
        match token {
            // A proc-macro group is one token at this level. Its contents may
            // contain separators, but those separators belong to the nested
            // pattern or expression rather than to the routing block.
            SudoToken::Group(_, _) => current.push(token),
            SudoToken::Punct(value) if value == delimiter => {
                groups.push(current);
                current = Vec::new();
            }
            token => current.push(token),
        }
    }

    groups.push(current);
    groups
}

fn join_sudo_tokens(tokens: &[SudoToken]) -> String {
    let mut source = String::new();
    for (index, token) in tokens.iter().enumerate() {
        let rendered = token.to_source();
        let joins_punctuation = index > 0
            && match (&tokens[index - 1], token) {
                (SudoToken::Punct(left), SudoToken::Punct(right)) => matches!(
                    (*left, *right),
                    ('=', '>')
                        | ('|', '|')
                        | ('&', '&')
                        | ('=', '=')
                        | ('!', '=')
                        | ('>', '=')
                        | ('<', '=')
                        | (':', ':')
                        | ('-', '>')
                        | ('.', '.')
                ),
                (SudoToken::Punct('.'), SudoToken::Ident(_) | SudoToken::Literal(_)) => true,
                (SudoToken::Ident(_) | SudoToken::Literal(_), SudoToken::Punct('.')) => true,
                _ => false,
            };
        if !source.is_empty() && !joins_punctuation {
            source.push(' ');
        }
        source.push_str(&rendered);
    }
    source
}

fn parse_sudo_mapping(tokens: Vec<SudoToken>) -> Result<SudoMapping, String> {
    let mut parser = SudoParser { tokens, index: 0 };
    let request_variant = parser.expect_ident_value("request variant")?;
    parser.expect_punct('=')?;
    parser.expect_punct('>')?;
    let response_variant = parser.expect_ident_value("response variant")?;
    let operation = if parser.index == parser.tokens.len() {
        lower_camel_case(&request_variant)
    } else {
        let metadata = parser.expect_group(Delimiter::Parenthesis, "mapping metadata")?;
        let mut metadata_parser = SudoParser {
            tokens: metadata,
            index: 0,
        };
        metadata_parser.expect_ident("operation")?;
        metadata_parser.expect_punct('=')?;
        let operation = metadata_parser.expect_literal_value("operation name")?;
        if metadata_parser.index != metadata_parser.tokens.len() {
            return Err(format!("sudo service mapping {request_variant} has unexpected tokens"));
        }
        operation
    };
    if parser.index != parser.tokens.len() {
        return Err(format!("sudo service mapping {request_variant} has unexpected tokens"));
    }
    Ok(SudoMapping {
        request_variant,
        response_variant,
        operation,
    })
}
