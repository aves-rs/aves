use proc_macro2::{Delimiter, TokenStream as TokenStream2};
use std::str::FromStr;

use crate::domain::{SudoServices, SudoToken};
use crate::error_code::render_error_codes;
use crate::registry::{RegisteredRequest, register_request, register_services};
use crate::request::{
    OperationKind, RequestEnum, RequestField, RequestFields, RequestOperation, RequestVariant, is_sensitive_field,
    parse_operation_kind, parse_privileged_reason_attributes, reject_duplicate_operations, render_named_debug_impl,
    validate_response_linkage,
};
use crate::response::ResponseEnum;
use crate::routing::{explicit_transport_error_conversions, normalize_qualified_paths, render_routing_blocks};
use crate::syntax::{compile_error, request_base_name};

mod tests {
    use super::*;

    #[test]
    fn generates_codes_for_each_variant_shape_and_preserves_aliases() {
        let input = TokenStream2::from_str(
            r#"enum Failure {
            Empty,
            Named { reason: String },
            #[error_code("OwnerDispatch")]
            Adapter(String),
        }"#,
        )
        .unwrap();
        let output = render_error_codes(input).unwrap();
        syn::parse2::<syn::ItemImpl>(output.clone()).unwrap();
        let rendered = output.to_string();
        assert!(rendered.contains("Self :: Empty => \"Empty\""));
        assert!(rendered.contains("Self :: Named { .. } => \"Named\""));
        assert!(rendered.contains("Self :: Adapter (..) => \"OwnerDispatch\""));
    }

    #[test]
    fn rejects_non_string_code_alias() {
        let input = TokenStream2::from_str("enum Failure { #[error_code(42)] Bad }").unwrap();
        assert!(render_error_codes(input).is_err());
    }

    #[test]
    fn feature_gated_variants_gate_generated_error_code_arms() {
        let input = TokenStream2::from_str("enum Failure { #[cfg(feature = \"render\")] Render(String), Unavailable }")
            .unwrap();
        let output = render_error_codes(input).unwrap();
        syn::parse2::<syn::ItemImpl>(output.clone()).unwrap();
        let rendered = output.to_string();
        assert!(rendered.contains("# [cfg (feature = \"render\")] Self :: Render (..) => \"Render\""));
        assert!(rendered.contains("Self :: Unavailable => \"Unavailable\""));
    }

    fn validate_privileged_mappings(domain: &str, privileged: &[String], mapped: &[String]) -> Result<(), String> {
        for variant in privileged {
            if !mapped.iter().any(|candidate| candidate == variant) {
                return Err(format!(
                    "domain `{domain}` has privileged request variant `{variant}` with no sudo service mapping"
                ));
            }
        }
        Ok(())
    }

    #[test]
    fn unmapped_privileged_variant_expands_to_compile_error() {
        let error = validate_privileged_mappings("knowledge", &["DeleteKnowledge".to_string()], &[])
            .expect_err("an unmapped privileged variant must be rejected");
        assert!(compile_error(error).to_string().contains("DeleteKnowledge"));
        assert!(
            compile_error(
                "domain `knowledge` has privileged request variant `DeleteKnowledge` with no sudo service mapping"
                    .to_string()
            )
            .to_string()
            .contains("knowledge")
        );
    }

    fn normalize_tokens(source: &str) -> String {
        source
            .chars()
            .filter(|character| !character.is_ascii_whitespace())
            .collect()
    }

    #[test]
    fn guarded_inbound_render_does_not_reconstruct_declaration_arrow() {
        let services = SudoServices::parse_tokens(parse_sudo_test_tokens(
            r#"request KnowledgeRequestPayload => KnowledgeResponse error = KnowledgeError transport = SharedInfraError target = "knowledge"; inbound TestBroadcast => KnowledgeRequestPayload { TestBroadcast::Config(ConfigBroadcast::ConfigValueChanged { key }) if key == "knowledge" || key.starts_with("knowledge.") => KnowledgeRequestPayload::KnowledgeConfigChanged, } outbound KnowledgeResponse => KnowledgeBroadcast { KnowledgeRecordsChanged => RecordsChanged, }"#,
        ))
        .expect("knowledge routing declaration should parse");

        let rendered = render_routing_blocks(&services);

        let route_body = rendered
            .split("match source {")
            .nth(1)
            .expect("inbound route body should render");
        let normalized_route_body = normalize_tokens(route_body);
        assert!(normalized_route_body.contains("key==\"knowledge\"||key.starts_with(\"knowledge.\")"));
        assert!(normalized_route_body.contains("=>Some((KnowledgeRequestPayload::KnowledgeConfigChanged))"));
        assert!(!route_body.contains("| |"));
        assert!(!route_body.contains("= >"));
        assert!(!rendered.contains("request KnowledgeRequestPayload => KnowledgeResponse"));
        assert!(!route_body.contains("KnowledgeResponse =>"));
    }

    #[test]
    fn empty_service_routing_domain_renders_typed_empty_aggregate() {
        let services = SudoServices::parse_tokens(parse_sudo_test_tokens(
            r#"marker ActionDomain; request ActionRequestPayload => ActionResponse error = ActionError transport = SharedInfraError target = "action"; inbound TestBroadcast => Option<ActionRequestPayload> { TestBroadcast::Enclave(event) => action_request_from_enclave_broadcast(event), }"#,
        ))
        .expect("routing-only domain should parse");

        assert!(services.services.is_empty());
        let rendered = services.render();
        syn::parse_file(&rendered)
            .unwrap_or_else(|error| panic!("empty-service output should be valid Rust: {error}\n{rendered}"));
        assert!(rendered.contains("pub const ACTION_SUDO_SERVICES: &[ActionSudoServiceMetadata] = &[];"));
        assert!(rendered.contains("type ProviderSet = ();"));
        assert!(rendered.contains("pub type ActionDomainSudoServiceProviderKeys = ();"));
        assert!(!rendered.contains("SudoServicePrivilegeMapping for"));
        assert!(!rendered.contains("SUDO_SERVICE_METADATA ="));
    }

    #[test]
    fn response_timeout_render_keeps_actor_response_await_and_service_module() {
        let services = SudoServices::parse_tokens(parse_sudo_test_tokens(
            r#"marker ActionDomain; request ActionRequestPayload => ActionResponse error = ActionError transport = SharedInfraError target = "action"; response_timeout { predicate = ActionRequestPayload::is_side_effecting, duration = ACTION_RESPONSE_TIMEOUT }; pub service ActionInternalSudoService { AttributedMutation => OrderExecuted, EvictConnectorCache => Ack, } outbound ActionOrderEvent => ActionBroadcast { event => OrderExecuted { event }, }"#,
        ))
        .expect("action routing declaration should parse");

        let rendered = services.render();

        assert!(rendered.contains("pub trait ActionInternalSudoService"));
        assert!(rendered.contains("pub(crate) struct ActionInternalSudoServiceImpl"));
        assert!(rendered.contains("let side_effecting = (ActionRequestPayload::is_side_effecting)"));
        assert!(rendered.contains("actor_response\n                .await"));
    }

    #[test]
    fn exact_action_domain_generation_is_parseable() {
        let services = SudoServices::parse_tokens(parse_sudo_test_tokens(
            r#"marker ActionDomain; request ActionRequestPayload => ActionResponse error = ActionError transport = SharedInfraError target = "action"; response_timeout { predicate = ActionRequestPayload::is_side_effecting, duration = ACTION_RESPONSE_TIMEOUT }; pub service ActionInternalSudoService { AttributedMutation => OrderExecuted, EvictConnectorCache => Ack, } inbound TestBroadcast => Option<ActionRequestPayload> { TestBroadcast::Enclave(event) => action_request_from_enclave_broadcast(event), } outbound ActionOrderEvent => ActionBroadcast { event => OrderExecuted { event }, }"#,
        ))
        .expect("exact Action domain declaration should parse");

        let generated =
            explicit_transport_error_conversions(services.render(), &services.error, &services.transport_error_path);

        assert!(generated.contains("ActionRequestPayload::is_side_effecting"));
        assert!(!generated.contains("ActionRequestPayload : : is_side_effecting"));
        assert!(generated.contains(
            "OneshotReplyTimeout { target_domain: \"action\".to_string(), operation: \"handler.actor_response\".to_string(), timeout: format!(\"{:?}\", ACTION_RESPONSE_TIMEOUT) }))?"
        ));
        assert!(!generated.contains("timeout: format!(\"{:?}\", ACTION_RESPONSE_TIMEOUT) }.into())?"));

        TokenStream2::from_str(&generated).unwrap_or_else(|error| {
            panic!("exact Action domain generation produced invalid Rust tokens: {error}\n{generated}")
        });
    }

    fn parse_sudo_test_tokens(source: &str) -> Vec<SudoToken> {
        fn parse_until(source: &[char], index: &mut usize, closing: Option<char>) -> Vec<SudoToken> {
            let mut tokens = Vec::new();
            while *index < source.len() {
                let character = source[*index];
                if closing == Some(character) {
                    *index += 1;
                    break;
                }
                if character.is_whitespace() {
                    *index += 1;
                    continue;
                }
                if matches!(character, '{' | '(') {
                    *index += 1;
                    let (delimiter, end) = if character == '{' {
                        (Delimiter::Brace, '}')
                    } else {
                        (Delimiter::Parenthesis, ')')
                    };
                    tokens.push(SudoToken::Group(delimiter, parse_until(source, index, Some(end))));
                    continue;
                }
                if character == '"' {
                    let start = *index;
                    *index += 1;
                    let mut escaped = false;
                    while *index < source.len() {
                        let current = source[*index];
                        *index += 1;
                        if current == '"' && !escaped {
                            break;
                        }
                        escaped = current == '\\' && !escaped;
                        if current != '\\' {
                            escaped = false;
                        }
                    }
                    tokens.push(SudoToken::Literal(source[start..*index].iter().collect()));
                    continue;
                }
                if character.is_ascii_alphanumeric() || character == '_' {
                    let start = *index;
                    *index += 1;
                    while *index < source.len() && (source[*index].is_ascii_alphanumeric() || source[*index] == '_') {
                        *index += 1;
                    }
                    tokens.push(SudoToken::Ident(source[start..*index].iter().collect()));
                    continue;
                }
                tokens.push(SudoToken::Punct(character));
                *index += 1;
            }
            tokens
        }

        let characters = source.chars().collect::<Vec<_>>();
        let mut index = 0;
        parse_until(&characters, &mut index, None)
    }

    #[test]
    fn parses_operation_kind_values() {
        let query = parse_operation_kind(
            r#"operation(name = "getConfig", kind = "query", response = ConfigOutput, response_variant = "Config")"#,
        )
        .expect("query operation kind should parse");
        let mutation = parse_operation_kind(
            r#"operation(name = "saveConfig", kind = "mutation", response = ConfigOutput, response_variant = "Config")"#,
        )
        .expect("mutation operation kind should parse");

        assert_eq!(query, OperationKind::Query);
        assert_eq!(mutation, OperationKind::Mutation);
    }

    #[test]
    fn parses_one_domain_registry_with_multiple_named_services() {
        let services = SudoServices::parse_tokens(
            parse_sudo_test_tokens(
                "marker EnclaveDomain; request EnclaveRequestPayload => EnclaveResponse error = EnclaveError transport = SharedInfraError target = \"enclave\"; pub service EnclaveWalletSudoService { CreateWallet => WalletCreated (operation = \"create_wallet\"), } pub service EnclaveAccountSecretSudoService { WriteAccountSecret => SecretWritten (operation = \"write_account_secret\"), }",
            ),
        )
        .expect("sudo service registry should parse");

        assert_eq!(services.request, "EnclaveRequestPayload");
        assert_eq!(services.marker, Some("EnclaveDomain".to_string()));
        assert_eq!(services.response, "EnclaveResponse");
        assert_eq!(services.services.len(), 2);
        assert_eq!(services.services[0].name, "EnclaveWalletSudoService");
        assert_eq!(services.services[1].mappings[0].response_variant, "SecretWritten");
    }

    #[test]
    fn parses_and_renders_markerless_infrastructure_service_registry() {
        let services = SudoServices::parse_tokens(parse_sudo_test_tokens(
            "registry BUS_SUDO_SERVICES; request BusControlRequestPayload => BusControlResponse error = BusControlError transport = BusControlTransportError target = \"bus\"; pub service BusSudoService { OpenRpc => RpcOpened (operation = \"openRpc\"), }",
        ))
        .expect("markerless infrastructure service registry should parse");

        assert_eq!(services.marker, None);
        let rendered = services.render();
        assert!(rendered.contains("pub trait BusSudoService: Send + Sync"));
        assert!(rendered.contains("pub(crate) struct BusSudoServiceImpl"));
        assert!(rendered.contains("pub const BUS_SUDO_SERVICES"));
        assert!(rendered.contains("pub const BUS_SUDO_SERVICE_MAPPINGS"));
        assert!(!rendered.contains("DomainProviderMetadata"));
        assert!(!rendered.contains("SudoServiceProviderKeys"));
    }

    #[test]
    fn renders_guarded_nested_destructuring_and_match_targets_as_expressions() {
        let services = SudoServices::parse_tokens(parse_sudo_test_tokens(
            r#"marker ActionDomain; request ActionRequestPayload => ActionResponse error = ActionError transport = SharedInfraError target = "action"; pub service ActionInternalSudoService { EvictConnectorCache => Ack, } inbound TestBroadcast => ActionRequestPayload { TestBroadcast::Enclave(EnclaveBroadcast::SecretChanged { account_id, slot_name }) if matches!(slot_name.as_str(), "hyperliquid_api_key" | "hyperliquid_api_secret" | "ibkr_flex_token") => ActionRequestPayload::EvictConnectorCache(match slot_name.as_str() { "hyperliquid_api_key" | "hyperliquid_api_secret" => Some((account_id, VenueKind::Hyperliquid)), "ibkr_flex_token" => Some((account_id, VenueKind::Ibkr)), _ => None, }), TestBroadcast::Enclave(EnclaveBroadcast::Locked) => ActionRequestPayload::EvictConnectorCache(None), }"#,
        ))
        .expect("guarded nested mapping should parse");

        assert_eq!(services.inbound.len(), 1);
        assert_eq!(services.inbound[0].mappings.len(), 2);
        assert!(
            normalize_tokens(&services.inbound[0].mappings[0].source_pattern)
                .contains("SecretChanged{account_id,slot_name}")
        );
        assert!(
            normalize_tokens(&services.inbound[0].mappings[0].target_expression)
                .contains("EvictConnectorCache(matchslot_name.as_str")
        );

        let rendered = services.render();
        let normalized = normalize_tokens(&rendered);
        assert!(normalized.contains("=>Some((ActionRequestPayload::EvictConnectorCache(matchslot_name.as_str"));
        assert!(normalized.contains("{account_id,slot_name}"));
        assert!(
            normalized.contains(
                "\"hyperliquid_api_key\"|\"hyperliquid_api_secret\"=>Some((account_id,VenueKind::Hyperliquid))"
            )
        );
        assert!(!rendered.contains("| |"));
        assert!(!rendered.contains("= >"));
    }

    #[test]
    fn normalizes_spacing_variants_in_qualified_paths() {
        for value in [
            "TestBroadcast :: Enclave",
            "TestBroadcast: :Enclave",
            "TestBroadcast :  : Enclave",
        ] {
            assert_eq!(normalize_qualified_paths(value), "TestBroadcast::Enclave");
        }
    }

    #[test]
    fn request_base_name_uses_the_final_path_segment() {
        assert_eq!(request_base_name("crate :: action :: ActionRequestPayload"), "Action");
        assert_eq!(request_base_name("ActionRequestPayload"), "Action");
    }

    #[test]
    fn parses_mapping_without_operation_metadata() {
        let services = SudoServices::parse_tokens(
            parse_sudo_test_tokens(
                "marker EnclaveDomain; request EnclaveRequestPayload => EnclaveResponse error = EnclaveError transport = SharedInfraError target = \"enclave\"; pub service EnclaveWalletSudoService { CreateWallet => WalletCreated, }",
            ),
        )
        .expect("sudo service registry should parse");

        assert_eq!(services.services[0].mappings[0].operation, "createWallet");
    }

    #[test]
    fn parses_explicit_inbound_and_multiple_outbound_routing_blocks() {
        let services = SudoServices::parse_tokens(parse_sudo_test_tokens(
            "marker ActionDomain; request ActionRequestPayload => ActionResponse error = ActionError transport = SharedInfraError target = \"action\"; pub service ActionSudoService { Evict => Ack, } inbound TestBroadcast => ActionRequestPayload { TestBroadcast::Enclave(EnclaveBroadcast::Locked) => Evict(None), } outbound ActionResponseEvent => ActionBroadcast { ActionResponseEvent::Order(event) => OrderExecuted { event }, } outbound ActionOrderEvent => ActionBroadcast { ActionOrderEvent::Accepted(event) => OrderExecuted { event }, }",
        ))
        .expect("routing blocks should parse");

        assert_eq!(services.inbound.len(), 1);
        assert_eq!(services.outbound.len(), 2);
        assert_eq!(services.inbound[0].source_type, "TestBroadcast");
        assert_eq!(services.inbound[0].target_type, "ActionRequestPayload");
        assert!(normalize_tokens(&services.inbound[0].mappings[0].source_pattern).contains("TestBroadcast::Enclave"));
        assert!(
            services.outbound[1]
                .source_type
                .contains("ActionOrderEvent")
        );
    }

    #[test]
    fn action_domain_routing_renders_parseable_option_and_outbound_mappings() {
        let services = SudoServices::parse_tokens(parse_sudo_test_tokens(
            r#"marker ActionDomain; request ActionRequestPayload => ActionResponse error = ActionError transport = SharedInfraError target = "action"; pub service ActionInternalSudoService { AttributedMutation => OrderExecuted, EvictConnectorCache => Ack, } inbound TestBroadcast => Option<ActionRequestPayload> { TestBroadcast::Enclave(event) => action_request_from_enclave_broadcast(event), } outbound ActionOrderEvent => ActionBroadcast { event => OrderExecuted { event }, }"#,
        ))
        .expect("the current Action domain declaration should parse");

        let rendered = services.render();
        assert!(rendered.contains("pub(crate) mod __Action_generated_routing"));
        syn::parse_file(&rendered).unwrap_or_else(|error| {
            panic!("the Action domain routing output should be valid Rust: {error}\n{rendered}")
        });
        let generated_source = normalize_tokens(&rendered);

        assert!(rendered.contains("pub trait ActionInternalSudoService: Send + Sync"));
        assert!(!rendered.contains("#[::async_trait::async_trait]"));
        assert!(rendered.contains("#[async_trait::async_trait]"));
        assert!(rendered.contains("pub(crate) struct ActionInternalSudoServiceImpl"));
        assert!(rendered.contains("pub(crate) mod __Action_generated_routing"));
        assert!(rendered.contains("pub(crate) struct ActionGeneratedHandler"));
        assert!(generated_source.contains("fnroute(source:Self::Source)->Option<ActionRequestPayload>"));
        assert!(generated_source.contains("->Option<ActionRequestPayload>"));
        assert!(
            generated_source.contains("TestBroadcast::Enclave(event)=>action_request_from_enclave_broadcast(event)")
        );
        assert!(generated_source.contains("event=>Some((ActionBroadcast::OrderExecuted{event}))"));
        assert!(!generated_source.contains("event=>Some((ActionBroadcast::OrderExecuted{event}).into())"));
        assert!(!generated_source.contains("event=>Some((ActionBroadcast::OrderExecuted{event})),_=>None"));
        assert!(generated_source.contains(&normalize_tokens(
            ") -> Self where __InboundRouter0: __InboundRouting0<Source = Root>"
        )));
        assert!(generated_source.contains("letbroadcast_rx=broadcast_tx.subscribe();Self{public_requests_rx"));
        assert!(generated_source.contains(&normalize_tokens(
            "pub(crate) async fn run(mut self) where __InboundRouter0: __InboundRouting0<Source = Root>, __OutboundRouter0: __OutboundRouting0<Broadcast>"
        )));
    }

    #[test]
    fn response_timeout_renders_parseable_awaited_side_effecting_branch() {
        let services = SudoServices::parse_tokens(parse_sudo_test_tokens(
            r#"marker ActionDomain; request ActionRequestPayload => ActionResponse error = ActionError transport = SharedInfraError target = "action"; response_timeout { predicate = ActionRequestPayload::is_side_effecting, duration = ACTION_RESPONSE_TIMEOUT }; pub service ActionInternalSudoService { AttributedMutation => OrderExecuted, EvictConnectorCache => Ack, } inbound TestBroadcast => Option<ActionRequestPayload> { TestBroadcast::Enclave(event) => action_request_from_enclave_broadcast(event), } outbound ActionOrderEvent => ActionBroadcast { event => OrderExecuted { event }, }"#,
        ))
        .expect("the Action timeout declaration should parse");

        let rendered = services.render();
        syn::parse_file(&rendered)
            .unwrap_or_else(|error| panic!("the Action timeout output should be valid Rust: {error}\n{rendered}"));
        let normalized = normalize_tokens(&rendered);
        assert!(normalized.contains("ifside_effecting{actor_response.await"));
        assert!(normalized.contains("timeout(ACTION_RESPONSE_TIMEOUT,actor_response).await"));
    }

    #[test]
    fn guarded_routing_mappings_render_the_guard_as_a_match_guard() {
        let services = SudoServices::parse_tokens(parse_sudo_test_tokens(
            r#"marker KnowledgeDomain; request KnowledgeRequestPayload => KnowledgeResponse error = KnowledgeError transport = SharedInfraError target = "knowledge"; inbound TestBroadcast => KnowledgeRequestPayload { TestBroadcast::Config(ConfigBroadcast::ConfigValueChanged { key }) if key == "knowledge" || key.starts_with("knowledge.") => KnowledgeRequestPayload::KnowledgeConfigChanged, }"#,
        ))
        .expect("the guarded Knowledge mapping should parse");

        let rendered_source = services.render();
        let rendered = normalize_tokens(&rendered_source);
        assert!(rendered.contains("key==\"knowledge\"||key.starts_with(\"knowledge.\")"));
        assert!(rendered.contains("TestBroadcast::Config(ConfigBroadcast::ConfigValueChanged{key})ifkey==\"knowledge\"||key.starts_with(\"knowledge.\")=>"));
        assert!(!rendered_source.contains("| |"));
        assert!(!rendered_source.contains("= >"));
    }

    #[test]
    fn guarded_routing_output_is_parseable_rust() {
        let services = SudoServices::parse_tokens(parse_sudo_test_tokens(
            r#"marker KnowledgeDomain; request KnowledgeRequestPayload => KnowledgeResponse error = KnowledgeError transport = SharedInfraError target = "knowledge"; inbound TestBroadcast => Option<KnowledgeRequestPayload> { TestBroadcast::Config(ConfigBroadcast::ConfigValueChanged { key }) if key == "knowledge" || key.starts_with("knowledge.") => KnowledgeRequestPayload::KnowledgeConfigChanged, }"#,
        ))
        .expect("the guarded Knowledge mapping should parse");

        let rendered = services.render();
        syn::parse_file(&rendered)
            .unwrap_or_else(|error| panic!("the guarded Knowledge output should be valid Rust: {error}\n{rendered}"));
    }

    #[test]
    fn generated_routing_omits_inbound_channel_when_not_declared() {
        let services = SudoServices::parse_tokens(parse_sudo_test_tokens(
            "marker KnowledgeDomain; request KnowledgeRequestPayload => KnowledgeResponse error = KnowledgeError transport = SharedInfraError target = \"knowledge\"; outbound KnowledgeResponse => KnowledgeBroadcast { KnowledgeRecordsChanged => RecordsChanged, }",
        ))
        .expect("outbound-only routing should parse");

        let rendered = services.render();
        assert!(!rendered.contains("broadcast_rx"));
        assert!(rendered.contains("Receiver<(KnowledgeRequest, ::aves::AuthProjection)>"));
    }

    #[test]
    fn generated_routing_keeps_auth_and_sudo_paths_separate() {
        let services = SudoServices::parse_tokens(parse_sudo_test_tokens(
            "marker ExampleDomain; request ExampleRequestPayload => ExampleResponse error = ExampleError transport = SharedInfraError target = \"example\"; pub service ExampleSudoService { Secret => Done, } inbound RootBroadcast => ExampleRequestPayload { RootBroadcast::Changed(value) => Update(value), } outbound ActorEvent => ExampleBroadcast { ActorEvent::Done(value) => Updated(value), }",
        ))
        .expect("routing declaration should parse");
        let rendered = services.render();

        assert!(rendered.contains("impl __InboundRouting0 for __InboundRouter0"));
        assert!(rendered.contains("actor_event_rx_0"));
        assert!(rendered.contains("check_authenticated_subject_and_capabilities"));
        assert!(rendered.contains("public_request_branch"));
        assert!(rendered.contains("ExampleGeneratedHandler"));
        assert!(rendered.contains("tokio::select!"));
        assert!(rendered.contains("sudo_requests_rx_0"));
        assert!(!rendered.contains("OpaqueRequestEnvelope"));
    }

    #[test]
    fn renders_channel_service_traits_implementations_and_aggregate_metadata() {
        let services = SudoServices::parse_tokens(
            parse_sudo_test_tokens(
                "marker EnclaveDomain; request EnclaveRequestPayload => EnclaveResponse error = EnclaveError transport = SharedInfraError target = \"enclave\"; pub service EnclaveWalletSudoService { CreateWallet => WalletCreated (operation = \"create_wallet\"), } pub service EnclaveAccountSecretSudoService { CreateWallet => WalletCreated (operation = \"create_wallet\"), WriteAccountSecret => SecretWritten (operation = \"write_account_secret\"), }",
            ),
        )
        .expect("sudo service registry should parse");

        register_request(RegisteredRequest {
            name: services.request.clone(),
            domain: "enclave".to_string(),
            privileged: vec!["CreateWallet".to_string(), "WriteAccountSecret".to_string()],
        })
        .expect("request should register");
        register_services(
            &services.request,
            "enclave",
            services
                .services
                .iter()
                .flat_map(|service| service.mappings.iter())
                .map(|mapping| mapping.request_variant.clone())
                .collect(),
        )
        .expect("service mappings should register");

        let rendered = services.render();
        assert!(rendered.contains("pub trait EnclaveWalletSudoService: Send + Sync"));
        assert!(rendered.contains("pub(crate) struct EnclaveWalletSudoServiceImpl"));
        assert!(rendered.contains("pub const ENCLAVE_SUDO_SERVICES"));
        assert!(rendered.contains("pub struct EnclaveWalletSudoServiceKey"));
        assert!(rendered.contains("impl ::aves::DomainProviderMetadata for EnclaveDomain"));
        assert!(rendered.contains("type ProviderSet = EnclaveDomainSudoServiceProviderKeys"));
        assert_eq!(
            rendered
                .matches("impl EnclaveSudoServicePrivilegeMapping for EnclavePrivilegedCreateWallet")
                .count(),
            1,
            "aggregate mapping must accept membership in more than one service"
        );
        assert!(rendered.contains("CreateWalletInput"));
        assert!(rendered.contains("EnclaveResponse"));
        assert!(rendered.contains("SecretWritten"));

        let trait_body = rendered
            .split("pub trait EnclaveWalletSudoService")
            .nth(1)
            .expect("wallet service trait should render")
            .split("pub(crate) struct EnclaveWalletSudoServiceImpl")
            .next()
            .expect("wallet service implementation should follow its trait");
        assert!(!trait_body.contains("self.sender"));
        assert!(rendered.contains("SharedInfraError::ChannelSend"));
        assert!(rendered.contains("SharedInfraError::ChannelRecv"));
        assert!(rendered.contains("match WalletCreatedOutput::try_from(response)"));
        assert!(rendered.contains("Ok(output) => Ok(output.into())"));
    }

    #[test]
    fn request_expansion_requires_every_privileged_variant_in_aggregate_registry() {
        let request = RequestEnum {
            name: "EnclaveRequestPayload".to_string(),
            domain: "enclave".to_string(),
            marker: Some("crate::enclave::EnclaveDomain".to_string()),
            context_type: "TestContext".to_string(),
            root_error: "TestError".to_string(),
            domain_error: "DomainError".to_string(),
            variants: vec![
                RequestVariant {
                    name: "CreateWallet".to_string(),
                    fields: RequestFields::Unit,
                    privileged: true,
                    operation: None,
                },
                RequestVariant {
                    name: "List".to_string(),
                    fields: RequestFields::Unit,
                    privileged: false,
                    operation: Some(RequestOperation {
                        name: "list".to_string(),
                        kind: OperationKind::Query,
                        response: "ListOutput".to_string(),
                        response_variant: "List".to_string(),
                    }),
                },
            ],
        };

        let rendered = request.render();

        assert!(rendered.contains("EnclavePrivilegedCreateWallet"));
        assert!(rendered.contains("fn assert_sudo_service_mapping_for_EnclavePrivilegedCreateWallet()"));
        assert!(rendered.contains("assert_sudo_service_mapping::<EnclavePrivilegedCreateWallet>();"));
        assert!(rendered.contains("EnclaveSudoServicePrivilegeMapping"));
        assert!(rendered.contains("pub const ENCLAVE_REQUEST_PAYLOAD_OPERATIONS"));
    }

    #[test]
    fn public_bus_client_into_payload_uses_the_payload_enum() {
        let request = RequestEnum {
            name: "ActionRequestPayload".to_string(),
            domain: "action".to_string(),
            marker: Some("crate::action::ActionDomain".to_string()),
            context_type: "TestContext".to_string(),
            root_error: "TestError".to_string(),
            domain_error: "DomainError".to_string(),
            variants: vec![RequestVariant {
                name: "PlaceLimit".to_string(),
                fields: RequestFields::Unit,
                privileged: false,
                operation: Some(RequestOperation {
                    name: "placeLimit".to_string(),
                    kind: OperationKind::Mutation,
                    response: "OrderExecutedOutput".to_string(),
                    response_variant: "OrderExecuted".to_string(),
                }),
            }],
        };

        let rendered = request.render_public_bus_clients();

        assert!(rendered.contains("fn into_payload(self) -> ActionRequestPayload;"));
        assert!(
            rendered.contains("fn into_payload(self) -> ActionRequestPayload { ActionRequestPayload::from(self) }")
        );
        assert!(!rendered.contains("fn into_payload(self) -> ActionRequest;"));
    }

    #[test]
    fn request_declaration_supplies_generic_context_and_root_error_paths() {
        let request = RequestEnum::parse(
            TokenStream2::from_str(
                "domain = \"arbitrary\", marker = crate::arbitrary::ArbitraryDomain, context = custom::Context, root_error = custom::RootError, domain_error = custom::DomainError",
            )
            .expect("request attributes should parse"),
            TokenStream2::from_str(
                "enum ArbitraryRequestPayload { #[operation(name = \"read\", kind = \"query\", response = ReadOutput, response_variant = \"Read\")] Read {} }",
            )
            .expect("request enum should parse"),
        )
        .expect("generic request declaration should parse");

        let rendered = request.render();
        let normalized = normalize_tokens(&rendered);

        assert!(normalized.contains(&normalize_tokens("rspc::Router<custom::Context>")));
        assert!(normalized.contains(&normalize_tokens(
            "RequestOperationDescriptor::<custom::Context, custom::RootError>"
        )));
        assert!(normalized.contains(&normalize_tokens(
            "custom::RootError: ::core::convert::From<::aves::GeneratedCallError<custom::DomainError>>"
        )));
        assert!(normalized.contains(&normalize_tokens(
            "Result<ReadOutput, ::aves::GeneratedCallError<custom::DomainError>>"
        )));
        assert!(normalized.contains(&normalize_tokens(
            "pub async fn dispatch_read(context: &::aves::BusRpcContext"
        )));
        assert!(normalized.contains(&normalize_tokens("let jwt = context.jwt()?")));
        assert!(normalized.contains(&normalize_tokens("let sender = context.bus_sender()?")));
        assert!(normalized.contains(&normalize_tokens(
            "OpaqueRequestEnvelope::new_with_rejection::<crate::arbitrary::ArbitraryDomain, _>(jwt, request, move |error|"
        )));
        assert!(normalized.contains(&normalize_tokens("ctx.admit()?")));
        assert!(normalized.contains(&normalize_tokens(
            "if let Ok(error) = error.downcast::<::aves::AvesAuthError>()"
        )));
        assert!(normalized.contains(&normalize_tokens("let _ = admission_error.send(*error)")));
        assert!(normalized.contains(&normalize_tokens("result = admission_error_receiver => match result")));
        assert!(normalized.contains(&normalize_tokens(
            "Err(::aves::GeneratedCallError::Authentication(error))"
        )));
        assert!(!normalized.contains(&normalize_tokens("crate::domain::")));
    }

    #[test]
    fn generated_handler_can_opt_into_typed_authorization_hook() {
        let services = SudoServices::parse_tokens(parse_sudo_test_tokens(
            "request ExampleRequestPayload => ExampleResponse error = DomainError transport = TransportError target = \"example\"; authorization_hook = crate::example::authorize; pub service ExampleSudoService { Secret => Done, }",
        ))
        .expect("handler authorization hook declaration should parse");
        let rendered = render_routing_blocks(&services);

        assert!(rendered.contains("crate :: example :: authorize"));
        assert!(rendered.contains("request.reply_to.send(Err(error))"));
        assert!(rendered.contains("&request.payload, &projection"));
        assert!(!rendered.contains("check_authenticated_subject_and_capabilities(&projection)"));

        let normalized = normalize_tokens(&rendered);
        assert!(normalized.contains(&normalize_tokens(
            "if let Err(error) = (crate::example::authorize)(&request.payload, &projection) { let _ = request.reply_to.send(Err(error)); return; } let _ = forward_request(actor_tasks, request).await;"
        )));
        assert!(
            normalized
                .find("request.reply_to.send(Err(error))")
                .unwrap()
                < normalized
                    .find("forward_request(actor_tasks,request)")
                    .unwrap()
        );
        syn::parse_file(&rendered)
            .unwrap_or_else(|error| panic!("the hooked handler output should be valid Rust: {error}\n{rendered}"));
    }

    #[test]
    fn generated_handler_keeps_unhooked_public_requests_on_default_path() {
        let services = SudoServices::parse_tokens(parse_sudo_test_tokens(
            "request ExampleRequestPayload => ExampleResponse error = DomainError transport = TransportError target = \"example\"; pub service ExampleSudoService { Secret => Done, }",
        ))
        .expect("unhooked handler routing declaration should parse");
        let rendered = render_routing_blocks(&services);
        let normalized = normalize_tokens(&rendered);

        assert!(normalized.contains(&normalize_tokens(
            "let _ = check_authenticated_subject_and_capabilities(&projection); let _ = forward_request(actor_tasks, request).await;"
        )));
        assert!(!normalized.contains("request.reply_to.send(Err(error))"));
    }

    #[test]
    fn serde_skipped_request_variants_are_internal_only() {
        let request = RequestEnum::parse(
            TokenStream2::from_str(
                "domain = \"knowledge\", marker = crate::knowledge::KnowledgeDomain, context = aves::BusRpcContext, root_error = TestError, domain_error = crate::knowledge::KnowledgeError",
            )
            .expect("request attributes should parse"),
            TokenStream2::from_str(
                "enum KnowledgeRequestPayload { #[serde(skip)] KnowledgeConfigChanged, #[operation(name = \"listKnowledge\", kind = \"query\", response = KnowledgeListedOutput, response_variant = \"KnowledgeListed\")] ListKnowledge {} }",
            )
            .expect("request enum should parse"),
        )
        .expect("serde-skipped request variants should parse");

        assert_eq!(request.variants.len(), 2);
        assert!(request.variants[0].operation.is_none());
        let rendered = request.render();
        assert!(rendered.contains("pub const KNOWLEDGE_REQUEST_PAYLOAD_OPERATIONS"));
        assert!(rendered.contains("pub struct ListKnowledgeInput"));
        assert!(!rendered.contains("KnowledgeConfigChangedInput"));
    }

    #[test]
    fn public_operations_without_marker_are_rejected() {
        let error = RequestEnum::parse(
            TokenStream2::from_str(
                "domain = \"arbitrary\", context = TestContext, root_error = TestError, domain_error = DomainError",
            )
            .expect("request attributes should parse"),
            TokenStream2::from_str(
                "enum ArbitraryRequestPayload { #[operation(name = \"read\", kind = \"query\", response = ReadOutput, response_variant = \"Read\")] Read {} }",
            )
            .expect("request enum should parse"),
        )
        .expect_err("public operations must declare a marker");

        assert_eq!(error, "bus_requests requires marker = <domain marker type path>");
    }

    #[test]
    fn markerless_all_sudo_request_renders_without_opaque_client() {
        let request = RequestEnum::parse(
            TokenStream2::from_str(
                "domain = \"bus\", context = TestContext, root_error = TestError, domain_error = DomainError",
            )
            .expect("request attributes should parse"),
            TokenStream2::from_str(
                "enum BusRequestPayload { #[privileged(reason = \"internal infrastructure\")] Open {} }",
            )
            .expect("request enum should parse"),
        )
        .expect("all-sudo markerless request should parse");

        let rendered = request.render();

        assert!(!rendered.contains("OpaqueRequestEnvelope::new"));
        assert!(!rendered.contains("PublicRequest"));
    }

    #[test]
    fn public_bus_client_preserves_domain_reply_errors() {
        let request = RequestEnum {
            name: "ConfigRequestPayload".to_string(),
            domain: "config".to_string(),
            marker: Some("crate::config::ConfigDomain".to_string()),
            context_type: "TestContext".to_string(),
            root_error: "TestError".to_string(),
            domain_error: "DomainError".to_string(),
            variants: vec![RequestVariant {
                name: "GetConfig".to_string(),
                fields: RequestFields::Unit,
                privileged: false,
                operation: Some(RequestOperation {
                    name: "getConfig".to_string(),
                    kind: OperationKind::Query,
                    response: "ConfigOutput".to_string(),
                    response_variant: "Config".to_string(),
                }),
            }],
        };

        let rendered = request.render_public_bus_clients();
        let normalized = normalize_tokens(&rendered);

        assert!(normalized.contains(&normalize_tokens("GeneratedCallError<DomainError>")));
        assert!(normalized.contains(&normalize_tokens(
            "<ConfigOutput as ::core::convert::TryFrom<ConfigResponse>>::try_from(response).map_err(::aves::GeneratedCallError::<DomainError>::from).map_err(<TestError as ::core::convert::From<::aves::GeneratedCallError<DomainError>>>::from)"
        )));
        assert!(normalized.contains(&normalize_tokens(
            "<ConfigOutput as ::core::convert::TryFrom<ConfigResponse>>::try_from(response).map_err(::aves::GeneratedCallError::<DomainError>::from)"
        )));
        assert!(normalized.contains(&normalize_tokens(".map_err(::aves::GeneratedCallError::DomainError)")));
        assert!(!normalized.contains(&normalize_tokens(
            "Ok(<ConfigOutput as ::core::convert::TryFrom<ConfigResponse>>"
        )));
        assert!(!normalized.contains(&normalize_tokens(
            "<ConfigOutput as ::core::convert::TryFrom<ConfigResponse>>::try_from(response)?"
        )));
        assert!(normalized.contains(&normalize_tokens(
            "pub async fn dispatch_get_config(context: &::aves::BusRpcContext, input: GetConfigInput) -> ::core::result::Result<ConfigOutput, TestError>"
        )));
        assert!(normalized.contains(&normalize_tokens(
            "pub async fn send_get_config(jwt: ::aves::Jwt, sender: &::tokio::sync::mpsc::Sender<::aves::OpaqueRequestEnvelope>, input: GetConfigInput) -> ::core::result::Result<ConfigOutput, ::aves::GeneratedCallError<DomainError>>"
        )));
        assert!(!normalized.contains(&normalize_tokens("crate::bus::OpaqueRequestEnvelope")));
        assert!(!normalized.contains(&normalize_tokens("::aves::bus::OpaqueRequestEnvelope")));
    }

    #[test]
    fn authenticated_normal_bus_request_waits_for_reply_after_admission_closes() {
        let request = RequestEnum {
            name: "ConfigRequestPayload".to_string(),
            domain: "config".to_string(),
            marker: Some("crate::config::ConfigDomain".to_string()),
            context_type: "TestContext".to_string(),
            root_error: "TestError".to_string(),
            domain_error: "DomainError".to_string(),
            variants: vec![RequestVariant {
                name: "GetConfig".to_string(),
                fields: RequestFields::Unit,
                privileged: false,
                operation: Some(RequestOperation {
                    name: "getConfig".to_string(),
                    kind: OperationKind::Query,
                    response: "ConfigOutput".to_string(),
                    response_variant: "Config".to_string(),
                }),
            }],
        };

        let normalized = normalize_tokens(&request.render_public_bus_clients());

        assert!(normalized.contains(&normalize_tokens("::tokio::select! { biased;")));
        let admission_branch = normalized
            .find(&normalize_tokens("result = admission_error_receiver =>"))
            .expect("generated client must resolve admission before the reply");
        let reply_branch = normalized
            .find(&normalize_tokens("result = &mut reply =>"))
            .expect("generated client must retain a reply branch");
        assert!(admission_branch < reply_branch);
        assert!(normalized.contains(&normalize_tokens(
            "Ok(error) => Err(::aves::GeneratedCallError::Authentication(error))"
        )));
        assert!(normalized.contains(&normalize_tokens(
            "Err(_) => reply.await.map_err(|_| ::aves::GeneratedCallError::ReplyDropped)?.map_err(::aves::GeneratedCallError::DomainError)"
        )));
        assert!(normalized.contains(&normalize_tokens(
            "OpaqueRequestEnvelope::new_with_rejection::<crate::config::ConfigDomain, _>"
        )));
        assert!(!normalized.contains(&normalize_tokens(
            "OpaqueRequestEnvelope::new::<crate::config::ConfigDomain, _>"
        )));
        assert!(!normalized.contains(&normalize_tokens(
            "Err(_) => Err(::aves::GeneratedCallError::ReplyDropped)"
        )));
    }

    #[test]
    fn privileged_variants_do_not_receive_public_bus_clients() {
        let request = RequestEnum {
            name: "ActionRequestPayload".to_string(),
            domain: "action".to_string(),
            marker: Some("crate::action::ActionDomain".to_string()),
            context_type: "TestContext".to_string(),
            root_error: "TestError".to_string(),
            domain_error: "DomainError".to_string(),
            variants: vec![RequestVariant {
                name: "EvictConnectorCache".to_string(),
                fields: RequestFields::Unit,
                privileged: true,
                operation: None,
            }],
        };

        let rendered = request.render_public_bus_clients();

        assert!(rendered.is_empty());
        assert!(!rendered.contains("PublicRequest"));
        assert!(!rendered.contains("send_evict_connector_cache"));
    }

    #[test]
    fn mixed_request_keeps_privileged_variants_out_of_all_public_surfaces() {
        let request = RequestEnum {
            name: "ActionRequestPayload".to_string(),
            domain: "action".to_string(),
            marker: Some("crate::action::ActionDomain".to_string()),
            context_type: "TestContext".to_string(),
            root_error: "TestError".to_string(),
            domain_error: "DomainError".to_string(),
            variants: vec![
                RequestVariant {
                    name: "ListOrders".to_string(),
                    fields: RequestFields::Unit,
                    privileged: false,
                    operation: Some(RequestOperation {
                        name: "listOrders".to_string(),
                        kind: OperationKind::Query,
                        response: "OrdersOutput".to_string(),
                        response_variant: "Orders".to_string(),
                    }),
                },
                RequestVariant {
                    name: "RotateSigner".to_string(),
                    fields: RequestFields::Named(vec![RequestField {
                        name: "secret".to_string(),
                        ty: "SecretString".to_string(),
                        attrs: String::new(),
                    }]),
                    privileged: true,
                    operation: None,
                },
            ],
        };

        let public_clients = request.render_public_bus_clients();
        let rpc = request.render_rpc_router();
        let descriptors = request.render_descriptors();
        let rendered = request.render();

        assert!(public_clients.contains("send_list_orders"));
        assert!(public_clients.contains("ListOrdersInput"));
        assert!(!public_clients.contains("RotateSignerInput"));
        assert!(!public_clients.contains("send_rotate_signer"));

        assert!(rpc.contains("listOrders"));
        assert!(rpc.contains("dispatch_list_orders(&ctx, input).await"));
        assert!(!rpc.contains("rotateSigner"));

        assert!(descriptors.contains("listOrders"));
        assert!(descriptors.contains("::aves::RequestOperationDescriptor::<TestContext, TestError>"));
        assert!(descriptors.contains("dispatch: dispatch_list_orders_json"));
        assert!(descriptors.contains("OrdersOutput"));
        assert!(!descriptors.contains("rotateSigner"));
        assert!(!descriptors.contains("RotateSigner"));

        assert!(rendered.contains("pub async fn dispatch_list_orders"));
        assert!(rendered.contains("pub(crate) fn dispatch_list_orders_json"));
        assert!(!rendered.contains("dispatch_rotate_signer"));
        // no internal request leak
        assert!(!rendered.contains("dispatch_rpc_request"));

        assert!(rendered.contains("ActionPrivilegedRotateSigner"));
        assert!(rendered.contains("assert_sudo_service_mapping::<ActionPrivilegedRotateSigner>();"));

        let privileged_input = rendered
            .split("RotateSignerInput")
            .nth(1)
            .expect("privileged input projection should be generated");
        assert!(rendered.contains("#[doc(hidden)]\npub struct RotateSignerInput"));
        assert!(!privileged_input.contains("::serde::Serialize"));
        assert!(!privileged_input.contains("::specta::Type"));
        assert!(!privileged_input.contains("::schemars::JsonSchema"));
    }

    #[test]
    fn operation_generation_emits_typed_json_rpc_and_mcp_surfaces() {
        let request = RequestEnum::parse(
            TokenStream2::from_str(
                "domain = \"market\", marker = crate::market::MarketDomain, context = TestContext, root_error = TestError, domain_error = DomainError",
            )
            .expect("request attributes should parse"),
            TokenStream2::from_str(
                "enum MarketRequestPayload { #[operation(name = \"searchAssets\", kind = \"query\", response = SearchAssetsOutput, response_variant = \"SearchAssets\")] SearchAssets {} }",
            )
            .expect("request enum should parse"),
        )
        .expect("operation request should parse");

        let rendered = request.render();

        assert!(rendered.contains("pub async fn dispatch_search_assets"));
        assert!(rendered.contains("pub(crate) fn dispatch_search_assets_json"));
        assert!(rendered.contains("dispatch: dispatch_search_assets_json"));
        assert!(rendered.contains(".procedure(\n            \"searchAssets\""));
        assert!(rendered.contains("mcp_eligible: true"));
        assert!(rendered.contains("canonical_variant: \"MarketRequestPayload::SearchAssets\""));
    }

    #[test]
    fn response_generation_requires_generic_root_error_projection_contract() {
        let response = ResponseEnum::parse(
            TokenStream2::new(),
            TokenStream2::from_str("enum SampleResponse { Accepted { value: String }, Rejected }")
                .expect("response enum should parse"),
        )
        .expect("response declaration should parse");

        let rendered = response.render();

        assert!(rendered.contains("type Error = ::aves::ResponseProjectionError"));
        // no internal error leak
        // no internal domain leak
        assert!(rendered.contains("ResponseProjectionError"));
    }

    #[test]
    fn mapped_privileged_generation_is_sudo_only() {
        let request = RequestEnum::parse(
            TokenStream2::from_str(
                "domain = \"enclave\", marker = crate::enclave::EnclaveDomain, context = TestContext, root_error = TestError, domain_error = DomainError",
            )
            .expect("request attributes should parse"),
            TokenStream2::from_str(
                "enum EnclaveRequestPayload { #[privileged(reason = \"internal workflow\")] RotateSigner { secret: SecretString } }",
            )
            .expect("request enum should parse"),
        )
        .expect("privileged request should parse");
        let services = SudoServices::parse_tokens(parse_sudo_test_tokens(
            "marker EnclaveDomain; request EnclaveRequestPayload => EnclaveResponse error = EnclaveError transport = SharedInfraError target = \"enclave\"; pub service EnclaveWalletSudoService { RotateSigner => Signed (operation = \"rotate_signer\"), }",
        ))
        .expect("sudo service mapping should parse");

        let request_rendered = request.render();
        let sudo_rendered = services.render();

        assert!(sudo_rendered.contains("pub trait EnclaveWalletSudoService"));
        assert!(sudo_rendered.contains("async fn rotate_signer(&self, input: RotateSignerInput)"));
        assert!(sudo_rendered.contains("pub(crate) struct EnclaveWalletSudoServiceImpl"));
        assert!(sudo_rendered.contains("let payload = EnclaveRequestPayload::from(input);"));
        assert!(sudo_rendered.contains("SharedInfraError::ChannelRecv"));
        assert!(sudo_rendered.contains("Ok(output.into())"));
        assert!(sudo_rendered.contains("ENCLAVE_WALLET_SUDO_SERVICE_MAPPINGS"));
        assert!(sudo_rendered.contains("pub const ENCLAVE_SUDO_SERVICES"));
        assert!(request_rendered.contains("EnclavePrivilegedRotateSigner"));

        assert!(!request_rendered.contains("pub async fn dispatch_rotate_signer"));
        assert!(!request_rendered.contains("dispatch_rotate_signer_json"));
        assert!(!request_rendered.contains(".procedure("));
        assert!(!request_rendered.contains("mcp_eligible"));
        assert!(!request_rendered.contains("\"rotate_signer\""));
        assert!(!request_rendered.contains("canonical_variant: \"EnclaveRequestPayload::RotateSigner\""));
    }

    #[test]
    fn sudo_service_mapping_rejects_public_variants() {
        let services = SudoServices::parse_tokens(parse_sudo_test_tokens(
            "marker MixedDomain; request MixedRequestPayload => MixedResponse error = MixedError transport = SharedInfraError target = \"mixed\"; pub service MixedSudoService { ListItems => ItemsListed, RotateSigner => Signed, }",
        ))
        .expect("mixed sudo service mapping should parse");

        register_request(RegisteredRequest {
            name: services.request.clone(),
            domain: services.target_domain.clone(),
            privileged: vec!["RotateSigner".to_string()],
        })
        .expect("mixed request should register");
        let error = register_services(
            &services.request,
            &services.target_domain,
            services.services[0]
                .mappings
                .iter()
                .map(|mapping| mapping.request_variant.clone())
                .collect(),
        )
        .expect_err("public operations must not enter sudo service mappings");

        assert!(error.contains("non-privileged request variant `ListItems`"));
    }

    #[test]
    fn rejects_missing_or_invalid_operation_kind() {
        let missing = parse_operation_kind(
            r#"operation(name = "getConfig", response = ConfigOutput, response_variant = "Config")"#,
        )
        .expect_err("missing operation kind should fail");
        let invalid = parse_operation_kind(
            r#"operation(name = "getConfig", kind = "subscription", response = ConfigOutput, response_variant = "Config")"#,
        )
        .expect_err("invalid operation kind should fail");
        let unquoted = parse_operation_kind(
            r#"operation(name = "getConfig", kind = query, response = ConfigOutput, response_variant = "Config")"#,
        )
        .expect_err("unquoted operation kind should fail");

        assert!(missing.contains("requires kind"));
        assert!(invalid.contains("must be \"query\" or \"mutation\""));
        assert!(unquoted.contains("must be a string literal"));
    }

    #[test]
    fn parses_privileged_marker_and_retains_service_input() {
        let attributes = vec!["privileged(reason = \"internal workflow\")".to_string()];
        assert_eq!(
            parse_privileged_reason_attributes(&attributes).expect("privileged metadata should parse"),
            Some("internal workflow".to_string())
        );

        let variant = RequestVariant {
            name: "CreateWallet".to_string(),
            fields: RequestFields::Unit,
            privileged: true,
            operation: None,
        };
        assert!(variant.operation.is_none());
    }

    #[test]
    fn renders_unit_request_input_as_an_empty_braced_struct() {
        let variant = RequestVariant {
            name: "ListAgents".to_string(),
            fields: RequestFields::Unit,
            privileged: false,
            operation: Some(RequestOperation {
                name: "listAgents".to_string(),
                kind: OperationKind::Query,
                response: "AgentsListOutput".to_string(),
                response_variant: "AgentsList".to_string(),
            }),
        };

        let rendered = variant.render_input_struct();

        assert!(rendered.contains("pub struct ListAgentsInput {}"));
        assert!(!rendered.contains("pub struct ListAgentsInput;"));
        assert!(rendered.contains("::schemars::JsonSchema"));
        assert!(rendered.contains("formatter.write_str(\"ListAgentsInput\")"));
    }

    #[test]
    fn requires_reasons_for_privileged_attributes() {
        for metadata in ["privileged".to_string(), "privileged(reason = \"\")".to_string()] {
            let error = parse_privileged_reason_attributes(&[metadata])
                .expect_err("a reasonless or empty privileged attribute must fail");

            assert!(error.contains("privileged metadata requires reason"));
        }
    }

    #[test]
    fn rejects_legacy_rpc_skip_attribute() {
        let error = parse_privileged_reason_attributes(&["rpc_skip(reason = \"legacy\")".to_string()])
            .expect_err("the legacy rpc_skip attribute must fail");

        assert!(error.contains("no longer accepts #[rpc_skip]"));
        assert!(error.contains("#[privileged"));
    }

    #[test]
    fn keeps_duplicate_operation_and_response_linkage_validation() {
        let variants = vec![
            RequestVariant {
                name: "First".to_string(),
                fields: RequestFields::Unit,
                privileged: false,
                operation: Some(RequestOperation {
                    name: "duplicate".to_string(),
                    kind: OperationKind::Query,
                    response: "FirstOutput".to_string(),
                    response_variant: "First".to_string(),
                }),
            },
            RequestVariant {
                name: "Second".to_string(),
                fields: RequestFields::Unit,
                privileged: false,
                operation: Some(RequestOperation {
                    name: "duplicate".to_string(),
                    kind: OperationKind::Mutation,
                    response: "SecondOutput".to_string(),
                    response_variant: "Second".to_string(),
                }),
            },
        ];

        let duplicate_error =
            reject_duplicate_operations("config", &variants).expect_err("duplicate operation names should still fail");
        let linkage_error = validate_response_linkage("WrongOutput", "Config")
            .expect_err("response linkage mismatch should still fail");

        assert!(duplicate_error.contains("duplicate operation name"));
        assert!(linkage_error.contains("expected ConfigOutput"));
    }

    #[test]
    fn redacts_legacy_sensitive_field_names() {
        for name in ["password", "private_key", "passphrase", "local_passphrase", "digest"] {
            assert!(
                is_sensitive_field(name),
                "legacy sensitive field `{name}` must be redacted"
            );
        }
    }

    #[test]
    fn redacts_account_secret_raw_field_names() {
        // Unified enclave account operations (SecretWriteRequest/SecretRotateRequest)
        // carry a raw `secret` field; inline account create/update variants may
        // carry raw `api_key`/`token` fields. All three must trigger redaction so
        // generated Debug impls never leak raw provider credentials or Flex tokens.
        for name in ["api_key", "secret", "token"] {
            assert!(
                is_sensitive_field(name),
                "account-secret field `{name}` must be redacted"
            );
        }
    }

    #[test]
    fn does_not_redact_non_secret_field_names() {
        for name in [
            "account_id",
            "slot_name",
            "account_name",
            "provider_account_id",
            "value",
            "config",
        ] {
            assert!(
                !is_sensitive_field(name),
                "non-secret field `{name}` must not be redacted"
            );
        }
    }

    #[test]
    fn generated_debug_impl_redacts_account_secret_fields() {
        // Simulates a SecretWriteRequest-shaped input struct: account_id, slot_name,
        // and a raw `secret` field. The generated Debug impl must print the raw
        // secret as "<redacted>" and must not embed the field value.
        let fields = vec![
            RequestField {
                name: "account_id".to_string(),
                ty: "SpectaUuid".to_string(),
                attrs: String::new(),
            },
            RequestField {
                name: "slot_name".to_string(),
                ty: "String".to_string(),
                attrs: String::new(),
            },
            RequestField {
                name: "secret".to_string(),
                ty: "String".to_string(),
                attrs: String::new(),
            },
        ];
        let debug_impl = render_named_debug_impl("SecretWriteInput", &fields);

        assert!(
            debug_impl.contains(".field(\"secret\", &\"<redacted>\")"),
            "generated Debug must redact the `secret` field: {debug_impl}"
        );
        assert!(
            !debug_impl.contains("&self.secret"),
            "generated Debug must not reference the raw `secret` value: {debug_impl}"
        );
        assert!(
            debug_impl.contains(".field(\"account_id\", &self.account_id)"),
            "generated Debug must surface non-secret `account_id`: {debug_impl}"
        );
        assert!(
            debug_impl.contains(".field(\"slot_name\", &self.slot_name)"),
            "generated Debug must surface non-secret `slot_name`: {debug_impl}"
        );
    }

    #[test]
    fn generated_debug_impl_redacts_api_key_and_token_fields() {
        // Covers inline account create/update variants that may carry raw
        // `api_key` or `token` fields alongside non-secret metadata.
        let fields = vec![
            RequestField {
                name: "account_name".to_string(),
                ty: "String".to_string(),
                attrs: String::new(),
            },
            RequestField {
                name: "api_key".to_string(),
                ty: "String".to_string(),
                attrs: String::new(),
            },
            RequestField {
                name: "token".to_string(),
                ty: "String".to_string(),
                attrs: String::new(),
            },
        ];
        let debug_impl = render_named_debug_impl("AccountCreateInput", &fields);

        assert!(
            debug_impl.contains(".field(\"api_key\", &\"<redacted>\")"),
            "generated Debug must redact `api_key`: {debug_impl}"
        );
        assert!(
            debug_impl.contains(".field(\"token\", &\"<redacted>\")"),
            "generated Debug must redact `token`: {debug_impl}"
        );
        assert!(
            !debug_impl.contains("&self.api_key") && !debug_impl.contains("&self.token"),
            "generated Debug must not reference raw `api_key`/`token` values: {debug_impl}"
        );
    }
}
