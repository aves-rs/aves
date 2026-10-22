use crate::domain::{RoutingBlock, SudoMapping, SudoService, SudoServices};
use crate::syntax::{request_base_name, to_snake_case};

pub(super) fn normalize_qualified_paths(value: &str) -> String {
    let characters = value.chars().collect::<Vec<_>>();
    let mut normalized = String::with_capacity(value.len());
    let mut index = 0;

    while index < characters.len() {
        if characters[index] == ':' {
            let mut second_colon = index + 1;
            while second_colon < characters.len() && characters[second_colon].is_whitespace() {
                second_colon += 1;
            }
            if second_colon < characters.len() && characters[second_colon] == ':' {
                while normalized
                    .chars()
                    .last()
                    .is_some_and(|character| character.is_whitespace())
                {
                    normalized.pop();
                }
                normalized.push_str("::");
                index = second_colon + 1;
                while index < characters.len() && characters[index].is_whitespace() {
                    index += 1;
                }
                continue;
            }
        }

        normalized.push(characters[index]);
        index += 1;
    }

    normalized
}

pub(super) fn render_routing_blocks(config: &SudoServices) -> String {
    let request_type = config.request.as_str();
    let domain = config.target_domain.as_str();
    let services = config.services.as_slice();
    let inbound = config.inbound.as_slice();
    let outbound = config.outbound.as_slice();
    let base = request_base_name(request_type);
    let routing_module = format!("__{base}_generated_routing");
    let request_wrapper = request_type
        .strip_suffix("Payload")
        .map_or_else(|| format!("{request_type}Request"), str::to_string);
    let (run_bounds, constructor_bounds) = render_handler_bounds(inbound, outbound);
    let mut rendered = format!(
        "#[doc = \"Generated handler routing for {domain}.\"]\npub(crate) mod {routing_module} {{\n    use super::*;\n"
    );
    rendered.push_str(&format!(
        "    fn check_authenticated_subject_and_capabilities(_projection: &::aves::AuthProjection) -> ::core::result::Result<(), ()> {{ Ok(()) }}\n\n    pub(crate) struct {base}GeneratedHandler<Root, Broadcast>\n    where\n        Root: Clone + Send + 'static,\n        Broadcast: Into<Root> + Clone + Send + 'static,\n    {{\n        public_requests_rx: ::tokio::sync::mpsc::Receiver<({request_wrapper}, ::aves::AuthProjection)>,\n        actor_tasks: ::tokio::sync::mpsc::Sender<{request_wrapper}>,\n"
    ));
    rendered.push_str(&render_handler_fields(inbound, outbound, services, &request_wrapper));
    rendered.push_str(&render_routing_mappings(inbound, outbound, &request_wrapper));
    rendered.push_str(&render_forward_request(config, &request_wrapper));
    rendered.push_str(&render_forward_payload(request_type, &request_wrapper));
    rendered.push_str(&render_public_request_branch(config, &request_wrapper));
    rendered.push_str(&render_constructor_start(base.as_str(), &request_wrapper));
    rendered.push_str(&render_constructor_fields(outbound, services, &request_wrapper));
    rendered.push_str(&format!("        ) -> Self{constructor_bounds} {{ "));
    if !inbound.is_empty() {
        rendered.push_str("let broadcast_rx = broadcast_tx.subscribe(); ");
    }
    rendered.push_str("Self { public_requests_rx, actor_tasks, ");
    if !inbound.is_empty() {
        rendered.push_str("broadcast_rx, ");
    }
    rendered.push_str("broadcast_tx,\n");
    rendered.push_str(&render_constructor_initializers(outbound, services));
    rendered.push_str(&format!(
        "        }} }}\n\n        pub(crate) async fn run(mut self){run_bounds} {{\n            loop {{\n                ::tokio::select! {{\n"
    ));
    rendered.push_str(&render_select_branches(inbound, outbound, services));
    rendered
        .push_str("                    else => { break; },\n                };\n            }\n        }\n    }\n}\n");
    rendered
}

pub(super) fn render_sudo_signature(mapping: &SudoMapping, error_type: &str) -> String {
    let input = format!("{}Input", mapping.request_variant);
    let output = format!("{}Output", mapping.response_variant);
    let method = to_snake_case(&mapping.request_variant);
    format!("    async fn {method}(&self, input: {input}) -> ::core::result::Result<{output}, {error_type}>;")
}

pub(super) fn render_sudo_impl_method(
    mapping: &SudoMapping,
    request_type: &str,
    request_wrapper: &str,
    error_type: &str,
    transport_error_path: &str,
    target_domain: &str,
) -> String {
    let input = format!("{}Input", mapping.request_variant);
    let output = format!("{}Output", mapping.response_variant);
    let method = to_snake_case(&mapping.request_variant);
    let operation = &mapping.operation;
    format!(
        "    async fn {method}(&self, input: {input}) -> ::core::result::Result<{output}, {error_type}> {{\n        let payload = {request_type}::from(input);\n        let (reply_to, receiver) = ::tokio::sync::oneshot::channel();\n        let request = {request_wrapper} {{ payload, reply_to }};\n        if let Err(error) = self.sender.send(request).await {{\n            return Err({transport_error_path}::ChannelSend {{ target_domain: \"{target_domain}\".to_string(), operation: \"{operation}\".to_string(), cause: error.to_string() }}.into());\n        }}\n        let response = match receiver.await {{\n            Ok(response) => response?,\n            Err(error) => return Err({transport_error_path}::ChannelRecv {{ target_domain: \"{target_domain}\".to_string(), operation: \"{operation}\".to_string(), cause: error.to_string() }}.into()),\n        }};\n        match {output}::try_from(response) {{\n            Ok(output) => Ok(output.into()),\n            Err(cause) => Err({transport_error_path}::ChannelRecv {{ target_domain: \"{target_domain}\".to_string(), operation: \"{operation}\".to_string(), cause: cause.to_string() }}.into()),\n        }}\n    }}"
    )
}

fn render_constructor_start(base: &str, request_wrapper: &str) -> String {
    format!(
        "    impl<Root, Broadcast> {base}GeneratedHandler<Root, Broadcast>\n    where\n        Root: Clone + Send + 'static,\n        Broadcast: Into<Root> + Clone + Send + 'static,\n    {{\n        pub(crate) fn new(\n            public_requests_rx: ::tokio::sync::mpsc::Receiver<({request_wrapper}, ::aves::AuthProjection)>,\n            actor_tasks: ::tokio::sync::mpsc::Sender<{request_wrapper}>,\n            broadcast_tx: ::aves::BroadcastAdapter<Broadcast, Root>,\n"
    )
}

fn render_authorization_check(hook: Option<&str>) -> String {
    hook.map_or_else(
        || "        let _ = check_authenticated_subject_and_capabilities(&projection);\n".to_string(),
        |hook| format!(
            "        if let Err(error) = ({hook})(&request.payload, &projection) {{\n            let _ = request.reply_to.send(Err(error));\n            return;\n        }}\n"
        ),
    )
}

fn render_constructor_fields(outbound: &[RoutingBlock], services: &[SudoService], request_wrapper: &str) -> String {
    let mut fields = String::new();
    for (index, block) in outbound.iter().enumerate() {
        fields.push_str(&format!(
            "            actor_event_rx_{index}: ::tokio::sync::mpsc::Receiver<{}>,\n",
            block.source_type
        ));
    }
    for (index, _) in services.iter().enumerate() {
        fields.push_str(&format!(
            "            sudo_requests_rx_{index}: ::tokio::sync::mpsc::Receiver<{request_wrapper}>,\n"
        ));
    }
    fields
}

fn render_constructor_initializers(outbound: &[RoutingBlock], services: &[SudoService]) -> String {
    let mut fields = String::new();
    for (index, _) in outbound.iter().enumerate() {
        fields.push_str(&format!("            actor_event_rx_{index},\n"));
    }
    for (index, _) in services.iter().enumerate() {
        fields.push_str(&format!("            sudo_requests_rx_{index},\n"));
    }
    fields
}

fn render_select_branches(inbound: &[RoutingBlock], outbound: &[RoutingBlock], services: &[SudoService]) -> String {
    let mut branches = String::from(
        "                    Some(request) = self.public_requests_rx.recv() => { public_request_branch(&self.actor_tasks, request).await; },\n",
    );
    for (index, _) in services.iter().enumerate() {
        branches.push_str(&format!("                    Some(request) = self.sudo_requests_rx_{index}.recv() => {{ let _ = forward_request(&self.actor_tasks, request).await; }},\n"));
    }
    for (index, _) in inbound.iter().enumerate() {
        branches.push_str(&format!("                    Ok(root_event) = self.broadcast_rx.recv() => {{ if let Some(payload) = <__InboundRouter{index} as __InboundRouting{index}>::route(root_event) {{ forward_payload(&self.actor_tasks, payload).await; }} }},\n"));
    }
    for (index, _) in outbound.iter().enumerate() {
        branches.push_str(&format!("                    Some(event) = self.actor_event_rx_{index}.recv() => {{ if let Some(broadcast) = <__OutboundRouter{index} as __OutboundRouting{index}<Broadcast>>::route(event) {{ let _ = self.broadcast_tx.send(broadcast); }} }},\n"));
    }
    branches
}

fn render_inbound_block(block: &RoutingBlock, index: usize, _request_wrapper: &str) -> String {
    let target_is_option = block
        .target_type
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>()
        .starts_with("Option<");
    let route_type = if target_is_option {
        block.target_type.clone()
    } else {
        format!("Option<{}>", block.target_type)
    };
    let arms = block
        .mappings
        .iter()
        .map(|mapping| {
            let source = qualify_pattern(&block.source_type, &mapping.source_pattern);
            let target = qualify_expression(&block.target_type, &mapping.target_expression);
            let guard = mapping
                .guard
                .as_deref()
                .map(|guard| format!(" if {}", normalize_qualified_paths(guard)))
                .unwrap_or_default();
            if target_is_option {
                format!("            {source}{guard} => {target},")
            } else {
                format!("            {source}{guard} => Some(({target})),")
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    let has_irrefutable_mapping = block
        .mappings
        .iter()
        .any(|mapping| is_irrefutable_pattern(&mapping.source_pattern));
    let fallback = if has_irrefutable_mapping {
        String::new()
    } else {
        "            _ => None,\n".to_string()
    };
    format!(
        "    pub(crate) trait __InboundRouting{index} {{\n        type Source;\n        fn route(source: Self::Source) -> {route_type};\n    }}\n    pub(crate) struct __InboundRouter{index};\n    impl __InboundRouting{index} for __InboundRouter{index} {{\n        type Source = {source_type};\n        fn route(source: Self::Source) -> {route_type} {{\n            match source {{\n{arms}\n{fallback}            }}\n        }}\n    }}\n\n",
        source_type = block.source_type,
        route_type = route_type,
    )
}

fn render_routing_mappings(inbound: &[RoutingBlock], outbound: &[RoutingBlock], request_wrapper: &str) -> String {
    let mut mappings = String::new();
    for (index, block) in inbound.iter().enumerate() {
        mappings.push_str(&render_inbound_block(block, index, request_wrapper));
    }
    for (index, block) in outbound.iter().enumerate() {
        mappings.push_str(&render_outbound_block(block, index));
    }
    mappings
}

fn render_forward_request(config: &SudoServices, request_wrapper: &str) -> String {
    let domain = config.target_domain.as_str();
    let error_type = config.error.as_str();
    let transport_error_path = config.transport_error_path.as_str();
    let mut rendered = format!(
        "    async fn forward_request(\n        actor_tasks: &::tokio::sync::mpsc::Sender<{request_wrapper}>,\n        request: {request_wrapper},\n    ) -> ::core::result::Result<(), {error_type}> {{\n        let caller_reply = request.reply_to;\n        let (actor_reply, actor_response) = ::tokio::sync::oneshot::channel();\n"
    );
    if let Some(timeout) = &config.response_timeout {
        rendered.push_str(&format!(
            "        let side_effecting = ({})(&request.payload);\n",
            timeout.side_effecting_predicate
        ));
    }
    rendered.push_str(&format!(
        "        actor_tasks.send({request_wrapper} {{ payload: request.payload, reply_to: actor_reply }}).await\n            .map_err(|error| {transport_error_path}::ChannelSend {{ target_domain: \"{domain}\".to_string(), operation: \"actor_request\".to_string(), cause: error.to_string() }}.into())?;\n"
    ));
    rendered.push_str(&render_response_wait(config));
    rendered.push_str(&format!(
        "        caller_reply.send(response)\n            .map_err(|_| {transport_error_path}::ChannelSend {{ target_domain: \"{domain}\".to_string(), operation: \"handler.inbound_response\".to_string(), cause: \"caller reply receiver dropped\".to_string() }}.into())?;\n        Ok(())\n    }}\n\n"
    ));
    rendered
}

fn render_forward_payload(request_type: &str, request_wrapper: &str) -> String {
    format!(
        "    async fn forward_payload(\n        actor_tasks: &::tokio::sync::mpsc::Sender<{request_wrapper}>,\n        payload: {request_type},\n    ) {{\n        let (reply_to, _reply) = ::tokio::sync::oneshot::channel();\n        let _ = actor_tasks.send({request_wrapper} {{ payload, reply_to }}).await;\n    }}\n\n"
    )
}

fn render_public_request_branch(config: &SudoServices, request_wrapper: &str) -> String {
    let authorization_check = render_authorization_check(config.authorization_hook.as_deref());
    format!(
        "    async fn public_request_branch(\n        actor_tasks: &::tokio::sync::mpsc::Sender<{request_wrapper}>,\n        request: ({request_wrapper}, ::aves::AuthProjection),\n    ) {{\n        let (request, projection) = request;\n{authorization_check}        let _ = forward_request(actor_tasks, request).await;\n    }}\n\n"
    )
}

fn render_handler_bounds(inbound: &[RoutingBlock], outbound: &[RoutingBlock]) -> (String, String) {
    let inbound_bounds = inbound
        .iter()
        .enumerate()
        .map(|(index, _)| format!("            __InboundRouter{index}: __InboundRouting{index}<Source = Root>,\n"))
        .collect::<String>();
    let outbound_bounds = outbound
        .iter()
        .enumerate()
        .map(|(index, _)| format!("        __OutboundRouter{index}: __OutboundRouting{index}<Broadcast>,\n"))
        .collect::<String>();
    let run_bounds = if inbound.is_empty() && outbound.is_empty() {
        String::new()
    } else {
        format!("\n        where\n{inbound_bounds}{outbound_bounds}")
    };
    let constructor_bounds = if inbound.is_empty() {
        String::new()
    } else {
        format!(" where\n{inbound_bounds}")
    };
    (run_bounds, constructor_bounds)
}

fn render_handler_fields(
    inbound: &[RoutingBlock],
    outbound: &[RoutingBlock],
    services: &[SudoService],
    request_wrapper: &str,
) -> String {
    let mut fields = String::new();
    if !inbound.is_empty() {
        fields.push_str("        broadcast_rx: ::tokio::sync::broadcast::Receiver<Root>,\n");
    }
    fields.push_str("        broadcast_tx: ::aves::BroadcastAdapter<Broadcast, Root>,\n");
    for (index, block) in outbound.iter().enumerate() {
        fields.push_str(&format!(
            "        actor_event_rx_{index}: ::tokio::sync::mpsc::Receiver<{}>,\n",
            block.source_type
        ));
    }
    for (index, _) in services.iter().enumerate() {
        fields.push_str(&format!(
            "        sudo_requests_rx_{index}: ::tokio::sync::mpsc::Receiver<{request_wrapper}>,\n"
        ));
    }
    fields.push_str("    }\n\n");
    fields
}

fn render_response_wait(config: &SudoServices) -> String {
    let domain = config.target_domain.as_str();
    let transport_error_path = config.transport_error_path.as_str();
    match &config.response_timeout {
        None => format!(
            "        let response = actor_response\n            .await\n            .map_err(|error| {transport_error_path}::ChannelRecv {{ target_domain: \"{domain}\".to_string(), operation: \"handler.actor_response\".to_string(), cause: error.to_string() }}.into())?;\n"
        ),
        Some(timeout) => format!(
            "        let response = if side_effecting {{\n            actor_response\n                .await\n                .map_err(|error| {transport_error_path}::ChannelRecv {{ target_domain: \"{domain}\".to_string(), operation: \"handler.actor_response\".to_string(), cause: error.to_string() }}.into())?\n        }} else {{\n            ::tokio::time::timeout({duration}, actor_response)\n                .await\n                .map_err(|_| {transport_error_path}::OneshotReplyTimeout {{ target_domain: \"{domain}\".to_string(), operation: \"handler.actor_response\".to_string(), timeout: format!(\"{{:?}}\", {duration}) }}.into())?\n                .map_err(|error| {transport_error_path}::OneshotReplyMissing {{ target_domain: \"{domain}\".to_string(), operation: \"handler.actor_response\".to_string(), cause: error.to_string() }}.into())?\n        }};\n",
            duration = timeout.duration,
        ),
    }
}

fn render_outbound_block(block: &RoutingBlock, index: usize) -> String {
    let arms = block
        .mappings
        .iter()
        .map(|mapping| {
            let source = qualify_pattern(&block.source_type, &mapping.source_pattern);
            let target = qualify_expression(&block.target_type, &mapping.target_expression);
            let guard = mapping
                .guard
                .as_deref()
                .map(|guard| format!(" if {}", normalize_qualified_paths(guard)))
                .unwrap_or_default();
            format!("            {source}{guard} => Some(({target})),")
        })
        .collect::<Vec<_>>()
        .join("\n");
    let has_irrefutable_mapping = block
        .mappings
        .iter()
        .any(|mapping| is_irrefutable_pattern(&mapping.source_pattern));
    let fallback = if has_irrefutable_mapping {
        String::new()
    } else {
        "            _ => None,\n".to_string()
    };
    format!(
        "    pub(crate) trait __OutboundRouting{index}<Target> {{\n        fn route(source: {source_type}) -> Option<Target>;\n    }}\n    pub(crate) struct __OutboundRouter{index};\n    impl __OutboundRouting{index}<{target_type}> for __OutboundRouter{index} {{\n        fn route(source: {source_type}) -> Option<{target_type}> {{\n            match source {{\n{arms}\n{fallback}            }}\n        }}\n    }}\n\n",
        source_type = block.source_type,
        target_type = block.target_type,
    )
}

fn qualify_pattern(type_name: &str, pattern: &str) -> String {
    let normalized = without_whitespace(pattern);
    if normalized.contains("::") || normalized.starts_with('_') || !starts_with_uppercase_ident(&normalized) {
        normalize_qualified_paths(pattern)
    } else {
        format!("{type_name}::{pattern}")
    }
}

fn is_irrefutable_pattern(pattern: &str) -> bool {
    let mut tokens = pattern.split_whitespace();
    let Some(identifier) = tokens.next() else {
        return false;
    };
    tokens.next().is_none() && starts_with_lowercase_ident(identifier)
}

fn qualify_expression(type_name: &str, expression: &str) -> String {
    let normalized = without_whitespace(expression);
    if normalized.contains("::") || !starts_with_uppercase_ident(&normalized) {
        normalize_qualified_paths(expression)
    } else {
        format!("{type_name}::{expression}")
    }
}

fn without_whitespace(value: &str) -> String {
    value
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect()
}

fn starts_with_uppercase_ident(value: &str) -> bool {
    value
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_uppercase())
}

fn starts_with_lowercase_ident(value: &str) -> bool {
    value
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_lowercase() || character == '_')
}

pub(super) fn explicit_transport_error_conversions(
    rendered: String,
    error_type: &str,
    transport_error_path: &str,
) -> String {
    let constructor = format!("::core::convert::Into::<{error_type}>::into({transport_error_path}::");
    let mut rendered = rendered
        .replace(
            &format!("{transport_error_path}::ChannelSend {{"),
            &format!("{constructor}ChannelSend {{"),
        )
        .replace(
            &format!("{transport_error_path}::ChannelRecv {{"),
            &format!("{constructor}ChannelRecv {{"),
        )
        .replace(
            &format!("{transport_error_path}::OneshotReplyTimeout {{"),
            &format!("{constructor}OneshotReplyTimeout {{"),
        )
        .replace(
            &format!("{transport_error_path}::OneshotReplyMissing {{"),
            &format!("{constructor}OneshotReplyMissing {{"),
        )
        .replace("cause: error.to_string() }.into()", "cause: error.to_string() })")
        .replace("cause: cause.to_string() }.into()", "cause: cause.to_string() })")
        .replace(
            "cause: \"caller reply receiver dropped\".to_string() }.into()",
            "cause: \"caller reply receiver dropped\".to_string() })",
        );

    let timeout_constructor = format!("{constructor}OneshotReplyTimeout {{");
    if let Some(timeout_start) = rendered.find(&timeout_constructor)
        && let Some(timeout_close) = rendered[timeout_start..].find(" }.into()")
    {
        let close_start = timeout_start + timeout_close;
        rendered.replace_range(close_start..close_start + " }.into()".len(), " })");
    }

    rendered
}
