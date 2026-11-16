//! Discuss-owned MCP tool collation and the service shared by ACP and HTTP transports.

use std::str::FromStr;
use std::{collections::HashSet, error::Error, fmt::Debug, future::Future, pin::Pin, sync::Arc};

use agent_client_protocol::v1::schema::McpServer;
use async_trait::async_trait;
use axum08::http::request::Parts;
use base64::Engine;
use rmcp::ServiceExt;
use rmcp::handler::server::ServerHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, ErrorData, Implementation, JsonObject,
    ListToolsResult, PaginatedRequestParams, ProtocolVersion, ServerCapabilities, ServerConfig, Tool,
};
use rmcp::service::{RequestContext, RoleServer};
use schemars::Schema;
use serde_json::Value;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use aves::auth::Jwt;
use aves::bus::{BusRpcContext, DomainPublication, RequestOperationDescriptor};

use super::actor::Actor;
use super::boot::{DiscussMcpDomain, DiscussMcpService};
use super::library::AgentId;
use super::mcp_direct::McpAcpTransport;
use super::messages::DiscussResponse;
use super::storage::{
    DiscussPersistence, McpAssignmentScope, delete_custom_mcp, delete_mcp_assignment, delete_mcp_assignments_by_name,
    delete_mcp_assignments_by_scope, insert_mcp_assignment, list_custom_mcps, list_mcp_assignments_by_scope,
    load_custom_mcp, load_discuss_session, upsert_custom_mcp,
};
use super::types::{
    CustomMcpEntry, CustomMcpScope, DiscussError, DiscussSessionId, DiscussSessionSetup, SensitiveMcpDescriptor,
    SessionLaunchConfig,
};

fn mcp_transport(server: &McpServer) -> Result<(&str, &'static str), DiscussError> {
    match server {
        McpServer::Stdio(server) => Ok((&server.name, "stdio")),
        McpServer::Http(server) => Ok((&server.name, "http")),
        _ => Err(DiscussError::Validation {
            message: "unsupported custom MCP transport".to_owned(),
        }),
    }
}

fn redact_custom_mcp(mut server: McpServer) -> Result<McpServer, DiscussError> {
    match &mut server {
        McpServer::Stdio(config) => {
            config.meta = None;
            for variable in &mut config.env {
                variable.value = "<redacted>".to_owned();
                variable.meta = None;
            }
        }
        McpServer::Http(config) => {
            config.meta = None;
            for header in &mut config.headers {
                header.value = "<redacted>".to_owned();
                header.meta = None;
            }
        }
        _ => {
            return Err(DiscussError::Validation {
                message: "unsupported custom MCP transport".to_owned(),
            });
        }
    }
    Ok(server)
}

fn safe_mcp_name(name: &str) -> Result<(), DiscussError> {
    AgentId::new(name)
        .map(|_| ())
        .map_err(|_| DiscussError::Validation {
            message: "invalid custom MCP name".to_owned(),
        })
}

fn assignment_scope(scope: CustomMcpScope) -> McpAssignmentScope {
    match scope {
        CustomMcpScope::Provider => McpAssignmentScope::Provider,
        CustomMcpScope::Team => McpAssignmentScope::Team,
        CustomMcpScope::Session => McpAssignmentScope::Session,
    }
}

/// Remove assignments for a deleted Team, Provider, or session scope.
pub(super) async fn clear_mcp_scope(
    conn: &turso::Connection,
    scope: CustomMcpScope,
    id: &str,
) -> Result<(), DiscussError> {
    delete_mcp_assignments_by_scope(conn, assignment_scope(scope), id).await
}

impl Actor {
    pub(super) async fn handle_list_custom_mcps(&self) -> Result<DiscussResponse, DiscussError> {
        let conn = self.persistence.acquire_connection().await?;
        let mut entries = Vec::new();
        for row in list_custom_mcps(&conn).await? {
            let (_, kind) = mcp_transport(&row.descriptor)?;
            entries.push(CustomMcpEntry {
                name: row.name,
                transport_kind: kind.to_owned(),
                updated_at_ms: row.updated_at_ms,
                descriptor: SensitiveMcpDescriptor(redact_custom_mcp(row.descriptor)?),
            });
        }
        Ok(DiscussResponse::ListCustomMcps { entries })
    }

    pub(super) async fn handle_write_custom_mcp(
        &self,
        name: &str,
        descriptor: SensitiveMcpDescriptor,
        create: bool,
    ) -> Result<DiscussResponse, DiscussError> {
        safe_mcp_name(name)?;
        if (self.mcp_domains)()?
            .iter()
            .any(|domain| domain.server_name == name)
        {
            return Err(DiscussError::ReservedMcpName);
        }
        if mcp_transport(&descriptor.0)?.0 != name {
            return Err(DiscussError::Validation {
                message: "custom MCP descriptor name mismatch".to_owned(),
            });
        }
        let conn = self.persistence.acquire_connection().await?;
        let exists = load_custom_mcp(&conn, name).await?.is_some();
        if create && exists {
            return Err(DiscussError::DuplicateMcpServerName);
        }
        if !create && !exists {
            return Err(DiscussError::Validation {
                message: "custom MCP does not exist".to_owned(),
            });
        }
        upsert_custom_mcp(&conn, name, &descriptor.0, chrono::Utc::now().timestamp_millis()).await?;
        Ok(DiscussResponse::Success)
    }

    pub(super) async fn handle_delete_custom_mcp(&self, name: &str) -> Result<DiscussResponse, DiscussError> {
        safe_mcp_name(name)?;
        let mut conn = self.persistence.acquire_connection().await?;
        let tx = conn.transaction().await?;
        delete_mcp_assignments_by_name(&tx, name).await?;
        delete_custom_mcp(&tx, name).await?;
        tx.commit().await?;
        Ok(DiscussResponse::Success)
    }

    pub(super) async fn handle_mcp_assignment(
        &self,
        scope: CustomMcpScope,
        id: &str,
        name: &str,
        assign: bool,
    ) -> Result<DiscussResponse, DiscussError> {
        safe_mcp_name(name)?;
        let conn = self.persistence.acquire_connection().await?;
        let mut scope_id = id.to_owned();
        match scope {
            CustomMcpScope::Provider if !self.agents.contains_key(id) => {
                return Err(DiscussError::UnknownAgent {
                    agent_key: id.to_owned(),
                });
            }
            CustomMcpScope::Team => {
                let team = AgentId::new(id).map_err(|_| DiscussError::Validation {
                    message: "invalid Team identity".to_owned(),
                })?;
                self.resolve_team(&team).await?;
            }
            CustomMcpScope::Session => {
                let session = DiscussSessionId::from_str(id).map_err(|_| DiscussError::Validation {
                    message: "invalid session identity".to_owned(),
                })?;
                load_discuss_session(&conn, &session.to_string())
                    .await?
                    .ok_or(DiscussError::SessionNotFound {
                        discuss_session_id: session,
                    })?;
                scope_id = session.to_string();
            }
            _ => {}
        }
        if assign {
            if load_custom_mcp(&conn, name).await?.is_none() {
                return Err(DiscussError::Validation {
                    message: "custom MCP does not exist".to_owned(),
                });
            }
            insert_mcp_assignment(&conn, assignment_scope(scope), &scope_id, name).await?;
        } else {
            delete_mcp_assignment(&conn, assignment_scope(scope), &scope_id, name).await?;
        }
        Ok(DiscussResponse::Success)
    }
}

/// Compose a transient launch copy without modifying its persisted base.
pub(super) async fn composed_launch_config(
    persistence: &DiscussPersistence,
    domains: fn() -> Result<Vec<DiscussMcpDomain>, DiscussError>,
    provider: &str,
    setup: Option<&DiscussSessionSetup>,
    session_id: DiscussSessionId,
    base: Option<&SessionLaunchConfig>,
) -> Result<Option<SessionLaunchConfig>, DiscussError> {
    let team = match setup {
        Some(DiscussSessionSetup::Team { leader_id } | DiscussSessionSetup::TeamAtRoot { leader_id }) => {
            Some(leader_id.as_str())
        }
        _ => None,
    };
    let mut config = base.cloned().unwrap_or_default();
    config.mcp_servers =
        compose_custom_mcps(persistence, domains, provider, team, session_id, &config.mcp_servers).await?;
    Ok((base.is_some() || !config.mcp_servers.is_empty()).then_some(config))
}

pub(super) async fn compose_custom_mcps(
    persistence: &DiscussPersistence,
    domains: fn() -> Result<Vec<DiscussMcpDomain>, DiscussError>,
    provider: &str,
    team: Option<&str>,
    session_id: DiscussSessionId,
    base: &[McpServer],
) -> Result<Vec<McpServer>, DiscussError> {
    let conn = persistence.acquire_connection().await?;
    let reserved: HashSet<_> = domains()?
        .into_iter()
        .map(|domain| domain.server_name)
        .collect();
    let mut servers = Vec::new();
    let session = session_id.to_string();
    for (scope, id) in [
        (McpAssignmentScope::Provider, Some(provider)),
        (McpAssignmentScope::Team, team),
        (McpAssignmentScope::Session, Some(session.as_str())),
    ] {
        if let Some(id) = id {
            for name in list_mcp_assignments_by_scope(&conn, scope, id).await? {
                let row = load_custom_mcp(&conn, &name)
                    .await?
                    .ok_or_else(|| DiscussError::Persistence {
                        code: "db_query".to_owned(),
                        message: "custom MCP assignment has no descriptor".to_owned(),
                    })?;
                servers.push(row.descriptor);
            }
        }
    }
    servers.extend_from_slice(base);
    let mut names = HashSet::new();
    for server in &servers {
        let name = match server {
            McpServer::Stdio(server) => &server.name,
            McpServer::Http(server) => &server.name,
            McpServer::Sse(server) => &server.name,
            McpServer::Acp(server) => &server.name,
            _ => {
                return Err(DiscussError::Validation {
                    message: "unsupported custom MCP descriptor".to_owned(),
                });
            }
        };
        if reserved.contains(name.as_str()) {
            return Err(DiscussError::ReservedMcpName);
        }
        if !names.insert(name) {
            return Err(DiscussError::DuplicateMcpServerName);
        }
    }
    Ok(servers)
}

pub(super) type DetailProjection<E> = fn(&E) -> Value;

/// A collated tool retains the SDK payload and its generated bus descriptor.
struct McpToolSpec<E: 'static> {
    tool: Tool,
    descriptor: &'static RequestOperationDescriptor<BusRpcContext, E>,
    image_response: bool,
}

pub(super) struct McpSpec<E: 'static> {
    tools: Vec<McpToolSpec<E>>,
}

impl<E: 'static> McpSpec<E> {
    fn tool(&self, name: &str) -> Option<&McpToolSpec<E>> {
        self.tools
            .iter()
            .find(|tool| tool.tool.name.as_ref() == name)
    }
}

/// Collated tools for one published application domain.
pub(super) struct McpDomainSpec<E: 'static> {
    pub(super) server_name: &'static str,
    pub(super) route_slug: &'static str,
    pub(super) spec: Arc<McpSpec<E>>,
}

#[derive(Clone)]
pub(super) struct HttpMcpBearer(pub(super) Jwt);

pub(super) fn collate_mcp_specs<E: 'static>(
    publications: &[DomainPublication<BusRpcContext, E>],
) -> Result<Vec<McpDomainSpec<E>>, McpCollationError> {
    publications
        .iter()
        .map(|domain| {
            let mut tools = Vec::new();
            let operations: &'static [_] = domain.operations;
            for descriptor in operations
                .iter()
                .filter(|descriptor| descriptor.mcp_eligible)
            {
                if tools
                    .iter()
                    .any(|tool: &McpToolSpec<E>| tool.descriptor.operation == descriptor.operation)
                {
                    return Err(McpCollationError::DuplicateQualifiedName {
                        qualified_operation: descriptor.qualified_operation.to_owned(),
                    });
                }
                tools.push(build_mcp_tool(descriptor)?);
            }
            Ok(McpDomainSpec {
                server_name: domain.server_name,
                route_slug: domain.route_slug,
                spec: Arc::new(McpSpec { tools }),
            })
        })
        .collect()
}

fn build_mcp_tool<E: 'static>(
    descriptor: &'static RequestOperationDescriptor<BusRpcContext, E>,
) -> Result<McpToolSpec<E>, McpCollationError> {
    let input_schema = schema_object((descriptor.input_schema)())
        .filter(|schema| schema.get("type").and_then(Value::as_str) == Some("object"))
        .ok_or_else(|| McpCollationError::MissingInputSchema {
            qualified_operation: descriptor.qualified_operation.to_owned(),
        })?;
    let output_schema =
        schema_object((descriptor.output_schema)()).ok_or_else(|| McpCollationError::MissingOutputSchema {
            qualified_operation: descriptor.qualified_operation.to_owned(),
        })?;
    let image_response = is_image_output_schema(&output_schema);
    let mut tool = Tool::new(descriptor.operation, descriptor.operation, input_schema);
    if !image_response && output_schema.get("type").and_then(Value::as_str) == Some("object") {
        tool.output_schema = Some(output_schema);
    }
    Ok(McpToolSpec {
        tool,
        descriptor,
        image_response,
    })
}

/// Builds native Discuss MCP services from generated domain publications.
pub fn discuss_mcp_domains<E>(
    publications: &[DomainPublication<BusRpcContext, E>],
    error_details: fn(&E) -> Value,
    application_name: &'static str,
) -> Result<Vec<DiscussMcpDomain>, DiscussError>
where
    E: Error + Send + Sync + 'static,
{
    let domains = collate_mcp_specs(publications)?;
    Ok(domains
        .into_iter()
        .map(|domain| DiscussMcpDomain {
            server_name: domain.server_name,
            service: Arc::new(NativeDiscussMcpService {
                spec: domain.spec,
                error_details,
                application_name,
            }),
        })
        .collect())
}

struct NativeDiscussMcpService<E: 'static> {
    spec: Arc<McpSpec<E>>,
    error_details: DetailProjection<E>,
    application_name: &'static str,
}

#[async_trait]
impl<E: 'static> DiscussMcpService for NativeDiscussMcpService<E>
where
    E: Error + Send + Sync + 'static,
{
    async fn start(
        &self,
        transport: McpAcpTransport,
        cancellation: CancellationToken,
        context: BusRpcContext,
        worker_jwt: Jwt,
    ) -> Option<Pin<Box<dyn Future<Output = ()> + Send>>> {
        let service = ApplicationMcpService::new(
            Arc::clone(&self.spec),
            context,
            worker_jwt,
            self.error_details,
            self.application_name,
        );
        let running = service.serve_with_ct(transport, cancellation).await.ok()?;
        Some(Box::pin(async move {
            let _ = running.waiting().await;
        }))
    }
}

/// Shared rmcp service for a single collated domain.
pub(super) struct ApplicationMcpService<E: 'static> {
    spec: Arc<McpSpec<E>>,
    context: BusRpcContext,
    worker_jwt: Jwt,
    error_details: DetailProjection<E>,
    application_name: &'static str,
}

impl<E: 'static> ApplicationMcpService<E> {
    pub(super) fn new(
        spec: Arc<McpSpec<E>>,
        context: BusRpcContext,
        worker_jwt: Jwt,
        error_details: DetailProjection<E>,
        application_name: &'static str,
    ) -> Self {
        Self {
            spec,
            context,
            worker_jwt,
            error_details,
            application_name,
        }
    }
}

impl<E: 'static> ServerHandler for ApplicationMcpService<E>
where
    E: Error + Send + Sync + 'static,
{
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_protocol_version(ProtocolVersion::V_2025_03_26)
            .with_server_info(Implementation::new(self.application_name, env!("CARGO_PKG_VERSION")))
            .with_instructions("Application tools")
    }

    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let tools: Vec<_> = self
            .spec
            .tools
            .iter()
            .map(|tool| tool.tool.clone())
            .collect();
        tracing::debug!(
            route = "tools/list",
            parameters = %truncated_debug(&request),
            tool_count = tools.len(),
            "MCP route request"
        );
        Ok(ListToolsResult::with_all_items(tools))
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.spec.tool(name).map(|tool| tool.tool.clone())
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        request_context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let tool_name = request.name.as_ref();
        let tool = resolve_mcp_tool(&self.spec, tool_name)?;
        let arguments = Value::Object(request.arguments.unwrap_or_default());
        tracing::debug!(route = tool_name, parameters = %arguments, "MCP route request");
        let bearer = request_context
            .extensions
            .get::<Parts>()
            .and_then(|parts| parts.extensions.get::<HttpMcpBearer>());
        let context = self
            .context
            .with_jwt(select_request_jwt(bearer, &self.worker_jwt));
        match dispatch_selected_tool(tool, context, arguments).await {
            Ok(value) => {
                tracing::debug!(result = %truncated_debug(&value), "MCP route success");
                Ok(mcp_success_response(tool.image_response, value).into())
            }
            Err(error) => {
                tracing::error!(error = %error, "MCP route error");
                Ok(mcp_tool_error(&error, self.error_details).into())
            }
        }
    }
}

fn select_request_jwt(bearer: Option<&HttpMcpBearer>, worker_jwt: &Jwt) -> Jwt {
    bearer
        .map(|bearer| bearer.0.clone())
        .unwrap_or_else(|| worker_jwt.clone())
}

async fn dispatch_selected_tool<E: 'static>(
    tool: &McpToolSpec<E>,
    context: BusRpcContext,
    arguments: Value,
) -> Result<Value, E> {
    (tool.descriptor.dispatch)(context, arguments).await
}

fn resolve_mcp_tool<'spec, E: 'static>(
    spec: &'spec McpSpec<E>,
    name: &str,
) -> Result<&'spec McpToolSpec<E>, ErrorData> {
    spec.tool(name).ok_or_else(|| {
        ErrorData::invalid_params(
            format!("unknown MCP tool: {name}"),
            Some(serde_json::json!({ "tool": name })),
        )
    })
}

fn mcp_success_response(image_response: bool, value: Value) -> CallToolResult {
    if image_response {
        image_tool_result(value)
    } else if !value.is_object() {
        CallToolResult::success(vec![ContentBlock::text(value.to_string())])
    } else {
        CallToolResult::structured(value)
    }
}

fn mcp_tool_error<E: Error>(error: &E, error_details: DetailProjection<E>) -> CallToolResult {
    let details = error_details(error);
    let content = serde_json::json!({ "error": details, "message": error.to_string() }).to_string();
    CallToolResult::error(vec![ContentBlock::text(content)])
}

/// Publishes an object schema, resolving only local raw JSON pointers with cycle protection.
fn schema_object(schema: Schema) -> Option<Arc<JsonObject>> {
    let value = schema.as_value();
    let mut object = value.as_object()?.clone();
    let mut resolved = value;
    let mut visited = HashSet::new();
    while resolved.get("type").is_none() {
        let Some(reference) = resolved.get("$ref").and_then(Value::as_str) else {
            break;
        };
        if !visited.insert(reference) {
            break;
        }
        let Some(target) = reference
            .strip_prefix('#')
            .and_then(|pointer| value.pointer(pointer))
        else {
            break;
        };
        resolved = target;
    }
    if let Some(schema_type) = resolved.get("type") {
        object.insert("type".to_owned(), schema_type.clone());
    }
    Some(Arc::new(object))
}

/// Converts the renderer contract to native MCP image content without decoding PNG bytes.
fn image_tool_result(value: Value) -> CallToolResult {
    let Value::Object(mut fields) = value else {
        return CallToolResult::error(vec![ContentBlock::text("image tool returned an invalid response")]);
    };
    let Some(Value::Array(bytes)) = fields.remove("image") else {
        return CallToolResult::error(vec![ContentBlock::text("image tool returned no PNG bytes")]);
    };
    let image = bytes
        .into_iter()
        .map(|byte| byte.as_u64().and_then(|byte| u8::try_from(byte).ok()))
        .collect::<Option<Vec<_>>>();
    let Some(image) = image else {
        return CallToolResult::error(vec![ContentBlock::text("image tool returned invalid PNG bytes")]);
    };
    let mut content = vec![ContentBlock::image(
        base64::engine::general_purpose::STANDARD.encode(image),
        "image/png",
    )];
    if let Some(Value::String(text)) = fields.remove("content") {
        content.push(ContentBlock::text(text));
    }
    let mut result = CallToolResult::success(content);
    if let Some(data) = fields.remove("data").filter(|data| !data.is_null()) {
        result.structured_content = Some(data);
    }
    result
}

fn is_image_output_schema(schema: &JsonObject) -> bool {
    let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
        return false;
    };
    let image_required = schema
        .get("required")
        .and_then(Value::as_array)
        .is_some_and(|required| required.iter().any(|field| field.as_str() == Some("image")));
    image_required
        && properties.contains_key("image")
        && properties.contains_key("content")
        && properties.contains_key("data")
}

fn truncated_debug(value: &(impl Debug + ?Sized)) -> String {
    let text = format!("{value:?}");
    let mut chars = text.chars();
    let mut result: String = chars.by_ref().take(480).collect();
    if chars.next().is_some() {
        result.push_str("...");
    }
    result
}

/// Structural failures while collating generated MCP operation metadata.
#[derive(Debug, Error)]
pub(super) enum McpCollationError {
    /// Two eligible operations share one domain-local tool name.
    #[error("duplicate qualified MCP tool name: {qualified_operation}")]
    DuplicateQualifiedName { qualified_operation: String },
    /// An eligible operation's input schema cannot be published as a JSON object.
    #[error("missing input schema for MCP tool: {qualified_operation}")]
    MissingInputSchema { qualified_operation: String },
    /// An eligible operation's output schema cannot be published as a JSON object.
    #[error("missing output schema for MCP tool: {qualified_operation}")]
    MissingOutputSchema { qualified_operation: String },
}

impl From<McpCollationError> for DiscussError {
    fn from(error: McpCollationError) -> Self {
        Self::McpSetup {
            message: error.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use agent_client_protocol::v1::schema::{EnvVariable, HttpHeader, McpServerHttp, McpServerStdio};
    use serde_json::json;

    use aves::bus::{OperationDispatchFuture, RequestOperationKind};

    use super::*;

    #[test]
    fn custom_descriptor_debug_redacts_secrets() {
        let stdio = McpServer::Stdio(
            McpServerStdio::new("example", "server")
                .args(vec!["--flag".to_owned()])
                .env(vec![EnvVariable::new("TOKEN", "secret-value")]),
        );
        let http = McpServer::Http(
            McpServerHttp::new("remote", "https://example.test")
                .headers(vec![HttpHeader::new("Authorization", "header-secret")]),
        );
        let stdio_public = serde_json::to_value(redact_custom_mcp(stdio.clone()).unwrap()).unwrap();
        let http_public = serde_json::to_value(redact_custom_mcp(http.clone()).unwrap()).unwrap();
        assert_eq!(stdio_public["command"], "server");
        assert_eq!(stdio_public["args"][0], "--flag");
        assert_eq!(
            stdio_public["env"][0],
            serde_json::json!({"name":"TOKEN", "value":"<redacted>"})
        );
        assert_eq!(http_public["url"], "https://example.test");
        assert_eq!(
            http_public["headers"][0],
            serde_json::json!({"name":"Authorization", "value":"<redacted>"})
        );
        assert!(
            serde_json::to_string(&stdio)
                .unwrap()
                .contains("secret-value")
        );
        assert!(
            serde_json::to_string(&http)
                .unwrap()
                .contains("header-secret")
        );
        assert!(!format!("{:?}", SensitiveMcpDescriptor(stdio)).contains("secret-value"));
        assert!(safe_mcp_name("../unsafe").is_err());
        assert!(safe_mcp_name("safe-name").is_ok());
    }

    static DISPATCHES: AtomicUsize = AtomicUsize::new(0);

    fn input_schema() -> Schema {
        json!({
            "type": "object",
            "properties": { "value": { "type": "string" } },
            "required": ["value"]
        })
        .try_into()
        .expect("object input schema")
    }

    fn object_output_schema() -> Schema {
        json!({
            "type": "object",
            "properties": { "ok": { "type": "boolean" } },
            "required": ["ok"]
        })
        .try_into()
        .expect("object output schema")
    }

    fn string_schema() -> Schema {
        schemars::schema_for!(String)
    }

    fn array_schema() -> Schema {
        schemars::schema_for!(Vec<String>)
    }

    fn image_output_schema() -> Schema {
        json!({
            "type": "object",
            "properties": {
                "image": { "type": "array", "items": { "type": "integer", "minimum": 0, "maximum": 255 } },
                "content": { "type": "string" },
                "data": { "type": "object" }
            },
            "required": ["image"]
        })
        .try_into()
        .expect("image output schema")
    }

    fn invalid_schema() -> Schema {
        false.into()
    }

    fn dispatch(_: BusRpcContext, arguments: Value) -> OperationDispatchFuture<std::io::Error> {
        DISPATCHES.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if arguments.get("fail").and_then(Value::as_bool) == Some(true) {
                Err(std::io::Error::other("typed descriptor failure"))
            } else {
                Ok(json!({ "ok": true }))
            }
        })
    }

    fn io_error_details(error: &std::io::Error) -> Value {
        json!({ "kind": format!("{:?}", error.kind()) })
    }

    static SYNTHETIC_OPERATIONS: [RequestOperationDescriptor<BusRpcContext, std::io::Error>; 1] =
        [RequestOperationDescriptor {
            domain: "synthetic",
            operation: "read",
            qualified_operation: "synthetic.read",
            canonical_variant: "Synthetic::Read",
            kind: RequestOperationKind::Query,
            mcp_eligible: true,
            input_type: "Input",
            response_type: "ObjectOutput",
            response_variant: "Read",
            input_schema,
            output_schema: object_output_schema,
            dispatch,
        }];

    static DUPLICATE_OPERATIONS: [RequestOperationDescriptor<BusRpcContext, std::io::Error>; 2] = [
        RequestOperationDescriptor {
            domain: "synthetic",
            operation: "read",
            qualified_operation: "synthetic.read",
            canonical_variant: "Synthetic::Read",
            kind: RequestOperationKind::Query,
            mcp_eligible: true,
            input_type: "Input",
            response_type: "ObjectOutput",
            response_variant: "Read",
            input_schema,
            output_schema: object_output_schema,
            dispatch,
        },
        RequestOperationDescriptor {
            domain: "synthetic",
            operation: "read",
            qualified_operation: "synthetic.other_read",
            canonical_variant: "Synthetic::OtherRead",
            kind: RequestOperationKind::Query,
            mcp_eligible: true,
            input_type: "Input",
            response_type: "ObjectOutput",
            response_variant: "OtherRead",
            input_schema,
            output_schema: object_output_schema,
            dispatch,
        },
    ];

    static INELIGIBLE_OPERATIONS: [RequestOperationDescriptor<BusRpcContext, std::io::Error>; 1] =
        [RequestOperationDescriptor {
            domain: "synthetic",
            operation: "hidden",
            qualified_operation: "synthetic.hidden",
            canonical_variant: "Synthetic::Hidden",
            kind: RequestOperationKind::Query,
            mcp_eligible: false,
            input_type: "Input",
            response_type: "ObjectOutput",
            response_variant: "Hidden",
            input_schema,
            output_schema: object_output_schema,
            dispatch,
        }];

    static DUPLICATE_PUBLICATION: [DomainPublication<BusRpcContext, std::io::Error>; 1] = [DomainPublication {
        server_name: "Duplicate",
        route_slug: "duplicate",
        operations: &DUPLICATE_OPERATIONS,
    }];

    static INELIGIBLE_PUBLICATION: [DomainPublication<BusRpcContext, std::io::Error>; 1] = [DomainPublication {
        server_name: "Ineligible",
        route_slug: "ineligible",
        operations: &INELIGIBLE_OPERATIONS,
    }];

    static SYNTHETIC_PUBLICATIONS: [DomainPublication<BusRpcContext, std::io::Error>; 3] = [
        DomainPublication {
            server_name: "Synthetic",
            route_slug: "synthetic",
            operations: &SYNTHETIC_OPERATIONS,
        },
        DomainPublication {
            server_name: "Second",
            route_slug: "second",
            operations: &SYNTHETIC_OPERATIONS,
        },
        DomainPublication {
            server_name: "Empty",
            route_slug: "empty",
            operations: &[],
        },
    ];

    fn descriptor() -> &'static RequestOperationDescriptor<BusRpcContext, std::io::Error> {
        &SYNTHETIC_OPERATIONS[0]
    }

    fn image_descriptor() -> &'static RequestOperationDescriptor<BusRpcContext, std::io::Error> {
        static DESCRIPTOR: RequestOperationDescriptor<BusRpcContext, std::io::Error> = RequestOperationDescriptor {
            domain: "synthetic",
            operation: "render",
            qualified_operation: "synthetic.render",
            canonical_variant: "Synthetic::Render",
            kind: RequestOperationKind::Mutation,
            mcp_eligible: true,
            input_type: "Input",
            response_type: "ImageOutput",
            response_variant: "Render",
            input_schema,
            output_schema: image_output_schema,
            dispatch,
        };
        &DESCRIPTOR
    }

    fn invalid_input_descriptor() -> &'static RequestOperationDescriptor<BusRpcContext, std::io::Error> {
        static DESCRIPTOR: RequestOperationDescriptor<BusRpcContext, std::io::Error> = RequestOperationDescriptor {
            domain: "synthetic",
            operation: "invalid_input",
            qualified_operation: "synthetic.invalid_input",
            canonical_variant: "Synthetic::InvalidInput",
            kind: RequestOperationKind::Query,
            mcp_eligible: true,
            input_type: "Input",
            response_type: "ObjectOutput",
            response_variant: "InvalidInput",
            input_schema: invalid_schema,
            output_schema: object_output_schema,
            dispatch,
        };
        &DESCRIPTOR
    }

    fn invalid_output_descriptor() -> &'static RequestOperationDescriptor<BusRpcContext, std::io::Error> {
        static DESCRIPTOR: RequestOperationDescriptor<BusRpcContext, std::io::Error> = RequestOperationDescriptor {
            domain: "synthetic",
            operation: "invalid_output",
            qualified_operation: "synthetic.invalid_output",
            canonical_variant: "Synthetic::InvalidOutput",
            kind: RequestOperationKind::Query,
            mcp_eligible: true,
            input_type: "Input",
            response_type: "ObjectOutput",
            response_variant: "InvalidOutput",
            input_schema,
            output_schema: invalid_schema,
            dispatch,
        };
        &DESCRIPTOR
    }

    fn array_input_descriptor() -> &'static RequestOperationDescriptor<BusRpcContext, std::io::Error> {
        static DESCRIPTOR: RequestOperationDescriptor<BusRpcContext, std::io::Error> = RequestOperationDescriptor {
            domain: "synthetic",
            operation: "array_input",
            qualified_operation: "synthetic.array_input",
            canonical_variant: "Synthetic::ArrayInput",
            kind: RequestOperationKind::Query,
            mcp_eligible: true,
            input_type: "Array",
            response_type: "ObjectOutput",
            response_variant: "ArrayInput",
            input_schema: array_schema,
            output_schema: object_output_schema,
            dispatch,
        };
        &DESCRIPTOR
    }

    fn string_output_descriptor() -> &'static RequestOperationDescriptor<BusRpcContext, std::io::Error> {
        static DESCRIPTOR: RequestOperationDescriptor<BusRpcContext, std::io::Error> = RequestOperationDescriptor {
            domain: "synthetic",
            operation: "string_output",
            qualified_operation: "synthetic.string_output",
            canonical_variant: "Synthetic::StringOutput",
            kind: RequestOperationKind::Query,
            mcp_eligible: true,
            input_type: "Input",
            response_type: "String",
            response_variant: "StringOutput",
            input_schema,
            output_schema: string_schema,
            dispatch,
        };
        &DESCRIPTOR
    }

    fn array_output_descriptor() -> &'static RequestOperationDescriptor<BusRpcContext, std::io::Error> {
        static DESCRIPTOR: RequestOperationDescriptor<BusRpcContext, std::io::Error> = RequestOperationDescriptor {
            domain: "synthetic",
            operation: "array_output",
            qualified_operation: "synthetic.array_output",
            canonical_variant: "Synthetic::ArrayOutput",
            kind: RequestOperationKind::Query,
            mcp_eligible: true,
            input_type: "Input",
            response_type: "Array",
            response_variant: "ArrayOutput",
            input_schema,
            output_schema: array_schema,
            dispatch,
        };
        &DESCRIPTOR
    }

    #[test]
    fn structured_success_contains_one_text_block_and_the_structured_value() {
        let value = json!({ "Obsidian": [] });
        let result = CallToolResult::structured(value.clone());
        assert_eq!(result.structured_content, Some(value));
        assert_eq!(result.content.len(), 1);
        assert_eq!(result.is_error, Some(false));
    }

    #[test]
    fn success_responses_preserve_object_and_primitive_shapes() {
        let object = mcp_success_response(false, json!({ "x": 1 }));
        assert_eq!(object.structured_content, Some(json!({ "x": 1 })));
        assert_eq!(object.content.len(), 1);
        let primitive = mcp_success_response(false, json!("value"));
        assert_eq!(primitive.content.len(), 1);
        assert_eq!(primitive.is_error, Some(false));
    }

    #[test]
    fn image_results_validate_bytes_and_preserve_optional_payloads() {
        let result = image_tool_result(json!({ "image": [0, 1, 255], "content": "done", "data": {"ok": true} }));
        assert_eq!(result.content.len(), 2);
        assert_eq!(result.structured_content, Some(json!({ "ok": true })));
        let image = result.content[0].as_image().expect("native image block");
        assert_eq!(image.data, "AAH/");
        assert_eq!(image.mime_type, "image/png");
        assert_eq!(result.content[1].as_text().unwrap().text, "done");
        let optional_values = image_tool_result(json!({ "image": [0], "content": 42, "data": null }));
        assert_eq!(optional_values.content.len(), 1);
        assert_eq!(optional_values.structured_content, None);
        let invalid_object = image_tool_result(json!("not an object"));
        assert_eq!(
            invalid_object.content[0].as_text().unwrap().text,
            "image tool returned an invalid response"
        );
        let missing_bytes = image_tool_result(json!({}));
        assert_eq!(
            missing_bytes.content[0].as_text().unwrap().text,
            "image tool returned no PNG bytes"
        );
        let negative_byte = image_tool_result(json!({ "image": [-1] }));
        assert_eq!(
            negative_byte.content[0].as_text().unwrap().text,
            "image tool returned invalid PNG bytes"
        );
        let fractional_byte = image_tool_result(json!({ "image": [1.5] }));
        assert_eq!(
            fractional_byte.content[0].as_text().unwrap().text,
            "image tool returned invalid PNG bytes"
        );
        assert_eq!(image_tool_result(json!({ "image": [256] })).is_error, Some(true));
    }

    #[test]
    fn error_projection_is_wrapped_as_the_stable_unstructured_content() {
        #[derive(Debug, thiserror::Error)]
        #[error("typed failure")]
        struct TypedError;
        let result = mcp_tool_error(&TypedError, |_| json!({ "code": "test" }));
        assert_eq!(result.structured_content, None);
        assert_eq!(result.is_error, Some(true));
        assert_eq!(result.content.len(), 1);
        assert_eq!(
            result.content[0].as_text().unwrap().text,
            r#"{"error":{"code":"test"},"message":"typed failure"}"#
        );
    }

    #[test]
    fn debug_truncation_counts_unicode_characters() {
        for (debug_length, source_chars, truncated) in [(479, 477, false), (480, 478, false), (481, 479, true)] {
            let value = "🦀".repeat(source_chars);
            let result = truncated_debug(&value);
            assert_eq!(result.chars().count(), if truncated { 483 } else { debug_length });
            assert_eq!(result.ends_with("..."), truncated);
            assert!(result.starts_with('"'));
            assert!(
                result
                    .chars()
                    .nth(1)
                    .is_some_and(|character| character == '🦀')
            );
        }
    }

    #[test]
    fn synthetic_descriptor_builds_sdk_tool_and_retains_typed_dispatch() {
        let tool = build_mcp_tool(descriptor()).expect("valid descriptor schemas");
        assert_eq!(tool.tool.name.as_ref(), "read");
        assert_eq!(
            tool.tool.input_schema.get("type"),
            Some(&Value::String("object".into()))
        );
        assert_eq!(
            tool.tool
                .output_schema
                .as_ref()
                .and_then(|schema| schema.get("type")),
            Some(&Value::String("object".into()))
        );
        assert!(!tool.image_response);
        assert_eq!(tool.descriptor.domain, "synthetic");
        assert_eq!(tool.descriptor.operation, "read");
        assert_eq!(tool.descriptor.qualified_operation, "synthetic.read");
        assert_eq!(tool.descriptor.canonical_variant, "Synthetic::Read");
        assert_eq!(tool.descriptor.kind, RequestOperationKind::Query);
        assert!(tool.descriptor.mcp_eligible);
        assert_eq!(tool.descriptor.input_type, "Input");
        assert_eq!(tool.descriptor.response_type, "ObjectOutput");
        assert_eq!(tool.descriptor.response_variant, "Read");
    }

    #[test]
    fn image_descriptor_omits_native_output_schema() {
        let tool = build_mcp_tool(image_descriptor()).expect("valid image descriptor");
        assert!(tool.image_response);
        assert!(tool.tool.output_schema.is_none());
    }

    #[test]
    fn invalid_input_and_output_schemas_keep_qualified_error_identity() {
        assert!(matches!(
            build_mcp_tool(invalid_input_descriptor()),
            Err(McpCollationError::MissingInputSchema { qualified_operation })
                if qualified_operation == "synthetic.invalid_input"
        ));
        assert!(matches!(
            build_mcp_tool(invalid_output_descriptor()),
            Err(McpCollationError::MissingOutputSchema { qualified_operation })
                if qualified_operation == "synthetic.invalid_output"
        ));
        assert!(matches!(
            build_mcp_tool(array_input_descriptor()),
            Err(McpCollationError::MissingInputSchema { qualified_operation })
                if qualified_operation == "synthetic.array_input"
        ));
        assert!(
            build_mcp_tool(array_output_descriptor())
                .expect("array output schema")
                .tool
                .output_schema
                .is_none()
        );
        assert!(
            build_mcp_tool(string_output_descriptor())
                .expect("string output schema")
                .tool
                .output_schema
                .is_none()
        );
    }

    #[test]
    fn tool_lookup_is_domain_local_and_unknown_names_never_dispatch() {
        let spec = McpSpec {
            tools: vec![build_mcp_tool(descriptor()).expect("valid descriptor")],
        };
        let error = match resolve_mcp_tool(&spec, "missing") {
            Err(error) => error,
            Ok(_) => panic!("unknown tools must not resolve"),
        };
        assert_eq!(error.message, "unknown MCP tool: missing");
    }

    #[test]
    fn collation_preserves_empty_domains_and_allows_same_tool_name_across_domains() {
        let domains = collate_mcp_specs(&SYNTHETIC_PUBLICATIONS).expect("valid publications");
        assert_eq!(domains.len(), 3);
        assert_eq!(domains[0].spec.tools.len(), 1);
        assert_eq!(domains[1].spec.tools.len(), 1);
        assert!(domains[2].spec.tools.is_empty());
        assert!(!Arc::ptr_eq(&domains[0].spec, &domains[1].spec));
    }

    #[test]
    fn collation_filters_ineligible_ops_and_rejects_duplicates_with_qualified_context() {
        let filtered = collate_mcp_specs(&INELIGIBLE_PUBLICATION).expect("valid empty catalog");
        assert!(filtered[0].spec.tools.is_empty());
        assert!(matches!(
            collate_mcp_specs(&DUPLICATE_PUBLICATION),
            Err(McpCollationError::DuplicateQualifiedName { qualified_operation })
                if qualified_operation == "synthetic.other_read"
        ));
    }

    #[test]
    fn schema_resolution_keeps_raw_json_pointer_and_cycle_guard_semantics() {
        let schema: Schema = json!({
            "$defs": { "First": { "$ref": "#/$defs/Second" }, "Second": { "type": "object" } },
            "$ref": "#/$defs/First"
        })
        .try_into()
        .expect("object schema");
        assert_eq!(schema_object(schema).unwrap().get("type"), Some(&json!("object")));

        let cycle: Schema = json!({ "$defs": { "Loop": { "$ref": "#/$defs/Loop" } }, "$ref": "#/$defs/Loop" })
            .try_into()
            .expect("object schema");
        assert_eq!(schema_object(cycle).unwrap().get("type"), None);
        let missing: Schema = json!({ "$ref": "#/$defs/Missing" })
            .try_into()
            .expect("object schema");
        assert_eq!(schema_object(missing).unwrap().get("type"), None);
        let external: Schema = json!({ "$ref": "https://example.test/schema" })
            .try_into()
            .expect("object schema");
        assert_eq!(schema_object(external).unwrap().get("type"), None);
    }

    #[test]
    fn native_token_selection_prefers_http_bearer_and_falls_back_to_worker() {
        let worker = Jwt::from_opaque("worker-token".into());
        let bearer = HttpMcpBearer(Jwt::from_opaque("http-token".into()));
        assert_eq!(select_request_jwt(Some(&bearer), &worker).as_str(), "http-token");
        assert_eq!(select_request_jwt(None, &worker).as_str(), "worker-token");
    }

    #[tokio::test]
    async fn selected_generated_descriptor_dispatches_success_and_typed_failure() {
        let tool = build_mcp_tool(descriptor()).expect("valid descriptor");
        let context = BusRpcContext::unavailable();
        DISPATCHES.store(0, Ordering::SeqCst);
        let success = dispatch_selected_tool(&tool, context.clone(), json!({})).await;
        assert_eq!(success.expect("descriptor success"), json!({ "ok": true }));
        let failure = dispatch_selected_tool(&tool, context, json!({ "fail": true }))
            .await
            .expect_err("descriptor preserves its typed error");
        assert_eq!(failure.to_string(), "typed descriptor failure");
        let error_response = mcp_tool_error(&failure, io_error_details);
        assert_eq!(error_response.is_error, Some(true));
        assert_eq!(error_response.content.len(), 1);
        assert_eq!(error_response.structured_content, None);
        assert_eq!(
            error_response.content[0].as_text().unwrap().text,
            r#"{"error":{"kind":"Other"},"message":"typed descriptor failure"}"#
        );
        assert_eq!(DISPATCHES.load(Ordering::SeqCst), 2);
    }
}
