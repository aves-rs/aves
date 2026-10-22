use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use agent_client_protocol::schema::v1::{
    McpError, McpServer, McpServerAcp, McpServerAcpId, MessageMcpNotification, MessageMcpRequest, MessageMcpResponse,
};
use agent_client_protocol::{Agent, ConnectionTo, Dispatch, HandleDispatchFrom, Handled, Responder};
use rmcp::model::{ClientNotification, ClientRequest, JsonRpcMessage, RequestId};
use rmcp::service::{RoleServer, RxJsonRpcMessage, TxJsonRpcMessage};
use rmcp::transport::Transport;
use tokio::sync::mpsc;

use aves::auth::Jwt;
use aves::bus::BusRpcContext;
use aves::utils::buffers::DEFAULT_BUFFER_SIZE;

use super::boot::{DiscussMcpDomain, DiscussMcpService};

pub struct McpAcpTransport {
    inbound: mpsc::Receiver<RxJsonRpcMessage<RoleServer>>,
    outbound: mpsc::Sender<TxJsonRpcMessage<RoleServer>>,
}

struct McpAcpConnection {
    inbound: mpsc::Sender<RxJsonRpcMessage<RoleServer>>,
    responders: Arc<Mutex<HashMap<RequestId, Responder<serde_json::Value>>>>,
    cancellation: tokio_util::sync::CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

/// Worker-local MCP-over-ACP state and dynamic handler owner.
///
/// Dropping this value removes its dynamic handler and aborts every connection
/// task. Native MCP requests address the declared server directly.
pub struct McpAcpShim {
    state: Arc<Mutex<McpAcpShimState>>,
}

struct McpAcpShimState {
    closed: bool,
    server_id: McpServerAcpId,
    descriptor: McpServer,
    service: Arc<dyn DiscussMcpService>,
    context: BusRpcContext,
    worker_jwt: Jwt,
    connections: HashMap<McpServerAcpId, McpAcpConnection>,
}

impl Transport<RoleServer> for McpAcpTransport {
    type Error = Infallible;

    fn send(
        &mut self,
        message: TxJsonRpcMessage<RoleServer>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        let outbound = self.outbound.clone();
        async move {
            // Worker shutdown closes the ACP-facing receiver. RMCP observes the
            // cancellation token; a late output has no surviving ACP caller.
            let _ = outbound.send(message).await;
            Ok(())
        }
    }

    async fn receive(&mut self) -> Option<RxJsonRpcMessage<RoleServer>> {
        self.inbound.recv().await
    }

    fn close(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        std::future::ready(Ok(()))
    }
}

impl McpAcpShim {
    /// Installs a fresh worker-local descriptor and dynamic handler state.
    pub fn new(domain: &DiscussMcpDomain, _owner: String, context: BusRpcContext, worker_jwt: Jwt) -> Self {
        let server_id = McpServerAcpId::new(format!("acp:{}", uuid::Uuid::new_v4()));
        let descriptor = sluice_mcp_acp_descriptor(domain.server_name.to_string(), server_id.clone());
        let state = McpAcpShimState {
            closed: false,
            server_id,
            descriptor,
            service: domain.service.clone(),
            context,
            worker_jwt,
            connections: HashMap::new(),
        };
        let state = Arc::new(Mutex::new(state));
        Self { state }
    }

    /// Returns the worker-local descriptor for either `session/new` or `session/load`.
    pub fn descriptor(&self) -> McpServer {
        self.state
            .lock()
            .expect("MCP-over-ACP state lock")
            .descriptor
            .clone()
    }

    /// Registers the public dynamic handler for this worker's ACP connection.
    pub fn dynamic_handler(&self) -> McpAcpDynamicHandler {
        McpAcpDynamicHandler {
            state: self.state.clone(),
        }
    }
}

impl Drop for McpAcpShim {
    fn drop(&mut self) {
        let connections = {
            let mut state = self.state.lock().expect("MCP-over-ACP state lock");
            state.closed = true;
            std::mem::take(&mut state.connections)
        };
        for connection in connections.into_values() {
            close_mcp_acp_connection(&connection);
            connection.task.abort();
        }
    }
}

pub struct McpAcpDynamicHandler {
    state: Arc<Mutex<McpAcpShimState>>,
}

impl McpAcpDynamicHandler {
    fn connect(&mut self) {
        let mut state = self.state.lock().expect("MCP-over-ACP state lock");
        if state.closed || state.connections.contains_key(&state.server_id) {
            return;
        }
        let server_id = state.server_id.clone();
        let (inbound, inbound_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
        let (outbound_tx, outbound_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
        let responders = Arc::new(Mutex::new(HashMap::new()));
        let cancellation = tokio_util::sync::CancellationToken::new();
        let task = tokio::spawn(run_mcp_acp_connection(
            McpAcpTransport {
                inbound: inbound_rx,
                outbound: outbound_tx,
            },
            outbound_rx,
            responders.clone(),
            cancellation.clone(),
            self.state.clone(),
            server_id.clone(),
        ));

        state.connections.insert(
            server_id,
            McpAcpConnection {
                inbound,
                responders,
                cancellation,
                task,
            },
        );
    }

    fn notification(&mut self, notification: MessageMcpNotification) -> Result<(), agent_client_protocol::Error> {
        let mcp_notification = match mcp_notification(&notification.method, notification.params) {
            Ok(notification) => notification,
            Err(_) => return Ok(()),
        };
        let state = self.state.lock().expect("MCP-over-ACP state lock");
        let Some(connection) = state.connections.get(&notification.server_id) else {
            return Ok(());
        };
        let _ = connection
            .inbound
            .try_send(JsonRpcMessage::notification(mcp_notification));
        Ok(())
    }

    fn message(
        &mut self,
        request: MessageMcpRequest,
        responder: Responder<serde_json::Value>,
    ) -> Result<(), agent_client_protocol::Error> {
        let mcp_request = match mcp_request(&request.method, request.params) {
            Ok(request) => request,
            Err(error) => {
                responder.respond_with_internal_error(error.to_string())?;
                return Ok(());
            }
        };
        self.connect();
        let state = self.state.lock().expect("MCP-over-ACP state lock");
        let Some(connection) = state.connections.get(&request.server_id) else {
            responder.respond_with_internal_error("MCP-over-ACP connection is not active")?;
            return Ok(());
        };
        let request_id = RequestId::String(request.request_id.0);
        let mut responders = connection
            .responders
            .lock()
            .expect("MCP-over-ACP responder lock");
        if responders.contains_key(&request_id) {
            responder.respond_with_internal_error("MCP-over-ACP request is already active")?;
            return Ok(());
        }
        responders.insert(request_id.clone(), responder);
        drop(responders);
        if connection
            .inbound
            .try_send(JsonRpcMessage::request(mcp_request, request_id.clone()))
            .is_err()
            && let Some(responder) = connection
                .responders
                .lock()
                .expect("MCP-over-ACP responder lock")
                .remove(&request_id)
        {
            responder.respond_with_internal_error("MCP-over-ACP connection is closed")?;
        }
        Ok(())
    }
}

impl HandleDispatchFrom<Agent> for McpAcpDynamicHandler {
    fn describe_chain(&self) -> impl std::fmt::Debug {
        "SluiceMcpAcpShim"
    }

    async fn handle_dispatch_from(
        &mut self,
        message: Dispatch,
        _connection: ConnectionTo<Agent>,
    ) -> Result<Handled<Dispatch>, agent_client_protocol::Error> {
        match message {
            Dispatch::Request(request, responder) if request.method() == "mcp/message" => {
                let message = match serde_json::from_value::<MessageMcpRequest>(request.params().clone()) {
                    Ok(message) => message,
                    Err(error) => {
                        responder.respond_with_internal_error(error)?;
                        return Ok(Handled::Yes);
                    }
                };
                if self
                    .state
                    .lock()
                    .expect("MCP-over-ACP state lock")
                    .server_id
                    != message.server_id
                {
                    return Ok(Handled::No {
                        message: Dispatch::Request(request, responder),
                        retry: false,
                    });
                }
                self.message(message, responder)?;
                Ok(Handled::Yes)
            }
            Dispatch::Notification(notification) if notification.method() == "mcp/message" => {
                let message = match serde_json::from_value::<MessageMcpNotification>(notification.params().clone()) {
                    Ok(message) => message,
                    Err(_) => return Ok(Handled::Yes),
                };
                if self
                    .state
                    .lock()
                    .expect("MCP-over-ACP state lock")
                    .server_id
                    != message.server_id
                {
                    return Ok(Handled::No {
                        message: Dispatch::Notification(notification),
                        retry: false,
                    });
                }
                self.notification(message)?;
                Ok(Handled::Yes)
            }
            message => Ok(Handled::No { message, retry: false }),
        }
    }
}

/// Creates the sole ACP transport descriptor for Sluice's in-process MCP service.
pub fn sluice_mcp_acp_descriptor(name: String, server_id: McpServerAcpId) -> McpServer {
    McpServer::Acp(McpServerAcp::new(name, server_id))
}

async fn run_mcp_acp_connection(
    transport: McpAcpTransport,
    mut outbound: mpsc::Receiver<TxJsonRpcMessage<RoleServer>>,
    responders: Arc<Mutex<HashMap<RequestId, Responder<serde_json::Value>>>>,
    cancellation: tokio_util::sync::CancellationToken,
    state: Arc<Mutex<McpAcpShimState>>,
    server_id: McpServerAcpId,
) {
    let (service, context, worker_jwt) = {
        let state = state.lock().expect("MCP-over-ACP state lock");
        (state.service.clone(), state.context.clone(), state.worker_jwt.clone())
    };
    tokio::select! {
        _ = async {
            if let Some(running) = service
                .start(transport, cancellation.clone(), context, worker_jwt)
                .await
            {
                running.await;
            }
        } => {}
        _ = async {
            while let Some(message) = outbound.recv().await {
                forward_mcp_acp_response(message, &responders);
            }
        } => {}
    }
    cancellation.cancel();
    remove_mcp_acp_connection(&state, &server_id);
}

fn mcp_notification(
    method: &str,
    params: Option<serde_json::Map<String, serde_json::Value>>,
) -> Result<ClientNotification, serde_json::Error> {
    serde_json::from_value(serde_json::json!({ "method": method, "params": params }))
}

fn mcp_request(
    method: &str,
    params: Option<serde_json::Map<String, serde_json::Value>>,
) -> Result<ClientRequest, serde_json::Error> {
    serde_json::from_value(serde_json::json!({ "method": method, "params": params }))
}

fn forward_mcp_acp_response(
    message: TxJsonRpcMessage<RoleServer>,
    responders: &Mutex<HashMap<RequestId, Responder<serde_json::Value>>>,
) {
    let Some((request_id, response)) = mcp_acp_response(message) else {
        return;
    };
    if let Some(responder) = responders
        .lock()
        .expect("MCP-over-ACP responder lock")
        .remove(&request_id)
    {
        let _ = responder.respond(serde_json::to_value(response).expect("MCP-over-ACP response is serializable"));
    }
}

fn mcp_acp_response(message: TxJsonRpcMessage<RoleServer>) -> Option<(RequestId, MessageMcpResponse)> {
    let response = match message {
        JsonRpcMessage::Response(response) => (
            response.id,
            MessageMcpResponse::success(serde_json::to_value(response.result).expect("MCP result is serializable")),
        ),
        JsonRpcMessage::Error(error) => {
            let request_id = error.id?;
            let mut inner = McpError::new(error.error.code.0, error.error.message);
            if let Some(data) = error.error.data {
                inner = inner.data(data);
            }
            (request_id, MessageMcpResponse::error(inner))
        }
        JsonRpcMessage::Request(_) | JsonRpcMessage::Notification(_) => return None,
    };
    Some(response)
}

fn close_mcp_acp_connection(connection: &McpAcpConnection) {
    connection.cancellation.cancel();
    for (_, responder) in connection
        .responders
        .lock()
        .expect("MCP-over-ACP responder lock")
        .drain()
    {
        let _ = responder.respond_with_internal_error("MCP-over-ACP connection is closed");
    }
}

fn remove_mcp_acp_connection(state: &Arc<Mutex<McpAcpShimState>>, server_id: &McpServerAcpId) {
    let connection = state
        .lock()
        .expect("MCP-over-ACP state lock")
        .connections
        .remove(server_id);
    if let Some(connection) = connection {
        close_mcp_acp_connection(&connection);
    }
}
