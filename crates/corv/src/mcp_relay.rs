use std::net::IpAddr;
use std::sync::Arc;

use agent_client_protocol::schema::v1::{HttpHeader, McpServer, McpServerHttp};

use aves::auth::Jwt;
use aves::bus::{BusControlError, BusMcpEndpoint, BusSudoService, CloseMcpInput, OpenMcpInput};

/// A bus-owned MCP listener lease used by one ACP worker.
///
/// Discuss only retains the bus-provided endpoint descriptors. The bus actor
/// owns the listener task and its graceful join.
pub(super) struct McpHttpLease {
    service: Arc<dyn BusSudoService>,
    descriptors: Vec<McpServer>,
    close_trigger: Option<CloseMcpInput>,
}

impl McpHttpLease {
    /// Opens an ephemeral loopback listener through the bus control service.
    pub(super) async fn open(
        _owner: String,
        service: Arc<dyn BusSudoService>,
        worker_jwt: Jwt,
    ) -> Result<Self, BusControlError> {
        let opened = service
            .open_mcp(OpenMcpInput {
                host: Some(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
                port: 0,
            })
            .await?;
        tracing::debug!("Opened MCP listener at {}:{}", opened.host, opened.port);

        let close_trigger = CloseMcpInput {
            host: opened.host,
            port: opened.port,
        };

        Ok(Self {
            service,
            descriptors: opened
                .endpoints
                .iter()
                .map(|endpoint| endpoint_to_acp_descriptor(endpoint, &worker_jwt))
                .collect(),
            close_trigger: Some(close_trigger),
        })
    }

    /// Returns the descriptors for the MCP listener.
    pub(super) fn descriptors(&self) -> &[McpServer] {
        &self.descriptors
    }

    /// Closes the exact listener returned by the bus and consumes the lease.
    pub(super) async fn close(mut self) -> Result<(), BusControlError> {
        self.close_inner().await
    }

    async fn close_inner(&mut self) -> Result<(), BusControlError> {
        tracing::debug!("Closing MCP listener (via close)");
        let Some(input) = self.close_trigger.take() else {
            tracing::warn!("Could not acquire close trigger for MCP closure");
            return Ok(());
        };

        tracing::debug!(
            "Closing MCP listener with close trigger {}:{}",
            input.host.clone(),
            input.port
        );
        closed_listener_result(self.service.close_mcp(input).await.map(|_| ()))
    }
}

impl Drop for McpHttpLease {
    fn drop(&mut self) {
        let Some(input) = self.close_trigger.take() else {
            tracing::debug!("MCP was been properly closed; dropping relay");
            return;
        };

        tracing::warn!("MCP has not been properly closed; forcing closure now before dropping relay");
        let service = Arc::clone(&self.service);
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if let Err(error) = closed_listener_result(service.close_mcp(input).await.map(|_| ())) {
                    tracing::warn!(%error, "Failed to close bus-owned MCP listener after worker exit");
                }
            });
        }
    }
}

fn closed_listener_result(result: Result<(), BusControlError>) -> Result<(), BusControlError> {
    match result {
        Err(BusControlError::ChannelSend(_) | BusControlError::ChannelReceive(_)) => Ok(()),
        result => result,
    }
}

pub(super) fn endpoint_to_acp_descriptor(endpoint: &BusMcpEndpoint, worker_jwt: &Jwt) -> McpServer {
    McpServer::Http(
        McpServerHttp::new(endpoint.server_name.clone(), endpoint.url.clone()).headers(vec![HttpHeader::new(
            "Authorization",
            format!("Bearer {}", worker_jwt.as_str()),
        )]),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listener_cleanup_accepts_closed_control_channels() {
        assert!(closed_listener_result(Err(BusControlError::ChannelSend("closed".into()))).is_ok());
        assert!(closed_listener_result(Err(BusControlError::ChannelReceive("closed".into()))).is_ok());
    }

    #[test]
    fn listener_cleanup_preserves_shutdown_failures() {
        assert!(matches!(
            closed_listener_result(Err(BusControlError::ShutdownTimeout { protocol: "mcp" })),
            Err(BusControlError::ShutdownTimeout { protocol: "mcp" })
        ));
    }

    #[test]
    fn endpoint_shaping_preserves_bus_name_and_url() {
        let endpoint = BusMcpEndpoint {
            server_name: "Sluice Discuss".to_string(),
            domain: "discuss".to_string(),
            url: "http://127.0.0.1:1234/mcp/discuss".to_string(),
        };
        let McpServer::Http(server) = endpoint_to_acp_descriptor(&endpoint, &Jwt::from_opaque("jwt".to_owned())) else {
            panic!("expected HTTP descriptor");
        };
        assert_eq!(server.name, endpoint.server_name);
        assert_eq!(server.url, endpoint.url);
        assert_eq!(server.headers.len(), 1);
        assert_eq!(server.headers[0].name, "Authorization");
        assert_eq!(server.headers[0].value, "Bearer jwt");
    }
}
