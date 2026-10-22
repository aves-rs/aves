use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
};

use thiserror::Error;
use tokio::sync::Mutex;

use crate::bus::{BusControlError, BusMcpEndpoint, BusSudoService, CloseMcpInput, OpenMcpInput};
use crate::runtime::ServiceReader;

use super::FlowExecutionId;

struct FlowMcpLease {
    close: CloseMcpInput,
    endpoints: Vec<BusMcpEndpoint>,
}

/// One lazily opened MCP listener per active Flow execution, independent of node workers.
#[derive(Clone)]
pub struct FlowMcpPorts {
    service: Option<ServiceReader<dyn BusSudoService>>,
    executions: Arc<Mutex<HashMap<FlowExecutionId, Option<FlowMcpLease>>>>,
}

impl FlowMcpPorts {
    pub fn new(service: Option<ServiceReader<dyn BusSudoService>>) -> Self {
        Self {
            service,
            executions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn start(&self, execution_id: FlowExecutionId) {
        self.executions.lock().await.insert(execution_id, None);
    }

    pub async fn endpoints(&self, execution_id: FlowExecutionId) -> Result<Vec<BusMcpEndpoint>, FlowMcpError> {
        let mut executions = self.executions.lock().await;
        let lease = executions
            .get_mut(&execution_id)
            .ok_or(FlowMcpError::ExecutionEnded)?;
        if let Some(lease) = lease {
            return Ok(lease.endpoints.clone());
        }
        let service = self
            .service
            .as_ref()
            .and_then(ServiceReader::get)
            .ok_or(FlowMcpError::ServiceUnavailable)?;
        let opened = service
            .open_mcp(OpenMcpInput {
                host: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
                port: 0,
            })
            .await?;
        let endpoints = opened.endpoints;
        *lease = Some(FlowMcpLease {
            close: CloseMcpInput {
                host: opened.host,
                port: opened.port,
            },
            endpoints: endpoints.clone(),
        });
        Ok(endpoints)
    }

    pub async fn finish(&self, execution_id: FlowExecutionId) {
        let lease = self.executions.lock().await.remove(&execution_id).flatten();
        if let Some(lease) = lease
            && let Some(service) = self.service.as_ref().and_then(ServiceReader::get)
            && let Err(error) = service.close_mcp(lease.close).await
        {
            tracing::warn!(%execution_id, %error, "Could not close Flow MCP listener");
        }
    }
}

#[derive(Debug, Error)]
pub enum FlowMcpError {
    #[error("Flow execution is no longer active")]
    ExecutionEnded,
    #[error("MCP listener service is unavailable")]
    ServiceUnavailable,
    #[error(transparent)]
    Listener(#[from] BusControlError),
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn terminated_flow_cannot_open_an_mcp_listener() {
        let ports = FlowMcpPorts::new(None);
        let execution_id = FlowExecutionId(Uuid::new_v4());
        ports.start(execution_id).await;

        assert!(matches!(
            ports.endpoints(execution_id).await,
            Err(FlowMcpError::ServiceUnavailable)
        ));
        ports.finish(execution_id).await;
        assert!(matches!(
            ports.endpoints(execution_id).await,
            Err(FlowMcpError::ExecutionEnded)
        ));
    }

    #[tokio::test]
    async fn finishing_one_flow_preserves_another_flows_mcp_endpoints() {
        let ports = FlowMcpPorts::new(None);
        let first = FlowExecutionId(Uuid::new_v4());
        let second = FlowExecutionId(Uuid::new_v4());
        ports.start(first).await;
        ports.start(second).await;
        let endpoint = BusMcpEndpoint {
            server_name: "Sluice Knowledge".to_owned(),
            domain: "knowledge".to_owned(),
            url: "http://127.0.0.1:1234/mcp/knowledge".to_owned(),
        };
        ports
            .executions
            .lock()
            .await
            .get_mut(&second)
            .unwrap()
            .replace(FlowMcpLease {
                close: CloseMcpInput {
                    host: IpAddr::V4(Ipv4Addr::LOCALHOST),
                    port: 1234,
                },
                endpoints: vec![endpoint.clone()],
            });
        let before = ports.endpoints(second).await.unwrap();

        ports.finish(first).await;

        assert_eq!(ports.endpoints(second).await.unwrap()[0].url, before[0].url);
        assert!(matches!(
            ports.endpoints(first).await,
            Err(FlowMcpError::ExecutionEnded)
        ));
    }
}
