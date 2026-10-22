//! Generic operation-publication contracts emitted by `aves_macros`.

use std::future::Future;
use std::pin::Pin;

/// Future returned by a generated JSON operation adapter.
pub type OperationDispatchFuture<RootError> =
    Pin<Box<dyn Future<Output = Result<serde_json::Value, RootError>> + Send>>;

/// Read/write classification for a generated operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestOperationKind {
    /// Read-only operation.
    Query,
    /// State-changing operation.
    Mutation,
}

/// Publication metadata and JSON adapter for one generated operation.
pub struct RequestOperationDescriptor<Context, RootError> {
    /// Stable domain name.
    pub domain: &'static str,
    /// Domain-local operation name.
    pub operation: &'static str,
    /// Globally unique operation name.
    pub qualified_operation: &'static str,
    /// Canonical request variant.
    pub canonical_variant: &'static str,
    /// Operation classification.
    pub kind: RequestOperationKind,
    /// Whether the operation is eligible for MCP publication.
    pub mcp_eligible: bool,
    /// Human-readable generated input type name.
    pub input_type: &'static str,
    /// Human-readable generated response type name.
    pub response_type: &'static str,
    /// Canonical response variant name.
    pub response_variant: &'static str,
    /// Input schema factory.
    pub input_schema: fn() -> schemars::Schema,
    /// Output schema factory.
    pub output_schema: fn() -> schemars::Schema,
    /// JSON adapter backed by the generated typed dispatcher.
    pub dispatch: fn(Context, serde_json::Value) -> OperationDispatchFuture<RootError>,
}

/// Publication metadata for one generated domain surface.
pub struct DomainPublication<Context: 'static, RootError: 'static> {
    /// Human-readable server name.
    pub server_name: &'static str,
    /// Stable route slug and RSPC prefix.
    pub route_slug: &'static str,
    /// Generated operations belonging to this domain.
    pub operations: &'static [RequestOperationDescriptor<Context, RootError>],
}

#[cfg(test)]
mod tests {
    use super::{OperationDispatchFuture, RequestOperationDescriptor, RequestOperationKind};

    fn accepts_generic_descriptor<Context, RootError>(_descriptor: RequestOperationDescriptor<Context, RootError>) {}

    fn dispatch(_: (), _: serde_json::Value) -> OperationDispatchFuture<std::io::Error> {
        Box::pin(async { Err(std::io::Error::other("test")) })
    }

    #[test]
    fn publication_contract_accepts_embedding_context_and_root_error() {
        accepts_generic_descriptor(RequestOperationDescriptor {
            domain: "test",
            operation: "read",
            qualified_operation: "test.read",
            canonical_variant: "TestRequest::Read",
            kind: RequestOperationKind::Query,
            mcp_eligible: true,
            input_type: "ReadInput",
            response_type: "ReadOutput",
            response_variant: "Read",
            input_schema: schemars::Schema::default,
            output_schema: schemars::Schema::default,
            dispatch,
        });
    }
}
