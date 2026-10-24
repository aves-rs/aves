# Aves

Build reliable Rust applications from independent, typed domains.

Aves gives long-running applications one place to manage messaging, lifecycle, authentication, and workflows.
Keep business logic in its owning domain while sharing dependable infrastructure.

- **Compose without coupling.** Add domains to one runtime instead of wiring unrelated actors together.
- **Make boundaries explicit.** Generate typed requests, privileged services, and RPC adapters from domain contracts.
- **Keep work durable.** Run Flow automations with persisted state and checkpointed execution workspaces.
- **Control access.** Authenticate bus requests and communicate with paired peers over Iroh.
- **Grow when needed.** Add discussion, knowledge, and messaging capabilities as separate crates.

| Crate | Purpose |
|---|---|
| [Aves](crates/aves/README.md) | Runtime, bus, Auth, Config, Flow, and Tasks |
| [Corv](crates/corv/README.md) | ACP-powered agents, sessions, teams, and MCP tools |
| [Kea](crates/kea/README.md) | Searchable notes, structured data, and Canvas dashboards |
| [Tyto](crates/tyto/README.md) | Email, Slack, and Telegram listeners and delivery |

## Quick Start

Follow one complete domain before building your own:

1. Read [Kea's request contracts](kea/src/messages.rs).
2. Follow [its domain declaration and callbacks](kea/src/domain.rs).
3. Inspect [its handler wiring](kea/src/handle.rs).
4. Keep state and work in [its actor](kea/src/actor.rs).
5. Follow [application composition](poseidon/src/app.rs) to install domains and construction inputs.

The following public-API example creates a typed configuration registration:

```rust
use aves::config::{ConfigError, SectionRegistration};
use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize)]
struct WorkerSettings {
  enabled: bool,
}

fn worker_section() -> Result<SectionRegistration, ConfigError> {
  SectionRegistration::new::<WorkerSettings>("worker")
}
```

Publish that registration through `ConfigRegistrationSudoService` during your domain's construction.
Construct Config first, then domains that register sections or read saved configuration.
Supply construction inputs explicitly rather than introducing process-global registries.

The sections below explain the remaining mechanisms and their real source implementations.
Source excerpts retain domain-local names; they are not standalone application files.
Public-API examples were source-reviewed, not compiled or executed for this README.

## Usage

Start small: keep application state in an actor and declare its typed operations.
The runtime supervises lifecycle and routes authenticated requests without knowing your domain's business logic.

Add durable automation with Flow when work spans steps, restarts, or execution workspaces.
Extend Flow through registrations, without changing its core dispatcher.
Follow the [node-kind guide](docs/guides/adding-a-flow-node-kind.md) for an owner-defined task.

Need agent sessions or searchable knowledge? Add only the crates your application uses:

```toml
[dependencies]
aves = "=VERSION"
aves-corv = "=VERSION"
aves-kea = "=VERSION"
```

Use Tyto for inbound subscriptions or outbound Email, Slack, and Telegram delivery.

The optional crates depend on Aves, never on a particular application.

Build and automate typed domains with an **async application runtime for Rust**.

## Usage

### Choose the right ownership boundary

Put application work in domains and reusable mechanisms in Aves.

| Layer | Owns |
|---|---|
| Runtime | Domain construction, service slots, lifecycle, Auth integration, bus ownership |
| Bus | Authenticated opaque-envelope delivery by domain-marker `TypeId` |
| Domain declaration | Operation metadata, generated routing, service mappings, broadcast projection |
| Handler | Actor construction and request/broadcast forwarding |
| Actor | State, validation, persistence decisions, and domain work |
| Application | Domain composition, construction inputs, root errors/broadcasts, transport factories |

Keep actors, internal state, and implementation modules private.
Re-export only contracts required by real callers.
Do not teach the generic bus about domain names or business operations.

Use a small domain layout:

```text
example/
  mod.rs
  domain.rs
  handle.rs
  actor.rs
  messages.rs
  types.rs
  queries/
```

Use `lib.rs` instead of `mod.rs` when the domain owns its own crate.
Add files only when they have a clear owner and purpose.

### Define requests separately from reply channels

Declare a payload containing operation parameters.
Wrap it with a typed reply channel for internal request/response handling.
Do not embed oneshot senders inside public payload variants.

Request-wrapper excerpt from [Kea](../kea/src/messages.rs):

```rust
pub struct KnowledgeRequest {
  pub payload: KnowledgeRequestPayload,
  pub reply_to: oneshot::Sender<Result<KnowledgeResponse, KnowledgeError>>,
}
```

Declare the payload's marker, context, and error boundaries explicitly.
This library-boundary style avoids relying on inferred names.

Selected payload declaration from the same source:

```rust
#[aves::bus_requests(
  marker = KnowledgeDomain,
  context = BusRpcContext,
  root_error = KnowledgeError,
  domain_error = KnowledgeError
)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KnowledgeRequestPayload {
  #[operation(
    name = "createKnowledge",
    kind = "mutation",
    response = KnowledgeCreatedOutput,
    response_variant = "KnowledgeCreated"
  )]
  CreateKnowledge { request: CreateKnowledgeRequest },
}
```

This excerpt selects one operation from Kea's complete payload enum.
Define referenced DTOs, errors, and responses in the owning domain.
Use typed identifiers and narrow input types, not stringly typed dispatch envelopes.
Document request fields: schema generation exposes those comments to external callers.

### Generate operation adapters and response projections

Annotate response enums with `#[aves::bus_responses]`.
The macro creates typed output structs and `TryFrom<Response>` projections.
Reference those outputs in request operation metadata.

Selected response declaration from [Kea](../kea/src/messages.rs):

```rust
#[aves::bus_responses]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KnowledgeResponse {
  KnowledgeCreated { knowledge: KnowledgeRecord },
}
```

For public operations, request generation supplies:

- A typed operation input and its conversion into the payload.
- A typed `send_<operation>` function for authenticated bus ingress.
- A `dispatch_<operation>` helper accepting a `BusRpcContext`.
- JSON dispatch adapters for transport integration.
- Input/output schemas and `RequestOperationDescriptor` metadata.
- An rspc router and operation descriptors adapted to embedding root errors.

Choose `kind = "query"` or `kind = "mutation"` intentionally.
Treat operation names and serialized error identities as client contracts.
Changing a Rust module path does not require changing those contracts.

Inspect [request generation](domain_setup/src/request.rs) and [response generation](domain_setup/src/response.rs).
Use the generated interfaces rather than writing parallel routing tables.
Do not assume every generated helper is re-exported by a domain crate.

### Distinguish publication, serialization, and privilege

Keep these mechanisms separate:

| Declaration | Effect |
|---|---|
| `#[operation(...)]` | Publish generated RPC/operation metadata for an operation |
| `#[privileged(reason = "...")]` | Exclude public generated clients and require a privileged service mapping |
| `#[serde(skip)]` | Exclude a variant or field from its serialized representation |

Do not use `#[serde(skip)]` as a substitute for privileged authorization.
Internal lifecycle messages may be unpublished without being privileged service operations.
Public does not mean unauthenticated: normal Aves admission still applies.

Declare privileged capabilities through named sudo services.
Selected service mappings from [Auth](src/auth/domain.rs):

```rust
pub service AuthSudoService {
  IssueJwt => DelegatedJwtIssued (operation = "issueJwt"),
  IssueRootJwt => JwtIssued (operation = "issueRootJwt"),
  VerifyJwt => Verified (operation = "verifyJwt"),
  RevokeJti => Success (operation = "revokeJti"),
}
```

Place those mappings inside the owning `aves::domain!` declaration.
The complete Auth declaration maps its additional privileged variants too.
Generation enforces privileged mappings and rejects public variants mapped as sudo operations.

Give consumers only the service interfaces they require.
Do not hand them another domain's actor, persistence connection, or broad management authority.
Privileged services remain in-process capabilities, not public RPC backdoors.

### Declare domain routing and broadcasts

Use `aves::domain!` to declare the domain marker and request/response/error boundaries.
Add services and broadcast mappings in the same owner declaration.

Selected routing declaration from [Kea](../kea/src/domain.rs):

```rust
aves::domain! {
  marker KnowledgeDomain;

  request KnowledgeRequestPayload => KnowledgeResponse
  error = KnowledgeError
  transport = SharedInfraError
  target = "knowledge";

  outbound KnowledgeResponse => KnowledgeBroadcast {
    KnowledgeCreated { knowledge: _ } => RecordsChanged,
    KnowledgeUpdated { knowledge: _ } => RecordsChanged,
    KnowledgeDeleted { result: _ } => RecordsChanged,
  }
}
```

Add explicit service blocks when a domain needs named service traits and implementations.
Kea's full source includes additional broadcast mappings.

Let successful responses drive the handler's declared broadcast projection.
Route internal service requests through that handler too.
Bypassing the handler can lose broadcasts even when the actor mutation succeeds.
Keep domain-specific work out of generated routing and transport adapters.

### Wire bounded actor and service channels

Register the domain's marker, request wrapper, and broadcast type with the bus.
Create a separate bounded actor inbox.
Let the generated handler forward admitted requests into that inbox.

Construction excerpt from [Kea's handler](../kea/src/handle.rs):

```rust
let (actor_tasks, actor_tasks_rx) = mpsc::channel(64);
let channels = driver.register::<KnowledgeDomain, KnowledgeRequest, KnowledgeBroadcast>();
let (broadcast_tx, actor_broadcast_rx) = mpsc::channel(64);

let generated_handler = KnowledgeGeneratedHandler::new(
  channels.requests_rx,
  actor_tasks.clone(),
  channels.broadcasts_tx,
  actor_broadcast_rx,
);
```

The enclosing handler provides these domain-local names and imports.
Pass `actor_tasks_rx` to the actor; retain the generated handler for forwarding.
Kea's handler also subscribes to configuration broadcasts.

For a declared service, use its generated channel constructor.
Service-channel excerpt from [Corv](../corv/src/handle.rs):

```rust
let (streaming_service_impl, streaming_service_rx) =
  super::DiscussStreamingServiceImpl::channel(DEFAULT_BUFFER_SIZE);
```

Publish the implementation through the construction facade's service publisher.
Pass its receiver to the generated handler alongside normal bus requests.
Do not construct generated implementation fields or create a second forwarding path.

Read dependencies through `ServiceReader`; publish through `ServicePublisher`.
Readers are late-bound, but immediate `.get()` calls still require an available provider.
Do not assume arbitrary construction order is safe for every domain.

### Implement actor work and typed errors

Keep the actor responsible for its state and domain operations.
Receive a request wrapper, match its payload, and send one typed result.
Use owned task messages for work that must return asynchronously.
Keep network or expensive operations from blocking unrelated actor work where practical.

Use thiserror for domain failures.
Carry reusable infrastructure failures through typed `From` conversions.
Use `aves::utils::errors::SharedInfraError` for shared channel/oneshot plumbing boundaries.
Keep that infrastructure error out of public wire contracts.

Adapt domain errors into the embedding application's root error explicitly.
Preserve structured domain and code identities instead of flattening everything into strings.
Read [Kea's error boundary](../kea/src/types.rs) and [generated call errors](src/runtime/supervisor.rs).

### Compose runtime domains

Use `aves::runtime_domains!` in the embedding application's composition root.
Associate each domain with its router, descriptors, route names, and broadcast projection.
Let generated composition provide lifecycle descriptors, RPC routing, and publication catalogs.

Selected entry from [Poseidon's real composition](../poseidon/src/app.rs):

```rust
ConfigDomain => config_router,
  normal: AVES_CONFIG_OPERATIONS.as_slice(),
  mcp: AVES_CONFIG_OPERATIONS.as_slice(),
  "Sluice Config", "config",
  broadcast: ConfigBroadcast => Config,
```

Place entries inside the macro's `entries` list.
Supply `context`, `root_error`, and `root_broadcast` as shown in the complete source.
Provide the root broadcast conversions required by your domain callbacks.
Do not manually copy a domain's generated operation registry.

Prepare typed inputs with `ConstructionInputs::new().with(value)`.
Request them during construction with `facade.input::<T>()`.
Typical inputs include `ConfigDirectory`, domain boot options, and shared node-registration supplies.
Do not silently create a second supply when domains must share one instance.

### Own the runtime lifecycle

Build `RuntimeOptions` with all required host inputs:

| Field | Responsibility |
|---|---|
| `keyring_service_namespace` | Application-owned namespace for Auth's identity storage |
| `construction_inputs` | Explicit typed inputs for domain constructors |
| `transport_bindings` | Host-provided RPC/MCP listener factories and route metadata |
| `data_root` | Auth peer state and native substrate data root |
| `remote_operation_installer` | Optional installer for generated public peer operations |

Then construct the runtime from the generated lifecycle list.
Application-lifecycle excerpt; application types and `options` come from the composition root:

```rust
let runtime = Runtime::<AppBroadcast>::new(options, AppDomains::lifecycle()).await?;
runtime.start().await?;
let context = runtime.rpc_context();
let broadcasts = runtime.subscribe_broadcasts();
```

Use the actual public APIs: `rpc_context()`, `request_sender()`, and `subscribe_broadcasts()`.
Use `service_reader::<dyn Service>()` when acquiring a runtime-published service.
Do not expose the private `BusHandle` to embedding applications.

Implement `DomainDescriptorFactory<EventBus<Root>>` for each domain marker.
Return `DomainCallbacks` containing `new`, `start`, and `stop` callbacks.
Use `DomainFuture` for asynchronous construction callbacks.
See [Kea's full factory implementation](../kea/src/domain.rs).

Understand the ordering guarantees:

1. Aves constructs built-in Auth and installs authenticated bus context.
2. It constructs supplied domains sequentially in descriptor order.
3. It finalizes the shared bus after construction.
4. `start()` seals configuration registration and runs start callbacks concurrently.
5. `shutdown()` stops Flow first, then other domain callbacks under a shared timeout.
6. It shuts down native Iroh handlers and the bus afterward.

Keep the bus alive while domain stop callbacks send lifecycle requests.
Use `dispatch_lifecycle_request` where the domain uses normal typed lifecycle messages.
Do not invent per-domain global lifecycle senders.

Do not equate sequential construction with ordered service startup.
Start/stop callbacks report their own domain-specific failures.
Construction panics do not provide automatic application-level rollback or compensation.
Keep shutdown behavior explicit and avoid promising cross-domain transactions.

### Supply RPC and MCP transport adapters

Use the authenticated `BusRpcContext` at generated operation boundaries.
Let transport adapters select operation inputs and serialize typed results.
Keep business logic in the domain actor.

`TransportBindings::new` accepts two listener factories and an MCP route catalog.
Each factory receives three arguments:

```text
TcpListener
mpsc::Sender<OpaqueRequestEnvelope>
oneshot::Receiver<()>
```

Return the required boxed listener future from each factory.
Use `aves::utils::transport::run_rpc` for the existing rspc HTTP adapter.
Use Corv's `run_mcp` only when your application includes Corv's MCP implementation.
Aves itself does not collate tools or implement MCP-over-ACP.

See [the real transport bindings](../poseidon/src/app.rs) for complete factory wiring.
The bus owns opening, closing, and stopping supplied listener factories.
An embedding application decides which listeners to request and expose.
Do not assume a production RPC endpoint exists simply because routing is generated.

### Register configuration and live hooks

Own each typed configuration section in the domain that consumes it.
Register before runtime start seals the registry.
Use `getConfigSection` and `saveConfigSection` through the Config domain's public operations.
Load from `config.toml`; do not assume environment or CLI overlays exist automatically.

Extend the Quick Start registration with a typed semantic guard:

```rust
use aves::config::{ConfigError, SectionRegistration};
use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize)]
struct QueueSettings {
  max_pending: u32,
}

fn queue_section() -> Result<SectionRegistration, ConfigError> {
  Ok(SectionRegistration::new::<QueueSettings>("queue")?
    .before_write(|_old: QueueSettings, new: QueueSettings| async move {
      if new.max_pending > 1_000 {
        return Err(ConfigError::InvalidValue {
          section: "queue".into(),
          message: "max_pending must not exceed 1000".into(),
        });
      }
      Ok(())
    }))
}
```

Use `on_change` for post-persistence owner notifications.
Its failure does not roll back the already-saved configuration file.
Use `before_write` when rejection must leave the saved configuration unchanged.
Design actor notifications and reload failures around that distinction.

See [Config registration](src/config/types.rs) and [Kea's reload handling](../kea/src/handle.rs).
Kea demonstrates keeping the previous active directory when a reopened root fails.

### Authenticate callers and paired peers

Let built-in Auth establish machine identity and the runtime audience.
Receive the runtime-issued context rather than manufacturing an unauthenticated substitute.
Treat root contexts and sudo-service access as trusted application authority.

`AuthSudoService` provides delegated issuance, verification, revocation, and peer management.
Its `IssueJwtInput` uses `sub`, `cap`, and an RFC3339 `exp` value.
Choose from the current capabilities: `AllBus` and `PullCloudBlobs`.

Use generated public-operation admission and domain checks as implemented.
Do not infer per-operation authorization policy beyond the declared safety boundary.
Do not serialize worker tokens into configuration, logs, or application state.

For remote operations, use the native authenticated Iroh connection and peer admission.
Pairing trust is local, unilateral, file-backed, and non-transitive.
Confirmed machines still need an audience appropriate to the target runtime.
Revocation changes local admission; it does not mutate the remote machine's files.

Inspect [Auth contracts](src/auth/messages.rs), [capabilities](src/auth/types.rs), and [peer runtime architecture](../../docs/trade.sluice/trade.sluice.engineering/architecture/aves/iroh-peer-runtime.md).
Use authenticated native peer operations for supported cross-runtime requests.
Peer transport is not distributed consensus or an application-level shared transaction.

### Automate with Flow

Represent orchestration through Aves Flow rather than adding work to the generic bus.
Use graph combinators and leaf-node registrations to compose owner execution.
Keep domain-specific execution and interpretation behind owner callbacks.

Flow owns an open, validated `NodeKind` and an opaque configuration envelope.
Owners provide `NodeKindRegistration` values through a shared `NodeRegistrationSupply`.
A new backend kind does not require a switch or enum extension in Flow core.

Register owner kinds during construction, before Flow drains the supply.
The supply is a construction input, not a service trait.
Construction-callback excerpt using a `facade` supplied by Aves:

```rust
let supply = facade
  .input::<NodeRegistrationSupply>()
  .cloned()
  .expect("shared node registration supply");
```

Provide the same supply through Flow's boot options and owner-domain construction inputs.
Do not push registrations after Flow has consumed the boot supply.

### Define a custom node kind

The following public-API example defines an immediate, pure custom node.
It records a greeting in terminal state without starting external work.

```rust
use aves::flow::{
  BoundOutputs, CancellationDisposition, FlowRegistrationError, NodeKind,
  NodeKindDescriptor, NodeKindRegistration, NodeLifecycleResult,
  SideEffectDisposition, WorkspacePreparation,
};
use serde::Deserialize;

#[derive(Deserialize)]
struct GreetingConfig {
  message: String,
}

fn greeting_registration() -> Result<NodeKindRegistration, FlowRegistrationError> {
  let descriptor = NodeKindDescriptor {
    kind: NodeKind::parse("GreetingNode")?,
    cancellation: CancellationDisposition::Cancellable,
    side_effects: SideEffectDisposition::Pure,
  };
  Ok(NodeKindRegistration::typed::<GreetingConfig, _, _, _, _, _, _, _>(
    descriptor,
    |config| {
      if config.message.trim().is_empty() {
        return Err(FlowRegistrationError::Configuration {
          message: "message must not be empty".into(),
        });
      }
      Ok(())
    },
    |_context, config| async move {
      Ok(NodeLifecycleResult::Succeeded {
        outputs: BoundOutputs::default(),
        state: serde_json::json!({"message": config.message}),
      })
    },
    |_context, _config| async { Ok(NodeLifecycleResult::Running) },
    |_context, _config| async { Ok(NodeLifecycleResult::Cancelled) },
    None,
    WorkspacePreparation::new(),
  ))
}
```

Push that registration from its owner's constructor:

```rust
supply.push(greeting_registration().expect("valid static greeting registration"));
```

This final line continues the construction excerpt above.
For asynchronous work, return `Running` and report terminal owner results on later ticks.
Do not block or busy-wait inside Flow while waiting for external work.

Handle all lifecycle results: `Running`, `Succeeded`, `Failed`, and `Cancelled`.
Declare `Cancellable` or `NonCancellable` honestly.
Declare `Pure` or `SideEffecting` according to observable effects.
Do not promise cancellation can undo an effect already accepted externally.

See [the registration API](src/flow/registration.rs) and [adding a node kind](../../docs/trade.sluice/trade.sluice.engineering/guides/adding-a-flow-node-kind.md).

### Bind data and prepare workspaces

Use Flow bindings to connect named leaf outputs and resolved blackboard inputs.
Keep owner configuration separate from the Flow-owned node envelope.
Use typed input and output views rather than inventing a second execution graph.

Each `NodeLifecycleContext` carries:

- Correlation IDs and the Flow-owned node envelope.
- Resolved `BoundInputs` and the execution-root directory.
- Flow's authenticated bus context.
- A distinct node-scoped token on start, when issued.
- Execution-owned MCP ports and optional owner state.

The transport context remains Flow-authenticated across start, tick, and cancel.
Do not substitute the start-only node token for transport authentication.
Use it only for the scoped guest or worker capability it represents.

Prepare content roots with `WorkspacePreparation::node_content_roots`.
Supply graph-level seeds through `WorkspacePreparation::graph_seeds` when required.
Flow owns one writable directory per execution at `executions/{execution_id}` under the data root.
Keep checkpoints and restoration in Flow's existing durability mechanism.

Use [Flow dataflow](../../docs/trade.sluice/trade.sluice.engineering/concepts/flow-dataflow.md) and [execution workspaces](../../docs/trade.sluice/trade.sluice.engineering/concepts/flow-execution-workspace.md) for complete contracts.
Do not treat a working-directory setting as general process confinement.

### Choose node owners and trigger sources

Compose the owner domain for each node kind you need:

| Owner | Registered kinds |
|---|---|
| Aves Tasks | `ScriptNode` |
| Corv Discuss | `AgentNode` |
| Tyto Messaging | `MessageNode`, `PermissionNode` |
| Application owners | Additional kinds, such as Poseidon's `OrderNode` |

Let source owners register and validate trigger adapters through Flow's trigger contracts.
Use ordinary public Flow operations such as `flow.start` and `flow.abort`.
Keep privileged retention/runtime control separate from public automation requests.

Flow persists definitions, triggers, execution state, and workspace durability references.
Owners retain responsibility for their external side effects.
Do not infer exactly-once execution or cross-system transactions from graph persistence alone.

Inspect [Flow's public contracts](src/flow/mod.rs) and [pluggable node architecture](../../docs/trade.sluice/trade.sluice.engineering/architecture/aves/pluggable-flow-nodes.md).

### Execute ScriptNode work

Compose `TasksDomain` and supply its required guest-RPC binding factory and registration supply.
Tasks transpiles TypeScript and runs JavaScript through QuickJS inside Wasmtime/WASI.
Use its generated guest RPC surface for normal public operations.
Privileged operations are structurally excluded from those guest bindings.

Expose only the execution directory through the single WASI preopen at guest `/`.
Treat guest filesystem access and guest RPC as separate mechanisms.
Apply the owner-provided finite fuel and timeout limits.
Do not assume an AgentNode's process working directory provides equivalent confinement.

Read [Tasks architecture](../../docs/trade.sluice/trade.sluice.engineering/architecture/aves/tasks-domain.md) and [the script adapter](src/tasks/adapter.rs).
Keep source compilation, execution, and terminal output handling with Tasks.
Keep graph scheduling, persistence, and checkpoint policy with Flow.

### Open databases and apply owner migrations

Use concrete utility paths instead of introducing a utility barrel.
Keep schemas and migration ledgers with the domain that owns the data.

Public-API example for a small application-owned database:

```rust
use std::path::Path;
use aves::utils::database::{
  DatabaseError, Migration, MigrationRunner, open_database_at_path,
};

const MIGRATIONS: &[Migration] = &[Migration {
  version: 1,
  name: "notes",
  sql: "CREATE TABLE IF NOT EXISTS notes (id TEXT PRIMARY KEY, body TEXT NOT NULL);",
}];

async fn migrate_notes(path: &Path) -> Result<(), DatabaseError> {
  let database = open_database_at_path(path).await?;
  MigrationRunner::new("__example_migrations", MIGRATIONS)
    .apply(&database)
    .await?;
  Ok(())
}
```

For production code, embed separately owned SQL assets with `include_str!`.
Use `connect_database` for configured connections and `maintain_database` for maintenance.
Choose `open_database_at_path_with_concurrency` when explicit coordination mode is required.
Do not rename existing ledger tables merely because a crate's name changes.
Do not claim application transactions cover separate services, keyrings, or remote providers.

See [database utilities](src/utils/database.rs), [Kea migrations](../kea/src/migrations.rs), and [Corv migrations](../corv/src/migration.rs).

### Reuse shared mechanisms

Import helpers from their concrete `aves::utils` modules:

| Modules | Responsibility |
|---|---|
| `database` | Database opening, connections, migration runner, maintenance |
| `errors` | Shared typed infrastructure failures |
| `retry` | Retry/backoff mechanisms and owner-selected retry policy |
| `time`, `uuid` | Time conversion and typed UUID utilities |
| `arithmetic`, `bigint`, `integers`, `decimals` | Reusable numerical mechanisms |
| `buffers`, `series`, `rolling` | Bounded buffers and series calculations |
| `transport` | RPC serving and endpoint metadata projection |

Check each module's actual API before creating another helper.
Keep provider SDKs, domain schemas, financial identity, and application policy outside shared utilities.
MCP serving belongs to Corv, not `aves::utils::transport`.

### Test without exposing production internals

Test state transitions, typed validation, serialization, and domain-owned persistence directly.
Use temporary databases and the owner's actual migration registration.
Do not widen actor visibility simply to support a test.

Enable downstream-only test helpers through a dev-dependency:

```toml
[dev-dependencies]
aves = { path = "../aves", features = ["test-support"] }
```

Use `aves::utils::test_support::{MockBus, reply_with}` for existing opaque-envelope fixtures.
Use `ServiceReader::from_published_test_service` for an injected service fixture.
The feature also exposes request-envelope test inspection methods.

Remember that `cfg(test)` does not propagate into dependency crates.
Use the test-support feature rather than enabling test scaffolding in every normal build.
Keep provider integration tests distinct from local parsing or actor tests.

Run commands from the repository root when verification is wanted:

```bash
cargo test --package aves_macros
```

```bash
cargo test --package aves
```

```bash
cargo test --package aves --no-default-features
```

Default features include `keyring` and `wasm-tests`.
Those flags gate relevant tests; disabling them does not remove all production keyring/native dependencies.
Use `test-support` only for downstream test helpers.
Do not interpret source-reviewed README examples as executed integration tests.

### Understand current boundaries

- Use local workspace dependencies until release packaging and publication are explicitly prepared.
- Account for native identity storage and Iroh setup during runtime construction.
- Register Flow kinds at boot, not dynamically after the supply is drained.
- Preserve domain-specific startup and recovery behavior; Aves does not invent owner compensation.
- Handle broadcast lag and bounded-channel backpressure in embedding consumers.
- Treat peer trust as admission, not distributed consensus or shared transactional state.
- Preserve existing protocol and storage identifiers unless a separate migration changes them.

Aves is reusable infrastructure, not an application, frontend framework, or universal provider SDK.
Do not add trading logic, installed personas, or UI lifecycle ownership to the runtime.

### Extend Aves with specialized domains

| Package | Role |
|---|---|
| [Corv](../corv/README.md) | Specialized discussion domain: ACP sessions, Teams, AgentNode, MCP |
| [Kea](../kea/README.md) | Specialized Knowledge domain: notes, search, Data KV, Canvas |
| [Tyto](../tyto/README.md) | Specialized messaging domain: Email/Slack/Telegram and Flow interactions |
| Poseidon | Application composition, trading domains, and application-owned static content |
| Zeus / Hades | Desktop/native bridge and frontend application surfaces |

Compose only the domain capabilities your application requires.
Aves does not depend on Corv, Kea, Tyto, or the trading application.
Specialized domain crates depend on the Aves async application runtime.

## Where It Came From

Name Aves after Class Aves: the root class containing all birds.
Compose independent domains without moving their business logic into shared infrastructure.
Generate request contracts, RPC adapters, privileged services, and routing from domain declarations.
Add durable Flow automation, configuration, authenticated messaging, and sandboxed script execution.

## Contributing

Follow [Aves conventions](AGENTS.md), [macro rules](domain_setup/AGENTS.md), and repository engineering standards.
Start with [domain RPC guidance](../../docs/trade.sluice/trade.sluice.engineering/guides/aves-domain-rpc.md).
Read [cross-domain requests](../../docs/trade.sluice/trade.sluice.engineering/guides/cross-domain-requests.md) before adding a second communication mechanism.
Use existing domain and registration patterns before introducing infrastructure abstractions.
