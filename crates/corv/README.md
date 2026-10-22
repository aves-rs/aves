# Corv

Embed a specialized discussion domain for the **Aves async application runtime**.

Manage ACP Providers, agent Teams, sessions, and agent-backed automation through typed domain contracts.
Name Corv after Corvidae: ravens and crows, the planning and discussion family.

Embed Corv as a library, not a standalone executable.

## Installation

Add Corv to your application:

```toml
[dependencies]
corv = { package = "aves-corv", version = "=0.5.0" }
aves = "=0.5.0"
tokio = { version = "=1.53.1", features = ["sync"] }
semver = "=1.0.28"
```

Declare dependencies directly when your application imports their types.
Use Rust 1.97 or newer.

## Quick Start

Prepare Corv's required boot options using public types:

```rust
use std::path::PathBuf;
use corv::{AssetLibrary, DiscussBootOptions, DiscussStreamEvent};
use tokio::sync::mpsc;

fn boot_options(
  config_dir: PathBuf,
  library: AssetLibrary,
  stream_tx: mpsc::Sender<DiscussStreamEvent>,
) -> DiscussBootOptions {
  DiscussBootOptions {
    config_dir,
    library,
    stream_tx,
    mcp_domains: || Ok(Vec::new()),
  }
}
```

This example supplies no application MCP tools.
It prepares construction inputs; it does not start a runtime or Provider.

Register `DiscussDomain` in your Aves domain composition.
Supply these boot options and a shared `NodeRegistrationSupply` through `ConstructionInputs`.
Construct Config before Discuss, then Discuss before Flow drains node registrations.
Consume the bounded `DiscussStreamEvent` receiver throughout the runtime's lifetime.

Read the [Aves README](https://docs.rs/crate/aves/0.5.0) for domain registration and lifecycle mechanisms.

## Usage

### Own your application catalog

Keep Corv application-neutral by supplying an immutable `AssetLibrary`.
Corv contains schemas, ID validation, and provider renderers—not application prompts or trading content.

Define a minimal embedded agent and Team using ordinary application-owned statics:

```rust
use std::sync::LazyLock;
use corv::{AgentDefinition, AgentId, AgentRole, AssetLibrary, TeamDefinition};
use semver::Version;

static AGENTS: LazyLock<Vec<AgentDefinition>> = LazyLock::new(|| vec![
  AgentDefinition {
    id: AgentId::new("planner").expect("valid embedded agent id"),
    version: Version::new(1, 0, 0),
    role: AgentRole::Leader,
    name: "Planner",
    job_title: "Planning assistant",
    description: "Plan and discuss application work",
    body: "Clarify goals, identify constraints, and propose a practical plan.",
  },
]);

static TEAMS: LazyLock<Vec<TeamDefinition>> = LazyLock::new(|| vec![
  TeamDefinition {
    leader: AgentId::new("planner").expect("valid embedded leader id"),
    followers: Vec::new(),
    skills: Vec::new(),
    version: Version::new(1, 0, 0),
  },
]);

fn library() -> AssetLibrary {
  AssetLibrary::new(&AGENTS, &[], &[], &TEAMS)
}
```

Supply agents, skills, commands, and Teams in that constructor order.
Use static embedded text or application-owned `include_str!` resources.
Inspect definitions with `agents_for()`, `skills_for()`, `commands_for()`, and `teams_for()`.
Validate IDs with `AgentId`, `SkillId`, and `CommandId` constructors.
Reject rooted paths, separators, dot segments, and control characters.

Preserve stable IDs across upgrades.
Identify each Team by its leader; preserve follower and skill order.
Bump Team versions when membership, order, member instructions, or bundled content changes.
Corv uses the supplied catalog for listing, upgrades, retention, and materialization.

### Compose Providers and Teams

Distinguish the three concepts:

| Concept | Responsibility |
|---|---|
| Provider | Launch and communicate with an ACP harness |
| Agent | Supply persona instructions and metadata |
| Team | Combine one leader, ordered followers, and ordered skills |

Configure Provider launch commands and environment through `discuss.addProvider`.
Let the embedding application install and make provider executables available.
Corv does not install those external programs.

Install Teams through `discuss.installTeam` using the supplied catalog's stable leader ID.
Corv's plugins materialize native Provider files under the configured agents workspace.
Preserve shared assets during uninstall through provider-specific retention scopes.
Treat successful mutations carrying cleanup warnings as success with possible leftover files.

Use advertised non-mode configuration options for sparse member overrides.
Do not invent Provider mode values from Team names or identities.

### Respect connection strategy

Use each plugin's explicit connection strategy rather than inferring multiplex safety.

| Provider identifier | Strategy |
|---|---|
| `opencode` | Shared |
| `codex-acp` | Shared |
| `claude-acp` | Shared |
| `grok-build` | Isolated |
| Custom/default | Isolated |

Shared runtimes multiplex session routing through one Provider connection.
Isolated runtimes create a connection per session.
Provider runtime state is process-local; persisted sessions remain after Provider stops.

### Route sessions and stream events

Generate a `SluiceSessionId` before requesting a genuinely new session:

```rust
use corv::SluiceSessionId;

let session_id = SluiceSessionId::new();
```

Retain that identity for reconnects and subsequent operations.
Treat session names as metadata, not routing keys.
Keep Provider `acp_session_id` values separate from the application's session identity.

Handle all five `DiscussStreamEvent` variants:

| Variant | Purpose |
|---|---|
| `SessionFailed` | Deliver typed failure and terminal-auth recovery methods |
| `AgentMessage` | Forward typed ACP content and tool/session updates |
| `UtilityUpdate` | Forward todo snapshots and subagent metadata |
| `PermissionRequested` | Obtain a permission answer through privileged session control |
| `ElicitationRequested` | Obtain structured input through privileged session control |

Route every event by `sluice_session_id`.
Live protocol events also carry the Provider ACP identity for protocol and diagnostics.
Do not use it as an application routing key.
Corv retains responders internally; answer through its typed services, not raw channels.
Drain the stream promptly to avoid applying unintended backpressure to session workers.

### Separate public operations from privileged control

Expose generated `discuss.*` operations through your chosen Aves transport adapters.
Examples include `newSession`, `listSessions`, `loadSession`, `renameSession`, and `removeSession`.
Provider operations include `listProviders`, `addProvider`, `removeProvider`, and `refreshProviderConfig`.
Use `startServer`, `stopServer`, `getServerStatus`, `startProvider`, and `stopProvider` for lifecycle control.

Keep turn sending, permission answers, elicitation answers, cancellation, and inspector control privileged.
Acquire the exported `DiscussStreamingService` through Aves's typed service mechanisms.
Privileged operations are excluded from public generated clients and MCP descriptors.
Public operations still require normal Aves admission; public does not mean unauthenticated.

Use [message contracts](src/messages.rs) as the operation reference.
Do not assume every generated operation has a crate-root Rust dispatcher re-export.

### Supply MCP tools

Build native ACP tool descriptors with the exported `discuss_mcp_domains` factory.
Supply your application's `DomainPublication` values and typed error-details projection.
Place that factory call in the zero-capture `DiscussBootOptions.mcp_domains` callback.
Inspect the [factory API](https://docs.rs/aves-corv/0.5.0/corv/fn.discuss_mcp_domains.html) for callback arguments.

Use Corv's exported `run_mcp` for HTTP MCP serving.
Compose it beside Aves RPC serving using the existing `TransportBindings::new` API.
Let the Aves bus own listener lifecycle, including session-scoped loopback leases.

Corv prefers native MCP-over-ACP when supported.
The HTTP fallback uses a loopback listener and a worker-JWT Bearer header.
HTTP admission requires exactly one well-formed, nonempty compact-JWT Bearer header.
Provider-level `sluice_mcp_enabled` gates both transports.
Unsupported MCP capability produces a warning without necessarily ending the session.

Observe `DiscussMcpStatus::{Off, Acp, Fallback, Broken}` for provisioning state.
Never persist or expose worker credentials as application configuration.

### Automate through AgentNode

Share one `NodeRegistrationSupply` between owner domains and Aves Flow.
Corv registers `AgentNode` during domain construction.
Construct it before Flow consumes the supply.
Use Aves Tasks for `ScriptNode`; Corv does not own script execution.

Run AgentNodes inside Flow's execution directory, not the shared chat workspace.
Treat CWD as working-directory selection, not filesystem confinement.
Apply configured AgentNode limits before opening noninteractive sessions:

| Limit | Default | Maximum |
|---|---|---|
| Turns | 1 | 100 |
| Tool calls | 100 | 1,000 |
| Duration | 1,800 seconds | 1,800 seconds |

Cancellation uses privileged lifecycle requests.
Do not assume cancelling can undo effects already accepted by an external system.
Inspect [Aves Flow](https://docs.rs/aves/0.5.0/aves/flow/index.html) for workspace contracts.

### Configure storage and lifecycle

Persist Provider settings under the Aves Config section `discuss`.
Configure `agents`, `agents_workspace`, and `subagent_retention_limit` through that section.
The default workspace is `config_dir/discuss`; default subagent retention is ten records.

Keep Discuss state in `config_dir/discuss.db`.
Corv maintains session metadata, bounded transcripts, Provider configuration, Team installations, and subagent records.
It owns the ordered persistence queue and database maintenance.
Keep SQL in `migrations/`, beside `src/`, registered through `src/migration.rs`.

StopServer gates new work, resolves pending interactions, and aborts live workers.
It preserves persisted sessions rather than deleting conversations.
Shared Provider failure interrupts its affected sessions and pending interactions.
Treat shutdown and Provider stop as potentially lossy for active work.

### Develop and test

Run from the repository root:

```bash
cargo test --package aves-corv
```

Run focused AgentNode tests:

```bash
cargo test --package aves-corv agent_nodes::
```

Use Aves's `test-support` feature for downstream test fixtures.
Keep test catalogs independent of application content.
Preserve the `discuss.*` contracts and storage identities during structural changes.
Examples here were source-reviewed, not compiled or executed as standalone programs.

## Contributing

Follow contribution guidance in the [source repository](https://github.com/aves-rs/aves).

## License

Use Corv under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).
