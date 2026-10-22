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

Start with the [Aves quick start](crates/aves/README.md#quick-start).
Define a domain's requests, register it with the runtime, and let Aves route messages.
Use the [RPC guide](docs/guides/aves-domain-rpc.md) when exposing operations to clients.

## Usage

Start small: keep application state in an actor and declare its typed operations.
The runtime supervises lifecycle and routes authenticated requests without knowing your domain's business logic.

Add durable automation with Flow when work spans steps, restarts, or execution workspaces.
Extend Flow through registrations, without changing its core dispatcher.
Follow the [node-kind guide](docs/guides/adding-a-flow-node-kind.md) for an owner-defined task.

Need agent sessions or searchable knowledge? Add only the crates your application uses:

```toml
[dependencies]
aves = "=0.5.0"
aves-corv = "=0.5.0"
aves-kea = "=0.5.0"
```

Use Tyto for inbound subscriptions or outbound Email, Slack, and Telegram delivery.

The optional crates depend on Aves, never on a particular application.

## Contributing

Read [workspace rules](AGENTS.md) and the nearest package rules before editing.
Run `direnv exec . cargo test --workspace` from the repository root.

## License

Apache-2.0. See [LICENSE](LICENSE).
