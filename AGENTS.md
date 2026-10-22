# Aves workspace

This repository contains the reusable Aves runtime and three optional domain crates. It does not contain the Sluice trading application. Use the nearest package `AGENTS.md` for binding ownership and contracts; start at [engineering navigation](docs/navigation.md) for cross-package explanations.

| Package | Owns | Local rules |
|---|---|---|
| `crates/aves/` | Runtime, bus, Auth, Config, Flow, Tasks, shared utilities | [Aves](crates/aves/AGENTS.md) |
| `crates/aves/macros/` | Compile-time bus and RPC generation | [Macros](crates/aves/macros/AGENTS.md) |
| `crates/corv/` | Discuss, ACP sessions, Teams, MCP | [Corv](crates/corv/AGENTS.md) |
| `crates/kea/` | Knowledge notes, Data, Canvas | [Kea](crates/kea/AGENTS.md) |
| `crates/tyto/` | Messaging subscriptions, Send, Flow nodes | [Tyto](crates/tyto/AGENTS.md) |

## Boundaries

- Aves never imports the optional domain crates. Corv and Kea depend on Aves, not an embedding application. Tyto depends on Aves and Kea.
- Applications supply domain composition, root broadcasts/errors, transport bindings, and any content or credential providers. Do not import the trading application into these crates.
- Preserve existing wire, migration, and storage identities unless a change explicitly calls for a migration. Package names do not rename domain contracts.
- Prefer private implementation modules, concrete shared utility imports, and typed generated service contracts. Do not widen production APIs for tests.

## Workflow

- Read [code standards](docs/standards/code-quality.md), [documentation standards](docs/standards/documentation.md), or [test standards](docs/standards/tests.md) as appropriate.
- Run Cargo through the repository's direnv environment: `direnv exec . cargo ...`. The bare shell toolchain may be incomplete.
- Use targeted tests: `direnv exec . cargo test --package aves`, `aves_macros`, `aves-corv`, `aves-kea`, or `aves-tyto`.
- Keep documentation links and [navigation](docs/navigation.md) current when moving or removing files.
