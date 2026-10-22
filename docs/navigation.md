---
title: Aves Engineering Navigation
type: reference
updated: 2026-10-23
references:
  - AGENTS.md
  - crates/aves/AGENTS.md
  - crates/aves/macros/AGENTS.md
  - crates/corv/AGENTS.md
  - crates/kea/AGENTS.md
  - crates/tyto/AGENTS.md
---

# Aves Engineering Navigation

Start with the nearest `AGENTS.md`; use this index to load only the relevant supporting docs. Code is authoritative when a doc is stale.

## Standards

| File | Use when |
|---|---|
| [Code Quality](standards/code-quality.md) | Editing Rust code |
| [Documentation](standards/documentation.md) | Editing docs |
| [Tests](standards/tests.md) | Writing or changing tests |

## Architecture

| File | Use when |
|---|---|
| [Runtime Supervisor](architecture/aves/runtime-supervisor.md) | Domain construction and lifecycle |
| [Auth Domain](architecture/aves/auth-domain.md) | JWT issuance and peer admission |
| [Flow Domain](architecture/aves/flow-domain.md) | Graph execution and persistence |
| [Tasks Domain](architecture/aves/tasks-domain.md) | ScriptNode and WASI |
| [Iroh Peer Runtime](architecture/aves/iroh-peer-runtime.md) | Native transport and trust |
| [Pluggable Flow Nodes](architecture/aves/pluggable-flow-nodes.md) | Node registration and dispatch |
| [Discuss Domain](architecture/corv/discuss-domain.md) | ACP sessions, providers and MCP |
| [Knowledge Domain](architecture/kea/knowledge-domain.md) | Notes, Data and Canvas |
| [Tyto Credentials](architecture/tyto/credentials.md) | Credential supply boundary |
| [Tyto Connection](architecture/tyto/connection.md) | Provider connections and inflows |
| [Tyto Subscriptions](architecture/tyto/subscriptions.md) | Inbound triggers and reconciliation |
| [Tyto Send](architecture/tyto/send.md) | Outbound provider delivery |

## Concepts

| File | Use when |
|---|---|
| [Bus JWT Admission](concepts/bus-jwt-admission.md) | Authenticated bus ingress |
| [Declaration-Driven Routing](concepts/declaration-driven-handler-routing.md) | Generated domain handlers |
| [Flow Dataflow](concepts/flow-dataflow.md) | Node inputs, outputs and blackboard |
| [Flow Execution Workspace](concepts/flow-execution-workspace.md) | Checkpoints and workspace files |
| [Knowledge Data Store](concepts/knowledge-data-store.md) | Kea JSON data |
| [Knowledge Markdown Directory](concepts/knowledge-markdown-directory.md) | Kea notes and search |
| [MCP-over-ACP Tooling](concepts/mcp-over-acp-tooling.md) | Discuss MCP transport |
| [MessageNode](concepts/message-node.md) | Tyto Flow delivery node |
| [Messaging Send and Persistence](concepts/messaging-send-and-persistence.md) | Tyto outbound send and Kea handoff |
| [Named Config References](concepts/named-config-references.md) | Tyto connection and credential identity |
| [PermissionNode](concepts/permission-node.md) | Tyto interactive Flow node |
| [Pluggable Flow Nodes](concepts/pluggable-flow-nodes.md) | Open node-kind envelope |
| [Selective Flow Mobility](concepts/selective-flow-mobility.md) | Replica handoff and promotion |

## Guides

| File | Use when |
|---|---|
| [Adding a Flow Node Kind](guides/adding-a-flow-node-kind.md) | Implementing a node registration |
| [Aves Domain RPC](guides/aves-domain-rpc.md) | Publishing generated RPC operations |
| [Stale Named References](guides/stale-named-references.md) | Diagnosing missing Tyto connections |
