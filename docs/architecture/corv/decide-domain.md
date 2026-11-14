---
title: Decide Domain Architecture
type: architecture
updated: 2026-11-14
references:
  - crates/corv/AGENTS.md
  - crates/corv/src/lib.rs
  - crates/corv/src/decide/mod.rs
  - crates/corv/src/decide/types.rs
  - crates/corv/src/decide/messages.rs
  - crates/corv/src/decide/domain.rs
  - crates/corv/src/decide/handle.rs
  - crates/corv/src/decide/actor.rs
  - crates/corv/src/decide/nodes.rs
  - crates/corv/src/decide/plugins/mod.rs
  - crates/corv/src/decide/plugins/systemone.rs
  - crates/corv/src/decide/plugins/jev.rs
  - crates/corv/src/decide/plugins/ollama.rs
  - crates/tyto/src/domain.rs
  - docs/external/typesafe-jev/decision-api.md
  - docs/external/ollama/systemone-decision-api.md
---

# Decide Domain Architecture

Decide asks a model one bounded, typed question and returns a typed answer. It is a second domain in the Corv crate, next to Discuss. It is not an ACP session: there is no harness, conversation, workspace, or tool use. Use [Using the Decide Domain](../../guides/using-the-decide-domain.md) to install and call it.

## Question types

| Type | Answer | Notes |
|---|---|---|
| `bool` | `BoolAnswer { probability }` | Probability of true in [0, 1]. No confidence field; not thresholded. Sent to providers as wire type `noul`. |
| `choice` | `ChoiceAnswer { choice, confidence, probabilities }` | 1–255 non-blank named criteria (core bound). |
| `score` | `ScoreAnswer { score, confidence, legend, probabilities }` | 1–10 ordered levels. `score` is a fractional level index in [0, n−1], never rounded. |

Image-region (`box`) questions are not supported. Every answer is returned with the model name the provider reported. Confidence is whatever the provider reports. It is not calibrated correctness and is not comparable across providers.

## Module layout

All modules are private; `src/lib.rs` re-exports the consumer contracts.

| File | Owns |
|---|---|
| `decide/types.rs` | `DecideError` and RPC codes, `DecideContent`, questions, answers, `DecideCredential`, endpoint validation, node configs, `DecideAskRequest`. |
| `decide/messages.rs` | `DecideRequestPayload`, `DecideResponse`, uninhabited `DecideBroadcast`. |
| `decide/domain.rs` | `aves::domain!` declaration, services, start/stop callbacks. |
| `decide/handle.rs` | Bus registration, service publication, actor spawn, node registration push. |
| `decide/actor.rs` | Actor state, lifecycle, credential custody, question routing, node completions, retry. |
| `decide/nodes.rs` | Three Flow `NodeKindRegistration`s. |
| `decide/plugins/` | `DecidePlugin` trait, provider selection, shared HTTP and System One helpers, Jev and Ollama plugins. |

**Provider-free core.** Every file outside `plugins/` is free of provider-specific names, endpoints, model defaults, wire schemas, authentication schemes and branches. The core owns only provider-neutral credential custody (`DecideCredential`, endpoint validation). The actor only sees the `DecidePlugin` trait and an injected lookup function (`plugins::for_provider`).

## Message surface

| Request | Access | Reply |
|---|---|---|
| `StartServer` / `StopServer` | Public `decide.startServer` / `decide.stopServer` | `Success` |
| `AskBool` / `AskChoice` / `AskScore` | Public `decide.askBool` / `askChoice` / `askScore` | `Bool` / `Choice` / `Score { answer, model }` |
| `SupplyCredential` / `RemoveCredential` | Privileged `DecideRuntimeService` | `Success` |
| `StartNode` / `TickNode` / `CancelNode` | Privileged `DecideNodeLifecycleService` | `NodeStarted` / `NodeTicked` / `Success` |

Decide emits no broadcasts and consumes none. `DecideBroadcast` exists only because bus registration requires a broadcast type.

## Lifecycle

Lifecycle follows Tyto. The domain start and stop callbacks dispatch `StartServer` and `StopServer` through one `lifecycle()` helper using `dispatch_lifecycle_request`.

- The actor starts not running. Asks and node starts fail with `ShuttingDown` until `StartServer`.
- `StartServer` sets running and replaces a cancelled shutdown token, so the domain can restart.
- `StopServer` cancels in-flight asks (they reply `ShuttingDown`) and node tasks, and clears node completions. Supplied credentials are kept.
- Credentials are cleared only when the request channel closes. Credential supply and removal work whether or not the server is running.

## Actor

`handle.rs` constructs `Actor::new(receiver, plugins::for_provider)` and spawns `run_actor(actor)`. `handle_message` dispatches each request.

- **Asks run off the actor loop.** `handle_message` takes a `QuestionContext` snapshot (running flag, lookup, credential map copy, shared semaphore, child cancellation token) and spawns `handle_question`. That method routes `AskBool`/`AskChoice`/`AskScore` the way `handle_message` routes everything else.
- **One resolution path.** `QuestionContext::prepare` serves both asks and `StartNode`, in this order: running gate; question bounds; non-empty provider and model; timeout (absent or 0 → 300 s, above 3600 → `TimeoutInvalid`); provider lookup (`UnknownProvider`); credential snapshot (`CredentialUnavailable`); endpoint resolution; one of 32 permits (`Busy`).
- **Retry.** `aves::utils::retry::retry_with_backoff` makes at most 3 attempts with 10–60 s backoff. Only `ProviderRetryable` is retried; a provider `Retry-After` is honoured up to 60 s.
- **Cancellation.** A spawned ask races inference against the shutdown token and the caller's reply channel closing.

## Credentials and endpoints

The embedding application supplies credentials through privileged `SupplyCredential`, as in Tyto. A `DecideCredential` is a `Zeroizing` API key plus the endpoint it is authorized for. It is never serialized and its `Debug` output is redacted.

- At supply: the key must be non-blank and the endpoint must be HTTPS, or HTTP to a numeric loopback address. Userinfo, query and fragment are rejected.
- Ask with `credentialId`: the request goes to the bound endpoint. An `endpoint` override is allowed only if it canonically equals the binding; otherwise `EndpointUnauthorized`. This stops a caller sending a stored key elsewhere.
- Ask without a credential: an override may be any http/https URL with a host and no userinfo, query or fragment, or absent, in which case the plugin's default applies.

## Plugins

`plugins/mod.rs` defines `DecidePlugin::ask(&DecideAskRequest, Option<&DecideCredential>)`, a single HTTP attempt. `for_provider` maps `"jev"` and `"ollama"` to static plugins; any other id fails with `UnknownProvider` and there is no fallback.

Shared `post_json` posts to `{base}/v1/systemone`:
- Redirects are disabled; one overall timeout covers connect, send and body read.
- Responses over 4 MiB are rejected. Non-2xx bodies are never read.
- Errors use fixed messages carrying only the status code, never URLs, keys or provider text. `Retry-After` is parsed as integer seconds only.

`systemone.rs` builds the request envelope and strictly decodes the response. The response must contain exactly one answer under the request's question key, of the requested type. Choice probability keys must equal the criteria; score legend entries must equal the requested levels. All numbers must be finite and in range. Probabilities are not renormalized.

| | Jev (TypeSafe System One) | Ollama (Clef) |
|---|---|---|
| Credential | Required; absent → `AuthenticationRequired` before I/O | Optional; keyless default `http://localhost:11434` |
| Content | Rich content passed through | State and structured instructions sent as JSON text; criteria must be text |
| Instructions | Optional | Required |
| Bounds | choice 1–255, score 1–10 | choice 2–255, score 2–10 |
| Retryable statuses | 408, 429, 5xx | 429, 502, 503 |

Wire contracts are cached in `docs/external/typesafe-jev/decision-api.md` and `docs/external/ollama/systemone-decision-api.md`.

## Flow nodes

`handle.rs` pushes three registrations into the shared `NodeRegistrationSupply` before returning. Flow's handle drains the supply when `FlowDomain` is constructed, so Decide must be constructed first. No Flow-core changes are involved.

| Node kind | Config | Allowed output bindings |
|---|---|---|
| `DecideBoolNode` | `BoolNodeConfig` | `probability`, `model` |
| `DecideChoiceNode` | `ChoiceNodeConfig` | `choice`, `confidence`, `probabilities`, `model` |
| `DecideScoreNode` | `ScoreNodeConfig` | `score`, `confidence`, `legend`, `probabilities`, `model` |

- All three are cancellable and pure. Validation checks non-empty provider and model, plus question bounds.
- The only allowed input binding is `state`, which replaces the configured state and must parse as `DecideContent`.
- Start dispatches `StartNode`; the actor keeps a volatile completion receiver per node execution. Tick returns `Running`, `Succeeded` (bound outputs read by name) or `Failed` (serialized `DecideError`).
- Cancel dispatches `CancelNode`, then returns `Failed` carrying `NodeCancelled`, not `Cancelled`. This follows the AgentNode precedent.

## Boundaries and known limits

- Decide has no database, migrations, replay or recovery policy. Flow owns persistence and recovery. A crash before Flow checkpoints a node can re-ask the provider.
- Answers reach durable storage only as Flow node outputs. Recording them in an application's own records is the embedding application's job.
- Node callback wiring cannot be unit-tested from Corv: `NodeLifecycleContext.mcp_ports` (`FlowMcpPorts`) lives in a private Aves module. The logic is tested through the private helpers `start_with`, `tick_with` and `cancel_with`.
