---
title: Using the Decide Domain
type: guide
updated: 2026-11-14
references:
  - crates/corv/src/lib.rs
  - crates/corv/src/decide/domain.rs
  - crates/corv/src/decide/messages.rs
  - crates/corv/src/decide/types.rs
  - crates/corv/src/decide/actor.rs
  - crates/corv/src/decide/nodes.rs
  - crates/corv/src/decide/plugins/mod.rs
  - crates/corv/src/decide/plugins/jev.rs
  - crates/corv/src/decide/plugins/ollama.rs
  - crates/tyto/AGENTS.md
---

# Using the Decide Domain

This guide shows an embedding application how to install Decide, supply credentials, ask questions, and use the three Flow nodes. For the design, see [Decide Domain Architecture](../architecture/corv/decide-domain.md).

## 1. Install the domain

Decide is exported from the Corv crate root: `DecideDomain`, `DecideBroadcast`, `DecideError`, `DecideRuntimeService`, `SupplyCredentialInput`, `RemoveCredentialInput`, `DecideCredential`, `DecideContent`, `DecideSuccessOutput`, the question and answer types, `AskBoolInput`/`AskChoiceInput`/`AskScoreInput`, `BoolOutput`/`ChoiceOutput`/`ScoreOutput`, `dispatch_ask_*`, `decide_operation_descriptors_with_root_error` and `decide_rpc_router`.

- Register `DecideDomain` like any other domain. The root broadcast type must implement `From<DecideBroadcast>`. `DecideBroadcast` has no variants, so the conversion is an empty `match`.
- Construct one `NodeRegistrationSupply::default()` and thread it through `ConstructionInputs`. Decide's construction panics if it is absent.
- Construct Decide **before** `FlowDomain`. Flow drains the supply when it is constructed, so later registrations are never seen.
- Mount `decide_rpc_router` and the operation descriptors with your root error, as for other domains (see [Aves Domain RPC](aves-domain-rpc.md)).
- Decide needs no Config section and no database.

The runtime start and stop callbacks call `decide.startServer` and `decide.stopServer` for you. Until start runs, asks fail with `shutting_down`.

## 2. Supply credentials

Credentials follow the Tyto pattern: the application owns secret storage and pushes secrets in through the privileged `DecideRuntimeService`, obtained with a `ServiceReader`. They are never sent over public RPC and are held only in memory.

- `supply_credential(SupplyCredentialInput { credential_id, credential: DecideCredential { api_key, endpoint } })` stores or replaces a key. `api_key` is a `Zeroizing<String>`. `endpoint` is the only URL the key may be sent to, for example `https://api.typesafe.ai`. It must be HTTPS, or HTTP to a numeric loopback address such as `http://127.0.0.1:11434`.
- `remove_credential(RemoveCredentialInput { credential_id })` deletes it.
- `StopServer` keeps credentials; they are lost when the process exits. Supply them again on each boot.

Local keyless Ollama needs no credential.

## 3. Ask a question

Public mutations take these fields (camelCase on the wire):

| Field | Meaning |
|---|---|
| `provider` | `"jev"` or `"ollama"`. Anything else fails with `unknown_provider`. |
| `model` | Provider model name, for example a Clef model on Ollama. |
| `state` | `DecideContent`: a string, JSON object, or JSON array describing the situation. |
| `question` | `BoolQuestion`, `ChoiceQuestion` or `ScoreQuestion`. |
| `credentialId` | Optional. Required for Jev. |
| `endpoint` | Optional. With a credential it must equal the credential's endpoint; without one, any http/https URL with a host and no userinfo, query or fragment. |
| `timeoutSecs` | Per attempt. Absent or 0 means 300; maximum 3600. |

Question shapes:

```json
{ "instructions": "Is the build green?", "trueDescription": "All checks pass", "falseDescription": null }
{ "instructions": "Pick the owner", "criteria": { "corv": "Discuss/Decide", "kea": null } }
{ "instructions": "Rate the risk", "criteria": ["low", "medium", "high"] }
```

Replies:
- `askBool` → `{ answer: { probability }, model }`
- `askChoice` → `{ answer: { choice, confidence, probabilities }, model }`
- `askScore` → `{ answer: { score, confidence, legend, probabilities }, model }`

`model` is the model the provider reports, which can differ from the one requested. `probability` is the chance of true, not a yes/no; apply your own threshold. `score` is fractional (for example 1.37 between "medium" and "high").

Retryable provider failures are retried up to 3 attempts in total, so a slow provider can take several timeouts to fail.

### Provider rules

- **Jev:** a credential is required. Rich `DecideContent` is passed through. Choice allows 1–255 criteria; score allows 1–10 levels.
- **Ollama/Clef:** without a credential, requests go to `endpoint` or `http://localhost:11434`; with one, a bearer key is sent to the bound endpoint (Ollama cloud). `instructions` is required. Object or array state and instructions are sent as JSON text. Criteria and descriptions must be plain strings. Choice needs 2–255 criteria; score needs 2–10 levels. Violations fail with `invalid_question` before any network call.

### Errors

Errors carry domain `decide` and a stable code. Commonly handled codes:

| Code | Cause |
|---|---|
| `unknown_provider`, `invalid_question`, `validation`, `timeout_invalid` | Bad request; do not retry. |
| `credential_unavailable`, `authentication_required`, `endpoint_unauthorized` | Credential missing, required, or used against a different endpoint. |
| `busy` | 32 questions already in flight. |
| `provider_retryable` | Still failing after retries; may succeed later. |
| `provider_request_failed`, `provider_response_invalid` | Provider rejected the request, or returned an answer that did not match the question. |
| `shutting_down` | Server not started, or stopped during the ask. |

## 4. Use the Flow nodes

Three node kinds are registered: `DecideBoolNode`, `DecideChoiceNode` and `DecideScoreNode`. The configuration matches the kind:

```json
{
  "provider": "ollama",
  "model": "<clef-model-name>",
  "state": "Ticket summary goes here",
  "question": { "instructions": "Rate the risk", "criteria": ["low", "medium", "high"] },
  "timeoutSecs": 120
}
```

- `credentialId` and `endpoint` are optional, as for asks. Unknown fields are rejected.
- Bind an input named `state` to feed upstream data in; it replaces the configured `state`. Any other input name fails the start.
- Output bindings may use only the kind's fields: bool `probability`, `model`; choice `choice`, `confidence`, `probabilities`, `model`; score `score`, `confidence`, `legend`, `probabilities`, `model`.
- A provider error fails the node with the serialized `DecideError` as its state. Cancelling also ends the node `Failed` with `node_cancelled`.
- Flow persists node outputs. Decide stores nothing, so a crash before Flow saves an answer may ask the provider again.

## 5. Test against Decide

- Never call live providers or use real keys in automated tests.
- Answer-decoding tests are pure unit tests: feed provider-shaped JSON to the decoder and assert every field with non-symmetric values.
- Actor tests inject a mock `DecidePlugin` through the provider lookup function.
- Node logic is tested through the context-free helpers in `decide/nodes.rs`, because downstream crates cannot build a `NodeLifecycleContext`.
