---
source: Official Ollama documentation + embedded OpenAPI 3.1.0 + ollama.com model pages + GitHub release notes
library: Ollama (System One decision API — Clef)
package: ollama
topic: systemone-decision-api
fetched: 2026-11-14
official_docs: https://docs.ollama.com/api/systemone
---

# Ollama System One decision API (`POST /v1/systemone`) — Clef

Decision-model endpoint for classification (`choice`), yes/no probability (`noul`), and ordered
scoring (`score`). There is **no Box question type**. The API is explicitly derived from
TypeSafe's Jev API: the v0.35.0 release note says the endpoint is "based on
[TypeSafe's Jev API](https://typesafe.ai)".

Verified 2026-11-14 from the pages below. Every field, bound, status code and version number
here is quoted from those sources; conflicts between pages are listed rather than resolved.

## Authoritative sources

| Source | URL |
|---|---|
| System One endpoint reference (embeds the OpenAPI operation + schemas) | https://docs.ollama.com/api/systemone.md |
| Decision capability guide (examples, limits, cloud usage) | https://docs.ollama.com/capabilities/decision.md |
| API introduction (base URLs, versioning) | https://docs.ollama.com/api/introduction.md |
| Authentication | https://docs.ollama.com/api/authentication.md |
| Cloud API keys | https://docs.ollama.com/cloud/api.md |
| Cloud models (`clef:cloud` guidance) | https://docs.ollama.com/cloud/models.md |
| Errors | https://docs.ollama.com/api/errors.md |
| Local FAQ (context default, queueing, 503) | https://docs.ollama.com/faq.md |
| Cloud FAQ (ZDR, key vs public key) | https://docs.ollama.com/cloud/faq.md |
| Pricing (concurrency, clef price) | https://ollama.com/pricing |
| Model page — clef | https://ollama.com/library/clef · https://ollama.com/library/clef/tags |
| Model page — clef-flash | https://ollama.com/library/clef-flash · https://ollama.com/library/clef-flash/tags |
| Decision model index | https://ollama.com/search?c=decision |
| Release note introducing the endpoint | https://github.com/ollama/ollama/releases/tag/v0.35.0 |
| Release note launching clef in the cloud | https://github.com/ollama/ollama/releases/tag/v0.40.6 |
| Full OpenAPI index entry | https://docs.ollama.com/openapi.yaml (`info.version: 0.1.0`) |

## Endpoint and base URLs

| Route | Local | Direct cloud |
|---|---|---|
| System One | `POST http://localhost:11434/v1/systemone` | `POST https://ollama.com/v1/systemone` |
| Model listing (adjacent) | `GET /api/tags`, `POST /api/show` | `GET https://ollama.com/api/tags` |

- OpenAPI `servers` lists exactly two entries for this operation: `http://localhost:11434`
  and `https://ollama.com`. Spec is OpenAPI 3.1.0, `info.version: 0.1.0`; the local server binds
  `127.0.0.1:11434` by default (`OLLAMA_HOST` overrides).
- "Ollama's API isn't strictly versioned, but the API is expected to be stable and backwards
  compatible."
- `:cloud` suffix is the *local server* indirection: `clef:cloud` goes through
  `localhost:11434` to ollama.com using the signed-in public key. Direct cloud calls use the
  bare model name `clef`.

## Auth

- **Local: none.** "The local API at `http://localhost:11434` does not require authentication."
  A supplied `Authorization` header is ignored — the Clef page states the TypeSafe SDK "requires
  an API key, but Ollama ignores it, so any value works".
- **Direct cloud: `Authorization: Bearer $OLLAMA_API_KEY`.** Keys are created at
  https://ollama.com/settings/keys, "do not expire", and are revoked in settings. No scopes,
  project/tenant header, or request signing is documented.
- **Cloud via local server:** `ollama signin` (Ollama public key), requires a paid plan or
  purchased usage credits. `OLLAMA_NO_CLOUD=1` / `{"disable_ollama_cloud": true}` disables it.
- OpenAPI declares the operation as `security: [{}, {bearerAuth: []}]` with
  `bearerAuth: {type: http, scheme: bearer, bearerFormat: API Key}` — i.e. auth is **optional**
  at the schema level; the 401 description is "A cloud model was requested without signing in,
  or with an invalid API key. Through a local server, the response includes a `signin_url`".

## Request (`SystemOneRequest`)

Required: `model`, `state`, `questions`. Optional: `images`, `keep_alive`.

```jsonc
{
  "model": "clef",                     // pattern \S; "Model trained for System One"
  "state": "…",                         // nonempty string | object | array
  "images": ["<base64>"],               // optional; shared by all questions
  "questions": {                        // minProperties 1, maxProperties 64
    "<question_id>": { "type": "choice" | "noul" | "score", "instructions": "…", "criteria": … }
  },
  "keep_alive": "5m"                    // optional; string duration or number of seconds
}
```

| Field | Required | Type / bounds | Notes |
|---|---|---|---|
| `model` | yes | string, `pattern: \S` | "Model trained for System One, such as nimble, or clef:cloud for Ollama's cloud." |
| `state` | yes | `SystemOneContent` | Nonempty string, **or** object/array serialized as JSON text. "Not interpreted as chat messages or multimodal input." No `null`. |
| `images` | no | array of base64 strings (`format: byte`) | "Base64-encoded images shared by all questions, in request order. Requires a model with vision, such as Clef. … URLs and data URLs are not supported." No multipart, no per-question images, no asset references. |
| `questions` | yes | object, 1–64 properties, names `pattern: \S` | "Answers are not passed to later questions." Cloud names: ≤100 chars of ASCII letters, digits, `_`, `.`, `-`. |
| `keep_alive` | no | string \| number | Local residency only (Clef page: "Optional, local only"). Duration string such as `5m`, seconds, `0` unloads after the request, negative keeps loaded. Defaults to server setting (`OLLAMA_KEEP_ALIVE`, 5m). |

### `choice`

| Field | Required | Bounds |
|---|---|---|
| `type` | yes | `"choice"` |
| `instructions` | **yes** | `SystemOneContent` |
| `criteria` | yes | object, `minProperties: 2`, `maxProperties: 255`, names `pattern: \S`, values `string \| null` |

- `null` value = "let the option name describe itself" (bare label).
- Option count is **model-dependent**: Clef page says "2 to 26 options locally, up to 255 in the
  cloud"; guide and clef-flash page say 2–26. Ties follow the model's option order.
- Unlike Jev, criterion *values* cannot be objects/arrays — string or null only.

Answer `SystemOneChoiceAnswer`: required `type`, `choice`, `probabilities`, `confidence`.

### `noul`

| Field | Required | Bounds |
|---|---|---|
| `type` | yes | `"noul"` |
| `instructions` | **yes** | `SystemOneContent` |
| `criteria` | no | object, only keys `"false"` and `"true"`, each a string; `additionalProperties: false` |

Answer `SystemOneNoulAnswer`: required `type`, `noul` (number 0–1, "Probability of true among the
false and true candidates. This is a number, not a Boolean"). No `confidence`.

### `score`

| Field | Required | Bounds |
|---|---|---|
| `type` | yes | `"score"` |
| `instructions` | **yes** | `SystemOneContent` |
| `criteria` | yes | array of **strings**, `minItems: 2`, `maxItems: 10`, index 0 = lowest |

Answer `SystemOneScoreAnswer`: required `type`, `score`, `legend`, `probabilities`, `confidence`.

| Answer field | Type | Meaning |
|---|---|---|
| `score` | number 0–9 | Probability-weighted average of zero-based indices. "Not rounded to a level or normalized to 0–1." |
| `legend` | object<string, string> | Zero-based indices → criterion description (strings only) |
| `probabilities` | object<string, number> | Keys are `"0"`…`"n-1"` |
| `confidence` | number 0–1 | Entropy concentration (below) |

## Response (`SystemOneResponse`)

Required: `model`, `answers`, `usage`.

```jsonc
{
  "model": "clef",                     // model name from the request (echo, not a versioned id)
  "answers": {
    "<question_id>": { "type": "choice", "choice": "billing",
                       "probabilities": {"billing": 0.9888, "technical": 0.0044, "other": 0.0068},
                       "confidence": 0.9667 }
  },
  "usage": { "input_tokens": 332, "output_tokens": 0 }
}
```

- `answers` is keyed by the request's question names; one entry per question, `type` matches.
- `usage.input_tokens`: "Total evaluated input tokens, including image positions. Shared context
  is counted again when the model scores questions separately."
- `usage.output_tokens`: "Tokens generated internally for scoring, including prefix preparation
  and retries. May exceed the question count; not the length of the JSON response."
- No `id`, `request_id`, `created`, `finish_reason`, `stream`, or error field. No documented
  response header carrying a request id.

### Confidence semantics (differs from Jev)

`confidence = 1 - H(p) / ln(N)`, where `H(p)` is entropy and `N` the candidate count. 0 = uniform,
near 1 = one candidate dominates. "Not calibrated correctness."

This is **not** Jev's formula. For the same distribution Jev computes
`(p_max − 1/n)/(1 − 1/n)`. Ollama's own example (`0.9781 / 0.0125 / 0.0093`, `confidence 0.8906`)
would be `0.9672` under Jev's Choice formula. Any stored or compared threshold must be
model-family specific.

## Versions

| Capability | Minimum |
|---|---|
| `/v1/systemone` (any decision model, local) | Ollama **v0.35.0** |
| Clef / Clef Flash locally, incl. images | Ollama **v0.35.1** |
| Clef in Ollama's cloud via local server (`clef:cloud`) | **v0.40.5** per docs pages, **v0.40.6** per the Clef model page; the v0.40.6 release note is "Launch clef in Ollama's cloud", so treat **0.40.6** as the real floor |
| Ollama Python library `ollama.systemone` | `ollama` ≥ 0.6.3 (Clef page); the clef-flash page still says decision models "aren't in the Ollama CLI or the Ollama Python and JavaScript libraries yet" — conflicting, do not depend on the client wrappers |

## Models

| Model | Local tags (context / size / input) | Cloud | Question support |
|---|---|---|---|
| Clef (Cloudflare, 27B, Apache-2.0) | `clef:latest`, `clef:27b` — 256K, 18–20 GB, text+image | `clef:cloud`, `clef:27b-cloud` (64K); direct `clef` | choice, noul, score; images |
| Clef Flash (Cloudflare, 9B, Apache-2.0) | `clef-flash:latest`, `clef-flash:9b` (256K, 11–12 GB), `:9b-mlx-bf16`, `:9b-mxfp8`, `:9b-q8_0` | **no cloud tag published** | choice, noul, score; images |
| Other documented decision models | `nimble`, `nimble-v3`, `tev1` | — | choice, noul, score |

- Clef model page: "Clef requires Ollama 0.35.1 or later"; "Clef is a 27B multimodal model
  created by Cloudflare … fine-tuned from Qwen3.8-27B"; "64K context window: Twice the state
  Jev's 32K window holds" (marketing copy) while the tag table lists **256K** locally and **64K**
  for `clef:cloud`. Cloud's hard limit is enforced as 65,536 input tokens.
- Cloud pricing lists `clef` at **$0.24 / M input tokens**, no cached-input and no output price
  (usage is "billed per input token"). Concurrency: 3 requests on Pay-as-you-go and Pro, 10 on
  Max and Team.
- `ollama create` accepts `"capabilities": ["decision"]` for compatible custom models, so
  "decision" is a model capability name; the `/api/show` response carries a `capabilities`
  array.

## Limits

| Limit | Local | Cloud |
|---|---|---|
| Request body | 32 MiB ("including base64 and JSON") | 13 MiB |
| Text + schema excluding images | 64 KiB | (covered by 13 MiB + token limit) |
| Context | "The complete input must fit the loaded context window." Default server context is **4096 tokens** (`OLLAMA_CONTEXT_LENGTH`); Clef tags advertise 256K. Nimble and Tev additionally "require two token positions for scoring". | 65,536 input tokens; longer input "is rejected, not truncated" |
| Questions | 64 (`maxProperties`) | 64 |
| Question names | nonblank (`\S`) | ≤100 chars `[A-Za-z0-9_.-]` |
| Images | up to request-size limits | ≤4 images, each ≤4 MiB and ≤16 MP, 8 MiB total, PNG/JPEG/WebP |
| Truncation | "Input is never truncated." | "Longer input is rejected, not truncated." |

## Errors, timeout, cancellation

Error body is always `{"error": "<message>"}`.

| Status | System One meaning |
|---|---|
| `400` | "Invalid request, unsupported model or runner, or a rendered prompt that exceeds the loaded context or a cloud model's token limit"; also generic bad request/missing parameters |
| `401` | Cloud model without sign-in / invalid key; via local server the body includes `signin_url` |
| `402` | Cloud model not in the Free plan, no credits, or payment failure |
| `404` | Model not found — "Download a local model before making a request" |
| `413` | Local text+schema > 64 KiB or request > 32 MiB; cloud request > 13 MiB or over context. Example body: `{"error":"text and schema must not exceed 64 KiB"}` |
| `429` | Rate limit exceeded (generic errors page; no numeric cloud rate limit is published) |
| `500` | "Model loading, rendering, or scoring failed." |
| `502` | Bad gateway, "e.g. when a cloud model cannot be reached" (generic errors page) |
| `503` | Local overload: "If too many requests are sent to the local Ollama server, it responds with a 503 overloaded error" (`OLLAMA_MAX_QUEUE`, default 512; `OLLAMA_NUM_PARALLEL` default 1; `OLLAMA_MAX_LOADED_MODELS` default 3×GPUs) |

- **No 422.** Schema violations are `400`; there is no `HTTPValidationError` shape.
- **No streaming**: "Returns one JSON response; streaming, video, tools, and generation controls
  are not supported." No `stream` field, no NDJSON.
- **Timeout**: no server-side request timeout, queue deadline, or retry contract is documented.
  Cloud requests beyond the plan's concurrency are queued, and "queued requests are held up to a
  fixed limit — if the queue is full, the request will be rejected" (status not specified).
- **Cancellation**: no cancellation, job, batch, or idempotency surface is documented. The only
  client-side lever is closing the HTTP connection / aborting the request; whether that stops
  the forward pass is not stated. `keep_alive` controls residency, not cancellation. Treat
  client timeouts as the only bound and expect long first-token latency on model load (the
  TypeSafe SDK examples pass `timeout=120`).
- **Retry safety**: no idempotency key exists. Choice/score/noul are read-only scorings, so a
  duplicate call is harmless but billable (cloud bills input tokens per attempt).

## Differences from TypeSafe Jev System One

| Aspect | Ollama `/v1/systemone` | Jev (`https://api.typesafe.ai/v1/systemone`) |
|---|---|---|
| Question types | `choice`, `noul`, `score` | `choice`, `noul`, `score`, **`box`** |
| `instructions` | **required** on all three types | optional (schema) on all four |
| Choice criteria | 2–255 (spec); 2–26 common; values `string \| null` | 1–255; values `string \| object \| array \| null` |
| Score criteria | array of **strings**, 2–10 | array, 1–10; items `string \| object \| array` |
| Noul criteria | only `{"false": string, "true": string}` | `{"true": …, "false": …}`, rich values |
| `confidence` | `1 − H(p)/ln N` on choice and score | choice `(p_max − 1/n)/(1 − 1/n)`; score uses MAD distance; box model-provided |
| Score answer | `score` 0–9, `legend` strings, `probabilities` keyed `"0".."n-1"` | same shape, rich `legend` values |
| Response `model` | echoes the request's model string | versioned id that answered (`jev-1.14.0`) |
| Request id | none documented | `x-typesafe-request-id` header |
| Images | top-level `images` array, base64 only, shared by all questions, model must be vision-capable | top-level `assets`, references substituted into text, JSON or multipart, danger-zone header required |
| Extra headers | none | `TypeSafe-Danger-Zone-Images`, `TypeSafe-Danger-Zone-Long-Context` |
| Body limit | 32 MiB local / 13 MiB cloud | 32 MiB always |
| Context | model window (Clef 256K local, 64K cloud); server default 4096 tokens | 64k request / 32k question; 262k with long-context header |
| Errors | 400/401/402/404/413/500 (+429/502/503) with `{"error": string}` | 400/401/413/415/422/429/529 with typed `detail` objects |
| Auth | none locally (header ignored); bearer key for `ollama.com`, optional in spec | always bearer key |
| Streaming/tools | none (explicitly) | none documented |
| Endpoint base | `localhost:11434` or `https://ollama.com` | `https://api.typesafe.ai` |
| Billing basis | cloud input tokens only ($0.24/M for clef) | input tokens ($42/B ≈ $0.042/M); output free |
| Aliases | none documented; `:cloud` is a local-server routing suffix | `jev-latest`, `jev-preview` |

Practical consequence for a shared abstraction: request/response shapes are close enough for one
`choice`/`noul`/`score` DTO pair, but (a) `instructions` must always be sent, (b) score criteria
must be plain strings, (c) choice criteria values must be strings or null, (d) confidence values
are not comparable across providers, (e) model ids are not versioned, and (f) Box must be
Jev-only.

## Unknowns / not documented

- Server-side request timeout, maximum queue wait, and whether an aborted HTTP request stops
  inference.
- Whether `probabilities` and `legend` keys preserve criteria order (JSON object order).
- Behaviour when `images` is sent to a non-vision decision model (400 "unsupported model or
  runner" is the likely, unconfirmed path).
- Whether unknown request fields are rejected or ignored.
- Which numeric rate limits apply to cloud (only concurrency counts are published).
- Whether `clef-flash` will get a `:cloud` variant; today only `clef` is cloud-enabled and listed
  in pricing.
- Exact per-image token accounting for Clef.
- Whether `GET /v1/models` exists on Ollama; model discovery is documented via `/api/tags`.
