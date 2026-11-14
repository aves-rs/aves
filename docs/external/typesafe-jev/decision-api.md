---
source: Official TypeSafe documentation + live OpenAPI 3.1.0 spec
library: Jev (TypeSafe System One)
package: typesafe (jev)
version: API spec 0.2.0; models jev-1.14.0 / jev-1.13.0
verified: 2026-11-14
official_docs: https://docs.typesafe.ai
---

# Jev decision-model API (TypeSafe System One)

Verified against the official docs and the live OpenAPI document
(`https://api.typesafe.ai/openapi.json`, `info.version: 0.2.0`, fetched 2026-11-14).
No schema below is inferred: every field, bound, and status code is quoted from those
sources.

## Authoritative sources

| Source | URL |
|---|---|
| Documentation root / index | https://docs.typesafe.ai |
| Full doc index (llms.txt) | https://docs.typesafe.ai/llms.txt |
| OpenAPI JSON | https://api.typesafe.ai/openapi.json |
| OpenAPI HTML | https://api.typesafe.ai/docs/ |
| System One endpoint reference | https://docs.typesafe.ai/api/system-one.md |
| List models reference | https://docs.typesafe.ai/api/list-models.md |
| Request rules and errors | https://docs.typesafe.ai/api/errors.md |
| Primitives overview | https://docs.typesafe.ai/primitives.md |
| Choice / Score / Noul / Box | https://docs.typesafe.ai/primitives/{choice,score,noul,box}.md |
| Confidence semantics | https://docs.typesafe.ai/confidence.md |
| Images | https://docs.typesafe.ai/images.md |
| Models, aliases, context, pricing | https://docs.typesafe.ai/models.md |
| Client SDKs | https://docs.typesafe.ai/sdk.md |
| Python SDK usage | https://docs.typesafe.ai/sdk/python/usage/basic.md |
| Python retry policy | https://docs.typesafe.ai/sdk/python/api/retries.md |
| Python exceptions | https://docs.typesafe.ai/sdk/python/api/exceptions.md |
| JS/TS client config | https://docs.typesafe.ai/sdk/javascript/api/interfaces/TypeSafeClientConfig.md |
| Console (keys, rate limits, playground) | https://console.typesafe.ai |
| Changelog / jaggedness | https://docs.typesafe.ai/changelog.md · https://docs.typesafe.ai/model-jaggedness.md |

Compatible third-party gateways documented by TypeSafe (same OpenAPI shape, different
base URL/key/model id): OpenRouter (`https://openrouter.ai/api`, model `~typesafe/jev-latest`),
Vercel AI Gateway (`https://ai-gateway.vercel.sh/typesafe`, model `typesafe-ai/jev`),
Pydantic AI Gateway (`https://gateway-us.pydantic.dev/proxy/typesafe`). OpenRouter also
publishes its own alpha Decisions endpoint (`POST /api/alpha/decisions`); that schema was
**not** verified here and must not be assumed identical to `api.typesafe.ai`.

Sites such as `systemonemodels.org`, `typesafe-jev.com`, `decisions-api.dev`, and
`aimlapi.com` are aggregators/unofficial; do not use them as contract authority.

## API surface

The OpenAPI document contains exactly two paths:

| Method | Path | Operation |
|---|---|---|
| `POST` | `/v1/systemone` | `systemone_v1_systemone_post` |
| `GET` | `/v1/models` | `models_v1_v1_models_get` |

Server: `https://api.typesafe.ai`. OpenAPI version 3.1.0, API version 0.2.0.

### Auth

- `security: HTTPBearer` (`type: http`, `scheme: bearer`), applied to both paths.
- Header: `Authorization: Bearer <API_KEY>`.
- Keys are created at https://console.typesafe.ai/keys. The Python SDK also reads
  `TYPESAFE_API_KEY`; it strips leading/trailing whitespace (including newlines from key
  files) and rejects empty keys, internal whitespace, control characters, and non-ASCII
  characters before sending. Missing/invalid key → `401`.
- No OAuth, project/tenant header, or per-request signing is documented.

## Request envelope (`SystemOneRequest`)

Required: `model`, `state`, `questions`. Optional: `assets`.

```jsonc
{
  "model": "jev-latest",          // string: model id or alias
  "state": "…",                    // string | object | array
  "questions": {                   // object, minProperties 1
    "<question_id>": { "type": "noul", "instructions": "…" }
  },
  "assets": {                      // optional object, minProperties 1
    "<reference>": { "type": "image", "format": "png", "data_base64": "…" }
  }
}
```

- `state` accepts a string, a JSON object, or an array (`x-accepts: string | object | array`).
  Text, nested JSON, and image references can be mixed.
- `questions` is a map keyed by caller-chosen ids; the response uses the same keys.
  The docs state question ids are **not** sent to the model.
- Every question object is discriminated by `type`, one of `noul`, `choice`, `score`, `box`.
- `instructions` accepts `string | object | array | null`. Only `type` is schema-required
  for Noul and Box; Choice and Score additionally require `criteria`.
- `criteria` nullability differs by type (verified in the OpenAPI schema): Noul's
  `criteria` is `anyOf [NoulCriteria, null]`, so `"criteria": null` is valid, and each
  side value may be `null`; Choice's `criteria` is required but every value may be `null`;
  Score's `criteria` is required and its **items cannot be null**; Box has no `criteria`
  property at all (an unknown `criteria` key is not declared).
- Content type: `application/json`, or `multipart/form-data` for images (below).

## Question types (exact schemas)

### Noul — yes/no probability

Question:

| Field | Required | Type | Notes |
|---|---|---|---|
| `type` | yes | `"noul"` | |
| `instructions` | no (schema) | string \| object \| array \| null | The yes/no question or statement |
| `criteria` | no | `{ "true": …, "false": … }` \| null | Each value string \| object \| array \| null |

Answer:

| Field | Required | Type | Notes |
|---|---|---|---|
| `type` | yes | `"noul"` | |
| `noul` | yes | number | Probability the answer is yes/true, 0–1; 0.5 = uncertain |

Semantics: probability of yes, not a degree scale. No separate `confidence` field.
Docs suggest starting thresholds 0.7 (yes) / 0.3 (no) and a middle review band.

### Choice — fixed choice

Question:

| Field | Required | Type | Notes |
|---|---|---|---|
| `type` | yes | `"choice"` | |
| `instructions` | no (schema) | string \| object \| array \| null | |
| `criteria` | **yes** | object, 1–255 properties | Keys are choice names; values are descriptions (`string \| object \| array \| null`). A bare name is interpreted by name alone |

Answer:

| Field | Required | Type | Notes |
|---|---|---|---|
| `type` | yes | `"choice"` | |
| `choice` | yes | string | Name of the highest-probability choice |
| `confidence` | yes | number | 0–1, derived from the distribution |
| `probabilities` | yes | object<string, number> | One entry per choice; sums to ~1 |

A Choice always returns one option, even if none fits. Docs recommend a companion Noul
("does any option fit?") instead of an `other`/`none` option.

### Score — ordered score

Question:

| Field | Required | Type | Notes |
|---|---|---|---|
| `type` | yes | `"score"` | |
| `instructions` | no (schema) | string \| object \| array \| null | |
| `criteria` | **yes** | array, 1–10 items | Ordered low→high; each item `string \| object \| array`. Position = level number starting at 0. Docs say "should have at least two levels; the API accepts up to 10" |

Answer:

| Field | Required | Type | Notes |
|---|---|---|---|
| `type` | yes | `"score"` | |
| `score` | yes | number | Probability-weighted mean of level numbers; may be fractional (e.g. 1.43) |
| `confidence` | yes | number | 0–1 |
| `legend` | yes | object<string, string\|object\|array> | Level number → supplied criteria |
| `probabilities` | yes | object<string, number> | Level number (string key) → probability; sums to 1 |

### Box — image box

Question:

| Field | Required | Type | Notes |
|---|---|---|---|
| `type` | yes | `"box"` | |
| `instructions` | no (schema) | string \| object \| array \| null | What to locate, and which image |

Box takes **no** `criteria`. Requires an image somewhere in the request and the
`TypeSafe-Danger-Zone-Images: true` header. Requires Jev 1.14 (not 1.13).

Answer:

| Field | Required | Type | Notes |
|---|---|---|---|
| `type` | yes | `"box"` | |
| `box` | yes | `{top,bottom,left,right}` | All numbers 0–1, fractions of height/width from the top-left; `left < right`, `top < bottom`. Coordinates are the expected value of each edge |
| `confidence` | yes | number \| **null** | Null when the model does not provide confidence |

A Box always returns a box; there is no "not found". Docs recommend pairing it with a
Noul that checks the target is present. One target per question.

## Response envelope (`SystemOneResponse`)

Required: `model`, `answers`, `usage`.

```jsonc
{
  "model": "jev-1.14.0",          // versioned id that answered; may differ from the alias sent
  "answers": {
    "<question_id>": { "type": "…", … }
  },
  "usage": { "input_tokens": 120, "output_tokens": 12 }
}
```

- `answers` has one entry per question, keyed by the same id; each answer's `type`
  matches the question's `type`.
- `usage.input_tokens` is billable; `output_tokens` is currently free.
- No `id`, `request_id`, `created`, `finish_reason`, `stream`, or error field appears in
  the response schema. The SDK exposes `x-typesafe-request-id` from response headers as
  `request_id` (Python `TypeSafeAPIError.request_id`).

## Confidence semantics (verified formulas)

- `confidence` is present on Choice, Score, and Box answers; **not** on Noul.
- It summarizes the returned probability distribution, on a 0–1 scale: 1 = all mass on
  one outcome; 0 = evenly spread. It is **not** the probability that the answer is
  correct.
- Choice: `confidence = (p_max − 1/n) / (1 − 1/n)`.
- Score: `confidence = max(0, 1 − Σ p_i·|i − m| / MAD_unif)`, where `m` is the most likely
  level and `MAD_unif = (1/n) Σ |i − (n−1)/2|`. Neighbouring-level mass lowers confidence
  less than distant-level mass.
- Noul: no `confidence`; the value itself is the probability. A confidence-style number
  can be computed as `|2p − 1|`.
- Box: confidence is model-provided, 0–1, may be null. No recommended threshold is
  published ("tune it on your own images").

## Image transport

Images are **off by default** and labelled "dreamer-ready" (not production-ready) by
TypeSafe. Every request containing an image or a Box question must send
`TypeSafe-Danger-Zone-Images: true` (string enum `"true"`); otherwise `400`.

Each image is listed once in top-level `assets`, keyed by an opaque reference string
(UUID recommended). Write the reference anywhere text can appear — `state`,
`instructions`, or `criteria` — as a whole string or inside a longer string; the model
receives the image in its place.

`Asset` fields:

| Field | Required | Notes |
|---|---|---|
| `type` | no | If present, const `"image"` |
| `format` | with `data_base64` | `"png" \| "jpg" \| "jpeg"`; actual format detected from bytes |
| `data_base64` | one of `data_base64`/`part` | Standard base64 alphabet with `=` padding, no whitespace, no `data:` prefix |
| `part` | one of `data_base64`/`part` | Multipart part name; if both are present, `part` wins |

Transport options:

1. `application/json` with base64 assets (≈33% larger; counts against the 32 MiB body).
2. `multipart/form-data` (recommended): first part named `request` with
   `Content-Type: application/json`; each image as its own part; part names unique,
   non-empty, `request` reserved; content type `image/png`, `image/jpeg`, `image/jpg`,
   `application/octet-stream`, or absent. Each part belongs to exactly one asset; unused
   parts, shared parts, and missing parts return `400`.

Reference rules:

- Every asset must be referenced somewhere or `400`.
- Exact whole-string values match first; longer strings replace left-to-right,
  non-overlapping.
- Object keys (question ids, Choice option names) are never replaced; put references in
  values/descriptions.
- Answers that echo criteria (e.g. Score `legend`) contain the reference, not the image.
- Max 1,024 UTF-8 bytes per reference; 65,536 bytes for all references in a request.
- Each use re-inserts the image into the model input and counts again toward limits.

Limits: PNG/JPEG still images only (animated rejected); 16 MiB per file after decode;
each side 280–1568 px; total pixels 78,400–2,458,624; request body 32 MiB; all distinct
images 32 MiB after processing; expanded request 32 MiB after reference substitution.
Images are re-encoded, metadata (EXIF/ICC) stripped, and EXIF orientation is **not**
applied — rotate pixels before sending. Image tokens are billed as input tokens; exact
image-token estimation guidance is "coming soon".

## Model selection

| Model | ID | Question types | Context |
|---|---|---|---|
| Jev 1.14 | `jev-1.14.0` | Choice, Score, Noul, **Box** | 64k per request / 32k per question |
| Jev 1.13 | `jev-1.13.0` | Choice, Score, Noul | 64k per request / 32k per question |

- Aliases: `jev-latest` → `jev-1.14.0`; `jev-preview` → `jev-1.14.0` (no preview build
  currently). SDK defaults to `jev-latest`; `TYPESAFE_DEFAULT_MODEL` overrides.
- `model` is required in the raw HTTP request; the response's `model` is the versioned id
  that answered.
- Price: $42 per billion input tokens / $0.042 per million; output tokens free.
- Long context: send `TypeSafe-Danger-Zone-Long-Context: true` per request to raise both
  limits to 262k tokens; slower and less accurate, off by default.
- No fine-tuning/LoRA or per-account weights; behaviour is shaped by `state`,
  `instructions`, `criteria`.
- `GET /v1/models` returns `{ "models": [ { name, description, release_date } ] }`;
  `release_date` is `YYYY-MM-DD`. It lists aliases; versioned ids are accepted even if
  absent.

## Errors, limits, rate limits

Request rules: body ≤ 32 MiB; UTF-8 JSON without BOM; ≤ 64 nesting levels in each of
`state`, `instructions`, `criteria`; finite numbers only (`NaN`, `Infinity`, `1e400`
rejected). Compression: one layer of `gzip`/`x-gzip` or `zstd` (zstd window ≤ 8 MiB);
32 MiB applies to compressed and decompressed bodies.

Declared vs documented statuses: the OpenAPI document declares only `200`, `400`, and
`422` responses on `/v1/systemone` (`200`/`400`/`422` on `/v1/models`). `401`, `413`,
`415`, `429`, and `529` are documented in `errors.md` but do **not** appear in the spec's
`responses` map — a generated client sees no typed error model for them.

| Status | Meaning |
|---|---|
| `400` | Request-rule violation, malformed body, image/asset rejection, missing danger-zone header, or `max_tokens_exceeded` (context length) |
| `401` | Missing/invalid API key |
| `413` | Body over 32 MiB or an image/expanded-request size limit |
| `415` | Unsupported `Content-Encoding` or more than one layer |
| `422` | Schema validation failure (`HTTPValidationError`) |
| `429` | Rate limit exceeded |
| `529` | TypeSafe temporarily overloaded |

- `400` body: `{ "detail": string | object }` (`HTTPBadRequestError`); string/message
  fields may be truncated with `…`.
- `422` body: `{ "detail": [ { loc: (string|integer)[], msg: string, type: string,
  input?: string, ctx?: object } ] }` (`HTTPValidationError` / `ValidationError`).
- SDKs additionally map `403` permission denied, `404` not found, and generic `5xx`
  internal server error.
- Rate limits are per organisation, measured in tokens/second and requests/second,
  visible at https://console.typesafe.ai/rate-limits; overage → `429`.

## SDK behaviour (documented)

Python: package `typesafe-sdk` (Python ≥3.10); `TypeSafeClient` and
`AsyncTypeSafeClient`; typed primitives `Noul`, `Choice`, `Score`, `NoulCriteria`,
`Image`; `response.nouls` / `.choices` / `.scores` / `.answers`.

Environment variables: `TYPESAFE_API_KEY` (required), `TYPESAFE_BASE_URL` (default
`https://api.typesafe.ai`), `TYPESAFE_DEFAULT_MODEL` (default `jev-latest`),
`TYPESAFE_LOG_LEVEL`.

Python `RetryPolicy` defaults: `max_retries=2` (3 total attempts), `backoff_initial=0.5 s`
doubling to `backoff_max=5.0 s`, `backoff_jitter=0.25`, retry statuses `{408, 429, 500–599}`,
honours `Retry-After`/`retry-after-ms`, retries connection and timeout errors, total retry
budget `timeout=30.0 s` per SDK call.

JavaScript/TypeScript: package `@typesafe-ai/sdk` (Node ≥20; Node ≥22.18 to run `.ts`
directly); `TypeSafeClient`, helpers `choice`, `noul`, `score`, `box`, `image`,
`withAssets`. Client config: `apiKey`, `baseURL`, `defaultModel`, `timeout` (per attempt,
ms, default `10000`), `retry`, `compression` (default `zstd` on Node ≥22.15, bodies
<1 KiB always uncompressed), `defaultHeaders`, `fetch`, `logger`, `logLevel`,
`dangerouslyAllowBrowser` (default false). Response exposes `request_id` via the
`x-typesafe-request-id` header.

## Async behaviour

There is **no documented asynchronous, batch, job, webhook, or streaming surface**.
The OpenAPI document contains only the two synchronous request/response endpoints above.
The Python `AsyncTypeSafeClient` is an asyncio wrapper over the same `POST /v1/systemone`;
concurrency is client-side. No idempotency key, deduplication, or server-side request
timeout is documented.

## Mapping to the Corv Decide PRD

| PRD question type | Jev type | Answer |
|---|---|---|
| yes/no probability | `noul` | `noul` 0–1, no `confidence` |
| fixed choice | `choice` | `choice`, `confidence`, `probabilities` (1–255 options) |
| ordered score | `score` | `score` (fractional), `legend`, `confidence`, `probabilities` (1–10 levels) |
| image box | `box` | `box{top,bottom,left,right}`, `confidence` (nullable) |

Constraints that matter for the domain model: Box requires Jev 1.14 plus an image and the
danger-zone header; image/Box support is explicitly labelled "dreamer-ready" and may
change between releases; a Box never reports absence.

## Unknowns / not documented

- Key format, prefixes, scopes, rotation, and multi-key behaviour.
- Server-side request timeout, maximum queue time, and whether retries can duplicate work.
- Numeric default rate limits (only that they are per-org and shown in the console).
- Exact image token accounting (docs say guidance is coming).
- Behaviour when `instructions` is omitted on Noul/Box although the schema allows it.
- Maximum number of questions (docs say none beyond context/body limits) and maximum
  response size.
- Box confidence calibration and any recommended threshold.
- Whether `probabilities` and `legend` keys are guaranteed in criteria order (JSON object
  order is not semantically guaranteed).
- Any SLA/availability contract or data-retention configuration beyond "not trained on
  customer data; ZDR for enterprise" in the legal pages.
