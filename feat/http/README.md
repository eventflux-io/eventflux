# HTTP Connector Specification for EventFlux

Status: **IMPLEMENTED (2026-07-16)** — dual-mode source + sink, factories, gating,
self-hosting integration tests, example, docs. Deferred items in the table below;
request/response mode tracked as eventflux-io/eventflux#134.
Design issue: eventflux-io/eventflux#133 · ROADMAP Priority 2

This document specifies the HTTP source (REST polling + webhook listener) and HTTP sink
(webhooks with retry), following the patterns established by the Kafka connector
(`feat/kafka/README.md`, #118) and the connector infrastructure (#115–#117).

## Overview

- **HTTP Source, poll mode**: periodically GETs a URL and feeds each response body into the
  pipeline
- **HTTP Source, webhook mode**: runs a lightweight HTTP listener; each accepted POST body
  becomes a payload
- **HTTP Sink**: POSTs each event's formatted payload to a URL, with exponential-backoff retry

## Architecture — best tool per side

Client-side work (sink, polling) is simple sequential I/O → a blocking client keeps it
trivially correct. Server-side work (webhook listener) genuinely benefits from async →
a production-grade server with built-in concurrency, chosen deliberately for the future
(middleware, HTTP/2, TLS, and the request/response mode sketched below all slot in without
re-architecting):

| Component | Crate | Model |
|-----------|-------|-------|
| Sink + polling source client | `ureq` 3 (rustls) | Blocking requests with per-request timeout |
| Webhook listener | `axum` (hyper/tower, on the already-present tokio) | Concurrent async handlers |

```
Poll source:    SourceWorker thread → ureq GET every interval → body bytes → SourceCallback → mapper
Webhook source: SourceWorker thread → tokio runtime → axum server (concurrent handlers)
                  → per request: spawn_blocking → SourceCallback → mapper
Sink:           per event: mapper → bytes → ureq request (+ bounded retry) → endpoint
```

- `ureq` with **rustls** keeps the build hermetic (no system TLS), consistent with the
  `ssl-vendored`/`curl-static` decisions in the Kafka build; tokio is already an
  unconditional core dependency, so axum adds no runtime that wasn't there
- The webhook worker follows the RabbitMQ/WebSocket source pattern: `SourceWorker` thread
  owns a tokio runtime; shutdown is axum's `with_graceful_shutdown`, driven by a small async
  poll of the worker's running flag (≤100ms) — in-flight requests complete, `stop()` stays
  synchronous
- Handlers deliver payloads via `tokio::task::spawn_blocking` (junction publish can block on
  backpressure; runtime workers must not); the shared `error_ctx` is mutex-wrapped since
  handlers run concurrently
- `tiny-http` was considered and rejected: fine for a toy listener, but concurrency,
  limits, and everything on the future list would have to be hand-rolled onto it

## Feature Gating

Per the checklist in `docs/writing_extensions.md`:

```toml
[features]
http = ["dep:ureq", "dep:axum"]
connectors-all = ["http", "kafka", "rabbitmq", "websocket"]
```

Both crates are pure Rust — no native toolchain requirement — but stay out of the default
build per the fully-minimal rule. All gating points apply (module decls, registration,
`GATED_OUT_EXTENSIONS`, CI per-connector check, justfile, docs).

## HTTP Source

One extension (`extension = 'http'`), two explicit modes.

### Common configuration

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `http.mode` | Yes | - | `poll` or `webhook` |
| `error.*` | No | - | Standard error-handling strategies (drop/retry/dlq/fail) via the shared helpers |

### Poll mode

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `http.url` | Yes | - | URL to poll (http/https) |
| `http.poll.interval.ms` | No | `5000` | Time between polls (interruptible via `sleep_while_running`) |
| `http.method` | No | `GET` | `GET` or `POST` (some APIs poll via POST) |
| `http.headers.<Name>` | No | - | Request headers, e.g. `"http.headers.Authorization" = 'Bearer ${TOKEN}'` |
| `http.timeout.ms` | No | `30000` | Per-request timeout (connect + response) |

**Poll loop semantics:**

- 2xx with a non-empty body → one payload delivered through
  `deliver_with_error_handling` (verdict semantics identical to Kafka: `Fail` stops the source)
- 2xx with an empty body / `204` → skipped (debug log)
- `304` → skipped (supports endpoints that implement conditional responses; conditional
  *request* headers are the user's business via `http.headers.*` in v1)
- Non-2xx / transport error → routed through the error strategy as `ConnectionUnavailable`
  (retry strategy gives polling backoff for free)
- Each poll is independent — there is no offset concept; deduplication is the endpoint's or
  the query's concern (documented)

### Webhook mode

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `http.port` | Yes | - | Port to listen on |
| `http.host` | No | `0.0.0.0` | Bind address |
| `http.path` | No | `/` | Request path to accept |
| `http.auth.header` | No | - | Header name to require (e.g. `Authorization`, `X-Webhook-Token`) |
| `http.auth.token` | No | - | Exact value required in `http.auth.header` (both must be set together) |
| `http.max.concurrent.requests` | No | `256` | Concurrency cap (tower `ConcurrencyLimitLayer`); excess requests queue |
| `http.max.body.bytes` | No | `1048576` | Payload size cap (axum `DefaultBodyLimit`); oversized → `413` |

**Request handling (concurrent — axum handlers run in parallel up to the concurrency cap):**

| Request | Response |
|---------|----------|
| POST on `http.path`, auth OK, payload delivered or disposed (drop/DLQ) | `200` |
| POST with empty body | `200` (accepted, skipped) |
| Auth header missing/mismatched | `401` |
| Wrong path | `404` |
| Non-POST method | `405` |
| Body over `http.max.body.bytes` | `413` |
| Delivery verdict `Fail` | `500`, then the source shuts down gracefully (fail-fast strategy) |
| Listener shutting down (stop or after a failure) | `503` |

Concurrency note: handlers run in parallel but deliveries are funneled over a bounded
mpsc channel to a single delivery thread that owns the error context — no lock to
contend on, and a full queue backpressures handlers naturally. Events enter the junction
in channel order; HTTP webhooks carry no ordering guarantee anyway (unlike Kafka
partitions).

### `HttpSource` structure

```rust
pub enum HttpSourceMode { Poll(PollConfig), Webhook(WebhookConfig) }

pub struct HttpSource {
    mode: HttpSourceMode,
    worker: SourceWorker,
    error_ctx: Option<SourceErrorContext>,
}
```

`validate_connectivity()`:

- **poll**: probe `http.url` — any HTTP response (including 4xx) proves reachability;
  connect failure/timeout fails the app start
- **webhook**: bind `host:port` and drop the listener — fails fast on port conflicts and
  permission errors

### `HttpSourceFactory`

`name() = "http"`, formats `json`/`csv`/`bytes`(*)
required `["http.mode"]` (mode-specific requirements validated in `from_properties`),
optional: the union of both mode tables + `error.*`.

(*) format applies to the payload bytes; webhook senders set whatever Content-Type they
like — the mapper, not the transport, interprets the bytes (consistent with all connectors).

## HTTP Sink

One event = one HTTP request (per the #115 contract). Transport-level batching is deferred —
it belongs in sink-side buffering, not the mapper.

### Configuration

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `http.url` | Yes | - | Endpoint to send to |
| `http.method` | No | `POST` | `POST`, `PUT`, or `PATCH` |
| `http.headers.<Name>` | No | - | Request headers (auth goes here; use `${ENV_VAR}` for secrets) |
| `http.content.type` | No | derived | Defaults from the stream format: json → `application/json`, csv → `text/csv`, bytes → `application/octet-stream`. A verbatim `http.headers.Content-Type` wins instead; setting both is rejected |
| `http.timeout.ms` | No | `30000` | Per-request timeout |
| `http.retry.max-attempts` | No | `3` | Retries after the first attempt (`0` disables) |
| `http.retry.initial-delay-ms` | No | `100` | First retry delay |
| `http.retry.max-delay-ms` | No | `10000` | Retry delay ceiling |
| `http.retry.backoff-multiplier` | No | `2.0` | Exponential factor |
| `http.validation.method` | No | `HEAD` | Startup reachability probe: `HEAD`, `GET`, `OPTIONS`, or `none` to skip |

### Publish semantics

- Success = any **2xx** response; the response body is discarded (request/response patterns
  are deferred)
- **Retryable**: transport errors (connect/timeout) and `408`, `429`, `5xx` — retried with
  exponential backoff up to `http.retry.max-attempts`, then the error is returned to the
  pipeline (the adapter logs it; the event is not silently lost — per-event isolation from
  #115 keeps the rest of the batch flowing)
- **Not retryable**: other `4xx` (the request itself is wrong — retrying can't fix it)
- Retries happen inside `publish()` (blocking) — backpressure by design, mirroring the
  Kafka sink's queue-full blocking

`validate_connectivity()` sends the configured probe method; any HTTP response (including
`405 Method Not Allowed`, common for webhook endpoints probed with HEAD) proves
reachability. `http.validation.method = 'none'` opts out for endpoints that can't tolerate
probes.

### `HttpSinkFactory`

Replaces the placeholder in `example_factories.rs` (the second and last placeholder there;
the module keeps `ExampleSourceFactory` as its teaching fixture — update the module docs and
any placeholder-based tests, as done for the Kafka stub in #118).

## SQL Usage Examples

```sql
-- Webhook in, webhook out
CREATE STREAM Alerts (service STRING, level STRING, message STRING) WITH (
    type = 'source',
    extension = 'http',
    format = 'json',
    "http.mode" = 'webhook',
    "http.port" = '8081',
    "http.path" = '/alerts',
    "http.auth.header" = 'X-Webhook-Token',
    "http.auth.token" = '${WEBHOOK_TOKEN}'
);

CREATE STREAM Escalations (service STRING, message STRING) WITH (
    type = 'sink',
    extension = 'http',
    format = 'json',
    "http.url" = 'https://hooks.example.com/escalate',
    "http.headers.Authorization" = 'Bearer ${HOOK_TOKEN}',
    "http.retry.max-attempts" = '5'
);

INSERT INTO Escalations
SELECT service, message FROM Alerts WHERE level = 'CRITICAL';

-- REST polling
CREATE STREAM Prices (symbol STRING, price DOUBLE) WITH (
    type = 'source',
    extension = 'http',
    format = 'json',
    "http.mode" = 'poll',
    "http.url" = 'https://api.example.com/v1/price?symbol=AAPL',
    "http.poll.interval.ms" = '2000',
    "http.headers.X-Api-Key" = '${API_KEY}'
);
```

Example file: `examples/http.eventflux` (webhook → filter → webhook sink; testable with
`curl` alone — no external infrastructure).

## Testing Plan — no external services needed

This connector is self-hosting for tests, so unlike Kafka/RabbitMQ the integration tests are
**not `#[ignore]`d** — they run in every `--features http` test invocation:

- **Unit tests** (in-module): config parsing for both modes and the sink (required/optional,
  invalid values, auth pair validation, header prefix parsing, content-type derivation,
  retry parameter validation)
- **Integration tests** (`tests/http_integration.rs`, in-process servers):
  - Round trip through public API: HTTP sink → webhook source (the connector receives its
    own output)
  - Poll source against an in-process `tiny_http` responder (dev-dependency): 2xx body
    delivered, empty/304 skipped, 5xx routed to error strategy
  - Webhook responses: 401 on bad token, 404 wrong path, 405 wrong method, 413 oversized,
    200 on success
  - Sink retry: responder fails twice then succeeds → delivered after backoff; 4xx → no
    retry; max-attempts exhausted → error
  - `validate_connectivity`: reachable/unreachable/`none` for the sink; port-conflict
    failure for the webhook source
  - Shutdown: both modes stop promptly (poll mid-interval via `sleep_while_running`,
    listener via axum graceful shutdown); a slow in-flight webhook request completes
    before the worker exits
  - Concurrency: N parallel webhook POSTs all delivered (none dropped, none serialized
    behind each other beyond the configured cap)
- **CI**: no new service container; the per-connector gating check gains
  `cargo check --all-targets --features http`

`tiny_http` is added as a (non-optional) dev-dependency for test responders — pure Rust,
compiles in seconds, acceptable in the minimal test build.

## Implementation Plan

| Phase | Scope |
|-------|-------|
| 1 | `http` cargo feature + `http_common.rs` if warranted (headers/timeout/retry parsing shared by source and sink) |
| 2 | `http_source.rs` — poll + webhook modes, factory, unit tests |
| 3 | `http_sink.rs` — client, retry, factory, unit tests; replace the placeholder in `example_factories.rs` |
| 4 | Registration, gating points, integration tests, `examples/http.eventflux` |
| 5 | Docs: website connector page + tables, README feature row, ROADMAP P2 → Done, this spec → IMPLEMENTED |

## Future: request/response mode (design sketch, not in v1)

The webhook mode is fire-and-forget: `200` acknowledges *acceptance*, not the query result.
A synchronous request-reply mode — client POSTs, the event flows through the query, and the
**correlated output event is returned as the HTTP response** — would turn EventFlux into a
decision service (fraud scoring, routing, enrichment APIs). Siddhi shipped exactly this as
its `http-request`/`http-response` source–sink pair, so the shape is proven:

- `http.mode = 'request'` source: for each request, generate a correlation id, inject it as
  a **declared attribute** of the stream (e.g. the first column), park the request on a
  `oneshot` channel in a registry keyed by `http.source.id` + correlation id
- A paired `http-response` **sink** (separate extension name) configured with the same
  `http.source.id` and `http.correlation.field`: on publish, it looks up the parked request
  and completes it with the payload as the response body
- `http.request.timeout.ms`: parked requests that never see a correlated output (e.g. the
  query's WHERE filtered the event out) respond `504`
- **Separation of concerns survives**: no `Source`/`Sink` trait changes — the coupling is
  contained in a shared registry (in `EventFluxContext`, like other cross-component
  registries) plus an explicit, user-visible schema convention for the correlation field.
  The correlation id rides through the query as ordinary data, which also makes the 1:1
  constraint honest: windows/aggregations that break per-event correspondence simply time
  the request out, and the docs say so
- axum makes the "hold thousands of requests open" part natural (async handlers awaiting
  oneshot channels) — this is a primary reason the listener is axum and not a minimal
  blocking server

Not in v1 because it deserves its own spec/PR: registry lifecycle, timeout sweeping, and
backpressure under parked-request load each need design attention. Tracked as a follow-up
issue.

## Forward-Compatibility Notes (decided at spec review)

Config keys and extension names are public API — these rules exist so future features are
additive, never breaking:

1. **Source metadata is one interface evolution, shared with Kafka.** Request/response
   correlation injection (#134) and transport-metadata exposure (Kafka key/headers/timestamp,
   HTTP request headers/path — the source-side twin of #126) need the same capability:
   metadata flowing alongside the payload through `SourceCallback`. Whichever lands first
   introduces the single shared shape (payload + transport-metadata map, adapter-side mapping
   into declared attributes); the other consumes it. HTTP v1 requires nothing here.
2. **`http.auth.*` means inbound verification only** (webhook callers proving themselves).
   Outbound credentials are `http.headers.*` today and `http.oauth.*` when OAuth lands —
   the prefix never switches direction.
3. **Extension families under one feature**: #134 adds an `http-response` sink under the
   same `http` cargo feature. The gating rule ("feature named after the extension") gains a
   footnote: a feature may own a family of related extensions.
4. **One port per webhook source (v1)**: two sources on the same port fail at validation
   (bind check). A future shared-listener registry (port → composed router) is an internal
   change — `http.host`/`http.port`/`http.path` keys are stable under it.
5. **`ureq` is HTTP/1.1 only** (client side). An HTTP/2 client need is met by swapping the
   client internally; no config key encodes the protocol.
6. **Counters parity**: source (requests/polls received, failed) and sink (requests sent,
   failed) keep the same local-counter shape as Kafka, logged at stop, so the observability
   work (#128) finds a uniform surface.
7. **Reserved injected properties** (added during implementation): keys with a leading
   underscore in the factory properties map are engine-reserved. `stream_initializer`
   injects `_format` (the stream's declared format) for every source/sink factory — the
   HTTP sink derives its default Content-Type from it, and any future connector may read
   it. Documented in `docs/writing_extensions.md`; this is a config-time metadata channel
   that the future transport-metadata interface (note 1) should be designed alongside.

## Deferred (post-v1)

| Feature | Reason |
|---------|--------|
| Request/response mode | Design sketch above; own spec + PR |
| Batched sink requests (N events per POST) | Needs sink-side buffering design; #115 contract keeps mapper per-event |
| Pagination / conditional GET (ETag/If-Modified-Since managed by the connector) | Stateful polling; v1 passes conditional headers through untouched |
| OAuth 2.0 flows / token refresh | Static headers + env vars cover v1 |
| TLS for the webhook listener | Reverse proxy for v1; axum + rustls acceptor is the natural in-process path later |
| Listener client-read timeouts (slowloris hardening) | Reverse proxy enforces client timeouts for v1 |
| Redirect following on the client side | Deliberately disabled — following a 301/302 rewrites POST to a body-less GET (silent payload loss); 3xx is a permanent error |
| Per-event dynamic URLs | Needs per-event sink context — eventflux-io/eventflux#126 |
