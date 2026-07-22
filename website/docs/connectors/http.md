---
sidebar_position: 3
title: HTTP Connector
description: REST polling, webhook ingestion, and webhook delivery
---

# HTTP Connector

The HTTP connector lets EventFlux ingest events over HTTP — by **polling** REST endpoints or by **listening** for webhooks — and deliver processed events as outbound webhook requests with retry. It supports JSON, CSV, and bytes formats.

The stack uses the best tool per side: a blocking `ureq` client (rustls — hermetic, no system TLS) for the sink and polling, and an **axum** server (on tokio) for the webhook listener, giving concurrent request handling out of the box.

## Prerequisites

The HTTP connector is feature-gated. Build EventFlux with the `http` feature (Docker images already include all connectors). Both underlying crates are pure Rust — no extra toolchain needed:

```bash
cargo build --release --features http
# or: cargo build --release --features connectors-all
```

## HTTP Source

One extension, two modes selected by `http.mode`.

### Poll Mode

Periodically requests a URL; each 2xx response body becomes one payload through the mapper.

```sql
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

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `http.mode` | Yes | - | `poll` |
| `http.url` | Yes | - | URL to poll (http/https) |
| `http.poll.interval.ms` | No | `5000` | Time between polls (interruptible — long intervals never delay shutdown) |
| `http.method` | No | `GET` | `GET` or `POST` |
| `http.headers.<Name>` | No | - | Request headers; put secrets in env vars: `'Bearer ${TOKEN}'` |
| `http.timeout.ms` | No | `30000` | Per-request timeout |
| `error.*` | No | - | Error strategies (`drop`/`retry`/`dlq`/`fail`) — same options as [Kafka](/docs/connectors/kafka#error-handling) |

Poll behavior: `2xx` non-empty body → delivered · empty body / `204` / `304` → skipped · other statuses (including un-followed `3xx` redirects) and transport errors → routed through the error strategy (use `retry` for polling backoff). Response bodies are capped at 10 MB (the HTTP client's read limit); larger responses surface as read errors. Each poll is independent — deduplication is the endpoint's or the query's concern.

### Webhook Mode

Runs an HTTP listener; each accepted POST body becomes one payload. Requests are handled **concurrently** (bounded by the configured cap).

```sql
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
```

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `http.mode` | Yes | - | `webhook` |
| `http.port` | Yes | - | Port to listen on |
| `http.host` | No | `0.0.0.0` | Bind address |
| `http.path` | No | `/` | Request path to accept |
| `http.auth.header` / `http.auth.token` | No | - | Inbound auth: required header name + exact value (set together) |
| `http.max.concurrent.requests` | No | `256` | Concurrency cap; excess requests queue |
| `http.max.body.bytes` | No | `1048576` | Payload size cap |
| `error.*` | No | - | Error strategies, as above |

Response codes:

| Situation | Response |
|-----------|----------|
| Payload accepted (delivered, or disposed by drop/DLQ strategy) | `200` |
| Empty body | `200` (skipped) |
| Auth missing/mismatched | `401` |
| Wrong path / wrong method | `404` / `405` |
| Body too large | `413` |
| Unrecoverable delivery failure (`fail` strategy) | `500`, listener shuts down gracefully |
| Listener shutting down (stop or after a failure) | `503` |

Deployment: run the listener behind a reverse proxy (nginx/caddy) — it terminates TLS and enforces client read timeouts (the listener itself does not time out slow clients).

## HTTP Sink

Sends each event as its own request (one event = one message). Retries with exponential backoff apply to transport errors and `408`/`429`/`5xx`; other `4xx` are returned to the pipeline immediately — retrying a wrong request can't fix it. Redirects are never followed (following a `302` would silently convert the POST to a body-less GET); a `3xx` is a permanent error — point `http.url` at the final location.

```sql
CREATE STREAM Escalations (service STRING, message STRING) WITH (
    type = 'sink',
    extension = 'http',
    format = 'json',
    "http.url" = 'https://hooks.example.com/escalate',
    "http.headers.Authorization" = 'Bearer ${HOOK_TOKEN}',
    "http.retry.max-attempts" = '5'
);
```

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `http.url` | Yes | - | Endpoint to send to |
| `http.method` | No | `POST` | `POST`, `PUT`, or `PATCH` |
| `http.headers.<Name>` | No | - | Request headers (auth goes here) |
| `http.content.type` | No | derived | From the stream format: json → `application/json`, csv → `text/csv`, bytes → `application/octet-stream`. A verbatim `http.headers.Content-Type` wins instead; setting both is rejected |
| `http.timeout.ms` | No | `30000` | Per-request timeout |
| `http.retry.max-attempts` | No | `3` | Retries after the first attempt (`0` disables) |
| `http.retry.initial-delay-ms` | No | `100` | First retry delay |
| `http.retry.max-delay-ms` | No | `10000` | Retry delay ceiling |
| `http.retry.backoff-multiplier` | No | `2.0` | Exponential factor |
| `http.validation.method` | No | `HEAD` | Startup reachability probe: `HEAD`, `GET`, `OPTIONS`, or `none` to skip |

Retries block inside the publish call — backpressure by design.

## Connection Validation

At startup (fail-fast):

- **Sink / poll source**: sends the probe (`HEAD` for polling; configurable for the sink). *Any* HTTP response — including `404` or `405` — proves reachability; only transport failures (connect/timeout/TLS) block startup. Endpoints that can't tolerate probes: `"http.validation.method" = 'none'`.
- **Webhook source**: binds the configured port and releases it — port conflicts and permission errors fail immediately.

## Complete End-to-End Example

See [`examples/http.eventflux`](https://github.com/eventflux-io/eventflux/blob/main/examples/http.eventflux) — a webhook-in → filter → webhook-out pipeline testable with `curl` alone, no external infrastructure.

## Not Yet Supported

- Request/response mode (query result as the HTTP response) — designed, tracked in [#134](https://github.com/eventflux-io/eventflux/issues/134)
- Batched sink requests (N events per POST)
- Connector-managed conditional GET (pass `If-None-Match`/`If-Modified-Since` through `http.headers.*` yourself; `304` responses are skipped)
- OAuth token flows (use static bearer headers with env vars)
- In-process TLS for the listener (reverse proxy)
