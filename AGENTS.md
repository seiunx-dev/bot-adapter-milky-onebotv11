# AGENTS.md

This file provides guidance to Codex and other AI coding agents working in this repository.

## Project

Rust bridge that adapts an upstream Milky gateway to the OneBot-11 protocol. Downstream bots speak OneBot-11 to this bridge; the bridge speaks Milky upstream via [`milky-rust-sdk`](https://crates.io/crates/milky-rust-sdk). The README is in Chinese.

Single binary crate, edition 2024. MSRV is the latest stable Rust (`dtolnay/rust-toolchain@stable`). Docker images target Alpine Linux (musl libc).

The repository was originally a Go implementation; that tree (`cmd/`, `internal/`, `go.mod`, `go.sum`) has been removed.

## Setup

```bash
# No extra tools needed beyond a stable Rust toolchain
cargo build
```

## Common commands

```bash
# Run from a config file
cargo run -- --config config.json

# Test the whole crate
cargo test

# Run a single module's tests
cargo test bridge::

# Run a single test by name
cargo test parse_cq_string_message

# Format / lint (CI enforces both as errors)
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings

# Production build
cargo build --release --locked
# binary: target/release/milky-ob11-bridge

# Docker build (requires Docker)
docker build -t milky-ob11-bridge .
```

CI (`.github/workflows/ci.yml`) runs on every push to `main` and on every PR: `cargo fmt --all --check`, `cargo clippy ... -D warnings`, `cargo test --locked`.



## Architecture

`src/main.rs` wires the layers together. The bridge is a single long-running async process (tokio multi-thread runtime) with three layers connected via channels:

1. **`src/milky/`** — upstream client. `Client::new` constructs a `milky_rust_sdk::MilkyClient` over WebSocket. `connect()` opens the WS, fetches login info, and spawns a translator task that converts SDK events into `types::InboundEvent`s. Outgoing methods (`send_*_message`, `delete_message`, `get_*`, `handle_*_request`) call the SDK plus segment-IR conversion (`segments.rs`).

2. **`src/bridge/`** — translation core. `Service::run` is the central event loop: pulls `InboundEvent`s, translates them via `translator::translate_event` into OneBot-11 payloads, and broadcasts via `onebot::Server::broadcast`. `handle_api` is the dispatch table for every supported OneBot-11 action. `message_ir.rs` is the intermediate representation for segment conversion, including CQ-code encode/decode.

3. **`src/onebot/`** — downstream surface (axum 0.8):
   - Forward WS: `/` (universal), `/api`, `/api/` (receive only), `/event`, `/event/` (send only).
   - HTTP API: `POST /http/<action>` (and `GET`) when `enable_http_api` is set.
   - Reverse WS: dials out to configured URLs with auto-reconnect; sends `X-Self-ID`, `X-Client-Role`, optional `Authorization: Bearer <token>` headers.

`Server` calls back into `bridge::Service` through the `onebot::Handler` trait (`handle_api`, `on_ws_connect`, `current_self_id`) — that trait is the seam between the layers. `Service::run` broadcasts to every connected forward and reverse WS client flagged `can_send`.

**State (`src/state/`)**:
- `MessageMap` — LRU map of OneBot `message_id` → Milky message ref.
- `RequestMap` — `flag` strings → friend/group request refs.
- `Runtime` — login info + online/good status, used by `get_login_info`, `get_status`, and the heartbeat ticker in `Service::run` that emits `meta_event` heartbeats at `bridge.heartbeat_interval_ms`.

**Config (`src/config.rs`)**: strict JSON decoding (`#[serde(deny_unknown_fields)]`); unknown keys fail loading. `Config::load` reads, parses, then runs `validate()`. `bridge.message_format` must be `"array"` or `"string"` — `array` is the default and is what the IR aligns to.

**Shutdown**: `tokio::sync::watch::channel<bool>`; `main` listens for SIGINT/SIGTERM (Unix) or Ctrl+C (Windows), then sends `true` to wake `server.run` and `service.run` together. After both join, `service.shutdown()` closes the SDK.

### Adding a new OneBot-11 action

1. Add a `match` arm to `Service::handle_api` in `src/bridge/service.rs`.
2. Define a params struct (with `#[serde(default)]` on every field) and decode via the `decode::<T>(params, &echo)` helper.
3. Call into `self.upstream`; add a method to `milky::Client` and the `Upstream` trait if needed.
4. Return `success(...)` or `failure(...)`. Unsupported actions fall through to `failure(1503, unsupported_action(...))`.

### Adding a new inbound event

1. Add a variant to `types::EventKind` in `src/types.rs`.
2. Emit it from `milky::events::translate_event` in `src/milky/events.rs`.
3. Translate it in `bridge::translator::translate_event` (`src/bridge/translator.rs`).

## Conventions

- **Logging**: `tracing` everywhere, initialized once in `main.rs` via `logging::init` at the level set by `bridge.log_level`. Use structured key/value pairs (`tracing::info!(field = %value, "msg")`), not format strings. The `ColoredFormatter` in `src/logging.rs` emits `[ts][LEVEL][component] message field=value` lines and treats `bot_id`/`group_id`/`user_id` as identity tags. ANSI escapes are written unconditionally; `enable-ansi-support` turns on VT processing so Windows cmd renders them.
- **Error codes** returned to OneBot clients: `1400` (bad params), `1500` (upstream/unknown), `1502` (not found), `1503` (unsupported).
- **Errors**: propagate via `MilkyClientError` (`thiserror`-derived) with `#[from]`. Do not introduce `anyhow`.
- **`Upstream` trait**: mirrors `milky::Client`'s public async methods for test stubbing. Add new methods to both the trait and `StubUpstream` when extending the client.
- **Tests**: in-crate (`#[cfg(test)] mod tests`), synchronous where possible; async tests use `#[tokio::test]`. No real network connections in tests.

## Git commits

All commit subjects must follow:

```text
[Type] Short description starting with capital letter
```

Allowed types:

| Type      | Usage                                                 |
|-----------|-------------------------------------------------------|
| `[Feat]`  | New feature or capability                             |
| `[Fix]`   | Bug fix                                               |
| `[Chore]` | Maintenance, refactoring, dependency or build changes |
| `[Docs]`  | Documentation-only changes                            |

Rules:

- Description starts with a capital letter.
- Use imperative mood: `Add ...`, not `Added ...`.
- No trailing period.
- Keep the subject at or below roughly 70 characters.
- **Agent attribution uses the standard Git `Co-authored-by:` trailer in the commit body, not a free-form `Agent:` line.** This makes GitHub render the co-author avatar on the commit page. The trailer must be on its own line, separated from the subject by a blank line, in the form `Co-authored-by: <Display Name> <email>`. Suggested values per agent:
  - Claude (any 4.x): `Co-authored-by: Claude Opus 4.7 <noreply@anthropic.com>` (substitute the actual model, e.g. `Claude Sonnet 4.6`, `Claude Haiku 4.5`)
  - Codex: `Co-authored-by: Codex <noreply@openai.com>`
  - Copilot: `Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>`

Examples from this repo's history:

```text
[Feat] Add Docker Alpine image, multi-arch build workflow, and agent docs
[Fix] Lowercase GHCR image name to satisfy Docker reference format
[Chore] Configure Dependabot updates
[Fix] Add openssl-libs-static to Docker builder for musl static link
```

## GitHub Actions workflows

Use the standardized workflow layout in `.github/workflows`:

- `ci.yml` runs on `main` pushes, pull requests targeting `main`, and manual dispatch.
- Rust CI order: `cargo fmt --all -- --check`, `cargo check --locked --all-targets`, `cargo clippy --locked --all-targets -- -D warnings`, then `cargo test --locked`.
- `release.yml` is the standard release build entrypoint. It runs on `v*` tags and manual dispatch, builds release artifacts, uploads them with `actions/upload-artifact`, and publishes GitHub Release assets on tag pushes.
- `docker.yml` is the standard Docker entrypoint. It runs on `main` pushes, `v*` tags, PRs that touch Docker/build inputs, and manual dispatch. PRs build only; non-PR runs push GHCR images with lowercase image names and Docker metadata tags.

Workflow maintenance rules:

- Keep workflow filenames and top-level names aligned: `CI`, `Release`, `Docker`, and optional package-specific names.
- Use `actions/checkout@v6`, `actions/setup-go@v6`, `actions/upload-artifact@v7`, `actions/download-artifact@v8`, `softprops/action-gh-release@v3`, and current Docker actions (`setup-buildx@v4`, `login@v4`, `metadata@v6`, `build-push@v7`).
- Keep `permissions` minimal: `contents: read` for CI/Docker build-only work, `contents: write` for release publishing, and `packages: write` only when pushing container images.
- Use workflow `concurrency` keyed by workflow name and ref, with release jobs using `release-${{ github.ref_name }}` and `cancel-in-progress: false`.
- Do not reintroduce legacy workflow names such as `rust-ci.yml`, `build.yml`, `release-build.yml`, `docker-build.yml`, or `docker-release.yml` unless a package-specific workflow already exists and is intentionally preserved.
