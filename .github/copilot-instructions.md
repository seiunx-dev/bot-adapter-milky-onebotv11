# GitHub Copilot Instructions

This is a Rust bridge between the Milky gateway protocol and OneBot-11. Single binary crate, edition 2024, using tokio async runtime.

[`AGENTS.md`](../AGENTS.md) is the single source of truth for this repository (architecture, extension guides, conventions); this file is a short summary of it.

## Key commands

```bash
cargo test                                               # run all tests
cargo fmt --all                                          # format
cargo clippy --all-targets --all-features -- -D warnings # lint
cargo build --release --locked                           # production build
```

## Project layout

| Path | Purpose |
|------|---------|
| `src/milky/` | Upstream Milky SDK client and event translation |
| `src/bridge/` | Core translation layer; `service.rs` dispatches OneBot-11 actions |
| `src/onebot/` | Downstream axum HTTP/WebSocket server |
| `src/state/` | In-memory state: message map, request map, runtime info |
| `src/config.rs` | Strict JSON config (`deny_unknown_fields`) |
| `src/logging.rs` | Custom colored tracing formatter |
| `src/types.rs` | Shared event and segment types |

## Coding conventions

- Use `tracing` for all logging with structured key/value fields — no format strings in log macros.
- Error handling uses `thiserror`-derived types; never introduce `anyhow`.
- OneBot-11 error codes: `1400` bad params, `1500` upstream error, `1502` not found, `1503` unsupported.
- When adding a new `milky::Client` method consumed by the bridge, also add it to the `Upstream` trait in `src/bridge/service.rs` and to `StubUpstream` in the tests module.
- Tests live in `#[cfg(test)] mod tests` blocks in the same file. No real network connections — use `StubUpstream` and the `Stub` handler.
- Config structs use `#[serde(default)]` on optional fields and `#[serde(deny_unknown_fields)]` on top-level types.

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

- `ci.yml` runs on pushes to `main` and `rust` (the default branch), pull requests targeting either, and manual dispatch.
- Rust CI order: `cargo fmt --all -- --check`, `cargo check --locked --all-targets`, `cargo clippy --locked --all-targets -- -D warnings`, then `cargo test --locked`.
- `release.yml` is the standard release build entrypoint. It runs on `v*` tags and manual dispatch, builds native release archives for `linux-x64`, `macos-arm64` and `windows-x64`, uploads them with `actions/upload-artifact`, and on tag pushes publishes them plus a `SHA256SUMS-<tag>.txt` as GitHub Release assets.
- `docker.yml` is the standard Docker entrypoint. It runs on `main` pushes, `v*` tags, PRs targeting `main` that touch Docker/build inputs, and manual dispatch (pushes to `rust` do not trigger it). PRs build only; non-PR runs push GHCR images with lowercase image names and Docker metadata tags.

Workflow maintenance rules:

- Keep workflow filenames and top-level names aligned: `CI`, `Release`, `Docker`, and optional package-specific names.
- Use `actions/checkout@v6`, `dtolnay/rust-toolchain@stable`, `Swatinem/rust-cache@v2`, `actions/upload-artifact@v7`, `actions/download-artifact@v8`, `softprops/action-gh-release@v3`, and current Docker actions (`setup-buildx@v4`, `login@v4`, `metadata@v6`, `build-push@v7`).
- Keep `permissions` minimal: `contents: read` for CI/Docker build-only work, `contents: write` for release publishing, and `packages: write` only when pushing container images.
- Use workflow `concurrency` keyed by workflow name and ref, with release jobs using `release-${{ github.ref_name }}` and `cancel-in-progress: false`.
- Do not reintroduce legacy workflow names such as `rust-ci.yml`, `build.yml`, `release-build.yml`, `docker-build.yml`, or `docker-release.yml` unless a package-specific workflow already exists and is intentionally preserved.
