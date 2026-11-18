# AGENTS.md

For coding agents working in this repo. agent-pi's runtime in Rust: several agents per process from the same agent.json, SQLite storage.

## Build and test

```bash
cargo build
cargo test
cargo clippy --all-targets
cargo fmt
```

## Read first

- `docs/design.md`
- `docs/configuration.md`
- `docs/api.md`
- `docs/storage.md`
- `docs/records/implementation-plan.md` (what's built)

## Rules

- This image serves its consumer docs at `/docs/mcp` with no token (the workspace's `contracts/docs/`). Anything marked `audience: consumer` is public to whoever reaches the service, so keep secrets and internal notes out, or mark the doc `contributor`.
- Methods, events and `agent.json` follow agent-pi: a change to the protocol starts there.
- A route change updates `docs/api.md` (and `docs/openapi.yaml` where there is one) in the same change.
- Every doc under `docs/` starts with front matter: `title`, `audience` (consumer, contributor or operator), `kind` (guide, reference, design or record), `status` and `summary`. Plans, reviews and validation logs go in `docs/records/`. The workspace's `make check` (`scripts/check-docs.py`) holds this and checks relative links.
- This repo is its own project inside the t10 workspace: it builds with nothing from the workspace. The workspace's `ARCHITECTURE.md` is the high-level design, and wins where they disagree.
