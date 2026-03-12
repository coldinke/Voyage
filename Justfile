# Voyage - LLM Token Analytics & Knowledge Platform

default:
    @just --list

# Build the project
build:
    cargo build

# Build release binary
release:
    cargo build --release

# Run all tests
test:
    cargo test

# Run tests with output
test-verbose:
    cargo test -- --nocapture

# Check compilation without building
check:
    cargo check

# Run clippy lints
lint:
    cargo clippy -- -W clippy::all

# Format code
fmt:
    cargo fmt

# Format check (CI)
fmt-check:
    cargo fmt -- --check

# Ingest all providers (Claude Code + OpenCode + Codex)
ingest:
    cargo run -- ingest

# Ingest Claude Code sessions only
ingest-claude:
    cargo run -- ingest --provider claude-code

# Ingest OpenCode sessions only
ingest-opencode:
    cargo run -- ingest --provider opencode

# Ingest Codex sessions only
ingest-codex:
    cargo run -- ingest --provider codex

# Show token usage stats (last N days, default 1)
stats days="1":
    cargo run -- stats --days {{days}}

# Show stats broken down by model
stats-by-model days="7":
    cargo run -- stats --days {{days}} --by-model

# List recent sessions
sessions days="7" limit="20":
    cargo run -- session list --days {{days}} --limit {{limit}}

# Generate HTML report
report days="30":
    cargo run -- report --days {{days}} --open

# Build vector index
index:
    cargo run -- index

# Semantic search
search query limit="10":
    cargo run -- search "{{query}}" --limit {{limit}}

# Full pipeline: ingest, index, report
pipeline:
    just ingest
    just index
    just report

# Clean build artifacts
clean:
    cargo clean

# Run CI checks (fmt, lint, test)
ci:
    just fmt-check
    just lint
    just test
