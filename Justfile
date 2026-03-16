# Voyage - LLM Session Analytics & Knowledge Platform

default:
    @just --list -u

alias r := ready
alias t := test

# Run all tests
test *args:
    cargo test {{args}}

# Run clippy with strict warnings
lint:
    cargo clippy --workspace --all-targets -- -D warnings

# Format code
fmt:
    cargo fmt --all

# Check formatting, lint, and test — run before pushing
ready:
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test

# Build release binary and install to ~/.cargo/bin
install:
    cargo install --path crates/voyage-cli --locked

# Full pipeline: ingest → index → report
pipeline:
    cargo run -- ingest
    cargo run -- index
    cargo run -- report --open
