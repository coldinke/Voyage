# Voyage — LLM Session Analytics & Knowledge Platform

Rust workspace with 6 crates: core, parser, store, embed, graph, cli.
All storage is SQLite (voyage.db, graph.db, vectors.db) in `~/.voyage/`.

## Build & Test

```bash
cargo build              # build all crates
cargo test               # run all tests (181+)
cargo clippy             # lint check
cargo run -- --help      # show CLI help
```

## Architecture

- **voyage-core**: Domain models (Session, Message, KnowledgeItem, UserProfile)
- **voyage-parser**: Session file parsers (Claude Code JSONL, OpenCode JSON/SQLite, Codex)
- **voyage-store**: SQLite storage + vector store + knowledge extraction
- **voyage-embed**: FastEmbed integration (AllMiniLmL6V2, MultilingualE5Small)
- **voyage-graph**: Entity extraction, edges, PageRank, communities
- **voyage-cli**: All commands (ingest, stats, search, distill, bank, profile, graph, etc.)

## Key Patterns

- Error handling: `thiserror` enums per crate, `Box<dyn Error>` at CLI boundary
- IDs: UUIDv4 for messages/sessions, UUIDv5 deterministic for entities
- Timestamps: Always `chrono::DateTime<Utc>`, stored as RFC3339 strings
- SQL: Parameterized queries, `INSERT OR REPLACE` for upserts, transactions for multi-step
- UTF-8 safety: Always check `is_char_boundary()` before string slicing
