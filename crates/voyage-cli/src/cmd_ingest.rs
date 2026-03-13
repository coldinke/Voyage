use std::path::{Path, PathBuf};

use voyage_core::model::{extract_summary, Session, Message, Role};
use voyage_parser::claude_code::ClaudeCodeParser;
use voyage_parser::codex::CodexParser;
use voyage_parser::opencode::OpenCodeParser;
use voyage_parser::traits::SessionParser;
use voyage_store::sqlite::SqliteStore;

/// Outcome of comparing a parsed session against the store.
enum Upsert {
    New,
    Updated { old_msgs: u32 },
    Unchanged,
    Empty,
}

/// Compute summary for a session if not already set (e.g. by OpenCode title).
fn compute_summary(session: &mut Session, messages: &[Message]) {
    if !session.summary.is_empty() {
        return;
    }
    let first_user = messages
        .iter()
        .find(|m| m.role == Role::User)
        .map(|m| m.content.as_str());
    session.summary = extract_summary(None, first_user, &session.model, &session.project);
}

fn classify(store: &SqliteStore, session: &Session) -> Result<Upsert, Box<dyn std::error::Error>> {
    if session.message_count == 0 {
        return Ok(Upsert::Empty);
    }
    match store.session_state(&session.id)? {
        None => Ok(Upsert::New),
        Some(old_msgs) if session.message_count > old_msgs => {
            Ok(Upsert::Updated { old_msgs })
        }
        Some(_) => Ok(Upsert::Unchanged),
    }
}

fn apply(
    store: &mut SqliteStore,
    session: &Session,
    messages: &[Message],
    filename: &str,
    upsert: &Upsert,
    counters: &mut (u32, u32, u32),
) -> Result<(), Box<dyn std::error::Error>> {
    match upsert {
        Upsert::New => {
            store.insert_session_with_messages(session, messages)?;
            println!(
                "  + {filename} ({} msgs, ${:.4})",
                session.message_count, session.estimated_cost_usd,
            );
            counters.0 += 1;
        }
        Upsert::Updated { old_msgs } => {
            store.insert_session_with_messages(session, messages)?;
            println!(
                "  ~ {filename} ({old_msgs} -> {} msgs, ${:.4})",
                session.message_count, session.estimated_cost_usd,
            );
            counters.1 += 1;
        }
        Upsert::Unchanged | Upsert::Empty => {
            counters.2 += 1;
        }
    }
    Ok(())
}

pub fn run(
    db_path: &Path,
    source: Option<PathBuf>,
    provider: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut store = SqliteStore::open(db_path)?;

    match provider {
        Some("claude-code" | "claude") => {
            let src = source.unwrap_or_else(default_claude_dir);
            ingest_claude_code(&mut store, &src)?;
        }
        Some("opencode") => {
            let src = source.unwrap_or_else(default_opencode_dir);
            ingest_opencode(&mut store, &src)?;
        }
        Some("codex") => {
            let src = source.unwrap_or_else(default_codex_dir);
            ingest_codex(&mut store, &src)?;
        }
        Some(other) => {
            return Err(format!("Unknown provider: {other}. Use 'claude-code', 'opencode', or 'codex'").into());
        }
        None => {
            let claude_dir = default_claude_dir();
            if claude_dir.is_dir() {
                ingest_claude_code(&mut store, &claude_dir)?;
            }
            let opencode_dir = default_opencode_dir();
            if opencode_dir.is_dir() {
                ingest_opencode(&mut store, &opencode_dir)?;
            }
            let codex_dir = default_codex_dir();
            if codex_dir.is_dir() {
                ingest_codex(&mut store, &codex_dir)?;
            }
        }
    }

    Ok(())
}

fn default_claude_dir() -> PathBuf {
    dirs_next::home_dir()
        .expect("Cannot determine home directory")
        .join(".claude/projects")
}

fn default_codex_dir() -> PathBuf {
    std::env::var("CODEX_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            dirs_next::home_dir()
                .expect("Cannot determine home directory")
                .join(".codex")
        })
}

fn default_opencode_dir() -> PathBuf {
    dirs_next::data_dir()
        .or_else(|| dirs_next::home_dir().map(|h| h.join(".local/share")))
        .expect("Cannot determine data directory")
        .join("opencode/storage")
}

fn print_summary(label: &str, new: u32, updated: u32, skipped: u32, errors: u32) {
    let parts: Vec<String> = [
        (new, "new"),
        (updated, "updated"),
        (skipped, "unchanged"),
        (errors, "errors"),
    ]
    .iter()
    .filter(|(n, _)| *n > 0)
    .map(|(n, l)| format!("{n} {l}"))
    .collect();
    println!("{label}: {}\n", if parts.is_empty() { "nothing to do".into() } else { parts.join(", ") });
}

fn ingest_claude_code(
    store: &mut SqliteStore,
    source: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Claude Code ===");
    if !source.is_dir() {
        println!("Source not found: {}", source.display());
        return Ok(());
    }

    let parser = ClaudeCodeParser::new();
    let session_files = parser.discover_sessions(source)?;
    if session_files.is_empty() {
        println!("No session files found in {}", source.display());
        return Ok(());
    }

    println!("Found {} session file(s)", session_files.len());
    let (mut counters, mut errors) = ((0u32, 0u32, 0u32), 0u32);

    for path in &session_files {
        match parser.parse_session(path) {
            Ok((mut session, messages)) => {
                compute_summary(&mut session, &messages);
                let upsert = classify(store, &session)?;
                let fname = path.file_name().unwrap_or_default().to_string_lossy().to_string();
                apply(store, &session, &messages, &fname, &upsert, &mut counters)?;
            }
            Err(e) => {
                eprintln!("  Error: {}: {e}", path.file_name().unwrap_or_default().to_string_lossy());
                errors += 1;
            }
        }
    }

    print_summary("Claude Code", counters.0, counters.1, counters.2, errors);
    Ok(())
}

fn ingest_opencode(
    store: &mut SqliteStore,
    storage_root: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== OpenCode ===");
    if !storage_root.is_dir() {
        println!("Source not found: {}", storage_root.display());
        return Ok(());
    }

    let parser = OpenCodeParser::new();
    let session_files = parser.discover_sessions(storage_root)?;
    if session_files.is_empty() {
        println!("No session files found in {}", storage_root.display());
        return Ok(());
    }

    println!("Found {} session file(s)", session_files.len());
    let (mut counters, mut errors) = ((0u32, 0u32, 0u32), 0u32);

    for path in &session_files {
        match parser.parse_session(path, storage_root) {
            Ok((mut session, messages)) => {
                compute_summary(&mut session, &messages);
                let upsert = classify(store, &session)?;
                let fname = path.file_name().unwrap_or_default().to_string_lossy().to_string();
                apply(store, &session, &messages, &fname, &upsert, &mut counters)?;
            }
            Err(e) => {
                eprintln!("  Error: {}: {e}", path.file_name().unwrap_or_default().to_string_lossy());
                errors += 1;
            }
        }
    }

    print_summary("OpenCode", counters.0, counters.1, counters.2, errors);
    Ok(())
}

fn ingest_codex(
    store: &mut SqliteStore,
    codex_home: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Codex ===");
    if !codex_home.is_dir() {
        println!("Source not found: {}", codex_home.display());
        return Ok(());
    }

    let parser = CodexParser::new();
    let session_files = parser.discover_sessions(codex_home)?;
    if session_files.is_empty() {
        println!("No session files found in {}", codex_home.display());
        return Ok(());
    }

    println!("Found {} session file(s)", session_files.len());
    let (mut counters, mut errors) = ((0u32, 0u32, 0u32), 0u32);

    for path in &session_files {
        match parser.parse_session(path) {
            Ok((mut session, messages)) => {
                compute_summary(&mut session, &messages);
                let upsert = classify(store, &session)?;
                let fname = path.file_name().unwrap_or_default().to_string_lossy().to_string();
                apply(store, &session, &messages, &fname, &upsert, &mut counters)?;
            }
            Err(e) => {
                eprintln!("  Error: {}: {e}", path.file_name().unwrap_or_default().to_string_lossy());
                errors += 1;
            }
        }
    }

    print_summary("Codex", counters.0, counters.1, counters.2, errors);
    Ok(())
}
