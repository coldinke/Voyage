use std::path::{Path, PathBuf};

use voyage_parser::claude_code::ClaudeCodeParser;
use voyage_parser::opencode::OpenCodeParser;
use voyage_parser::traits::SessionParser;
use voyage_store::sqlite::SqliteStore;

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
        Some(other) => {
            return Err(format!("Unknown provider: {other}. Use 'claude-code' or 'opencode'").into());
        }
        None => {
            // Ingest all providers with default paths
            let claude_dir = default_claude_dir();
            if claude_dir.is_dir() {
                ingest_claude_code(&mut store, &claude_dir)?;
            }
            let opencode_dir = default_opencode_dir();
            if opencode_dir.is_dir() {
                ingest_opencode(&mut store, &opencode_dir)?;
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

fn default_opencode_dir() -> PathBuf {
    dirs_next::data_dir()
        .or_else(|| dirs_next::home_dir().map(|h| h.join(".local/share")))
        .expect("Cannot determine data directory")
        .join("opencode/storage")
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
    let (mut ingested, mut skipped, mut errors) = (0u32, 0u32, 0u32);

    for path in &session_files {
        let session_id = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| uuid::Uuid::parse_str(s).ok());

        if let Some(id) = session_id {
            if store.session_exists(&id)? {
                skipped += 1;
                continue;
            }
        }

        match parser.parse_session(path) {
            Ok((session, messages)) => {
                if session.message_count == 0 {
                    skipped += 1;
                    continue;
                }
                store.insert_session_with_messages(&session, &messages)?;
                println!(
                    "  Ingested: {} ({} msgs, ${:.4})",
                    path.file_name().unwrap_or_default().to_string_lossy(),
                    session.message_count,
                    session.estimated_cost_usd,
                );
                ingested += 1;
            }
            Err(e) => {
                eprintln!(
                    "  Error: {}: {e}",
                    path.file_name().unwrap_or_default().to_string_lossy()
                );
                errors += 1;
            }
        }
    }

    println!("Claude Code: {ingested} ingested, {skipped} skipped, {errors} errors\n");
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
    let (mut ingested, mut skipped, mut errors) = (0u32, 0u32, 0u32);

    for path in &session_files {
        match parser.parse_session(&path, storage_root) {
            Ok((session, messages)) => {
                if store.session_exists(&session.id)? {
                    skipped += 1;
                    continue;
                }
                if session.message_count == 0 {
                    skipped += 1;
                    continue;
                }
                store.insert_session_with_messages(&session, &messages)?;
                println!(
                    "  Ingested: {} ({} msgs, ${:.4})",
                    path.file_name().unwrap_or_default().to_string_lossy(),
                    session.message_count,
                    session.estimated_cost_usd,
                );
                ingested += 1;
            }
            Err(e) => {
                eprintln!(
                    "  Error: {}: {e}",
                    path.file_name().unwrap_or_default().to_string_lossy()
                );
                errors += 1;
            }
        }
    }

    println!("OpenCode: {ingested} ingested, {skipped} skipped, {errors} errors\n");
    Ok(())
}
