use std::path::Path;

use voyage_parser::claude_code::ClaudeCodeParser;
use voyage_parser::traits::SessionParser;
use voyage_store::sqlite::SqliteStore;

pub fn run(db_path: &Path, source: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let mut store = SqliteStore::open(db_path)?;
    let parser = ClaudeCodeParser::new();

    if !source.is_dir() {
        return Err(format!("Source directory not found: {}", source.display()).into());
    }

    let session_files = parser.discover_sessions(source)?;
    if session_files.is_empty() {
        println!("No session files found in {}", source.display());
        return Ok(());
    }

    println!(
        "Found {} session file(s) in {}",
        session_files.len(),
        source.display()
    );

    let mut ingested = 0;
    let mut skipped = 0;
    let mut errors = 0;

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
                    "  Ingested: {} ({} msgs, {} turns, ${:.4})",
                    path.file_name().unwrap_or_default().to_string_lossy(),
                    session.message_count,
                    session.turn_count,
                    session.estimated_cost_usd,
                );
                ingested += 1;
            }
            Err(e) => {
                eprintln!(
                    "  Error parsing {}: {e}",
                    path.file_name().unwrap_or_default().to_string_lossy()
                );
                errors += 1;
            }
        }
    }

    println!("\nDone: {ingested} ingested, {skipped} skipped, {errors} errors");
    Ok(())
}
