use std::collections::HashMap;
use std::path::{Path, PathBuf};

use uuid::Uuid;

use voyage_core::model::{Message, Role, Session, extract_summary, merge_parsed_sessions};
use voyage_graph::store::GraphStore;
use voyage_parser::claude_code::ClaudeCodeParser;
use voyage_parser::codex::CodexParser;
use voyage_parser::opencode::OpenCodeParser;
use voyage_parser::traits::SessionParser;
use voyage_store::sqlite::SqliteStore;

use crate::cmd_graph;

/// Outcome of comparing a parsed session against the store.
enum Upsert {
    New,
    Updated,
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
    match store.session_state_full(&session.id)? {
        None => Ok(Upsert::New),
        Some(state) => {
            if session.message_count != state.message_count || session.summary != state.summary {
                Ok(Upsert::Updated)
            } else {
                Ok(Upsert::Unchanged)
            }
        }
    }
}

fn apply(
    store: &mut SqliteStore,
    graph: Option<&GraphStore>,
    session: &Session,
    messages: &[Message],
    filename: &str,
    upsert: &Upsert,
    counters: &mut (u32, u32, u32),
) -> Result<(), Box<dyn std::error::Error>> {
    match upsert {
        Upsert::New => {
            store.insert_session_with_messages(session, messages)?;
            if let Some(graph) = graph {
                let entity_count =
                    cmd_graph::extract_session_entities(graph, session, messages)
                        .unwrap_or(0);
                println!(
                    "  + {filename} ({} msgs, ${:.4}, {entity_count} entities)",
                    session.message_count, session.estimated_cost_usd,
                );
            } else {
                println!(
                    "  + {filename} ({} msgs, ${:.4})",
                    session.message_count, session.estimated_cost_usd,
                );
            }
            counters.0 += 1;
        }
        Upsert::Updated => {
            store.replace_session_with_messages(session, messages)?;
            if let Some(graph) = graph {
                let entity_count =
                    cmd_graph::extract_session_entities(graph, session, messages)
                        .unwrap_or(0);
                println!(
                    "  ~ {filename} ({} msgs, ${:.4}, {entity_count} entities)",
                    session.message_count, session.estimated_cost_usd,
                );
            } else {
                println!(
                    "  ~ {filename} ({} msgs, ${:.4})",
                    session.message_count, session.estimated_cost_usd,
                );
            }
            counters.1 += 1;
        }
        Upsert::Unchanged | Upsert::Empty => {
            counters.2 += 1;
        }
    }
    Ok(())
}

fn apply_with_context(
    store: &mut SqliteStore,
    graph: Option<&GraphStore>,
    session: &Session,
    messages: &[Message],
    label: &str,
    counters: &mut (u32, u32, u32),
) -> Result<(), Box<dyn std::error::Error>> {
    let upsert = classify(store, session)?;
    apply(store, graph, session, messages, label, &upsert, counters)
}

pub fn run(
    db_path: &Path,
    source: Option<PathBuf>,
    provider: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut store = SqliteStore::open(db_path)?;

    // Open graph store alongside the main db
    let graph_path = db_path.with_file_name("graph.db");
    let graph = GraphStore::open(&graph_path)?;

    match provider {
        Some("claude-code" | "claude") => {
            let src = source.unwrap_or_else(default_claude_dir);
            ingest_claude_code(&mut store, &graph, &src)?;
        }
        Some("opencode") => {
            let src = source.unwrap_or_else(default_opencode_dir);
            ingest_opencode(&mut store, &graph, &src)?;
        }
        Some("codex") => {
            let src = source.unwrap_or_else(default_codex_dir);
            ingest_codex(&mut store, &graph, &src)?;
        }
        Some(other) => {
            return Err(format!(
                "Unknown provider: {other}. Use 'claude-code', 'opencode', or 'codex'"
            )
            .into());
        }
        None => {
            let claude_dir = default_claude_dir();
            if claude_dir.is_dir() {
                ingest_claude_code(&mut store, &graph, &claude_dir)?;
            }
            let opencode_dir = default_opencode_dir();
            if opencode_dir.is_dir()
                || opencode_dir
                    .parent()
                    .is_some_and(|p| p.join("opencode.db").exists())
            {
                ingest_opencode(&mut store, &graph, &opencode_dir)?;
            }
            let codex_dir = default_codex_dir();
            if codex_dir.is_dir() {
                ingest_codex(&mut store, &graph, &codex_dir)?;
            }
        }
    }

    // Refresh session counts after all ingestion
    graph.refresh_session_counts()?;

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
    // OpenCode uses XDG_DATA_HOME on all platforms, which defaults to
    // ~/.local/share — but dirs_next::data_dir() returns ~/Library/Application
    // Support on macOS. Check both locations.
    let xdg = dirs_next::home_dir().map(|h| h.join(".local/share/opencode"));
    let platform = dirs_next::data_dir().map(|d| d.join("opencode"));

    // Prefer whichever actually exists
    if let Some(ref p) = xdg
        && (p.is_dir()
            || p.parent()
                .is_some_and(|pp| pp.join("opencode/opencode.db").exists()))
    {
        return p.clone();
    }
    if let Some(ref p) = platform
        && p.is_dir()
    {
        return p.clone();
    }

    // Fall back to XDG path (most likely on Linux), then platform path
    xdg.or(platform).expect("Cannot determine data directory")
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
    println!(
        "{label}: {}\n",
        if parts.is_empty() {
            "nothing to do".into()
        } else {
            parts.join(", ")
        }
    );
}

fn ingest_claude_code(
    store: &mut SqliteStore,
    graph: &GraphStore,
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

    // Parse all files and group by session ID for subagent aggregation
    let mut session_map: HashMap<Uuid, (Session, Vec<Message>)> = HashMap::new();
    let mut errors = 0u32;

    for path in &session_files {
        match parser.parse_session(path) {
            Ok((mut session, messages)) => {
                compute_summary(&mut session, &messages);
                let sid = session.id;
                match session_map.get_mut(&sid) {
                    Some((existing_session, existing_msgs)) => {
                        merge_parsed_sessions(existing_session, existing_msgs, messages);
                    }
                    None => {
                        session_map.insert(sid, (session, messages));
                    }
                }
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

    // Classify and apply each merged session
    let mut counters = (0u32, 0u32, 0u32);
    for (sid, (session, messages)) in &session_map {
        let label = sid.to_string()[..8].to_string();
        if let Err(e) =
            apply_with_context(store, Some(graph), session, messages, &label, &mut counters)
        {
            eprintln!("  Error: {label}: {e}");
            errors += 1;
        }
    }

    print_summary("Claude Code", counters.0, counters.1, counters.2, errors);
    Ok(())
}

fn ingest_opencode(
    store: &mut SqliteStore,
    graph: &GraphStore,
    opencode_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== OpenCode ===");

    // Detect SQLite vs legacy JSON path
    let db_path = opencode_dir.join("opencode.db");
    if db_path.exists() {
        return ingest_opencode_sqlite(store, graph, &db_path);
    }

    // Legacy JSON path: look for storage/ subdirectory
    let storage_root = opencode_dir.join("storage");
    if !storage_root.is_dir() {
        // Also try if opencode_dir itself is the storage root (backward compat)
        if opencode_dir.join("session").is_dir() {
            return ingest_opencode_json(store, graph, opencode_dir);
        }
        println!("Source not found: {}", opencode_dir.display());
        return Ok(());
    }

    ingest_opencode_json(store, graph, &storage_root)
}

fn ingest_opencode_json(
    store: &mut SqliteStore,
    graph: &GraphStore,
    storage_root: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
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
                let label = path
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string();
                if let Err(e) =
                    apply_with_context(store, Some(graph), &session, &messages, &label, &mut counters)
                {
                    eprintln!("  Error: {label}: {e}");
                    errors += 1;
                }
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

    print_summary("OpenCode", counters.0, counters.1, counters.2, errors);
    Ok(())
}

fn ingest_opencode_sqlite(
    store: &mut SqliteStore,
    graph: &GraphStore,
    db_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    use rusqlite::Connection;
    use voyage_parser::opencode::{discover_sessions_from_db, parse_session_from_db};

    let conn = Connection::open_with_flags(db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let session_ids = discover_sessions_from_db(&conn)?;

    if session_ids.is_empty() {
        println!("No sessions found in {}", db_path.display());
        return Ok(());
    }

    println!("Found {} session(s) in SQLite", session_ids.len());

    // Parse all sessions, tracking parent_id for subagent merging
    let mut parsed: HashMap<String, (Session, Vec<Message>)> = HashMap::new();
    let mut parent_map: HashMap<String, String> = HashMap::new();
    let mut errors = 0u32;

    for sid in &session_ids {
        match parse_session_from_db(&conn, sid) {
            Ok((mut session, messages, parent_id)) => {
                compute_summary(&mut session, &messages);
                if let Some(pid) = parent_id {
                    parent_map.insert(sid.clone(), pid);
                }
                parsed.insert(sid.clone(), (session, messages));
            }
            Err(e) => {
                eprintln!("  Error: {sid}: {e}");
                errors += 1;
            }
        }
    }

    // Merge subagent sessions into their parents
    let child_ids: Vec<String> = parent_map.keys().cloned().collect();
    for child_id in &child_ids {
        if let Some(parent_id) = parent_map.get(child_id)
            && let Some((_, child_msgs)) = parsed.remove(child_id)
            && let Some((parent_session, parent_msgs)) = parsed.get_mut(parent_id)
        {
            merge_parsed_sessions(parent_session, parent_msgs, child_msgs);
        }
    }

    // Classify and apply each session
    let mut counters = (0u32, 0u32, 0u32);
    for (sid, (session, messages)) in &parsed {
        let label = if sid.len() > 8 { &sid[..8] } else { sid };
        if let Err(e) = apply_with_context(store, Some(graph), session, messages, label, &mut counters) {
            eprintln!("  Error: {label}: {e}");
            errors += 1;
        }
    }

    print_summary("OpenCode", counters.0, counters.1, counters.2, errors);
    Ok(())
}

fn ingest_codex(
    store: &mut SqliteStore,
    graph: &GraphStore,
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
                let label = path
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string();
                if let Err(e) =
                    apply_with_context(store, Some(graph), &session, &messages, &label, &mut counters)
                {
                    eprintln!("  Error: {label}: {e}");
                    errors += 1;
                }
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

    print_summary("Codex", counters.0, counters.1, counters.2, errors);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;
    use voyage_core::model::Provider;

    fn make_jsonl_session(dir: &Path, session_id: &str, lines: &[&str]) -> PathBuf {
        let project_dir = dir.join("-Users-test-project");
        std::fs::create_dir_all(&project_dir).unwrap();
        let path = project_dir.join(format!("{session_id}.jsonl"));
        let mut f = std::fs::File::create(&path).unwrap();
        for line in lines {
            writeln!(f, "{line}").unwrap();
        }
        path
    }

    fn make_subagent_jsonl(
        dir: &Path,
        session_id: &str,
        agent_name: &str,
        lines: &[&str],
    ) -> PathBuf {
        let project_dir = dir.join("-Users-test-project");
        let subagents_dir = project_dir.join(session_id).join("subagents");
        std::fs::create_dir_all(&subagents_dir).unwrap();
        let path = subagents_dir.join(format!("{agent_name}.jsonl"));
        let mut f = std::fs::File::create(&path).unwrap();
        for line in lines {
            writeln!(f, "{line}").unwrap();
        }
        path
    }

    #[test]
    fn classify_detects_summary_change() {
        let store = SqliteStore::open_in_memory().unwrap();
        let sid = Uuid::parse_str("9550f7c1-2907-414c-8527-eb992e7af55d").unwrap();

        // Insert initial session
        let mut session = Session::new(
            sid,
            "test".into(),
            Provider::ClaudeCode,
            "opus".into(),
            "/tmp".into(),
        );
        session.message_count = 5;
        session.summary = "old".into();
        store.insert_session(&session).unwrap();

        // New parse with same count but different summary
        session.summary = "new".into();
        let result = classify(&store, &session).unwrap();
        assert!(matches!(result, Upsert::Updated));
    }

    #[test]
    fn classify_detects_message_count_change() {
        let store = SqliteStore::open_in_memory().unwrap();
        let sid = Uuid::parse_str("9550f7c1-2907-414c-8527-eb992e7af55d").unwrap();

        let mut session = Session::new(
            sid,
            "test".into(),
            Provider::ClaudeCode,
            "opus".into(),
            "/tmp".into(),
        );
        session.message_count = 10;
        session.summary = "hello".into();
        store.insert_session(&session).unwrap();

        // Fewer messages (e.g. re-parse after cleanup)
        session.message_count = 8;
        let result = classify(&store, &session).unwrap();
        assert!(matches!(result, Upsert::Updated));
    }

    #[test]
    fn classify_unchanged_when_same() {
        let store = SqliteStore::open_in_memory().unwrap();
        let sid = Uuid::parse_str("9550f7c1-2907-414c-8527-eb992e7af55d").unwrap();

        let mut session = Session::new(
            sid,
            "test".into(),
            Provider::ClaudeCode,
            "opus".into(),
            "/tmp".into(),
        );
        session.message_count = 5;
        session.summary = "hello".into();
        store.insert_session(&session).unwrap();

        let result = classify(&store, &session).unwrap();
        assert!(matches!(result, Upsert::Unchanged));
    }

    #[test]
    fn ingest_claude_code_aggregates_subagents() {
        let dir = TempDir::new().unwrap();
        let source = dir.path();
        let session_id = "9550f7c1-2907-414c-8527-eb992e7af55d";

        // Main session: 1 user + 1 assistant = 2 messages
        let user_line = format!(
            r#"{{"parentUuid":null,"isSidechain":false,"type":"user","message":{{"role":"user","content":"hello"}},"uuid":"a989fd6e-cc80-4861-a21a-9a96dc1eb1e6","timestamp":"2026-03-12T13:17:35.480Z","userType":"external","cwd":"/Users/test","sessionId":"{session_id}","version":"2.1.74","gitBranch":"main"}}"#
        );
        let assistant_line = format!(
            r#"{{"parentUuid":"a989fd6e","isSidechain":false,"message":{{"model":"claude-opus-4-6","id":"msg_01","type":"message","role":"assistant","content":[{{"type":"text","text":"hi"}}],"usage":{{"input_tokens":100,"output_tokens":50}}}},"type":"assistant","uuid":"1b1eebf7-df53-45b1-922b-5b4cef31f63e","timestamp":"2026-03-12T13:17:39.302Z","userType":"external","cwd":"/Users/test","sessionId":"{session_id}","version":"2.1.74"}}"#
        );
        make_jsonl_session(source, session_id, &[&user_line, &assistant_line]);

        // Subagent: 1 user + 1 assistant = 2 messages (same sessionId)
        let sub_user = format!(
            r#"{{"parentUuid":null,"isSidechain":false,"type":"user","message":{{"role":"user","content":"sub task"}},"uuid":"c989fd6e-cc80-4861-a21a-9a96dc1eb1e6","timestamp":"2026-03-12T13:18:00.000Z","userType":"external","cwd":"/Users/test","sessionId":"{session_id}","version":"2.1.74","gitBranch":"main"}}"#
        );
        let sub_assistant = format!(
            r#"{{"parentUuid":"c989fd6e","isSidechain":false,"message":{{"model":"claude-opus-4-6","id":"msg_02","type":"message","role":"assistant","content":[{{"type":"text","text":"done"}}],"usage":{{"input_tokens":200,"output_tokens":100}}}},"type":"assistant","uuid":"d989fd6e-cc80-4861-a21a-9a96dc1eb1e6","timestamp":"2026-03-12T13:18:05.000Z","userType":"external","cwd":"/Users/test","sessionId":"{session_id}","version":"2.1.74"}}"#
        );
        make_subagent_jsonl(
            source,
            session_id,
            "agent-xxx",
            &[&sub_user, &sub_assistant],
        );

        let db_dir = TempDir::new().unwrap();
        let db_path = db_dir.path().join("test.db");
        let mut store = SqliteStore::open(&db_path).unwrap();
        let graph = GraphStore::open_in_memory().unwrap();

        ingest_claude_code(&mut store, &graph, source).unwrap();

        // Should have 1 session with 4 messages (2 from main + 2 from subagent)
        let sessions = store.list_sessions(None, None, 100).unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].message_count, 4);
        assert_eq!(sessions[0].usage.input_tokens, 300); // 100 + 200
        assert_eq!(sessions[0].usage.output_tokens, 150); // 50 + 100
    }

    #[test]
    fn default_opencode_dir_is_not_storage() {
        let dir = default_opencode_dir();
        // Should end with "opencode", not "opencode/storage"
        assert!(
            dir.ends_with("opencode"),
            "Expected path to end with 'opencode', got: {}",
            dir.display()
        );
        assert!(
            !dir.ends_with("opencode/storage"),
            "Path should not end with 'opencode/storage'"
        );
    }

    #[test]
    fn ingest_opencode_sqlite_merges_subagents() {
        use rusqlite::Connection;

        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("opencode.db");

        // Create OpenCode SQLite DB with parent + child sessions
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE session (id TEXT PRIMARY KEY, data TEXT NOT NULL);
             CREATE TABLE message (id TEXT PRIMARY KEY, data TEXT NOT NULL);
             CREATE TABLE part (id TEXT PRIMARY KEY, data TEXT NOT NULL);",
        )
        .unwrap();

        // Parent session
        conn.execute(
            "INSERT INTO session (id, data) VALUES (?1, ?2)",
            rusqlite::params![
                "parent1",
                r#"{"id":"parent1","directory":"/tmp","title":"Main session","time":{"created":1741776000.0,"updated":1741777800.0}}"#
            ],
        ).unwrap();

        // Child/subagent session
        conn.execute(
            "INSERT INTO session (id, data) VALUES (?1, ?2)",
            rusqlite::params![
                "child1",
                r#"{"id":"child1","directory":"/tmp","title":"Subagent","parentId":"parent1","time":{"created":1741776005.0}}"#
            ],
        ).unwrap();

        // Parent message
        conn.execute(
            "INSERT INTO message (id, data) VALUES (?1, ?2)",
            rusqlite::params![
                "msg1",
                r#"{"id":"msg1","role":"user","sessionId":"parent1","time":{"created":1741776000.0}}"#
            ],
        ).unwrap();

        // Child message
        conn.execute(
            "INSERT INTO message (id, data) VALUES (?1, ?2)",
            rusqlite::params![
                "msg2",
                r#"{"id":"msg2","role":"assistant","sessionId":"child1","modelId":"claude-sonnet-4-6","time":{"created":1741776005.0},"tokens":{"input":500,"output":200}}"#
            ],
        ).unwrap();

        drop(conn);

        // Ingest
        let store_dir = TempDir::new().unwrap();
        let store_path = store_dir.path().join("voyage.db");
        let mut store = SqliteStore::open(&store_path).unwrap();
        let graph = GraphStore::open_in_memory().unwrap();

        ingest_opencode_sqlite(&mut store, &graph, &db_path).unwrap();

        // Should have 1 merged session (child merged into parent)
        let sessions = store.list_sessions(None, None, 100).unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].message_count, 2);
    }
}
