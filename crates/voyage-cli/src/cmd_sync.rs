use std::io::{self, Write};
use std::path::Path;

use voyage_store::knowledge::generate_context;
use voyage_store::sqlite::SqliteStore;

use crate::{cmd_distill, cmd_graph, cmd_ingest};

/// Run the full pipeline: ingest → extract → distill → promote.
/// Optionally configure CC SessionStart hook for context injection.
pub fn run(data_dir: &Path, inject: bool) -> Result<(), Box<dyn std::error::Error>> {
    let db_path = data_dir.join("voyage.db");
    let graph_path = data_dir.join("graph.db");

    // ── Stage 1: Ingest ──
    cmd_ingest::run(&db_path, None, None)?;

    // ── Stage 2: Graph Extract ──
    println!("=== Graph Extract ===");
    cmd_graph::run_extract(&graph_path, &db_path, false)?;

    // ── Stage 3: Distill + Promote ──
    println!("=== Distill ===");
    cmd_distill::run(&db_path, false, true)?;

    // ── Summary ──
    let store = SqliteStore::open(&db_path)?;
    let ctx = generate_context(&store)?;
    let line_count = ctx.lines().count();

    if ctx.is_empty() {
        println!("\n✓ Pipeline complete. No knowledge extracted yet.");
        return Ok(());
    }

    // ── Show context preview ──
    println!("\n✓ Pipeline complete.\n");
    println!("Context preview ({line_count} lines):");
    println!("{}", "-".repeat(60));
    // Show first 15 lines as preview
    for line in ctx.lines().take(15) {
        println!("  {line}");
    }
    if line_count > 15 {
        println!("  ... ({} more lines)", line_count - 15);
    }
    println!("{}", "-".repeat(60));

    // ── CC Injection ──
    let should_inject = if inject {
        true
    } else {
        prompt_inject(data_dir)?
    };

    if should_inject {
        inject_cc_hook(data_dir, &ctx)?;
    }

    Ok(())
}

/// Ask user whether to inject context into CC.
fn prompt_inject(data_dir: &Path) -> Result<bool, Box<dyn std::error::Error>> {
    let settings_path = find_project_settings(data_dir);
    let already_configured = settings_path
        .as_ref()
        .and_then(|p| std::fs::read_to_string(p).ok())
        .map(|s| s.contains("voyage context"))
        .unwrap_or(false);

    if already_configured {
        println!("\n✓ CC hook already configured. Context will auto-load next session.");
        return Ok(false);
    }

    print!("\nInject into Claude Code? [y/N] ");
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(input.trim().eq_ignore_ascii_case("y"))
}

/// Configure CC SessionStart hook to run `voyage context`.
fn inject_cc_hook(
    data_dir: &Path,
    _ctx: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Find the project's .claude directory
    let project_root = find_project_root(data_dir);
    let claude_dir = project_root.join(".claude");
    std::fs::create_dir_all(&claude_dir)?;

    let settings_path = claude_dir.join("settings.json");

    // Read existing settings or start fresh
    let mut settings: serde_json::Value = if settings_path.exists() {
        let content = std::fs::read_to_string(&settings_path)?;
        serde_json::from_str(&content).unwrap_or(serde_json::json!({}))
    } else {
        serde_json::json!({})
    };

    // Determine the voyage binary path
    let voyage_bin = which_voyage();

    // Add SessionStart hook
    let hook = serde_json::json!({
        "hooks": [
            {
                "type": "command",
                "command": format!("{voyage_bin} context")
            }
        ]
    });

    // Merge into existing hooks
    let hooks = settings
        .as_object_mut()
        .unwrap()
        .entry("hooks")
        .or_insert(serde_json::json!({}));

    let session_start = hooks
        .as_object_mut()
        .unwrap()
        .entry("SessionStart")
        .or_insert(serde_json::json!([]));

    // Check if voyage hook already exists
    let already_exists = session_start
        .as_array()
        .map(|arr| arr.iter().any(|h| {
            h.get("hooks")
                .and_then(|hooks| hooks.as_array())
                .map(|hooks| hooks.iter().any(|h| {
                    h.get("command")
                        .and_then(|c| c.as_str())
                        .map(|c| c.contains("voyage context"))
                        .unwrap_or(false)
                }))
                .unwrap_or(false)
        }))
        .unwrap_or(false);

    if !already_exists {
        session_start
            .as_array_mut()
            .unwrap()
            .push(hook);
    }

    // Write back
    let formatted = serde_json::to_string_pretty(&settings)?;
    std::fs::write(&settings_path, formatted)?;

    println!("✓ Hook configured: {}", settings_path.display());
    println!("  `voyage context` will run at the start of each CC session.");
    println!("  To remove: edit {} and delete the SessionStart hook.", settings_path.display());

    Ok(())
}

/// Find project root by walking up from data_dir or cwd.
fn find_project_root(data_dir: &Path) -> std::path::PathBuf {
    // If data_dir is under a project, use its parent.
    // Otherwise fall back to cwd.
    let cwd = std::env::current_dir().unwrap_or_else(|_| data_dir.to_path_buf());

    // Walk up to find .git or .claude
    let mut dir = cwd.as_path();
    loop {
        if dir.join(".git").exists() || dir.join(".claude").exists() {
            return dir.to_path_buf();
        }
        match dir.parent() {
            Some(parent) => dir = parent,
            None => return cwd,
        }
    }
}

fn find_project_settings(data_dir: &Path) -> Option<std::path::PathBuf> {
    let root = find_project_root(data_dir);
    let path = root.join(".claude/settings.json");
    if path.exists() { Some(path) } else { None }
}

fn which_voyage() -> String {
    // 1. Try the release binary in the project
    let project_root = std::env::current_dir().unwrap_or_default();
    let release_bin = project_root.join("target/release/voyage");
    if release_bin.exists() {
        return release_bin.to_string_lossy().to_string();
    }

    // 2. Try PATH
    if let Ok(output) = std::process::Command::new("which").arg("voyage").output()
        && output.status.success()
    {
        let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !path.is_empty() {
            return path;
        }
    }

    // 3. Fallback: ~/.cargo/bin/
    let home = dirs_next::home_dir().unwrap_or_default();
    home.join(".cargo/bin/voyage").to_string_lossy().to_string()
}
