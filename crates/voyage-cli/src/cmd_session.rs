use std::path::Path;

use chrono::{Duration, Utc};
use voyage_store::sqlite::SqliteStore;

pub fn run_list(
    db_path: &Path,
    days: u32,
    limit: usize,
    project: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    if !db_path.exists() {
        println!("No data yet. Run `voyage ingest` first.");
        return Ok(());
    }

    let store = SqliteStore::open(db_path)?;
    let since = Utc::now() - Duration::days(days as i64);
    let sessions = store.list_sessions(Some(since), project, limit)?;

    if sessions.is_empty() {
        println!("No sessions found for the last {days} day(s).");
        return Ok(());
    }

    println!(
        "{:<38} {:<20} {:<22} {:>8} {:>6} {:>10}",
        "Session ID", "Project", "Started", "Tokens", "Turns", "Cost"
    );
    println!("{}", "-".repeat(108));

    for s in &sessions {
        let project_short = if s.project.len() > 18 {
            format!("...{}", &s.project[s.project.len() - 15..])
        } else {
            s.project.clone()
        };
        let total = s.usage.total();
        let tokens_str = if total >= 1_000_000 {
            format!("{:.1}M", total as f64 / 1_000_000.0)
        } else if total >= 1_000 {
            format!("{:.1}K", total as f64 / 1_000.0)
        } else {
            total.to_string()
        };

        println!(
            "{:<38} {:<20} {:<22} {:>8} {:>6} {:>10}",
            s.id,
            project_short,
            s.started_at.format("%Y-%m-%d %H:%M:%S"),
            tokens_str,
            s.turn_count,
            format!("${:.4}", s.estimated_cost_usd),
        );
    }

    println!("\n{} session(s) shown", sessions.len());
    Ok(())
}
