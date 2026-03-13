use std::path::Path;

use chrono::{DateTime, Utc};
use voyage_store::sqlite::SqliteStore;

pub fn run_list(
    db_path: &Path,
    since: Option<DateTime<Utc>>,
    days: u32,
    limit: usize,
    project: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    if !db_path.exists() {
        println!("No data yet. Run `voyage ingest` first.");
        return Ok(());
    }

    let store = SqliteStore::open(db_path)?;
    let sessions = store.list_sessions(since, project, limit)?;
    let period_label = if since.is_none() {
        "all time".to_string()
    } else {
        format!("last {days} day(s)")
    };

    if sessions.is_empty() {
        println!("No sessions found for {period_label}.");
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

pub fn run_show(db_path: &Path, id: &str) -> Result<(), Box<dyn std::error::Error>> {
    if !db_path.exists() {
        println!("No data yet. Run `voyage ingest` first.");
        return Ok(());
    }

    let store = SqliteStore::open(db_path)?;

    // Try exact UUID first, then prefix match
    let session = if let Ok(uuid) = uuid::Uuid::parse_str(id) {
        store.get_session(&uuid)?
    } else {
        store.find_session_by_prefix(id)?
    };

    let session = match session {
        Some(s) => s,
        None => {
            println!("No session found matching '{id}'.");
            return Ok(());
        }
    };

    // Session metadata
    println!("Session: {}", session.id);
    println!("  Project:    {}", session.project);
    println!("  Provider:   {:?}", session.provider);
    println!("  Model:      {}", session.model);
    println!(
        "  Started:    {}",
        session.started_at.format("%Y-%m-%d %H:%M:%S")
    );
    if let Some(ended) = session.ended_at {
        println!("  Ended:      {}", ended.format("%Y-%m-%d %H:%M:%S"));
    }
    println!("  Messages:   {}", session.message_count);
    println!("  Turns:      {}", session.turn_count);
    println!("  Total cost: ${:.4}", session.estimated_cost_usd);
    if !session.summary.is_empty() {
        println!("  Summary:    {}", session.summary);
    }

    // Message-level cost breakdown
    let messages = store.get_message_costs(&session.id)?;
    if messages.is_empty() {
        println!("\nNo message details available.");
        return Ok(());
    }

    println!("\nMessage cost breakdown:\n");
    println!(
        "{:>3} {:<10} {:<22} {:>10} {:>10} {:>10} {:<}",
        "#", "ROLE", "MODEL", "TOKENS", "COST", "", "PREVIEW"
    );
    println!("{}", "-".repeat(100));

    let mut total_cost = 0.0;
    let mut total_tokens: u64 = 0;

    for (i, m) in messages.iter().enumerate() {
        let msg_tokens =
            m.input_tokens + m.output_tokens + m.cache_read_tokens + m.cache_creation_tokens;
        total_cost += m.estimated_cost;
        total_tokens += msg_tokens;

        let tokens_str = format_tokens(msg_tokens);
        let model_short = if m.model.len() > 20 {
            format!("{}...", &m.model[..17])
        } else {
            m.model.clone()
        };
        let preview = if m.content_preview.len() > 40 {
            let end = m
                .content_preview
                .char_indices()
                .nth(37)
                .map(|(idx, _)| idx)
                .unwrap_or(m.content_preview.len().min(37));
            format!("{}...", &m.content_preview[..end])
        } else {
            m.content_preview.clone()
        };

        println!(
            "{:>3} {:<10} {:<22} {:>10} {:>10} {:>10} {:<}",
            i + 1,
            m.role,
            model_short,
            tokens_str,
            format!("${:.4}", m.estimated_cost),
            "",
            preview,
        );
    }

    println!("{}", "-".repeat(100));
    println!(
        "{:>3} {:<10} {:<22} {:>10} {:>10}",
        "",
        "TOTAL",
        "",
        format_tokens(total_tokens),
        format!("${:.4}", total_cost),
    );

    Ok(())
}

fn format_tokens(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}
