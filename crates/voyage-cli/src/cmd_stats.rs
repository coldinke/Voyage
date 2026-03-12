use std::path::Path;

use chrono::{Duration, Utc};
use voyage_store::sqlite::SqliteStore;

pub fn run(
    db_path: &Path,
    days: u32,
    project: Option<&str>,
    by_model: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if !db_path.exists() {
        println!("No data yet. Run `voyage ingest` first.");
        return Ok(());
    }

    let store = SqliteStore::open(db_path)?;
    let since = Utc::now() - Duration::days(days as i64);

    if by_model {
        let stats = store.get_stats_by_model(Some(since))?;
        if stats.is_empty() {
            println!("No usage data for the last {days} day(s).");
            return Ok(());
        }
        println!("Token usage by model (last {days} day(s)):\n");
        println!(
            "{:<25} {:>12} {:>12} {:>10} {:>8}",
            "Model", "Input", "Output", "Cost", "Sessions"
        );
        println!("{}", "-".repeat(70));
        for s in &stats {
            println!(
                "{:<25} {:>12} {:>12} {:>10} {:>8}",
                s.model,
                format_tokens(s.input_tokens),
                format_tokens(s.output_tokens),
                format!("${:.4}", s.total_cost_usd),
                s.session_count,
            );
        }
    } else {
        let stats = store.get_stats(Some(since), project)?;
        if stats.session_count == 0 {
            println!("No usage data for the last {days} day(s).");
            return Ok(());
        }
        let total_tokens = stats.input_tokens
            + stats.output_tokens
            + stats.cache_read_tokens
            + stats.cache_creation_tokens;

        println!("Token usage (last {days} day(s)):\n");
        if let Some(p) = project {
            println!("  Project:          {p}");
        }
        println!("  Sessions:         {}", stats.session_count);
        println!("  Input tokens:     {}", format_tokens(stats.input_tokens));
        println!("  Output tokens:    {}", format_tokens(stats.output_tokens));
        println!(
            "  Cache read:       {}",
            format_tokens(stats.cache_read_tokens)
        );
        println!(
            "  Cache creation:   {}",
            format_tokens(stats.cache_creation_tokens)
        );
        println!("  Total tokens:     {}", format_tokens(total_tokens));
        println!("  Estimated cost:   ${:.4}", stats.total_cost_usd);
    }

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
