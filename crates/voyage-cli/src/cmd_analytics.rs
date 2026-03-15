use std::path::Path;

use voyage_store::sqlite::SqliteStore;

use crate::OutputFormat;

pub fn run(db_path: &Path, format: OutputFormat) -> Result<(), Box<dyn std::error::Error>> {
    let store = SqliteStore::open(db_path)?;

    // Tool usage stats
    let tool_stats = store.get_tool_stats(None)?;
    let sessions = store.list_sessions(None, None, 100_000)?;
    let total_sessions = sessions.len();

    if format == OutputFormat::Machine {
        return output_machine(db_path, &store, &sessions);
    }

    println!("=== Voyage Analytics ===\n");

    // Tool usage
    if !tool_stats.is_empty() {
        println!("  Tool Usage:");
        println!("  {:<14} {:>10} {:>12}", "TOOL", "TOTAL", "AVG/SESSION");
        println!("  {}", "-".repeat(40));
        for ts in &tool_stats {
            let avg = if total_sessions > 0 {
                ts.count as f64 / total_sessions as f64
            } else {
                0.0
            };
            println!("  {:<14} {:>10} {:>12.1}", ts.tool, ts.count, avg);
        }
        println!();
    }

    // Cost anomaly detection
    println!("  Cost Anomalies:");
    let mut anomalies = 0u32;
    for s in &sessions {
        if s.estimated_cost_usd > 2.0 && s.turn_count < 5 {
            let summary = if s.summary.is_empty() {
                "(no summary)"
            } else {
                &s.summary
            };
            println!(
                "  ! ${:.2} in {} turns: {} [{}]",
                s.estimated_cost_usd,
                s.turn_count,
                truncate(summary, 50),
                &s.id.to_string()[..8],
            );
            anomalies += 1;
        }
        if s.turn_count > 50 {
            let summary = if s.summary.is_empty() {
                "(no summary)"
            } else {
                &s.summary
            };
            println!(
                "  ! {} turns (possible loop): {} [{}]",
                s.turn_count,
                truncate(summary, 50),
                &s.id.to_string()[..8],
            );
            anomalies += 1;
        }
    }
    if anomalies == 0 {
        println!("  No anomalies detected.");
    }
    println!();

    // Summary stats
    let total_cost: f64 = sessions.iter().map(|s| s.estimated_cost_usd).sum();
    let avg_cost = if total_sessions > 0 {
        total_cost / total_sessions as f64
    } else {
        0.0
    };
    let avg_turns: f64 = if total_sessions > 0 {
        sessions.iter().map(|s| s.turn_count as f64).sum::<f64>() / total_sessions as f64
    } else {
        0.0
    };

    println!("  Summary:");
    println!("  Total sessions:  {total_sessions}");
    println!("  Total cost:      ${total_cost:.2}");
    println!("  Avg cost/session: ${avg_cost:.2}");
    println!("  Avg turns/session: {avg_turns:.1}");
    println!();

    Ok(())
}

fn output_machine(
    _db_path: &Path,
    store: &SqliteStore,
    sessions: &[voyage_core::model::Session],
) -> Result<(), Box<dyn std::error::Error>> {
    let tool_stats = store.get_tool_stats(None)?;
    let total_sessions = sessions.len();
    let total_cost: f64 = sessions.iter().map(|s| s.estimated_cost_usd).sum();

    let anomalies: Vec<serde_json::Value> = sessions
        .iter()
        .filter(|s| (s.estimated_cost_usd > 2.0 && s.turn_count < 5) || s.turn_count > 50)
        .map(|s| {
            serde_json::json!({
                "session_id": s.id.to_string(),
                "summary": s.summary,
                "cost_usd": s.estimated_cost_usd,
                "turns": s.turn_count,
                "reason": if s.turn_count > 50 { "high_turns" } else { "high_cost_low_turns" },
            })
        })
        .collect();

    let tools: Vec<serde_json::Value> = tool_stats
        .iter()
        .map(|ts| {
            serde_json::json!({
                "tool": ts.tool,
                "total_uses": ts.count,
                "avg_per_session": ts.count as f64 / total_sessions.max(1) as f64,
            })
        })
        .collect();

    let output = serde_json::json!({
        "total_sessions": total_sessions,
        "total_cost_usd": total_cost,
        "avg_cost_per_session": total_cost / total_sessions.max(1) as f64,
        "tool_usage": tools,
        "anomalies": anomalies,
    });

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let mut end = max.min(s.len());
        while !s.is_char_boundary(end) && end > 0 {
            end -= 1;
        }
        format!("{}...", &s[..end])
    }
}
