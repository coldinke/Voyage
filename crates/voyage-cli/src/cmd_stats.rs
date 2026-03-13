use std::path::Path;

use chrono::{DateTime, Utc};
use voyage_core::model::cost_rates;
use voyage_store::sqlite::SqliteStore;

pub fn run(
    db_path: &Path,
    since: Option<DateTime<Utc>>,
    days: u32,
    project: Option<&str>,
    by_model: bool,
    daily: bool,
    blocks: bool,
    by_provider: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if !db_path.exists() {
        println!("No data yet. Run `voyage ingest` first.");
        return Ok(());
    }

    let store = SqliteStore::open(db_path)?;

    if daily {
        return run_daily(&store, since, days);
    }
    if blocks {
        return run_blocks(&store);
    }
    if by_provider {
        return run_by_provider(&store, since, days);
    }

    let period_label = if since.is_none() {
        "all time".to_string()
    } else {
        format!("last {days} day(s)")
    };

    if by_model {
        let stats = store.get_stats_by_model(since)?;
        if stats.is_empty() {
            println!("No usage data for {period_label}.");
            return Ok(());
        }
        println!("Token usage by model ({period_label}):\n");
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
        let stats = store.get_stats(since, project)?;
        if stats.session_count == 0 {
            println!("No usage data for {period_label}.");
            return Ok(());
        }
        let total_tokens = stats.input_tokens
            + stats.output_tokens
            + stats.cache_read_tokens
            + stats.cache_creation_tokens;

        println!("Token usage ({period_label}):\n");
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

        // Cache efficiency metrics
        let cache_denominator =
            stats.input_tokens + stats.cache_read_tokens + stats.cache_creation_tokens;
        if cache_denominator > 0 {
            let cache_hit_rate =
                stats.cache_read_tokens as f64 / cache_denominator as f64 * 100.0;
            println!("  Cache hit rate:   {cache_hit_rate:.1}%");

            // Compute cache savings: sum of (cache_read * (input_rate - cache_read_rate)) per model
            let cache_by_model = store.get_cache_read_by_model(since)?;
            let savings: f64 = cache_by_model
                .iter()
                .map(|(model, cache_read)| {
                    let (input_rate, _, cache_read_rate, _) = cost_rates(model);
                    *cache_read as f64 * (input_rate - cache_read_rate) / 1_000_000.0
                })
                .sum();
            if savings > 0.0 {
                println!("  Cache savings:    ${savings:.2}");
            }
        }
    }

    Ok(())
}

fn run_daily(
    store: &SqliteStore,
    since: Option<DateTime<Utc>>,
    days: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let daily = store.get_daily_stats(since)?;
    let period_label = if since.is_none() {
        "all time".to_string()
    } else {
        format!("last {days} day(s)")
    };

    if daily.is_empty() {
        println!("No daily data for {period_label}.");
        return Ok(());
    }

    println!("Daily usage ({period_label}):\n");
    println!(
        "{:<12} {:>8} {:>6} {:>10} {:>10} {:>10}  {}",
        "DATE", "SESSIONS", "TURNS", "INPUT", "OUTPUT", "COST", ""
    );
    println!("{}", "-".repeat(78));

    let max_cost = daily
        .iter()
        .map(|d| d.total_cost_usd)
        .fold(0.0f64, f64::max);

    let mut total_sessions: u64 = 0;
    let mut total_turns: u64 = 0;
    let mut total_input: u64 = 0;
    let mut total_output: u64 = 0;
    let mut total_cost: f64 = 0.0;

    for d in &daily {
        total_sessions += d.session_count;
        total_turns += d.turn_count;
        total_input += d.input_tokens;
        total_output += d.output_tokens;
        total_cost += d.total_cost_usd;

        let bar_len = if max_cost > 0.0 {
            (d.total_cost_usd / max_cost * 20.0) as usize
        } else {
            0
        };
        let bar: String = "\u{2588}".repeat(bar_len);

        println!(
            "{:<12} {:>8} {:>6} {:>10} {:>10} {:>10}  {}",
            d.date,
            d.session_count,
            d.turn_count,
            format_tokens(d.input_tokens),
            format_tokens(d.output_tokens),
            format!("${:.4}", d.total_cost_usd),
            bar,
        );
    }

    let n = daily.len() as f64;
    println!("{}", "-".repeat(78));
    println!(
        "{:<12} {:>8} {:>6} {:>10} {:>10} {:>10}",
        "TOTAL",
        total_sessions,
        total_turns,
        format_tokens(total_input),
        format_tokens(total_output),
        format!("${:.4}", total_cost),
    );
    println!(
        "{:<12} {:>8.1} {:>6.1} {:>10} {:>10} {:>10}",
        "AVG/DAY",
        total_sessions as f64 / n,
        total_turns as f64 / n,
        format_tokens((total_input as f64 / n) as u64),
        format_tokens((total_output as f64 / n) as u64),
        format!("${:.4}", total_cost / n),
    );

    Ok(())
}

fn run_blocks(store: &SqliteStore) -> Result<(), Box<dyn std::error::Error>> {
    let windows = store.get_billing_window_stats(5)?;

    if windows.is_empty() {
        println!("No billing window data.");
        return Ok(());
    }

    println!("Billing windows (5-hour):\n");
    println!(
        "{:<40} {:>8} {:>12} {:>10}",
        "WINDOW", "SESSIONS", "TOKENS", "COST"
    );
    println!("{}", "-".repeat(74));

    let now = Utc::now();

    for (i, w) in windows.iter().enumerate() {
        let total_tokens = w.input_tokens + w.output_tokens + w.cache_read_tokens;
        let window_label = format!("{} — {}", &w.window_start[..16], &w.window_end[..16]);

        // Check if this is the current active window
        let is_active = if let Ok(end) =
            DateTime::parse_from_rfc3339(&format!("{}+00:00", w.window_end.replace(' ', "T")))
        {
            now < end.with_timezone(&Utc)
        } else {
            i == 0
        };

        let marker = if is_active { " *" } else { "" };

        println!(
            "{:<40} {:>8} {:>12} {:>10}{}",
            window_label,
            w.session_count,
            format_tokens(total_tokens),
            format!("${:.4}", w.total_cost_usd),
            marker,
        );
    }

    println!("\n  * = current window");

    Ok(())
}

fn run_by_provider(
    store: &SqliteStore,
    since: Option<DateTime<Utc>>,
    days: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let stats = store.get_stats_by_provider(since)?;
    let period_label = if since.is_none() {
        "all time".to_string()
    } else {
        format!("last {days} day(s)")
    };

    if stats.is_empty() {
        println!("No usage data for {period_label}.");
        return Ok(());
    }

    println!("Token usage by provider ({period_label}):\n");
    println!(
        "{:<20} {:>12} {:>12} {:>10} {:>8}",
        "PROVIDER", "INPUT", "OUTPUT", "COST", "SESSIONS"
    );
    println!("{}", "-".repeat(66));

    for s in &stats {
        println!(
            "{:<20} {:>12} {:>12} {:>10} {:>8}",
            s.provider,
            format_tokens(s.input_tokens),
            format_tokens(s.output_tokens),
            format!("${:.4}", s.total_cost_usd),
            s.session_count,
        );
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
