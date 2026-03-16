use std::path::Path;

use voyage_store::knowledge::compute_profile;
use voyage_store::sqlite::SqliteStore;

use crate::OutputFormat;

pub fn run(
    db_path: &Path,
    refresh: bool,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let store = SqliteStore::open(db_path)?;

    let profile = if refresh {
        let p = compute_profile(&store)?;
        store.save_profile(&p)?;
        println!("Profile refreshed.");
        p
    } else {
        match store.load_profile()? {
            Some(p) => p,
            None => {
                println!("No profile found. Run `voyage profile --refresh` to compute.");
                return Ok(());
            }
        }
    };

    if format == OutputFormat::Machine {
        println!("{}", serde_json::to_string_pretty(&profile)?);
        return Ok(());
    }

    // Human-readable output
    println!("User Profile (computed: {})\n", profile.computed_at);
    println!("Sessions analyzed: {}\n", profile.session_count);

    // Tech stack
    if !profile.tech_stack.is_empty() {
        println!("Tech Stack:");
        for entry in &profile.tech_stack {
            println!("  {:<20} ({} mentions)", entry.name, entry.frequency);
        }
        println!();
    }

    // Working style
    println!("Working Style:");
    println!("  Avg turns/session:  {:.1}", profile.working_style.avg_turns);
    println!("  Avg cost/session:   ${:.4}", profile.working_style.avg_cost);
    if !profile.working_style.preferred_hours.is_empty() {
        let hours: Vec<String> = profile
            .working_style
            .preferred_hours
            .iter()
            .map(|h| format!("{h:02}:00"))
            .collect();
        println!("  Active hours:       {}", hours.join(", "));
    }
    println!();

    // Cost patterns
    println!("Cost Patterns:");
    println!("  Average: ${:.4}", profile.cost_patterns.avg);
    println!("  P50:     ${:.4}", profile.cost_patterns.p50);
    println!("  P90:     ${:.4}", profile.cost_patterns.p90);
    println!();

    // Expertise
    if !profile.expertise_areas.is_empty() {
        println!("Top Expertise Areas:");
        for area in &profile.expertise_areas {
            println!(
                "  {:<40} (confidence: {:.1}, sessions: {})",
                truncate(&area.area, 40),
                area.confidence,
                area.session_count,
            );
        }
        println!();
    }

    // Preferences
    if !profile.preferences.is_empty() {
        println!("Preferences:");
        for pref in &profile.preferences {
            println!("  {} (confidence: {:.1})", pref.key, pref.confidence);
        }
        println!();
    }

    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let mut end = max;
        while !s.is_char_boundary(end) && end > 0 {
            end -= 1;
        }
        format!("{}...", &s[..end])
    }
}
