use std::path::Path;

use voyage_store::knowledge::generate_context;
use voyage_store::sqlite::SqliteStore;

use crate::cmd_search;

/// `voyage context [query]` — output CC-optimized project context.
///
/// Without a query: static context from knowledge bank (recent sessions,
/// preferences, tech stack, known issues, cost baseline).
///
/// With a query: static context + targeted search results merged into
/// a single LLM-optimized output.
pub fn run(
    data_dir: &Path,
    query: Option<&str>,
    limit: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let db_path = data_dir.join("voyage.db");
    let store = SqliteStore::open(&db_path)?;

    // Always emit static context
    let static_ctx = generate_context(&store)?;

    if static_ctx.is_empty() && query.is_none() {
        eprintln!("No context available. Run `voyage ingest` then `voyage distill --promote` first.");
        return Ok(());
    }

    if !static_ctx.is_empty() {
        print!("{static_ctx}");
    }

    // If query provided, append search results
    if let Some(q) = query {
        let results = cmd_search::search_enriched(data_dir, q, limit, None, None)?;
        if !results.is_empty() {
            let formatted = cmd_search::format_results_context(&results);
            print!("{formatted}");
        }
    }

    Ok(())
}
