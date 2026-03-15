use std::path::Path;

use voyage_graph::store::GraphStore;
use voyage_store::sqlite::SqliteStore;

pub fn run(
    data_dir: &Path,
    file: Option<&str>,
    entity: Option<&str>,
    cost: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let db_path = data_dir.join("voyage.db");
    let graph_path = data_dir.join("graph.db");

    let store = SqliteStore::open(&db_path)?;

    if let Some(file_name) = file {
        return suggest_by_file(&graph_path, &store, file_name);
    }
    if let Some(entity_name) = entity {
        return suggest_by_entity(&graph_path, &store, entity_name);
    }
    if let Some(description) = cost {
        return suggest_cost(&store, description);
    }

    eprintln!("Usage: voyage suggest --file <path> | --entity <name> | --cost <description>");
    Ok(())
}

fn suggest_by_file(
    graph_path: &Path,
    store: &SqliteStore,
    file_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let graph = GraphStore::open(graph_path)?;

    let sessions = graph.sessions_for_entity(file_name)?;
    if sessions.is_empty() {
        println!("No past sessions found touching: {file_name}");
        return Ok(());
    }

    println!("## Sessions touching \"{file_name}\"\n");
    for (sid, ts, mention_count) in sessions.iter().take(10) {
        let session = store.get_session(sid)?;
        if let Some(s) = session {
            let rating = store
                .get_rating(&s.id)
                .ok()
                .flatten()
                .map(|r| format!(" [{r}/5]"))
                .unwrap_or_default();
            println!(
                "- {} (${:.2}, {} turns){}: {}",
                ts.format("%Y-%m-%d"),
                s.estimated_cost_usd,
                s.turn_count,
                rating,
                if s.summary.is_empty() {
                    "(no summary)"
                } else {
                    &s.summary
                },
            );
            if *mention_count > 1 {
                println!("  ({mention_count} mentions)");
            }
        }
    }
    println!();
    Ok(())
}

fn suggest_by_entity(
    graph_path: &Path,
    store: &SqliteStore,
    entity_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let graph = GraphStore::open(graph_path)?;

    let sessions = graph.sessions_for_entity(entity_name)?;
    if sessions.is_empty() {
        println!("No past sessions found for entity: {entity_name}");
        return Ok(());
    }

    println!("## Sessions related to \"{entity_name}\"\n");
    for (sid, ts, _) in sessions.iter().take(10) {
        let session = store.get_session(sid)?;
        if let Some(s) = session {
            println!(
                "- {} (${:.2}): {}",
                ts.format("%Y-%m-%d"),
                s.estimated_cost_usd,
                if s.summary.is_empty() {
                    "(no summary)"
                } else {
                    &s.summary
                },
            );
        }
    }
    println!();

    // Show related entities
    let related = graph.related_entities_pmi(entity_name, 5)?;
    if !related.is_empty() {
        println!(
            "Related: {}\n",
            related
                .iter()
                .map(|(e, _)| e.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    Ok(())
}

fn suggest_cost(store: &SqliteStore, _description: &str) -> Result<(), Box<dyn std::error::Error>> {
    let sessions = store.list_sessions(None, None, 1000)?;
    if sessions.is_empty() {
        println!("No sessions available for cost estimation.");
        return Ok(());
    }

    let total_cost: f64 = sessions.iter().map(|s| s.estimated_cost_usd).sum();
    let avg_cost = total_cost / sessions.len() as f64;
    let avg_turns: f64 =
        sessions.iter().map(|s| s.turn_count as f64).sum::<f64>() / sessions.len() as f64;

    // Sort by cost to find percentiles
    let mut costs: Vec<f64> = sessions.iter().map(|s| s.estimated_cost_usd).collect();
    costs.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let p50 = costs[costs.len() / 2];
    let p90 = costs[costs.len() * 9 / 10];

    println!("## Cost Estimate\n");
    println!("Based on {} past sessions:", sessions.len());
    println!("  Average:  ${avg_cost:.2} ({avg_turns:.0} turns)");
    println!("  Median:   ${p50:.2}");
    println!("  P90:      ${p90:.2}");
    println!();

    Ok(())
}
