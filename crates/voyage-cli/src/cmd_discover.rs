use std::collections::HashSet;
use std::path::Path;

use chrono::{Duration, Utc};

use voyage_core::model::{KnowledgeKind, Role};
use voyage_graph::store::GraphStore;
use voyage_store::sqlite::SqliteStore;

/// `voyage discover` — proactive gap detection.
///
/// Scans recent sessions to find:
/// 1. Redundant sessions — questions the knowledge bank already answers
/// 2. Entity gaps — high-frequency entities without EntityPage items
/// 3. Unresolved errors — recurring errors without resolution context
pub fn run(data_dir: &Path, days: u32) -> Result<(), Box<dyn std::error::Error>> {
    let db_path = data_dir.join("voyage.db");
    let store = SqliteStore::open(&db_path)?;
    let since = Utc::now() - Duration::days(days as i64);
    let sessions = store.list_sessions(Some(since), None, 500)?;

    if sessions.is_empty() {
        println!("No sessions found in the last {days} days.");
        return Ok(());
    }

    println!(
        "Scanning {} sessions from the last {days} days...\n",
        sessions.len()
    );

    // ── 1. Redundant Sessions ──
    // Sessions whose first user message closely matches existing knowledge
    let mut redundant = Vec::new();
    for session in &sessions {
        if session.turn_count < 3 || session.summary.is_empty() {
            continue;
        }
        let messages = store.get_messages_by_session(&session.id, 5)?;
        let first_user = messages.iter().find(|m| m.role == Role::User);
        if let Some(msg) = first_user {
            // Extract first ~100 chars as query
            let query: String = msg.content.chars().take(100).collect();
            let query = query.lines().next().unwrap_or(&query).trim();
            if query.len() < 10 {
                continue;
            }
            // Search knowledge bank
            if let Ok(matches) = store.search_knowledge_fts(query, 3) {
                let strong_match = matches.iter().any(|m| {
                    m.level >= 2
                        && (m.kind == KnowledgeKind::Experience || m.kind == KnowledgeKind::World)
                });
                if strong_match {
                    let date = session.started_at.format("%Y-%m-%d").to_string();
                    let cost = session.estimated_cost_usd;
                    redundant.push((date, truncate(&session.summary, 60), cost));
                }
            }
        }
    }

    if !redundant.is_empty() {
        println!("## Redundant Sessions");
        println!("Sessions where knowledge bank already had the answer:\n");
        let mut total_waste = 0.0;
        for (date, summary, cost) in &redundant {
            println!("- [{date}] {summary} (${cost:.2})");
            total_waste += cost;
        }
        println!(
            "\n{} sessions, ${:.2} potentially avoidable\n",
            redundant.len(),
            total_waste
        );
    }

    // ── 2. Entity Gaps ──
    // High-mention entities without EntityPage knowledge items
    let graph_path = data_dir.join("graph.db");
    if graph_path.exists()
        && let Ok(graph) = GraphStore::open(&graph_path)
    {
            let entities = graph.list_entities(None, 100)?;
            let entity_pages =
                store.list_knowledge_items(Some(KnowledgeKind::EntityPage), None, 1000)?;
            let page_names: HashSet<String> = entity_pages
                .iter()
                .map(|p| p.title.to_lowercase())
                .collect();

            let mut gaps: Vec<_> = entities
                .iter()
                .filter(|e| {
                    e.mention_count >= 5
                        && !page_names.contains(&e.name.to_lowercase())
                        && !page_names
                            .contains(&format!("entity: {}", e.name.to_lowercase()))
                })
                .map(|e| {
                    (
                        e.kind.as_str().to_string(),
                        e.name.clone(),
                        e.mention_count,
                        e.session_count,
                    )
                })
                .collect();
            gaps.sort_by(|a, b| b.2.cmp(&a.2));
            gaps.truncate(15);

            if !gaps.is_empty() {
                println!("## Entity Gaps");
                println!("High-frequency entities without EntityPage summaries:\n");
                for (kind, name, mentions, sessions) in &gaps {
                    println!("- {name} ({kind}) — {mentions} mentions, {sessions} sessions");
                }
                println!();
            }
    }

    // ── 3. Unresolved Errors ──
    // Experience items with "Error:" title that recur but lack resolution
    let errors =
        store.list_knowledge_items(Some(KnowledgeKind::Experience), None, 500)?;
    let unresolved: Vec<_> = errors
        .iter()
        .filter(|e| {
            e.title.starts_with("Error:")
                && e.mention_count >= 2
                && !e.content.contains("Resolution context:")
        })
        .take(10)
        .collect();

    if !unresolved.is_empty() {
        println!("## Unresolved Errors");
        println!("Recurring errors without extracted resolutions:\n");
        for item in &unresolved {
            let err = item.title.strip_prefix("Error: ").unwrap_or(&item.title);
            println!(
                "- {} (seen {} times, since {})",
                truncate(err, 70),
                item.mention_count,
                item.source_date
            );
        }
        println!();
    }

    // ── Summary ──
    if redundant.is_empty() && unresolved.is_empty() {
        println!("No significant gaps found. Knowledge bank is in good shape.");
    }

    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    let first_line = s.lines().next().unwrap_or(s);
    if first_line.chars().count() > max {
        let truncated: String = first_line.chars().take(max - 3).collect();
        format!("{truncated}...")
    } else {
        first_line.to_string()
    }
}
