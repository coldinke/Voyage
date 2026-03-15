use std::path::Path;

use voyage_core::model::{Message, Role, Session};
use voyage_graph::entity::EntityKind;
use voyage_graph::extract::{ExtractionContext, extract_entities};
use voyage_graph::store::GraphStore;
use voyage_store::sqlite::SqliteStore;

/// Run entity extraction for a single session.
pub fn extract_session_entities(
    graph: &GraphStore,
    session: &Session,
    messages: &[Message],
) -> Result<u32, Box<dyn std::error::Error>> {
    // Clear old mentions (handles Updated scenario)
    graph.delete_mentions_for_session(&session.id)?;

    let mut count = 0u32;

    for msg in messages {
        let ctx = ExtractionContext {
            session_id: session.id,
            message_id: Some(msg.id),
            timestamp: msg.timestamp,
            cwd: session.cwd.clone(),
            git_branch: session.git_branch.clone(),
            tool_calls: msg.tool_calls.clone(),
            is_user_message: msg.role == Role::User,
        };
        let extraction = extract_entities(&msg.content, &ctx);
        for (entity, mention) in extraction.entities {
            graph.record_mention(&entity, &mention)?;
            count += 1;
        }
    }

    graph.mark_session_extracted(&session.id, count)?;

    // Build edges for this session
    graph.build_edges_for_session(&session.id)?;

    Ok(count)
}

/// `voyage graph stats` — show entity overview.
pub fn run_stats(graph_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let graph = GraphStore::open(graph_path)?;

    let total_entities = graph.entity_count()?;
    let total_mentions = graph.mention_count()?;
    let total_edges = graph.edge_count()?;
    let extracted = graph.extracted_session_count()?;

    println!("=== Knowledge Graph ===\n");
    println!("  Sessions extracted:  {extracted}");
    println!("  Total entities:      {total_entities}");
    println!("  Total mentions:      {total_mentions}");
    println!("  Total edges:         {total_edges}");

    let stats = graph.entity_stats()?;
    if !stats.is_empty() {
        println!("\n  By kind:");
        for (kind, count) in &stats {
            println!("    {:<14} {count:>6}", kind.as_str());
        }
    }

    println!();
    Ok(())
}

/// `voyage graph list` — list entities of a given kind.
pub fn run_list(
    graph_path: &Path,
    kind: Option<&str>,
    limit: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let graph = GraphStore::open(graph_path)?;

    let entity_kind = match kind {
        Some(k) => Some(
            k.parse::<EntityKind>()
                .map_err(|_| format!("Unknown entity kind: {k}. Valid: file, function, module, concept, tool, error, dependency, git_branch"))?,
        ),
        None => None,
    };

    let entities = graph.list_entities(entity_kind, limit)?;

    if entities.is_empty() {
        println!("No entities found.");
        return Ok(());
    }

    let kind_label = kind.unwrap_or("all");
    println!("=== Entities ({kind_label}) ===\n");
    println!(
        "  {:<14} {:<40} {:>8} {:>8}",
        "KIND", "NAME", "MENTIONS", "SESSIONS"
    );
    println!("  {}", "-".repeat(74));

    for e in &entities {
        println!(
            "  {:<14} {:<40} {:>8} {:>8}",
            e.kind.as_str(),
            truncate(&e.name, 40),
            e.mention_count,
            e.session_count,
        );
    }
    println!();
    Ok(())
}

/// `voyage graph mentions <name>` — show sessions that mention an entity.
pub fn run_mentions(
    graph_path: &Path,
    db_path: &Path,
    name: &str,
    limit: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let graph = GraphStore::open(graph_path)?;

    let entity = graph.find_entity_by_name(name)?;
    if entity.is_none() {
        println!("Entity not found: {name}");
        return Ok(());
    }
    let entity = entity.unwrap();

    println!(
        "=== Mentions of {} ({}) ===\n",
        entity.name,
        entity.kind.as_str()
    );
    println!("  Total mentions: {}", entity.mention_count);
    println!();

    let sessions = graph.sessions_for_entity(name)?;
    if sessions.is_empty() {
        println!("  No sessions found.");
        return Ok(());
    }

    // Cross-reference with voyage.db for session summaries
    let store = SqliteStore::open(db_path).ok();

    println!(
        "  {:<10} {:<20} {:>8}  SUMMARY",
        "SESSION", "DATE", "MENTIONS"
    );
    println!("  {}", "-".repeat(74));

    for (sid, ts, count) in sessions.iter().take(limit) {
        let summary = store
            .as_ref()
            .and_then(|s| s.get_session(sid).ok())
            .and_then(|s| s)
            .map(|s| s.summary)
            .unwrap_or_default();
        println!(
            "  {:<10} {:<20} {:>8}  {}",
            &sid.to_string()[..8],
            ts.format("%Y-%m-%d %H:%M"),
            count,
            truncate(&summary, 40),
        );
    }
    println!();
    Ok(())
}

/// `voyage graph extract` — extract entities from already-ingested sessions.
pub fn run_extract(
    graph_path: &Path,
    db_path: &Path,
    reextract: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let graph = GraphStore::open(graph_path)?;
    let store = SqliteStore::open(db_path)?;

    if reextract {
        println!("Clearing all graph data for re-extraction...");
        graph.clear_all()?;
    } else {
        // Apply temporal decay to existing edge weights before adding new data
        graph.apply_edge_decay(0.95)?;
    }

    let extracted = graph.all_extracted_session_ids()?;
    let sessions = store.list_sessions(None, None, 100_000)?;

    let mut new_count = 0u32;
    let mut skip_count = 0u32;
    let mut total_entities = 0u32;

    for session in &sessions {
        if !reextract && extracted.contains(&session.id) {
            skip_count += 1;
            continue;
        }

        let messages = store.get_messages_by_session(&session.id, 10_000)?;
        if messages.is_empty() {
            skip_count += 1;
            continue;
        }

        match extract_session_entities(&graph, session, &messages) {
            Ok(count) => {
                total_entities += count;
                new_count += 1;
            }
            Err(e) => {
                eprintln!("  Error extracting {}: {e}", &session.id.to_string()[..8]);
            }
        }
    }

    // Refresh session counts
    graph.refresh_session_counts()?;

    // Compute PageRank and communities
    graph.compute_pagerank()?;
    graph.compute_communities()?;

    println!(
        "Extracted {} entities from {} sessions ({} skipped)",
        total_entities, new_count, skip_count
    );
    println!();
    Ok(())
}

/// `voyage graph related <name>` — show related entities using PMI scoring.
pub fn run_related(
    graph_path: &Path,
    name: &str,
    limit: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let graph = GraphStore::open(graph_path)?;

    let entity = graph.find_entity_by_name(name)?;
    if entity.is_none() {
        println!("Entity not found: {name}");
        return Ok(());
    }

    let related = graph.related_entities_pmi(name, limit)?;
    if related.is_empty() {
        println!("No related entities found for: {name}");
        return Ok(());
    }

    println!("=== Related to {name} (PMI) ===\n");
    println!("  {:<14} {:<40} {:>8}", "KIND", "NAME", "PMI");
    println!("  {}", "-".repeat(66));

    for (e, pmi) in &related {
        println!(
            "  {:<14} {:<40} {:>8.2}",
            e.kind.as_str(),
            truncate(&e.name, 40),
            pmi,
        );
    }
    println!();
    Ok(())
}

/// `voyage graph cost <name>` — show cost associated with an entity.
pub fn run_cost(
    graph_path: &Path,
    db_path: &Path,
    name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let graph = GraphStore::open(graph_path)?;
    let store = SqliteStore::open(db_path)?;

    let entity = graph.find_entity_by_name(name)?;
    if entity.is_none() {
        println!("Entity not found: {name}");
        return Ok(());
    }
    let entity = entity.unwrap();

    let session_ids = graph.session_ids_for_entity(name)?;
    let mut total_cost = 0.0f64;
    let mut total_tokens = 0u64;
    let mut session_count = 0u32;

    for sid in &session_ids {
        if let Ok(Some(session)) = store.get_session(sid) {
            total_cost += session.estimated_cost_usd;
            total_tokens += session.usage.total();
            session_count += 1;
        }
    }

    println!(
        "=== Cost for {} ({}) ===\n",
        entity.name,
        entity.kind.as_str()
    );
    println!("  Sessions:      {session_count}");
    println!("  Total cost:    ${total_cost:.4}");
    println!("  Total tokens:  {}", format_tokens(total_tokens));
    println!("  Mentions:      {}", entity.mention_count);
    println!();
    Ok(())
}

/// `voyage graph timeline <name>` — show activity timeline.
pub fn run_timeline(graph_path: &Path, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let graph = GraphStore::open(graph_path)?;

    let entity = graph.find_entity_by_name(name)?;
    if entity.is_none() {
        println!("Entity not found: {name}");
        return Ok(());
    }

    let timeline = graph.entity_timeline(name)?;
    if timeline.is_empty() {
        println!("No activity found for: {name}");
        return Ok(());
    }

    println!("=== Timeline for {name} ===\n");
    let max_count = timeline.iter().map(|(_, c)| *c).max().unwrap_or(1);

    for (date, count) in &timeline {
        let bar_len = (*count as f64 / max_count as f64 * 40.0) as usize;
        let bar: String = "█".repeat(bar_len);
        println!("  {date}  {bar} {count}");
    }
    println!();
    Ok(())
}

/// `voyage graph rank` — compute and show PageRank rankings.
pub fn run_rank(graph_path: &Path, limit: usize) -> Result<(), Box<dyn std::error::Error>> {
    let graph = GraphStore::open(graph_path)?;

    // Recompute PageRank
    graph.compute_pagerank()?;

    let entities = graph.list_entities(None, limit)?;
    if entities.is_empty() {
        println!("No entities found.");
        return Ok(());
    }

    println!("=== Top Entities by PageRank ===\n");
    println!(
        "  {:<4} {:<14} {:<40} {:>10} {:>8}",
        "#", "KIND", "NAME", "PAGERANK", "MENTIONS"
    );
    println!("  {}", "-".repeat(80));

    for (i, e) in entities.iter().enumerate() {
        println!(
            "  {:<4} {:<14} {:<40} {:>10.6} {:>8}",
            i + 1,
            e.kind.as_str(),
            truncate(&e.name, 40),
            e.pagerank,
            e.mention_count,
        );
    }
    println!();
    Ok(())
}

/// `voyage graph communities` — show detected communities.
pub fn run_communities(graph_path: &Path, limit: usize) -> Result<(), Box<dyn std::error::Error>> {
    let graph = GraphStore::open(graph_path)?;

    // Recompute communities
    graph.compute_communities()?;

    let communities = graph.list_communities()?;
    if communities.is_empty() {
        println!("No communities detected.");
        return Ok(());
    }

    println!("=== Communities ({} detected) ===\n", communities.len());

    for (i, (_cid, members)) in communities.iter().take(limit).enumerate() {
        // Use the top member's name as community label
        let label = members
            .first()
            .map(|e| e.name.as_str())
            .unwrap_or("unnamed");
        println!(
            "  Community {} — \"{}\" ({} members)",
            i + 1,
            label,
            members.len()
        );
        for member in members.iter().take(10) {
            println!(
                "    {:<14} {:<40} PR={:.4}",
                member.kind.as_str(),
                truncate(&member.name, 40),
                member.pagerank,
            );
        }
        if members.len() > 10 {
            println!("    ... and {} more", members.len() - 10);
        }
        println!();
    }
    Ok(())
}

/// `voyage graph cleanup` — remove invalid entities (stopwords, short names, noise).
pub fn run_cleanup(graph_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let graph = GraphStore::open(graph_path)?;

    let before = graph.entity_count()?;
    let removed = graph.cleanup_invalid_entities()?;
    let after = graph.entity_count()?;

    println!("=== Graph Cleanup ===\n");
    println!("  Entities before:  {before}");
    println!("  Removed:          {removed}");
    println!("  Entities after:   {after}");
    println!();
    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}...", &s[..max - 3])
    }
}

fn format_tokens(tokens: u64) -> String {
    if tokens >= 1_000_000 {
        format!("{:.1}M", tokens as f64 / 1_000_000.0)
    } else if tokens >= 1_000 {
        format!("{:.1}K", tokens as f64 / 1_000.0)
    } else {
        tokens.to_string()
    }
}
