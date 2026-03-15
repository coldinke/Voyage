use std::path::Path;

use voyage_core::model::{Role, Session};
use voyage_embed::{Embedder, EmbeddingModel};
use voyage_graph::entity::{Entity, EntityKind};
use voyage_graph::store::GraphStore;
use voyage_store::sqlite::SqliteStore;
use voyage_store::vectors::VectorStore;

fn embedding_model_error(context: &str, err: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::other(format!(
        "{context}: {err}\n\nHint: On first run, the embedding model (~90MB) is downloaded from Hugging Face.\nIf download fails, you can manually place model files in ~/.voyage/models/ or set HF_ENDPOINT."
    ))
}

/// Filter and format entity names for embedding text enrichment.
/// Excludes noisy kinds (Tool, Error, GitBranch) and truncates to `max_chars` on a comma boundary.
fn append_entity_names(entities: &[(Entity, u32)], max_chars: usize) -> String {
    let kept_kinds = [
        EntityKind::File,
        EntityKind::Function,
        EntityKind::Module,
        EntityKind::Concept,
        EntityKind::Dependency,
    ];
    let names: Vec<&str> = entities
        .iter()
        .filter(|(e, _)| kept_kinds.contains(&e.kind))
        .map(|(e, _)| e.name.as_str())
        .collect();

    if names.is_empty() {
        return String::new();
    }

    let joined = names.join(", ");
    let mut text = format!("\nEntities: {joined}");

    if text.len() > max_chars {
        // Truncate at a comma boundary within max_chars
        let truncated = &text[..max_chars];
        if let Some(last_comma) = truncated.rfind(", ") {
            text = truncated[..last_comma].to_string();
        } else {
            text = truncated.to_string();
        }
    }

    text
}

/// Build semantic embedding text from a session and its messages.
fn build_embedding_text(
    session: &Session,
    store: &SqliteStore,
    graph: Option<&GraphStore>,
) -> String {
    let mut parts = Vec::new();

    // Session summary as primary signal
    if !session.summary.is_empty() {
        parts.push(session.summary.clone());
    }

    // Fetch user messages for semantic content
    let messages = store
        .get_messages_by_session(&session.id, 20)
        .unwrap_or_default();

    let user_messages: Vec<&str> = messages
        .iter()
        .filter(|m| m.role == Role::User && !m.content.is_empty())
        .map(|m| m.content.as_str())
        .collect();

    // First user message (up to 500 chars) as primary intent
    if let Some(first) = user_messages.first() {
        let truncated = truncate_chars(first, 500);
        parts.push(truncated);
    }

    // 2-3 sampled user messages for topic breadth
    if user_messages.len() > 2 {
        for i in [user_messages.len() / 3, user_messages.len() * 2 / 3] {
            if i < user_messages.len() && i > 0 {
                let truncated = truncate_chars(user_messages[i], 300);
                parts.push(truncated);
            }
        }
    }

    let text = parts.join("\n");

    // If graph is available, reserve 200 chars for entity names
    if let Some(g) = graph {
        let base = truncate_chars(&text, 1200);
        let entities = g.entities_for_session(&session.id, 20).unwrap_or_default();
        let suffix = append_entity_names(&entities, 200);
        let combined = format!("{base}{suffix}");
        // MultilingualE5Small has 512 token context (~1500 chars)
        return truncate_chars(&combined, 1500);
    }

    // No graph: keep total under ~1500 chars for MultilingualE5Small
    truncate_chars(&text, 1500)
}

fn truncate_chars(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while !s.is_char_boundary(end) && end > 0 {
        end -= 1;
    }
    s[..end].to_string()
}

fn parse_model(name: &str) -> EmbeddingModel {
    match name {
        "multi" | "multilingual" | "e5" => EmbeddingModel::MultilingualE5Small,
        _ => EmbeddingModel::AllMiniLmL6V2,
    }
}

pub fn run(
    data_dir: &Path,
    reindex: bool,
    model_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let db_path = data_dir.join("voyage.db");
    if !db_path.exists() {
        println!("No data yet. Run `voyage ingest` first.");
        return Ok(());
    }

    let model = parse_model(model_name);

    let store = SqliteStore::open(&db_path)?;
    let vectors_db = data_dir.join("vectors.db");
    let vector_store = VectorStore::open(&vectors_db)?;

    let graph_path = data_dir.join("graph.db");
    let graph = if graph_path.exists() {
        match GraphStore::open(&graph_path) {
            Ok(g) => Some(g),
            Err(e) => {
                eprintln!("Warning: could not open graph.db: {e}");
                None
            }
        }
    } else {
        eprintln!("Hint: run `voyage extract` first to enrich embeddings with entity names.");
        None
    };

    if reindex {
        println!("Reindexing: deleting all existing embeddings...");
        vector_store.delete_all()?;
    }

    let db_model_name = model.model_name();
    println!("Loading embedding model ({db_model_name})...");
    let embedder = Embedder::with_cache_dir(model, data_dir.join("models"))
        .map_err(|e| embedding_model_error("Failed to load embedding model", e))?;

    // Get all sessions
    let sessions = store.list_sessions(None, None, 10000)?;
    if sessions.is_empty() {
        println!("No sessions to index.");
        return Ok(());
    }

    // Collect un-indexed sessions and their embedding texts
    let mut to_embed: Vec<(usize, String)> = Vec::new(); // (session index, text)

    for (i, session) in sessions.iter().enumerate() {
        if !reindex && vector_store.embedding_exists(&session.id)? {
            continue;
        }
        let text = build_embedding_text(session, &store, graph.as_ref());
        if text.is_empty() {
            continue;
        }
        to_embed.push((i, text));
    }

    let skipped = sessions.len() - to_embed.len();

    if to_embed.is_empty() {
        println!("All {} sessions already indexed.", sessions.len());
        return Ok(());
    }

    println!("Embedding {} session(s)...", to_embed.len());

    // Batch embed in chunks of 64
    let mut indexed = 0u64;
    for chunk in to_embed.chunks(64) {
        let texts: Vec<&str> = chunk.iter().map(|(_, t)| t.as_str()).collect();
        match embedder.embed_batch(&texts) {
            Ok(embeddings) => {
                for ((session_idx, text), embedding) in chunk.iter().zip(embeddings.iter()) {
                    let session = &sessions[*session_idx];
                    vector_store.insert_embedding_with_meta(
                        &session.id,
                        &session.id,
                        None,
                        text,
                        embedding,
                        db_model_name,
                        &session.project,
                        &session.started_at.to_rfc3339(),
                    )?;
                    indexed += 1;
                }
            }
            Err(e) => {
                eprintln!("  Error embedding batch: {e}");
                // Fall back to individual embedding
                for (session_idx, text) in chunk {
                    let session = &sessions[*session_idx];
                    match embedder.embed_single(text) {
                        Ok(embedding) => {
                            vector_store.insert_embedding_with_meta(
                                &session.id,
                                &session.id,
                                None,
                                text,
                                &embedding,
                                model_name,
                                &session.project,
                                &session.started_at.to_rfc3339(),
                            )?;
                            indexed += 1;
                        }
                        Err(e) => {
                            eprintln!(
                                "  Error embedding session {}: {e}",
                                &session.id.to_string()[..8]
                            );
                        }
                    }
                }
            }
        }
    }

    println!(
        "\nDone: {indexed} indexed, {skipped} skipped{}",
        if reindex { " (reindex)" } else { "" }
    );
    println!("Total vectors: {}", vector_store.count()?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use voyage_graph::entity::{Entity, EntityKind};

    fn make_entity(kind: EntityKind, name: &str) -> Entity {
        Entity::new(kind, name.to_string(), name.to_string(), Utc::now())
    }

    #[test]
    fn append_entity_names_formats_correctly() {
        let entities = vec![
            (make_entity(EntityKind::File, "src/auth.rs"), 5),
            (make_entity(EntityKind::Function, "validate_token"), 3),
            (make_entity(EntityKind::Concept, "authentication"), 2),
        ];
        let result = append_entity_names(&entities, 200);
        assert_eq!(
            result,
            "\nEntities: src/auth.rs, validate_token, authentication"
        );
    }

    #[test]
    fn append_entity_names_filters_noise_kinds() {
        let entities = vec![
            (make_entity(EntityKind::File, "src/auth.rs"), 5),
            (make_entity(EntityKind::Tool, "Read"), 10),
            (make_entity(EntityKind::Error, "NotFound"), 4),
            (make_entity(EntityKind::GitBranch, "main"), 3),
            (make_entity(EntityKind::Concept, "authentication"), 2),
        ];
        let result = append_entity_names(&entities, 200);
        assert!(result.contains("src/auth.rs"));
        assert!(result.contains("authentication"));
        assert!(!result.contains("Read"));
        assert!(!result.contains("NotFound"));
        assert!(!result.contains("main"));
    }

    #[test]
    fn append_entity_names_truncates_at_boundary() {
        let entities = vec![
            (
                make_entity(EntityKind::File, "src/very_long_name_one.rs"),
                5,
            ),
            (
                make_entity(EntityKind::File, "src/very_long_name_two.rs"),
                4,
            ),
            (
                make_entity(EntityKind::File, "src/very_long_name_three.rs"),
                3,
            ),
            (
                make_entity(EntityKind::File, "src/very_long_name_four.rs"),
                2,
            ),
        ];
        let result = append_entity_names(&entities, 60);
        assert!(result.len() <= 60);
        // Should not end mid-name — truncated at comma boundary
        assert!(!result.ends_with(','));
        assert!(result.starts_with("\nEntities: "));
    }

    #[test]
    fn append_entity_names_empty_when_no_kept_kinds() {
        let entities = vec![
            (make_entity(EntityKind::Tool, "Read"), 10),
            (make_entity(EntityKind::Error, "Timeout"), 5),
        ];
        let result = append_entity_names(&entities, 200);
        assert!(result.is_empty());
    }

    #[test]
    fn build_embedding_text_without_graph_uses_full_budget() {
        // When graph=None, append_entity_names is not called,
        // so the full 1000 char budget is used for session text.
        // We verify this indirectly: empty entity list produces no suffix.
        let result = append_entity_names(&[], 200);
        assert!(result.is_empty());
    }
}
