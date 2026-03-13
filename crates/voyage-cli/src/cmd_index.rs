use std::path::Path;

use voyage_core::model::{Role, Session};
use voyage_embed::{Embedder, EmbeddingModel};
use voyage_store::sqlite::SqliteStore;
use voyage_store::vectors::VectorStore;

fn embedding_model_error(context: &str, err: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::other(format!(
        "{context}: {err}. On first run, Voyage needs network access to download the embedding model, or an existing fastembed cache."
    ))
}

/// Build semantic embedding text from a session and its messages.
fn build_embedding_text(session: &Session, store: &SqliteStore) -> String {
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

    // First user message (up to 300 chars) as primary intent
    if let Some(first) = user_messages.first() {
        let truncated = truncate_chars(first, 300);
        parts.push(truncated);
    }

    // 2-3 sampled user messages for topic breadth
    if user_messages.len() > 2 {
        for i in [user_messages.len() / 3, user_messages.len() * 2 / 3] {
            if i < user_messages.len() && i > 0 {
                let truncated = truncate_chars(user_messages[i], 200);
                parts.push(truncated);
            }
        }
    }

    let text = parts.join("\n");
    // Keep total under ~1000 chars for AllMiniLmL6V2's ~256 token context
    truncate_chars(&text, 1000)
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

pub fn run(data_dir: &Path, reindex: bool) -> Result<(), Box<dyn std::error::Error>> {
    let db_path = data_dir.join("voyage.db");
    if !db_path.exists() {
        println!("No data yet. Run `voyage ingest` first.");
        return Ok(());
    }

    let store = SqliteStore::open(&db_path)?;
    let vectors_db = data_dir.join("vectors.db");
    let vector_store = VectorStore::open(&vectors_db)?;

    if reindex {
        println!("Reindexing: deleting all existing embeddings...");
        vector_store.delete_all()?;
    }

    println!("Loading embedding model...");
    let embedder = Embedder::new(EmbeddingModel::AllMiniLmL6V2)
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
        let text = build_embedding_text(session, &store);
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
                    vector_store.insert_embedding(
                        &session.id,
                        &session.id,
                        None,
                        text,
                        embedding,
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
                            vector_store.insert_embedding(
                                &session.id,
                                &session.id,
                                None,
                                text,
                                &embedding,
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
