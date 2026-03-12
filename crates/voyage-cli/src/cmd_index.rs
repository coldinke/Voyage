use std::path::Path;

use voyage_embed::{Embedder, EmbeddingModel};
use voyage_store::sqlite::SqliteStore;
use voyage_store::vectors::VectorStore;

pub fn run(data_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let db_path = data_dir.join("voyage.db");
    if !db_path.exists() {
        println!("No data yet. Run `voyage ingest` first.");
        return Ok(());
    }

    let store = SqliteStore::open(&db_path)?;
    let vectors_db = data_dir.join("vectors.db");
    let vector_store = VectorStore::open(&vectors_db)?;

    println!("Loading embedding model...");
    let embedder = Embedder::new(EmbeddingModel::AllMiniLmL6V2)?;

    // Get all sessions
    let sessions = store.list_sessions(None, None, 1000)?;
    if sessions.is_empty() {
        println!("No sessions to index.");
        return Ok(());
    }

    let mut indexed = 0u64;
    let mut skipped = 0u64;

    for session in &sessions {
        // Use session ID as embedding ID for session-level embedding
        if vector_store.embedding_exists(&session.id)? {
            skipped += 1;
            continue;
        }

        // Build a text summary of the session for embedding
        let summary = format!(
            "Project: {} | Model: {} | {} messages, {} turns | Cost: ${:.4}",
            session.project, session.model, session.message_count, session.turn_count,
            session.estimated_cost_usd
        );

        // For now, embed the session summary
        // Future: embed individual messages for finer-grained search
        match embedder.embed_single(&summary) {
            Ok(embedding) => {
                vector_store.insert_embedding(
                    &session.id,
                    &session.id,
                    None,
                    &summary,
                    &embedding,
                )?;
                indexed += 1;
                println!("  Indexed: {} ({})", &session.id.to_string()[..8], session.project);
            }
            Err(e) => {
                eprintln!("  Error embedding session {}: {e}", &session.id.to_string()[..8]);
            }
        }
    }

    println!("\nDone: {indexed} indexed, {skipped} skipped");
    println!("Total vectors: {}", vector_store.count()?);
    Ok(())
}
