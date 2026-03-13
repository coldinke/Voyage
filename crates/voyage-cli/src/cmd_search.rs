use std::path::Path;

use voyage_embed::{Embedder, EmbeddingModel};
use voyage_store::vectors::VectorStore;

pub fn run(data_dir: &Path, query: &str, limit: usize) -> Result<(), Box<dyn std::error::Error>> {
    let vectors_db = data_dir.join("vectors.db");
    if !vectors_db.exists() {
        println!("No vector index yet. Run `voyage index` first.");
        return Ok(());
    }

    let store = VectorStore::open(&vectors_db)?;
    let count = store.count()?;
    if count == 0 {
        println!("Vector index is empty. Run `voyage index` first.");
        return Ok(());
    }

    println!("Loading embedding model...");
    let embedder = Embedder::new(EmbeddingModel::AllMiniLmL6V2)?;

    let query_vec = embedder.embed_single(query)?;
    let results = store.search(&query_vec, limit)?;

    if results.is_empty() {
        println!("No results found for: \"{query}\"");
        return Ok(());
    }

    println!("\nSearch results for \"{}\" ({} indexed):\n", query, count);
    println!("{:<6} {:<38} {}", "Score", "Session", "Content");
    println!("{}", "-".repeat(100));

    for r in &results {
        let preview = if r.content_preview.len() > 60 {
            format!("{}...", &r.content_preview[..57])
        } else {
            r.content_preview.clone()
        };
        println!(
            "{:<6.3} {:<38} {}",
            r.score,
            r.session_id.to_string(),
            preview,
        );
    }

    Ok(())
}
