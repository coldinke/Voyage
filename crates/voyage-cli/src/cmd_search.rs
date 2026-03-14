use std::path::Path;

use voyage_embed::{Embedder, EmbeddingModel};
use voyage_store::vectors::VectorStore;

fn embedding_model_error(context: &str, err: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::other(format!(
        "{context}: {err}\n\nHint: On first run, the embedding model (~90MB) is downloaded from Hugging Face.\nIf download fails, you can manually place model files in .fastembed_cache/ or set HF_ENDPOINT."
    ))
}

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
    let embedder = Embedder::new(EmbeddingModel::AllMiniLmL6V2)
        .map_err(|e| embedding_model_error("Failed to load embedding model", e))?;

    let query_vec = embedder
        .embed_single(query)
        .map_err(|e| embedding_model_error("Failed to embed search query", e))?;
    let results = store.search(&query_vec, limit)?;

    if results.is_empty() {
        println!("No results found for: \"{query}\"");
        return Ok(());
    }

    println!("\nSearch results for \"{}\" ({} indexed):\n", query, count);
    println!("{:<6} {:<38} Content", "Score", "Session");
    println!("{}", "-".repeat(100));

    for r in &results {
        let preview = if r.content_preview.chars().count() > 60 {
            let truncated: String = r.content_preview.chars().take(57).collect();
            format!("{truncated}...")
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
