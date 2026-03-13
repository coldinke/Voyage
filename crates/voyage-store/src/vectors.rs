//! Local vector store using SQLite for embedding storage and brute-force cosine search.
//! This serves as the MVP vector backend — no external Qdrant server needed.
//! Can be replaced with Qdrant client when scaling up.

use std::path::Path;

use rusqlite::{Connection, params};
use uuid::Uuid;

use crate::sqlite::StoreError;

pub struct VectorStore {
    conn: Connection,
}

impl VectorStore {
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        let conn = Connection::open(path)?;
        let store = Self { conn };
        store.migrate()?;
        Ok(store)
    }

    pub fn open_in_memory() -> Result<Self, StoreError> {
        let conn = Connection::open_in_memory()?;
        let store = Self { conn };
        store.migrate()?;
        Ok(store)
    }

    fn migrate(&self) -> Result<(), StoreError> {
        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS embeddings (
                id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                message_id TEXT,
                content_preview TEXT NOT NULL DEFAULT '',
                embedding BLOB NOT NULL,
                dimensions INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_embeddings_session ON embeddings(session_id);
            ",
        )?;
        Ok(())
    }

    pub fn insert_embedding(
        &self,
        id: &Uuid,
        session_id: &Uuid,
        message_id: Option<&Uuid>,
        content_preview: &str,
        embedding: &[f32],
    ) -> Result<(), StoreError> {
        let blob = embedding_to_blob(embedding);
        self.conn.execute(
            "INSERT OR REPLACE INTO embeddings (id, session_id, message_id, content_preview, embedding, dimensions)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                id.to_string(),
                session_id.to_string(),
                message_id.map(|m| m.to_string()),
                truncate(content_preview, 200),
                blob,
                embedding.len() as i64,
            ],
        )?;
        Ok(())
    }

    pub fn embedding_exists(&self, id: &Uuid) -> Result<bool, StoreError> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM embeddings WHERE id = ?1",
            params![id.to_string()],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    pub fn search(
        &self,
        query_embedding: &[f32],
        limit: usize,
    ) -> Result<Vec<SearchResult>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, session_id, message_id, content_preview, embedding FROM embeddings",
        )?;

        let mut results: Vec<SearchResult> = stmt
            .query_map([], |row| {
                let blob: Vec<u8> = row.get(4)?;
                let stored = blob_to_embedding(&blob);
                let score = cosine_similarity(query_embedding, &stored);
                Ok(SearchResult {
                    id: Uuid::parse_str(&row.get::<_, String>(0)?).unwrap(),
                    session_id: Uuid::parse_str(&row.get::<_, String>(1)?).unwrap(),
                    message_id: row
                        .get::<_, Option<String>>(2)?
                        .and_then(|s| Uuid::parse_str(&s).ok()),
                    content_preview: row.get(3)?,
                    score,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();

        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        results.truncate(limit);
        Ok(results)
    }

    pub fn delete_all(&self) -> Result<(), StoreError> {
        self.conn.execute("DELETE FROM embeddings", [])?;
        Ok(())
    }

    pub fn count(&self) -> Result<u64, StoreError> {
        let count: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM embeddings", [], |row| row.get(0))?;
        Ok(count as u64)
    }
}

#[derive(Debug, Clone)]
pub struct SearchResult {
    pub id: Uuid,
    pub session_id: Uuid,
    pub message_id: Option<Uuid>,
    pub content_preview: String,
    pub score: f32,
}

fn embedding_to_blob(embedding: &[f32]) -> Vec<u8> {
    embedding.iter().flat_map(|f| f.to_le_bytes()).collect()
}

fn blob_to_embedding(blob: &[u8]) -> Vec<f32> {
    blob.chunks_exact(4)
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect()
}

fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }
    dot / (norm_a * norm_b)
}

fn truncate(s: &str, max_len: usize) -> &str {
    if s.len() <= max_len {
        s
    } else {
        let mut end = max_len;
        while !s.is_char_boundary(end) && end > 0 {
            end -= 1;
        }
        &s[..end]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedding_blob_roundtrip() {
        let original = vec![1.0f32, -0.5, 0.333, 42.0, 0.0];
        let blob = embedding_to_blob(&original);
        let recovered = blob_to_embedding(&blob);
        assert_eq!(original, recovered);
    }

    #[test]
    fn cosine_similarity_identical() {
        let a = vec![1.0, 0.0, 0.0];
        assert!((cosine_similarity(&a, &a) - 1.0).abs() < 0.001);
    }

    #[test]
    fn cosine_similarity_orthogonal() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        assert!(cosine_similarity(&a, &b).abs() < 0.001);
    }

    #[test]
    fn cosine_similarity_opposite() {
        let a = vec![1.0, 0.0];
        let b = vec![-1.0, 0.0];
        assert!((cosine_similarity(&a, &b) + 1.0).abs() < 0.001);
    }

    #[test]
    fn insert_and_search() {
        let store = VectorStore::open_in_memory().unwrap();
        let sid = Uuid::new_v4();

        // Insert three embeddings with known directions
        let rust_vec = vec![0.9, 0.1, 0.0]; // "rust-like"
        let code_vec = vec![0.8, 0.2, 0.1]; // similar to rust
        let food_vec = vec![0.0, 0.1, 0.9]; // very different

        store
            .insert_embedding(&Uuid::new_v4(), &sid, None, "rust programming", &rust_vec)
            .unwrap();
        store
            .insert_embedding(&Uuid::new_v4(), &sid, None, "code review", &code_vec)
            .unwrap();
        store
            .insert_embedding(&Uuid::new_v4(), &sid, None, "cooking recipe", &food_vec)
            .unwrap();

        // Search for something rust-like
        let query = vec![0.85, 0.15, 0.0];
        let results = store.search(&query, 2).unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].content_preview, "rust programming");
        assert_eq!(results[1].content_preview, "code review");
        assert!(results[0].score > results[1].score);
    }

    #[test]
    fn embedding_exists_check() {
        let store = VectorStore::open_in_memory().unwrap();
        let id = Uuid::new_v4();
        let sid = Uuid::new_v4();

        assert!(!store.embedding_exists(&id).unwrap());
        store
            .insert_embedding(&id, &sid, None, "test", &[1.0, 0.0])
            .unwrap();
        assert!(store.embedding_exists(&id).unwrap());
    }

    #[test]
    fn count_embeddings() {
        let store = VectorStore::open_in_memory().unwrap();
        assert_eq!(store.count().unwrap(), 0);

        let sid = Uuid::new_v4();
        store
            .insert_embedding(&Uuid::new_v4(), &sid, None, "a", &[1.0])
            .unwrap();
        store
            .insert_embedding(&Uuid::new_v4(), &sid, None, "b", &[0.5])
            .unwrap();
        assert_eq!(store.count().unwrap(), 2);
    }

    #[test]
    fn search_empty_store() {
        let store = VectorStore::open_in_memory().unwrap();
        let results = store.search(&[1.0, 0.0], 10).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn truncate_preserves_utf8() {
        assert_eq!(truncate("hello", 10), "hello");
        assert_eq!(truncate("hello", 3), "hel");
        // Multi-byte: 你好 = 6 bytes
        let s = "你好世界";
        let t = truncate(s, 7);
        assert!(t.len() <= 7);
        assert_eq!(t, "你好"); // 6 bytes, next char boundary is 9
    }
}
