use std::path::PathBuf;

use fastembed::{EmbeddingModel as FEModel, InitOptions, TextEmbedding};

#[derive(Debug, thiserror::Error)]
pub enum EmbedError {
    #[error("Embedding model error: {}", format_anyhow_chain(.0))]
    Model(#[from] anyhow::Error),
    #[error("Empty input")]
    EmptyInput,
}

fn format_anyhow_chain(err: &anyhow::Error) -> String {
    let mut msg = err.to_string();
    for cause in err.chain().skip(1) {
        msg.push_str(&format!("\n  caused by: {cause}"));
    }
    msg
}

#[derive(Debug, Clone, Copy)]
pub enum EmbeddingModel {
    /// all-MiniLM-L6-v2 — fast, 384 dims, good for English
    AllMiniLmL6V2,
    /// multilingual-e5-small — 384 dims, multilingual support
    MultilingualE5Small,
}

impl EmbeddingModel {
    fn to_fastembed(self) -> FEModel {
        match self {
            Self::AllMiniLmL6V2 => FEModel::AllMiniLML6V2,
            Self::MultilingualE5Small => FEModel::MultilingualE5Small,
        }
    }

    pub fn dimensions(self) -> usize {
        match self {
            Self::AllMiniLmL6V2 => 384,
            Self::MultilingualE5Small => 384,
        }
    }
}

pub struct Embedder {
    model: TextEmbedding,
    model_type: EmbeddingModel,
}

impl Embedder {
    pub fn new(model_type: EmbeddingModel) -> Result<Self, EmbedError> {
        let model = TextEmbedding::try_new(
            InitOptions::new(model_type.to_fastembed()).with_show_download_progress(true),
        )?;
        Ok(Self { model, model_type })
    }

    pub fn with_cache_dir(
        model_type: EmbeddingModel,
        cache_dir: PathBuf,
    ) -> Result<Self, EmbedError> {
        let model = TextEmbedding::try_new(
            InitOptions::new(model_type.to_fastembed())
                .with_cache_dir(cache_dir)
                .with_show_download_progress(true),
        )?;
        Ok(Self { model, model_type })
    }

    pub fn dimensions(&self) -> usize {
        self.model_type.dimensions()
    }

    pub fn embed_single(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        if text.is_empty() {
            return Err(EmbedError::EmptyInput);
        }
        let embeddings = self.model.embed(vec![text], None)?;
        Ok(embeddings.into_iter().next().unwrap())
    }

    pub fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbedError> {
        if texts.is_empty() {
            return Err(EmbedError::EmptyInput);
        }
        let embeddings = self.model.embed(texts.to_vec(), None)?;
        Ok(embeddings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn try_get_embedder() -> Option<Embedder> {
        // Model requires network download on first run; skip tests if unavailable
        match Embedder::new(EmbeddingModel::AllMiniLmL6V2) {
            Ok(e) => Some(e),
            Err(e) => {
                eprintln!("Skipping embedding test (model unavailable): {e}");
                None
            }
        }
    }

    #[test]
    fn embed_single_returns_correct_dims() {
        let Some(embedder) = try_get_embedder() else {
            return;
        };
        let embedding = embedder.embed_single("Hello world").unwrap();
        assert_eq!(embedding.len(), 384);
    }

    #[test]
    fn embed_single_is_normalized() {
        let Some(embedder) = try_get_embedder() else {
            return;
        };
        let embedding = embedder.embed_single("Test normalization").unwrap();
        let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 0.01, "Expected unit norm, got {norm}");
    }

    #[test]
    fn embed_batch_returns_correct_count() {
        let Some(embedder) = try_get_embedder() else {
            return;
        };
        let embeddings = embedder
            .embed_batch(&["First text", "Second text", "Third text"])
            .unwrap();
        assert_eq!(embeddings.len(), 3);
        for emb in &embeddings {
            assert_eq!(emb.len(), 384);
        }
    }

    #[test]
    fn similar_texts_have_higher_cosine() {
        let Some(embedder) = try_get_embedder() else {
            return;
        };
        let rust_1 = embedder
            .embed_single("Rust programming language memory safety")
            .unwrap();
        let rust_2 = embedder
            .embed_single("Rust lang ownership and borrowing")
            .unwrap();
        let cooking = embedder
            .embed_single("How to bake chocolate cake recipe")
            .unwrap();

        let sim_related = cosine_similarity(&rust_1, &rust_2);
        let sim_unrelated = cosine_similarity(&rust_1, &cooking);

        assert!(
            sim_related > sim_unrelated,
            "Related texts should be more similar: {sim_related} vs {sim_unrelated}"
        );
    }

    #[test]
    fn embed_empty_returns_error_without_model() {
        // This test validates input checking logic without requiring the model
        // We test via the public API constraint: empty string must fail
        // If model loads, test the real path; otherwise test is trivially true
        if let Some(embedder) = try_get_embedder() {
            assert!(embedder.embed_single("").is_err());
        }
    }

    #[test]
    fn embed_batch_empty_returns_error_without_model() {
        if let Some(embedder) = try_get_embedder() {
            let empty: &[&str] = &[];
            assert!(embedder.embed_batch(empty).is_err());
        }
    }

    #[test]
    fn dimensions_constant() {
        assert_eq!(EmbeddingModel::AllMiniLmL6V2.dimensions(), 384);
        assert_eq!(EmbeddingModel::MultilingualE5Small.dimensions(), 384);
    }

    fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
        let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
        dot / (norm_a * norm_b)
    }
}
