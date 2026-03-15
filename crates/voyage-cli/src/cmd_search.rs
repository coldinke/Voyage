use std::collections::HashMap;
use std::path::Path;

use chrono::{Duration, Utc};
use voyage_core::model::Session;
use voyage_embed::{Embedder, EmbeddingModel};
use voyage_graph::entity::EntityKind;
use voyage_graph::store::GraphStore;
use voyage_store::sqlite::SqliteStore;
use voyage_store::vectors::{SearchResult, VectorStore};

use crate::OutputFormat;

fn embedding_model_error(context: &str, err: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::other(format!(
        "{context}: {err}\n\nHint: On first run, the embedding model (~90MB) is downloaded from Hugging Face.\nIf download fails, you can manually place model files in ~/.voyage/models/ or set HF_ENDPOINT."
    ))
}

/// Entity kinds worth surfacing in search results.
/// Tool, Error, GitBranch appear in nearly every session — pure noise.
const SIGNAL_KINDS: &[EntityKind] = &[
    EntityKind::File,
    EntityKind::Function,
    EntityKind::Module,
    EntityKind::Concept,
    EntityKind::Dependency,
];

#[derive(Clone, serde::Serialize)]
struct EntitySummary {
    kind: String,
    name: String,
    mentions: u32,
}

struct EnrichedResult {
    result: SearchResult,
    session: Option<Session>,
    entities: Vec<EntitySummary>,
    rating: Option<u8>,
}

#[derive(serde::Serialize)]
struct MachineResultEntry {
    session_id: String,
    score: f32,
    summary: String,
    timestamp: String,
    project: String,
    cwd: String,
    git_branch: Option<String>,
    model: String,
    turns: u32,
    cost_usd: f64,
    entities: Vec<EntitySummary>,
}

#[derive(serde::Serialize)]
struct MachineOutput {
    query: String,
    total_indexed: u64,
    results: Vec<MachineResultEntry>,
}

fn format_results_human(query: &str, total_indexed: u64, results: &[EnrichedResult]) -> String {
    if results.is_empty() {
        return format!("No results found for: \"{query}\"");
    }

    let mut out = String::new();
    out.push_str(&format!(
        "\nSearch: \"{}\" ({} indexed)\n",
        query, total_indexed
    ));

    for (i, er) in results.iter().enumerate() {
        out.push('\n');

        // Line 1: rank, score, date, summary
        let date = er
            .session
            .as_ref()
            .map(|s| s.started_at.format("%Y-%m-%d").to_string())
            .unwrap_or_default();
        let summary = er
            .session
            .as_ref()
            .map(|s| {
                if s.summary.is_empty() {
                    truncate_preview(&er.result.content_preview, 80)
                } else {
                    truncate_preview(&s.summary, 80)
                }
            })
            .unwrap_or_else(|| truncate_preview(&er.result.content_preview, 80));

        out.push_str(&format!(
            "#{:<2}  {:.3}  {}  {}\n",
            i + 1,
            er.result.score,
            date,
            summary,
        ));

        // Line 2: project path + branch (context)
        if let Some(s) = &er.session {
            let path = shorten_home(&s.cwd);
            let branch_suffix = s
                .git_branch
                .as_deref()
                .map(|b| format!(" ({b})"))
                .unwrap_or_default();
            out.push_str(&format!("     {path}{branch_suffix}\n"));
        }

        // Line 3: signal entities (files, functions, concepts — not tools/branches)
        if !er.entities.is_empty() {
            let entity_strs: Vec<String> = er
                .entities
                .iter()
                .map(|e| format!("{} ({})", e.name, e.kind))
                .collect();
            out.push_str(&format!("     → {}\n", entity_strs.join(", ")));
        }
    }

    out
}

fn format_results_context(results: &[EnrichedResult]) -> String {
    if results.is_empty() {
        return "No relevant past sessions found.".to_string();
    }

    let mut out = String::new();
    for er in results {
        let s = match &er.session {
            Some(s) => s,
            None => continue,
        };
        let date = s.started_at.format("%Y-%m-%d").to_string();
        let rating_str = match er.rating {
            Some(r) => format!(", Rating: {r}/5"),
            None => String::new(),
        };
        out.push_str(&format!(
            "## Past: \"{}\" ({date}{rating_str}, ${:.2})\n",
            truncate_preview(&s.summary, 60),
            s.estimated_cost_usd
        ));

        // Files
        let files: Vec<&str> = er
            .entities
            .iter()
            .filter(|e| e.kind == "file")
            .take(5)
            .map(|e| e.name.as_str())
            .collect();
        if !files.is_empty() {
            out.push_str(&format!("Files: {}\n", files.join(", ")));
        }

        // Tools
        let tools: Vec<String> = er
            .entities
            .iter()
            .filter(|e| e.kind == "tool")
            .map(|e| format!("{}({})", e.name, e.mentions))
            .collect();
        if !tools.is_empty() {
            out.push_str(&format!("Tools: {}\n", tools.join(", ")));
        }

        out.push('\n');
    }

    out
}

fn format_results_machine(query: &str, total_indexed: u64, results: &[EnrichedResult]) -> String {
    let entries: Vec<MachineResultEntry> = results
        .iter()
        .map(|er| {
            let s = er.session.as_ref();
            MachineResultEntry {
                session_id: er.result.session_id.to_string(),
                score: er.result.score,
                summary: s.map(|s| s.summary.clone()).unwrap_or_default(),
                timestamp: s.map(|s| s.started_at.to_rfc3339()).unwrap_or_default(),
                project: s.map(|s| s.project.clone()).unwrap_or_default(),
                cwd: s.map(|s| s.cwd.clone()).unwrap_or_default(),
                git_branch: s.and_then(|s| s.git_branch.clone()),
                model: s.map(|s| s.model.clone()).unwrap_or_default(),
                turns: s.map(|s| s.turn_count).unwrap_or(0),
                cost_usd: s.map(|s| s.estimated_cost_usd).unwrap_or(0.0),
                entities: er.entities.clone(),
            }
        })
        .collect();
    let output = MachineOutput {
        query: query.to_string(),
        total_indexed,
        results: entries,
    };
    serde_json::to_string_pretty(&output).expect("Failed to serialize search results")
}

/// Truncate text for display, first line only, with ellipsis.
fn truncate_preview(s: &str, max: usize) -> String {
    // Take first line only
    let first_line = s.lines().next().unwrap_or(s);
    if first_line.chars().count() > max {
        let truncated: String = first_line.chars().take(max - 3).collect();
        format!("{truncated}...")
    } else {
        first_line.to_string()
    }
}

/// Replace home directory prefix with ~
fn shorten_home(path: &str) -> String {
    if let Some(home) = dirs_next::home_dir() {
        let home_str = home.to_string_lossy();
        if let Some(rest) = path.strip_prefix(home_str.as_ref()) {
            return format!("~{rest}");
        }
    }
    path.to_string()
}

/// Parse a --since value into an RFC3339 timestamp string.
fn parse_since(since: &str) -> Option<String> {
    // Try "7d", "30d" format
    if let Some(days_str) = since.strip_suffix('d')
        && let Ok(days) = days_str.parse::<i64>()
    {
        let dt = Utc::now() - Duration::days(days);
        return Some(dt.to_rfc3339());
    }
    // Try ISO date "2026-01-01"
    if since.len() == 10 && since.chars().nth(4) == Some('-') {
        return Some(format!("{since}T00:00:00+00:00"));
    }
    None
}

pub fn run(
    data_dir: &Path,
    query: &str,
    limit: usize,
    format: OutputFormat,
    project: Option<&str>,
    since: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let vectors_db = data_dir.join("vectors.db");
    if !vectors_db.exists() {
        eprintln!("No vector index yet. Run `voyage index` first.");
        return Ok(());
    }

    let vector_store = VectorStore::open(&vectors_db)?;
    let count = vector_store.count()?;
    if count == 0 {
        eprintln!("Vector index is empty. Run `voyage index` first.");
        return Ok(());
    }

    eprintln!("Loading embedding model...");
    let embedder =
        Embedder::with_cache_dir(EmbeddingModel::AllMiniLmL6V2, data_dir.join("models"))
            .map_err(|e| embedding_model_error("Failed to load embedding model", e))?;

    // Open session DB for metadata enrichment + FTS search
    let db_path = data_dir.join("voyage.db");
    let session_store = if db_path.exists() {
        SqliteStore::open(&db_path).ok()
    } else {
        None
    };

    // Open graph DB for entity enrichment
    let graph_path = data_dir.join("graph.db");
    let graph = if graph_path.exists() {
        GraphStore::open(&graph_path).ok()
    } else {
        None
    };

    let since_rfc = since.and_then(parse_since);

    // 1. Semantic search (top 2*limit candidates)
    let query_vec = embedder
        .embed_single(query)
        .map_err(|e| embedding_model_error("Failed to embed search query", e))?;
    let semantic_results =
        vector_store.search_filtered(&query_vec, limit * 2, project, since_rfc.as_deref())?;

    // Build candidate map: session_id -> (semantic_score, fts_score)
    let mut candidates: HashMap<uuid::Uuid, (f32, f64)> = HashMap::new();
    for r in &semantic_results {
        candidates.insert(r.session_id, (r.score, 0.0));
    }

    // 2. FTS5 keyword search (top 2*limit candidates)
    if let Some(ss) = &session_store
        && let Ok(fts_results) = ss.search_fts(query, limit * 2)
    {
        // BM25 rank is negative (more negative = better match). Normalize.
        let min_rank = fts_results.iter().map(|(_, r)| *r).fold(f64::MAX, f64::min);
        let max_rank = fts_results.iter().map(|(_, r)| *r).fold(f64::MIN, f64::max);
        let range = (max_rank - min_rank).abs().max(0.001);

        for (sid, rank) in &fts_results {
            // Normalize rank to [0, 1] (more negative = higher score)
            let normalized = 1.0 - (rank - min_rank) / range;
            candidates
                .entry(*sid)
                .and_modify(|(_, fts)| *fts = normalized)
                .or_insert((0.0, normalized));
        }
    }

    // 3. Score and rank
    let now = Utc::now();
    let mut scored: Vec<(uuid::Uuid, f32)> = candidates
        .iter()
        .map(|(sid, (sem, kw))| {
            let session = session_store
                .as_ref()
                .and_then(|ss| ss.get_session(sid).ok().flatten());

            // Recency score: exp(-age_days / 30)
            let recency = session
                .as_ref()
                .map(|s| {
                    let age_days = (now - s.started_at).num_days().max(0) as f64;
                    (-age_days / 30.0).exp()
                })
                .unwrap_or(0.0);

            // Rating boost
            let rating_boost = session
                .as_ref()
                .and_then(|s| {
                    session_store
                        .as_ref()
                        .and_then(|ss| ss.get_rating(&s.id).ok().flatten())
                })
                .map(|r| match r {
                    5 => 0.10,
                    4 => 0.05,
                    3 => 0.0,
                    _ => -0.05,
                })
                .unwrap_or(0.0);

            let final_score = 0.50 * *sem as f64 + 0.30 * kw + 0.10 * recency + 0.10 * rating_boost;

            (*sid, final_score as f32)
        })
        .collect();

    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    scored.truncate(limit);

    // 4. Build enriched results
    let enriched: Vec<EnrichedResult> = scored
        .into_iter()
        .map(|(sid, score)| {
            let session = session_store
                .as_ref()
                .and_then(|ss| ss.get_session(&sid).ok().flatten());

            let rating = session_store
                .as_ref()
                .and_then(|ss| ss.get_rating(&sid).ok().flatten());

            // Find the semantic result for content_preview, or create a dummy
            let search_result = semantic_results
                .iter()
                .find(|r| r.session_id == sid)
                .cloned()
                .unwrap_or(SearchResult {
                    id: sid,
                    session_id: sid,
                    message_id: None,
                    content_preview: session
                        .as_ref()
                        .map(|s| s.summary.clone())
                        .unwrap_or_default(),
                    score,
                });

            let entities: Vec<EntitySummary> = graph
                .as_ref()
                .and_then(|g| g.entities_for_session(&sid, 15).ok())
                .unwrap_or_default()
                .into_iter()
                .filter(|(e, _)| SIGNAL_KINDS.contains(&e.kind))
                .take(5)
                .map(|(e, mention_count)| EntitySummary {
                    kind: e.kind.as_str().to_string(),
                    name: e.name,
                    mentions: mention_count,
                })
                .collect();

            EnrichedResult {
                result: SearchResult {
                    score,
                    ..search_result
                },
                session,
                entities,
                rating,
            }
        })
        .collect();

    let output = match format {
        OutputFormat::Human => format_results_human(query, count, &enriched),
        OutputFormat::Machine => format_results_machine(query, count, &enriched),
        OutputFormat::Context => format_results_context(&enriched),
    };
    println!("{output}");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;
    use voyage_core::model::{Provider, TokenUsage};

    fn make_session(summary: &str, cwd: &str) -> Session {
        Session {
            id: Uuid::nil(),
            project: "test-project".to_string(),
            provider: Provider::ClaudeCode,
            model: "claude-sonnet-4-20250514".to_string(),
            started_at: Utc::now(),
            ended_at: None,
            cwd: cwd.to_string(),
            git_branch: Some("main".to_string()),
            usage: TokenUsage::default(),
            estimated_cost_usd: 0.42,
            message_count: 10,
            turn_count: 5,
            summary: summary.to_string(),
        }
    }

    fn sample_results() -> Vec<EnrichedResult> {
        vec![
            EnrichedResult {
                result: SearchResult {
                    id: Uuid::nil(),
                    session_id: Uuid::nil(),
                    message_id: None,
                    content_preview: "implement token counting for embeddings".to_string(),
                    score: 0.923,
                },
                session: Some(make_session(
                    "Implement token counting for embedding cost tracking",
                    "/Users/testuser/lab/Voyage",
                )),
                entities: vec![
                    EntitySummary {
                        kind: "file".to_string(),
                        name: "src/counter.rs".to_string(),
                        mentions: 12,
                    },
                    EntitySummary {
                        kind: "function".to_string(),
                        name: "token_count".to_string(),
                        mentions: 5,
                    },
                ],
                rating: Some(4),
            },
            EnrichedResult {
                result: SearchResult {
                    id: Uuid::nil(),
                    session_id: Uuid::nil(),
                    message_id: None,
                    content_preview: "fix cost calculation bug".to_string(),
                    score: 0.871,
                },
                session: Some(make_session(
                    "Fix cost calculation rounding error",
                    "/Users/testuser/lab/Voyage",
                )),
                entities: vec![],
                rating: None,
            },
        ]
    }

    #[test]
    fn machine_output_is_valid_json() {
        let results = sample_results();
        let json = format_results_machine("token cost", 42, &results);
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("should be valid JSON");
        assert_eq!(parsed["results"].as_array().unwrap().len(), 2);
        assert!(parsed["results"][0]["score"].as_f64().unwrap() > 0.9);
    }

    #[test]
    fn machine_output_contains_metadata() {
        let results = sample_results();
        let json = format_results_machine("token cost", 42, &results);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["query"].as_str().unwrap(), "token cost");
        assert_eq!(parsed["total_indexed"].as_u64().unwrap(), 42);
        // Session metadata
        let r0 = &parsed["results"][0];
        assert!(!r0["summary"].as_str().unwrap().is_empty());
        assert!(!r0["timestamp"].as_str().unwrap().is_empty());
        assert!(!r0["cwd"].as_str().unwrap().is_empty());
        assert_eq!(r0["git_branch"].as_str().unwrap(), "main");
        assert!(r0["turns"].as_u64().unwrap() > 0);
        assert!(r0["cost_usd"].as_f64().unwrap() > 0.0);
    }

    #[test]
    fn human_output_shows_summary_not_uuid() {
        let results = sample_results();
        let out = format_results_human("token cost", 42, &results);
        // Should show summary, not raw UUID
        assert!(out.contains("Implement token counting"));
        // Should show date
        assert!(out.contains(&Utc::now().format("%Y-%m-%d").to_string()));
        // Should NOT show full UUID
        assert!(!out.contains("00000000-0000-0000-0000-000000000000"));
    }

    #[test]
    fn human_output_shows_project_path() {
        let results = sample_results();
        let out = format_results_human("token cost", 42, &results);
        // Should show cwd with branch
        assert!(out.contains("(main)"));
    }

    #[test]
    fn human_output_truncates_long_previews() {
        let results = vec![EnrichedResult {
            result: SearchResult {
                id: Uuid::nil(),
                session_id: Uuid::nil(),
                message_id: None,
                content_preview: "a".repeat(120),
                score: 0.5,
            },
            session: Some(make_session(&"a".repeat(120), "/tmp")),
            entities: vec![],
            rating: None,
        }];
        let out = format_results_human("q", 1, &results);
        assert!(out.contains("..."));
        assert!(!out.contains(&"a".repeat(120)));
    }

    #[test]
    fn machine_output_empty_results() {
        let json = format_results_machine("nonexistent", 100, &[]);
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("should be valid JSON");
        assert_eq!(parsed["query"].as_str().unwrap(), "nonexistent");
        assert_eq!(parsed["total_indexed"].as_u64().unwrap(), 100);
        assert!(parsed["results"].as_array().unwrap().is_empty());
    }

    #[test]
    fn machine_output_includes_entities() {
        let results = sample_results();
        let json = format_results_machine("token cost", 42, &results);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let entities = parsed["results"][0]["entities"].as_array().unwrap();
        assert_eq!(entities.len(), 2);
        assert_eq!(entities[0]["kind"].as_str().unwrap(), "file");
        assert_eq!(entities[0]["name"].as_str().unwrap(), "src/counter.rs");
        assert_eq!(entities[0]["mentions"].as_u64().unwrap(), 12);
    }

    #[test]
    fn machine_output_empty_entities() {
        let results = sample_results();
        let json = format_results_machine("token cost", 42, &results);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let entities = parsed["results"][1]["entities"].as_array().unwrap();
        assert!(entities.is_empty());
    }

    #[test]
    fn human_output_shows_entity_line() {
        let results = sample_results();
        let out = format_results_human("token cost", 42, &results);
        assert!(out.contains("→ src/counter.rs (file), token_count (function)"));
    }

    #[test]
    fn human_output_no_entity_line_when_empty() {
        let results = vec![EnrichedResult {
            result: SearchResult {
                id: Uuid::nil(),
                session_id: Uuid::nil(),
                message_id: None,
                content_preview: "no entities here".to_string(),
                score: 0.5,
            },
            session: None,
            entities: vec![],
            rating: None,
        }];
        let out = format_results_human("q", 1, &results);
        assert!(!out.contains("→"));
    }

    #[test]
    fn human_output_falls_back_to_content_preview() {
        let results = vec![EnrichedResult {
            result: SearchResult {
                id: Uuid::nil(),
                session_id: Uuid::nil(),
                message_id: None,
                content_preview: "raw embedding text here".to_string(),
                score: 0.5,
            },
            session: Some(make_session("", "/tmp")), // empty summary
            entities: vec![],
            rating: None,
        }];
        let out = format_results_human("q", 1, &results);
        assert!(out.contains("raw embedding text here"));
    }

    #[test]
    fn truncate_preview_first_line_only() {
        let text = "first line\nsecond line\nthird line";
        assert_eq!(truncate_preview(text, 80), "first line");
    }

    #[test]
    fn shorten_home_replaces_prefix() {
        // This test is environment-dependent but verifies the logic
        if let Some(home) = dirs_next::home_dir() {
            let path = format!("{}/lab/Voyage", home.display());
            let short = shorten_home(&path);
            assert!(short.starts_with("~/"));
            assert!(short.contains("lab/Voyage"));
        }
    }
}
