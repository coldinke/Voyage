//! Knowledge extraction from sessions — rule-based, no LLM needed.
//!
//! Four extractors, each returning `Vec<KnowledgeItem>`:
//! 1. World — stable facts (environment, tools, project paths)
//! 2. Experience — lessons learned (error→solution pairs, session outcomes)
//! 3. Opinion — user preferences (explicit statements)
//! 4. EntityPage — aggregated entity profiles (Level 2 only, via promotion)

use std::collections::HashSet;

use chrono::Utc;
use regex::Regex;
use uuid::Uuid;

use voyage_core::model::{KnowledgeItem, KnowledgeKind, Message, Role, Session};

use crate::sqlite::SqliteStore;

/// Extract all knowledge from a session and its messages.
/// Deduplicates against existing items by title similarity (Jaccard > 0.7).
pub fn extract_knowledge(
    session: &Session,
    messages: &[Message],
    existing: &[KnowledgeItem],
) -> Vec<KnowledgeItem> {
    let mut items = Vec::new();
    let source_date = session.started_at.format("%Y-%m-%d").to_string();
    let sid = Some(session.id);

    items.extend(extract_world(session, messages, &source_date, sid));
    items.extend(extract_experience(session, messages, &source_date, sid));
    items.extend(extract_opinions(messages, &source_date, sid));

    // Deduplicate against existing items
    let mut final_items = Vec::new();
    for item in items {
        if let Some(existing_item) = find_duplicate(&item, existing) {
            let mut updated = existing_item.clone();
            updated.mention_count += 1;
            updated.updated_at = Utc::now().to_rfc3339();
            // Merge content if new content is longer/richer
            if item.content.len() > updated.content.len() {
                updated.content = item.content;
            }
            final_items.push(updated);
        } else if !find_duplicate_in_batch(&item, &final_items) {
            final_items.push(item);
        }
    }

    final_items
}

fn find_duplicate<'a>(
    item: &KnowledgeItem,
    existing: &'a [KnowledgeItem],
) -> Option<&'a KnowledgeItem> {
    existing.iter().find(|e| {
        e.kind == item.kind && e.superseded_by.is_none() && e.title_similarity(&item.title) > 0.7
    })
}

fn find_duplicate_in_batch(item: &KnowledgeItem, batch: &[KnowledgeItem]) -> bool {
    batch
        .iter()
        .any(|e| e.kind == item.kind && e.title_similarity(&item.title) > 0.7)
}

// ── World Extractor ──
// Only extracts high-confidence facts: project paths and known tech terms.

/// Known technology/tool names that are worth tracking.
const TECH_TERMS: &[&str] = &[
    "rust", "python", "typescript", "javascript", "go", "java", "kotlin", "swift",
    "react", "vue", "svelte", "angular", "nextjs", "nuxt", "astro",
    "node", "deno", "bun", "npm", "pnpm", "yarn", "cargo", "pip",
    "docker", "kubernetes", "k8s", "terraform", "ansible", "nginx",
    "postgres", "postgresql", "mysql", "sqlite", "redis", "mongodb",
    "git", "github", "gitlab", "neovim", "vim", "vscode", "emacs",
    "linux", "macos", "ubuntu", "debian", "arch", "nixos",
    "tmux", "kitty", "alacritty", "wezterm", "zsh", "bash", "fish",
    "claude", "openai", "gpt", "llm", "mcp",
    "tailwind", "prisma", "drizzle", "supabase", "firebase",
    "aws", "gcp", "azure", "vercel", "cloudflare",
    "graphql", "grpc", "rest", "websocket",
    "tokio", "axum", "actix", "warp", "rocket",
    "fastapi", "django", "flask", "express",
    "zig", "elixir", "erlang", "haskell", "ocaml", "scala", "clojure",
    "tauri", "electron",
];

fn extract_world(
    session: &Session,
    messages: &[Message],
    source_date: &str,
    sid: Option<Uuid>,
) -> Vec<KnowledgeItem> {
    let mut items = Vec::new();
    let mut seen_titles: HashSet<String> = HashSet::new();

    // Record unique project path (deduplicate by cwd, not project name)
    if !session.cwd.is_empty() {
        let title = format!("Project: {}", session.cwd);
        if seen_titles.insert(title.clone()) {
            items.push(KnowledgeItem::new(
                KnowledgeKind::World,
                title,
                format!(
                    "Project at {} (name: {}, branch: {})",
                    session.cwd,
                    session.project,
                    session.git_branch.as_deref().unwrap_or("unknown"),
                ),
                sid,
                source_date.to_string(),
            ));
        }
    }

    // Extract tech terms from user messages via whitelist matching
    let tech_set: HashSet<&str> = TECH_TERMS.iter().copied().collect();
    for msg in messages.iter().filter(|m| m.role == Role::User) {
        for word in msg.content.split(|c: char| !c.is_alphanumeric() && c != '-' && c != '_') {
            let lower = word.to_lowercase();
            if tech_set.contains(lower.as_str()) && word.len() >= 2 {
                let title = format!("Uses {lower}");
                if seen_titles.insert(title.clone()) {
                    items.push(KnowledgeItem::new(
                        KnowledgeKind::World,
                        title,
                        format!("User works with {lower}"),
                        sid,
                        source_date.to_string(),
                    ));
                }
            }
        }
    }

    items
}

// ── Experience Extractor ──
// Extracts: session outcomes, error→resolution pairs.

fn extract_experience(
    session: &Session,
    messages: &[Message],
    source_date: &str,
    sid: Option<Uuid>,
) -> Vec<KnowledgeItem> {
    let mut items = Vec::new();

    // Session outcome as experience
    if !session.summary.is_empty() && session.turn_count >= 3 {
        let outcome = if session.estimated_cost_usd > 50.0 {
            "expensive"
        } else if session.turn_count > 200 {
            "long"
        } else {
            "normal"
        };
        let mut item = KnowledgeItem::new(
            KnowledgeKind::Experience,
            truncate_title(&session.summary, 100),
            format!(
                "{} session ({} turns, ${:.2}): {}",
                outcome, session.turn_count, session.estimated_cost_usd, session.summary
            ),
            sid,
            source_date.to_string(),
        );
        item.confidence = 0.7;
        items.push(item);
    }

    // Error→resolution pairs: find errors in assistant messages, then look for
    // resolution context in surrounding messages.
    let error_re =
        Regex::new(r"(?:error\[E\d+\]|Error|panic!?|FAILED):\s*(.{10,120})").unwrap();

    let mut seen_errors: HashSet<String> = HashSet::new();
    for (i, msg) in messages.iter().enumerate() {
        if msg.role != Role::Assistant {
            continue;
        }
        for cap in error_re.captures_iter(&msg.content) {
            let error_text = cap[1].trim();
            // Normalize: take first line, skip noise
            let error_line = error_text.lines().next().unwrap_or(error_text).trim();
            if error_line.len() < 10 || seen_errors.contains(error_line) {
                continue;
            }

            // Look for resolution: next assistant message that doesn't contain the same error
            let resolution = messages
                .iter()
                .skip(i + 1)
                .filter(|m| m.role == Role::Assistant)
                .take(3)
                .find(|m| !m.content.contains(error_line))
                .map(|m| {
                    let preview: String = m.content.chars().take(200).collect();
                    truncate_title(&preview, 150)
                });

            let content = match &resolution {
                Some(res) => format!("Error: {error_line}\nResolution context: {res}"),
                None => format!("Error encountered: {error_line}"),
            };

            seen_errors.insert(error_line.to_string());
            items.push(KnowledgeItem::new(
                KnowledgeKind::Experience,
                format!("Error: {}", truncate_title(error_line, 80)),
                content,
                sid,
                source_date.to_string(),
            ));
        }
    }

    items
}

// ── Opinion Extractor ──
// Captures explicit user preference statements with full sentence context.

fn extract_opinions(
    messages: &[Message],
    source_date: &str,
    sid: Option<Uuid>,
) -> Vec<KnowledgeItem> {
    let mut items = Vec::new();
    let mut seen_prefs: HashSet<String> = HashSet::new();

    let pref_re = Regex::new(
        r"(?i)\b(?:I prefer|don't use|always use|I like to|I don't like|never use|偏好|习惯)\s+(.{3,120})"
    ).unwrap();

    for msg in messages.iter().filter(|m| m.role == Role::User) {
        for cap in pref_re.captures_iter(&msg.content) {
            let raw = cap[1].trim();
            // Take up to sentence boundary but preserve more context
            let pref = raw
                .split(['\n', '\r'])
                .next()
                .unwrap_or(raw)
                .trim()
                .trim_end_matches(['.', ',', '!', '?', ';']);
            if pref.len() < 4 {
                continue;
            }

            // Normalize for dedup
            let normalized = pref.to_lowercase();
            if seen_prefs.contains(&normalized) {
                continue;
            }
            seen_prefs.insert(normalized);

            let mut item = KnowledgeItem::new(
                KnowledgeKind::Opinion,
                format!("Preference: {pref}"),
                format!(
                    "User stated: \"{}\"",
                    cap[0].chars().take(200).collect::<String>()
                ),
                sid,
                source_date.to_string(),
            );
            item.confidence = 0.9;
            items.push(item);
        }
    }

    items
}

// ── Promotion Logic ──

/// Promote Level 1 items to Level 2 based on promotion criteria.
pub fn promote_items(store: &SqliteStore) -> Result<u32, Box<dyn std::error::Error>> {
    let mut promoted = 0u32;
    let limits: &[(&str, usize)] = &[
        ("world", 50),
        ("experience", 100),
        ("opinion", 50),
        ("entity_page", 30),
    ];

    for (kind_str, cap) in limits {
        let kind = KnowledgeKind::parse(kind_str);
        let items = store.list_knowledge_items(Some(kind), Some(1), 10_000)?;

        for item in &items {
            let dominated = match kind {
                KnowledgeKind::World => item.mention_count >= 2 || item.confidence >= 0.9,
                KnowledgeKind::Experience => item.mention_count >= 2,
                KnowledgeKind::Opinion => item.confidence >= 0.8 || item.mention_count >= 3,
                KnowledgeKind::EntityPage => item.mention_count >= 10,
            };

            if dominated {
                let mut promoted_item = item.clone();
                promoted_item.level = 2;
                promoted_item.updated_at = Utc::now().to_rfc3339();
                store.upsert_knowledge_item(&promoted_item)?;
                promoted += 1;
            }
        }

        // Enforce cap: archive weakest items beyond the limit
        let level2 = store.list_knowledge_items(Some(kind), Some(2), 100_000)?;
        if level2.len() > *cap {
            // Sort: keep items with highest mention_count * confidence
            let mut scored: Vec<_> = level2
                .into_iter()
                .map(|item| {
                    let score = item.mention_count as f64 * item.confidence;
                    (item, score)
                })
                .collect();
            scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

            // Archive everything beyond the cap
            for (item, _) in scored.iter().skip(*cap) {
                let mut archived = item.clone();
                archived.superseded_by = Some("archived".to_string());
                archived.updated_at = Utc::now().to_rfc3339();
                store.upsert_knowledge_item(&archived)?;
            }
        }
    }

    Ok(promoted)
}

// ── Profile Computation ──

use voyage_core::model::{
    CostPatterns, ExpertiseArea, Preference, TechStackEntry, UserProfile, WorkingStyle,
};

pub fn compute_profile(store: &SqliteStore) -> Result<UserProfile, Box<dyn std::error::Error>> {
    let sessions = store.list_sessions(None, None, 100_000)?;
    let session_count = sessions.len() as u32;

    // Tech stack from world knowledge items
    let world_items = store.list_knowledge_items(Some(KnowledgeKind::World), None, 500)?;
    let mut tech_stack: Vec<TechStackEntry> = Vec::new();
    for item in &world_items {
        if let Some(name) = item.title.strip_prefix("Uses ") {
            // Deduplicate by name
            if !tech_stack.iter().any(|t| t.name == name) {
                tech_stack.push(TechStackEntry {
                    name: name.to_string(),
                    frequency: item.mention_count,
                    last_seen: item.updated_at.clone(),
                });
            }
        }
    }
    tech_stack.sort_by(|a, b| b.frequency.cmp(&a.frequency));
    tech_stack.truncate(20);

    // Working style
    let (total_turns, total_cost) = sessions.iter().fold((0u64, 0.0f64), |(t, c), s| {
        (t + s.turn_count as u64, c + s.estimated_cost_usd)
    });
    let avg_turns = if session_count > 0 {
        total_turns as f64 / session_count as f64
    } else {
        0.0
    };
    let avg_cost = if session_count > 0 {
        total_cost / session_count as f64
    } else {
        0.0
    };

    let mut hour_counts = [0u32; 24];
    for s in &sessions {
        let hour = s.started_at.format("%H").to_string().parse::<usize>().unwrap_or(0);
        if hour < 24 {
            hour_counts[hour] += 1;
        }
    }
    let preferred_hours: Vec<u32> = hour_counts
        .iter()
        .enumerate()
        .filter(|(_, c)| **c > 0)
        .map(|(h, _)| h as u32)
        .collect();

    let working_style = WorkingStyle {
        avg_turns,
        avg_cost,
        preferred_hours,
        plans_first: 0.0,
    };

    // Preferences from opinion items (deduplicated)
    let opinion_items =
        store.list_knowledge_items(Some(KnowledgeKind::Opinion), Some(2), 50)?;
    let mut seen_pref_keys: HashSet<String> = HashSet::new();
    let preferences: Vec<Preference> = opinion_items
        .iter()
        .filter(|item| seen_pref_keys.insert(item.title.to_lowercase()))
        .map(|item| Preference {
            key: item.title.clone(),
            value: item.content.clone(),
            confidence: item.confidence,
        })
        .collect();

    let (avg, p50, p90) = store.get_cost_percentiles()?;
    let cost_patterns = CostPatterns { avg, p50, p90 };

    // Expertise from experience items (skip pure error entries)
    let exp_items =
        store.list_knowledge_items(Some(KnowledgeKind::Experience), Some(2), 50)?;
    let expertise_areas: Vec<ExpertiseArea> = exp_items
        .iter()
        .filter(|item| !item.title.starts_with("Error:"))
        .take(10)
        .map(|item| ExpertiseArea {
            area: item.title.clone(),
            confidence: item.confidence,
            session_count: item.mention_count,
        })
        .collect();

    Ok(UserProfile {
        tech_stack,
        working_style,
        expertise_areas,
        preferences,
        cost_patterns,
        computed_at: Utc::now().to_rfc3339(),
        session_count,
    })
}

// ── Context Generation (for CC integration) ──

/// Generate a compact markdown context block optimized for LLM consumption.
/// This is the main output for CC integration — designed to be injected into
/// CLAUDE.md, a skill, or a SessionStart hook.
pub fn generate_context(store: &SqliteStore) -> Result<String, Box<dyn std::error::Error>> {
    let mut out = String::new();

    // ── Recent Activity (last 7 days) ──
    let since = Utc::now() - chrono::Duration::days(7);
    let recent = store.list_sessions(Some(since), None, 50)?;
    if !recent.is_empty() {
        out.push_str("## Recent Sessions\n\n");
        let mut current_date = String::new();
        for s in &recent {
            // Skip empty/noise sessions
            if s.turn_count == 0 || s.summary.starts_with('<') {
                continue;
            }
            let date = s.started_at.format("%Y-%m-%d").to_string();
            if date != current_date {
                current_date = date.clone();
                out.push_str(&format!("**{date}**\n"));
            }
            out.push_str(&format!(
                "- {} ({} turns, ${:.2})\n",
                truncate_title(&s.summary, 70),
                s.turn_count,
                s.estimated_cost_usd,
            ));
        }
        out.push('\n');
    }

    // ── User Preferences (cleaned) ──
    let opinions = store.list_knowledge_items(Some(KnowledgeKind::Opinion), Some(2), 20)?;
    let mut seen: HashSet<String> = HashSet::new();
    let unique_opinions: Vec<_> = opinions
        .iter()
        .filter(|o| seen.insert(o.title.to_lowercase()))
        .collect();
    if !unique_opinions.is_empty() {
        out.push_str("## Preferences\n\n");
        for item in &unique_opinions {
            let pref = item.title.strip_prefix("Preference: ").unwrap_or(&item.title);
            // Clean: remove XML tags, literal \n, truncate at sentence
            let clean = clean_for_context(pref);
            if clean.len() > 3 {
                out.push_str(&format!("- {clean}\n"));
            }
        }
        out.push('\n');
    }

    // ── Tech Stack (only items with mention_count >= 2, i.e. seen across sessions) ──
    let world = store.list_knowledge_items(Some(KnowledgeKind::World), Some(2), 100)?;
    let tech: Vec<&str> = world
        .iter()
        .filter(|w| w.mention_count >= 2)
        .filter_map(|w| w.title.strip_prefix("Uses "))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    if !tech.is_empty() {
        let mut sorted = tech;
        sorted.sort();
        out.push_str(&format!("## Tech Stack\n\n{}\n\n", sorted.join(", ")));
    }

    // ── Known Issues (errors that recurred) ──
    let errors = store.list_knowledge_items(Some(KnowledgeKind::Experience), Some(2), 50)?;
    let recurring_errors: Vec<_> = errors
        .iter()
        .filter(|e| e.title.starts_with("Error:") && e.mention_count >= 2)
        .take(5)
        .collect();
    if !recurring_errors.is_empty() {
        out.push_str("## Known Issues\n\n");
        for item in &recurring_errors {
            let err = item.title.strip_prefix("Error: ").unwrap_or(&item.title);
            out.push_str(&format!("- {}\n", truncate_title(err, 80)));
        }
        out.push('\n');
    }

    // ── Cost Context ──
    let (avg, p50, p90) = store.get_cost_percentiles()?;
    if avg > 0.0 {
        out.push_str(&format!(
            "## Cost Baseline\n\nAvg ${avg:.2} / P50 ${p50:.2} / P90 ${p90:.2} per session\n\n"
        ));
    }

    Ok(out)
}

// ── Helpers ──

/// Clean text for context output: strip XML tags, literal escapes, truncate at sentence.
fn clean_for_context(s: &str) -> String {
    let mut cleaned = s.to_string();
    // Remove literal \n
    cleaned = cleaned.replace("\\n", " ");
    // Remove XML-like tags
    let tag_re = Regex::new(r"<[^>]+>").unwrap();
    cleaned = tag_re.replace_all(&cleaned, "").to_string();
    // Truncate at first sentence boundary within 80 chars
    let first_line = cleaned.lines().next().unwrap_or(&cleaned);
    if first_line.len() > 80
        && let Some(pos) = first_line[..80].rfind(['.', '!', '?'])
    {
        return first_line[..=pos].trim().to_string();
    }
    first_line.trim().to_string()
}

fn truncate_title(s: &str, max: usize) -> String {
    let first_line = s.lines().next().unwrap_or(s);
    if first_line.len() <= max {
        first_line.to_string()
    } else {
        let mut end = max;
        while !first_line.is_char_boundary(end) && end > 0 {
            end -= 1;
        }
        format!("{}...", &first_line[..end])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use voyage_core::model::{Provider, TokenUsage};

    fn sample_session() -> Session {
        Session {
            id: Uuid::new_v4(),
            project: "test-project".into(),
            provider: Provider::ClaudeCode,
            model: "claude-opus-4-6".into(),
            started_at: "2026-03-12T10:00:00Z".parse().unwrap(),
            ended_at: Some("2026-03-12T10:30:00Z".parse().unwrap()),
            cwd: "/Users/test/project".into(),
            git_branch: Some("main".into()),
            usage: TokenUsage::default(),
            estimated_cost_usd: 0.5,
            message_count: 10,
            turn_count: 5,
            summary: "Fix authentication bug".into(),
        }
    }

    fn sample_messages(session_id: Uuid) -> Vec<Message> {
        vec![
            Message {
                id: Uuid::new_v4(),
                session_id,
                role: Role::User,
                content: "I use Rust and I prefer functional style over OOP".into(),
                usage: TokenUsage::default(),
                model: None,
                tool_calls: vec![],
                timestamp: "2026-03-12T10:00:00Z".parse().unwrap(),
            },
            Message {
                id: Uuid::new_v4(),
                session_id,
                role: Role::Assistant,
                content: "I'll help with that. Error: cannot find value `foo` in this scope\nLet me fix that by importing the module.".into(),
                usage: TokenUsage::default(),
                model: Some("claude-opus-4-6".into()),
                tool_calls: vec![],
                timestamp: "2026-03-12T10:01:00Z".parse().unwrap(),
            },
        ]
    }

    #[test]
    fn extract_world_captures_cwd() {
        let session = sample_session();
        let messages = sample_messages(session.id);
        let items = extract_knowledge(&session, &messages, &[]);
        assert!(items
            .iter()
            .any(|i| i.kind == KnowledgeKind::World && i.title.contains("/Users/test/project")));
    }

    #[test]
    fn extract_world_captures_tech_terms() {
        let session = sample_session();
        let messages = sample_messages(session.id);
        let items = extract_knowledge(&session, &messages, &[]);
        assert!(items
            .iter()
            .any(|i| i.kind == KnowledgeKind::World && i.title == "Uses rust"));
    }

    #[test]
    fn extract_world_no_noise() {
        let session = sample_session();
        let messages = sample_messages(session.id);
        let items = extract_knowledge(&session, &messages, &[]);
        let world_items: Vec<_> = items.iter().filter(|i| i.kind == KnowledgeKind::World).collect();
        // Should only have: project path + "rust" — no noise words
        for item in &world_items {
            assert!(
                item.title.starts_with("Project:") || item.title.starts_with("Uses "),
                "Unexpected world item: {}",
                item.title
            );
        }
    }

    #[test]
    fn extract_experience_captures_summary() {
        let session = sample_session();
        let messages = sample_messages(session.id);
        let items = extract_knowledge(&session, &messages, &[]);
        assert!(items
            .iter()
            .any(|i| i.kind == KnowledgeKind::Experience && i.title.contains("Fix")));
    }

    #[test]
    fn extract_opinions_captures_full_preference() {
        let session = sample_session();
        let messages = sample_messages(session.id);
        let items = extract_knowledge(&session, &messages, &[]);
        let opinion = items
            .iter()
            .find(|i| i.kind == KnowledgeKind::Opinion)
            .expect("Should find opinion");
        assert!(
            opinion.title.contains("functional style over OOP"),
            "Opinion should capture full phrase: {}",
            opinion.title
        );
    }

    #[test]
    fn deduplication_updates_mention_count() {
        let session = sample_session();
        let messages = sample_messages(session.id);
        let first_run = extract_knowledge(&session, &messages, &[]);
        let second_run = extract_knowledge(&session, &messages, &first_run);

        for item in &second_run {
            if first_run
                .iter()
                .any(|e| e.title_similarity(&item.title) > 0.7 && e.kind == item.kind)
            {
                assert!(
                    item.mention_count > 1,
                    "Expected incremented mention_count for '{}'",
                    item.title
                );
            }
        }
    }

    #[test]
    fn title_similarity_works() {
        let item = KnowledgeItem::new(
            KnowledgeKind::World,
            "Uses Rust programming".into(),
            "content".into(),
            None,
            "2026-03-12".into(),
        );
        assert!(item.title_similarity("Uses Rust programming") > 0.99);
        assert!(item.title_similarity("Uses Rust") > 0.5);
        assert!(item.title_similarity("Something completely different") < 0.3);
    }

    #[test]
    fn promote_and_cap() {
        let store = SqliteStore::open_in_memory().unwrap();

        let mut item = KnowledgeItem::new(
            KnowledgeKind::World,
            "Uses Docker".into(),
            "Docker is used".into(),
            None,
            "2026-03-12".into(),
        );
        item.mention_count = 5;
        store.upsert_knowledge_item(&item).unwrap();

        let promoted = promote_items(&store).unwrap();
        assert!(promoted > 0);

        let l2 = store
            .list_knowledge_items(Some(KnowledgeKind::World), Some(2), 100)
            .unwrap();
        assert!(!l2.is_empty());
    }

    #[test]
    fn context_generation_produces_output() {
        let store = SqliteStore::open_in_memory().unwrap();
        let ctx = generate_context(&store).unwrap();
        // Empty DB produces minimal output
        assert!(ctx.is_empty() || ctx.contains("##"));
    }
}
