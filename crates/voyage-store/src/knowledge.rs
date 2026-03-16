//! Knowledge extraction from sessions — rule-based, no LLM needed.
//!
//! Four extractors, each returning `Vec<KnowledgeItem>`:
//! 1. World — stable facts (environment, tools, project paths)
//! 2. Experience — lessons learned (good/bad sessions, error patterns)
//! 3. Opinion — user preferences (explicit and implicit)
//! 4. EntityPage — aggregated entity profiles (Level 2 only, via promotion)

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
    items.extend(extract_experience(session, messages, existing, &source_date, sid));
    items.extend(extract_opinions(messages, existing, &source_date, sid));

    // Deduplicate against existing items
    let mut final_items = Vec::new();
    for item in items {
        if let Some(existing_item) = find_duplicate(&item, existing) {
            // Update mention_count on existing (caller should persist)
            let mut updated = existing_item.clone();
            updated.mention_count += 1;
            updated.updated_at = Utc::now().to_rfc3339();
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

fn extract_world(
    session: &Session,
    messages: &[Message],
    source_date: &str,
    sid: Option<Uuid>,
) -> Vec<KnowledgeItem> {
    let mut items = Vec::new();

    // Record project/cwd
    if !session.cwd.is_empty() {
        items.push(KnowledgeItem::new(
            KnowledgeKind::World,
            format!("Project: {}", session.project),
            format!("Working directory: {}", session.cwd),
            sid,
            source_date.to_string(),
        ));
    }

    // Extract "I use/I'm using/we use" patterns — require capitalized tool name or known tech pattern
    let use_re = Regex::new(r"(?i)\b(?:I use|I'm using|we use|我用|I work with)\s+([A-Z][A-Za-z0-9_./-]{1,30}|[a-z]+(?:\.js|\.rs|\.py|\.go))").unwrap();

    for msg in messages.iter().filter(|m| m.role == Role::User) {
        for cap in use_re.captures_iter(&msg.content) {
            let tool = cap[1].trim().to_string();
            if tool.len() > 2 && !is_common_word(&tool) {
                items.push(KnowledgeItem::new(
                    KnowledgeKind::World,
                    format!("Uses {tool}"),
                    format!("User mentioned using {tool} in session context"),
                    sid,
                    source_date.to_string(),
                ));
            }
        }
    }

    items
}

// ── Experience Extractor ──

fn extract_experience(
    session: &Session,
    messages: &[Message],
    _existing: &[KnowledgeItem],
    source_date: &str,
    sid: Option<Uuid>,
) -> Vec<KnowledgeItem> {
    let mut items = Vec::new();

    // Session summary as experience (always capture)
    if !session.summary.is_empty() && session.turn_count >= 2 {
        let mut item = KnowledgeItem::new(
            KnowledgeKind::Experience,
            session.summary.clone(),
            format!(
                "Session: {} turns, ${:.4} cost. {}",
                session.turn_count, session.estimated_cost_usd, session.summary
            ),
            sid,
            source_date.to_string(),
        );
        item.confidence = 0.7;
        items.push(item);
    }

    // Error patterns from messages
    let error_re =
        Regex::new(r"(?:Error|Exception|panic|Failed|error\[E\d+\]):\s*(.{10,100})").unwrap();
    for msg in messages.iter().filter(|m| m.role == Role::Assistant) {
        for cap in error_re.captures_iter(&msg.content) {
            let error_desc = cap[1].trim().to_string();
            items.push(KnowledgeItem::new(
                KnowledgeKind::Experience,
                format!("Error: {}", truncate_title(&error_desc, 80)),
                format!("Encountered error: {error_desc}"),
                sid,
                source_date.to_string(),
            ));
        }
    }

    items
}

// ── Opinion Extractor ──

fn extract_opinions(
    messages: &[Message],
    _existing: &[KnowledgeItem],
    source_date: &str,
    sid: Option<Uuid>,
) -> Vec<KnowledgeItem> {
    let mut items = Vec::new();

    let pref_re = Regex::new(
        r"(?i)\b(?:I prefer|don't use|always use|I like|I don't like|偏好|习惯|never use)\s+(.{3,60})"
    ).unwrap();

    for msg in messages.iter().filter(|m| m.role == Role::User) {
        for cap in pref_re.captures_iter(&msg.content) {
            let pref = cap[1].trim().to_string();
            // Clean up: take up to first sentence boundary
            let pref = pref.split(['.', ',', '!', '?', '\n']).next().unwrap_or(&pref).trim();
            if pref.len() > 3 {
                let mut item = KnowledgeItem::new(
                    KnowledgeKind::Opinion,
                    format!("Preference: {pref}"),
                    format!("User stated preference: {}", &cap[0]),
                    sid,
                    source_date.to_string(),
                );
                item.confidence = 0.9;
                items.push(item);
            }
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

        // Enforce cap: archive weakest if over limit
        let level2 = store.list_knowledge_items(Some(kind), Some(2), *cap + 50)?;
        if level2.len() > *cap {
            // Sort by confidence ASC, then updated_at ASC (weakest first)
            let mut to_archive: Vec<_> = level2.into_iter().collect();
            to_archive.sort_by(|a, b| {
                a.confidence
                    .partial_cmp(&b.confidence)
                    .unwrap()
                    .then(a.updated_at.cmp(&b.updated_at))
            });
            let excess = to_archive.len() - cap;
            for item in to_archive.iter().take(excess) {
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
        if item.title.starts_with("Uses ") {
            let name = item.title.strip_prefix("Uses ").unwrap_or(&item.title);
            tech_stack.push(TechStackEntry {
                name: name.to_string(),
                frequency: item.mention_count,
                last_seen: item.updated_at.clone(),
            });
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

    // Hour histogram
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
        plans_first: 0.0, // would need content analysis
    };

    // Preferences from opinion items
    let opinion_items =
        store.list_knowledge_items(Some(KnowledgeKind::Opinion), Some(2), 50)?;
    let preferences: Vec<Preference> = opinion_items
        .iter()
        .map(|item| Preference {
            key: item.title.clone(),
            value: item.content.clone(),
            confidence: item.confidence,
        })
        .collect();

    // Cost patterns
    let (avg, p50, p90) = store.get_cost_percentiles()?;
    let cost_patterns = CostPatterns { avg, p50, p90 };

    // Expertise from experience items
    let exp_items =
        store.list_knowledge_items(Some(KnowledgeKind::Experience), Some(2), 50)?;
    let expertise_areas: Vec<ExpertiseArea> = exp_items
        .iter()
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

// ── Helpers ──

fn is_common_word(s: &str) -> bool {
    matches!(
        s.to_lowercase().as_str(),
        "the" | "a" | "an" | "is" | "it" | "its" | "this" | "that" | "my" | "our" | "your"
            | "for" | "to" | "and" | "or" | "not" | "but" | "with" | "from" | "at" | "by"
            | "on" | "in" | "of" | "as" | "so" | "if" | "do" | "has" | "had" | "have"
            | "was" | "were" | "will" | "would" | "could" | "should" | "can" | "may"
            | "are" | "been" | "being" | "just" | "also" | "too" | "very" | "much"
            | "some" | "any" | "all" | "each" | "every" | "both" | "few" | "more"
            | "most" | "other" | "another" | "same" | "new" | "old" | "first" | "last"
            | "code" | "file" | "files" | "local" | "inside" | "here" | "there"
            | "above" | "below" | "before" | "after" | "now" | "then" | "still"
            | "already" | "again" | "only" | "well" | "make" | "made" | "get"
            | "got" | "set" | "run" | "running" | "let" | "clean" | "clear"
            | "minor" | "fresh" | "read-only" | "process" | "count" | "tasks"
            | "guide" | "apps" | "test" | "tests" | "unittests" | "comprehensive"
            | "transitional" | "native" | "well-architected" | "concurrently"
            | "concurrently." | "partnership"
    )
}

fn truncate_title(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let mut end = max;
        while !s.is_char_boundary(end) && end > 0 {
            end -= 1;
        }
        format!("{}...", &s[..end])
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
                content: "I use Rust and I prefer functional style".into(),
                usage: TokenUsage::default(),
                model: None,
                tool_calls: vec![],
                timestamp: "2026-03-12T10:00:00Z".parse().unwrap(),
            },
            Message {
                id: Uuid::new_v4(),
                session_id,
                role: Role::Assistant,
                content: "I'll help with that. Error: cannot find value `foo` in this scope".into(),
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
        assert!(items.iter().any(|i| i.kind == KnowledgeKind::World
            && i.title.contains("Project")));
    }

    #[test]
    fn extract_world_captures_tools() {
        let session = sample_session();
        let messages = sample_messages(session.id);
        let items = extract_knowledge(&session, &messages, &[]);
        assert!(items
            .iter()
            .any(|i| i.kind == KnowledgeKind::World && i.title.contains("Rust")));
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
    fn extract_opinions_captures_preference() {
        let session = sample_session();
        let messages = sample_messages(session.id);
        let items = extract_knowledge(&session, &messages, &[]);
        assert!(items
            .iter()
            .any(|i| i.kind == KnowledgeKind::Opinion && i.title.contains("functional")));
    }

    #[test]
    fn deduplication_updates_mention_count() {
        let session = sample_session();
        let messages = sample_messages(session.id);
        let first_run = extract_knowledge(&session, &messages, &[]);

        // Second run with first_run as existing
        let second_run = extract_knowledge(&session, &messages, &first_run);

        // Duplicates should have mention_count > 1
        for item in &second_run {
            if first_run.iter().any(|e| e.title_similarity(&item.title) > 0.7 && e.kind == item.kind) {
                assert!(item.mention_count > 1, "Expected incremented mention_count for '{}'", item.title);
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

        // Insert some L1 items with high mention_count
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

        // Check it's now level 2
        let l2 = store
            .list_knowledge_items(Some(KnowledgeKind::World), Some(2), 100)
            .unwrap();
        assert!(!l2.is_empty());
    }
}
