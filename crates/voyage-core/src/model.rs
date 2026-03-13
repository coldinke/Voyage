use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    ClaudeCode,
    OpenCode,
    Codex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    User,
    Assistant,
    System,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TokenUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_creation_tokens: u64,
}

impl TokenUsage {
    pub fn total(&self) -> u64 {
        self.input_tokens + self.output_tokens + self.cache_read_tokens + self.cache_creation_tokens
    }

    pub fn estimated_cost_usd(&self, model: &str) -> f64 {
        let (input_rate, output_rate, cache_read_rate, cache_write_rate) = cost_rates(model);
        (self.input_tokens as f64 * input_rate
            + self.output_tokens as f64 * output_rate
            + self.cache_read_tokens as f64 * cache_read_rate
            + self.cache_creation_tokens as f64 * cache_write_rate)
            / 1_000_000.0
    }
}

impl std::ops::AddAssign for TokenUsage {
    fn add_assign(&mut self, rhs: Self) {
        self.input_tokens += rhs.input_tokens;
        self.output_tokens += rhs.output_tokens;
        self.cache_read_tokens += rhs.cache_read_tokens;
        self.cache_creation_tokens += rhs.cache_creation_tokens;
    }
}

/// Cost per million tokens (input, output, cache_read, cache_write)
pub fn cost_rates(model: &str) -> (f64, f64, f64, f64) {
    match model {
        m if m.contains("opus") => (15.0, 75.0, 1.5, 18.75),
        m if m.contains("sonnet") => (3.0, 15.0, 0.3, 3.75),
        m if m.contains("haiku") => (0.8, 4.0, 0.08, 1.0),
        // Codex models — pricing varies; using GPT-4o-class rates as estimate
        m if m.contains("codex") || m.contains("gpt-5") => (2.5, 10.0, 1.25, 2.5),
        m if m.contains("gpt-4o") => (2.5, 10.0, 1.25, 2.5),
        m if m.contains("o3") || m.contains("o4") => (10.0, 40.0, 2.5, 10.0),
        _ => (10.0, 30.0, 1.0, 12.5), // conservative default
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: Uuid,
    pub session_id: Uuid,
    pub role: Role,
    pub content: String,
    pub usage: TokenUsage,
    pub model: Option<String>,
    pub tool_calls: Vec<String>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub id: Uuid,
    pub project: String,
    pub provider: Provider,
    pub model: String,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub cwd: String,
    pub git_branch: Option<String>,
    pub usage: TokenUsage,
    pub estimated_cost_usd: f64,
    pub message_count: u32,
    pub turn_count: u32,
    pub summary: String,
}

impl Session {
    pub fn new(id: Uuid, project: String, provider: Provider, model: String, cwd: String) -> Self {
        Self {
            id,
            project,
            provider,
            model,
            started_at: Utc::now(),
            ended_at: None,
            cwd,
            git_branch: None,
            usage: TokenUsage::default(),
            estimated_cost_usd: 0.0,
            message_count: 0,
            turn_count: 0,
            summary: String::new(),
        }
    }

    pub fn add_message(&mut self, msg: &Message) {
        self.usage += msg.usage.clone();
        self.message_count += 1;
        if msg.role == Role::Assistant {
            self.turn_count += 1;
        }
        if let Some(ref m) = msg.model
            && self.model.is_empty()
        {
            self.model = m.clone();
        }
        self.estimated_cost_usd = self.usage.estimated_cost_usd(&self.model);
        match self.ended_at {
            None => self.ended_at = Some(msg.timestamp),
            Some(t) if msg.timestamp > t => self.ended_at = Some(msg.timestamp),
            _ => {}
        }
    }
}

/// Merge a secondary session's messages into a primary session.
///
/// Messages are appended and sorted by timestamp, then session stats are
/// recalculated from scratch so counts and usage totals remain consistent.
pub fn merge_parsed_sessions(
    primary: &mut Session,
    primary_msgs: &mut Vec<Message>,
    secondary_msgs: Vec<Message>,
) {
    // Reassign session_id on secondary messages to the primary session
    let primary_id = primary.id;
    primary_msgs.extend(secondary_msgs.into_iter().map(|mut m| {
        m.session_id = primary_id;
        m
    }));
    primary_msgs.sort_by_key(|m| m.timestamp);

    // Recalculate session stats from scratch
    primary.usage = TokenUsage::default();
    primary.estimated_cost_usd = 0.0;
    primary.message_count = 0;
    primary.turn_count = 0;
    primary.ended_at = None;
    // Keep model — re-derive from first message that has one
    let orig_model = std::mem::take(&mut primary.model);
    for msg in primary_msgs.iter() {
        primary.add_message(msg);
    }
    // If no message set the model, restore the original
    if primary.model.is_empty() {
        primary.model = orig_model;
    }
}

/// Returns true if the title looks like a Claude Code auto-generated generic title
/// (e.g. "New session - 2026-01-22T01:53:39.024Z").
fn is_generic_title(t: &str) -> bool {
    t.starts_with("New session - ")
}

/// Extract a human-readable summary for a session.
///
/// Priority: (1) explicit title if non-empty and non-generic, (2) first user
/// message truncated to ~120 chars at a word boundary, (3) fallback to
/// "model on project".
pub fn extract_summary(
    title: Option<&str>,
    first_user_message: Option<&str>,
    model: &str,
    project: &str,
) -> String {
    // 1. Explicit title (e.g. from OpenCode) — skip generic auto-titles
    if let Some(t) = title {
        let t = t.trim();
        if !t.is_empty() && !is_generic_title(t) {
            return truncate_at_boundary(t, 120);
        }
    }
    // 2. First user message content
    if let Some(msg) = first_user_message {
        let msg = msg.trim();
        if !msg.is_empty() {
            return truncate_at_boundary(msg, 120);
        }
    }
    // 3. Fallback
    if model.is_empty() {
        return project.to_string();
    }
    format!("{model} on {project}")
}

/// Truncate a string to at most `max` chars, breaking at a word or sentence boundary.
pub fn truncate_at_boundary(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    // Snap max to a char boundary (avoids panic on multi-byte UTF-8)
    let mut max = max;
    while !s.is_char_boundary(max) && max > 0 {
        max -= 1;
    }
    // Find a good break point: sentence end (. ! ?) or last space
    let region = &s[..max];
    // Try sentence boundary first
    if let Some(pos) = region.rfind(['.', '!', '?'])
        && pos > max / 3
    {
        return s[..=pos].to_string();
    }
    // Fall back to word boundary
    if let Some(pos) = region.rfind(' ')
        && pos > max / 3
    {
        return format!("{}...", &s[..pos]);
    }
    // Hard truncate (max is already on a char boundary)
    format!("{}...", &s[..max])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_usage_total() {
        let usage = TokenUsage {
            input_tokens: 100,
            output_tokens: 200,
            cache_read_tokens: 50,
            cache_creation_tokens: 30,
        };
        assert_eq!(usage.total(), 380);
    }

    #[test]
    fn token_usage_default_is_zero() {
        let usage = TokenUsage::default();
        assert_eq!(usage.total(), 0);
    }

    #[test]
    fn token_usage_add_assign() {
        let mut a = TokenUsage {
            input_tokens: 10,
            output_tokens: 20,
            cache_read_tokens: 5,
            cache_creation_tokens: 3,
        };
        let b = TokenUsage {
            input_tokens: 100,
            output_tokens: 200,
            cache_read_tokens: 50,
            cache_creation_tokens: 30,
        };
        a += b;
        assert_eq!(a.input_tokens, 110);
        assert_eq!(a.output_tokens, 220);
        assert_eq!(a.cache_read_tokens, 55);
        assert_eq!(a.cache_creation_tokens, 33);
    }

    #[test]
    fn estimated_cost_opus() {
        let usage = TokenUsage {
            input_tokens: 1_000_000,
            output_tokens: 0,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
        };
        let cost = usage.estimated_cost_usd("claude-opus-4-6");
        assert!((cost - 15.0).abs() < 0.001);
    }

    #[test]
    fn estimated_cost_sonnet() {
        let usage = TokenUsage {
            input_tokens: 0,
            output_tokens: 1_000_000,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
        };
        let cost = usage.estimated_cost_usd("claude-sonnet-4-6");
        assert!((cost - 15.0).abs() < 0.001);
    }

    #[test]
    fn estimated_cost_mixed() {
        let usage = TokenUsage {
            input_tokens: 500_000,
            output_tokens: 100_000,
            cache_read_tokens: 200_000,
            cache_creation_tokens: 50_000,
        };
        // opus: 500k*15 + 100k*75 + 200k*1.5 + 50k*18.75 = 7.5M + 7.5M + 0.3M + 0.9375M = 16.2375M
        // / 1M = 16.2375
        let cost = usage.estimated_cost_usd("claude-opus-4-6");
        assert!((cost - 16.2375).abs() < 0.001);
    }

    #[test]
    fn session_new_defaults() {
        let session = Session::new(
            Uuid::new_v4(),
            "test-project".into(),
            Provider::ClaudeCode,
            String::new(),
            "/home/user/project".into(),
        );
        assert_eq!(session.usage.total(), 0);
        assert_eq!(session.message_count, 0);
        assert_eq!(session.turn_count, 0);
        assert_eq!(session.estimated_cost_usd, 0.0);
        assert!(session.ended_at.is_none());
    }

    #[test]
    fn session_add_message_accumulates() {
        let sid = Uuid::new_v4();
        let mut session = Session::new(
            sid,
            "test".into(),
            Provider::ClaudeCode,
            "claude-opus-4-6".into(),
            "/tmp".into(),
        );

        let msg1 = Message {
            id: Uuid::new_v4(),
            session_id: sid,
            role: Role::User,
            content: "hello".into(),
            usage: TokenUsage {
                input_tokens: 100,
                output_tokens: 0,
                cache_read_tokens: 0,
                cache_creation_tokens: 0,
            },
            model: None,
            tool_calls: vec![],
            timestamp: Utc::now(),
        };

        let msg2 = Message {
            id: Uuid::new_v4(),
            session_id: sid,
            role: Role::Assistant,
            content: "hi there".into(),
            usage: TokenUsage {
                input_tokens: 50,
                output_tokens: 200,
                cache_read_tokens: 30,
                cache_creation_tokens: 10,
            },
            model: Some("claude-opus-4-6".into()),
            tool_calls: vec!["Read".into()],
            timestamp: Utc::now(),
        };

        session.add_message(&msg1);
        session.add_message(&msg2);

        assert_eq!(session.message_count, 2);
        assert_eq!(session.turn_count, 1); // only assistant counts
        assert_eq!(session.usage.input_tokens, 150);
        assert_eq!(session.usage.output_tokens, 200);
        assert!(session.estimated_cost_usd > 0.0);
        assert!(session.ended_at.is_some());
    }

    #[test]
    fn session_model_set_from_first_message() {
        let sid = Uuid::new_v4();
        let mut session = Session::new(
            sid,
            "test".into(),
            Provider::ClaudeCode,
            String::new(),
            "/tmp".into(),
        );

        let msg = Message {
            id: Uuid::new_v4(),
            session_id: sid,
            role: Role::Assistant,
            content: "response".into(),
            usage: TokenUsage::default(),
            model: Some("claude-sonnet-4-6".into()),
            tool_calls: vec![],
            timestamp: Utc::now(),
        };

        session.add_message(&msg);
        assert_eq!(session.model, "claude-sonnet-4-6");
    }

    #[test]
    fn provider_serde_roundtrip() {
        let json = serde_json::to_string(&Provider::ClaudeCode).unwrap();
        assert_eq!(json, "\"claude_code\"");
        let parsed: Provider = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, Provider::ClaudeCode);
    }

    #[test]
    fn role_serde_roundtrip() {
        let json = serde_json::to_string(&Role::Assistant).unwrap();
        assert_eq!(json, "\"assistant\"");
        let parsed: Role = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, Role::Assistant);
    }

    #[test]
    fn extract_summary_prefers_title() {
        let result = extract_summary(Some("My title"), Some("user msg"), "opus", "proj");
        assert_eq!(result, "My title");
    }

    #[test]
    fn extract_summary_uses_first_user_message() {
        let result = extract_summary(None, Some("Help me fix this bug"), "opus", "proj");
        assert_eq!(result, "Help me fix this bug");
    }

    #[test]
    fn extract_summary_fallback() {
        let result = extract_summary(None, None, "opus", "proj");
        assert_eq!(result, "opus on proj");
    }

    #[test]
    fn extract_summary_empty_title_falls_through() {
        let result = extract_summary(Some("  "), Some("real msg"), "opus", "proj");
        assert_eq!(result, "real msg");
    }

    #[test]
    fn truncate_at_boundary_short() {
        assert_eq!(truncate_at_boundary("hello", 120), "hello");
    }

    #[test]
    fn truncate_at_boundary_sentence() {
        let s = "First sentence. Second sentence that is much longer and goes on.";
        let result = truncate_at_boundary(s, 40);
        assert_eq!(result, "First sentence.");
    }

    #[test]
    fn truncate_at_boundary_word() {
        let s = "word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20";
        let result = truncate_at_boundary(s, 50);
        assert!(result.len() <= 53); // 50 + "..."
        assert!(result.ends_with("..."));
    }

    #[test]
    fn truncate_at_boundary_multibyte_utf8() {
        // CJK string where naive byte slicing would land inside a multi-byte char
        let s = "这是一个测试字符串，用来验证多字节字符不会导致崩溃的情况";
        // Each CJK char is 3 bytes; ， (fullwidth comma) is also 3 bytes.
        // Slicing at any byte offset should never panic.
        for max in 0..=s.len() + 5 {
            let result = truncate_at_boundary(s, max);
            assert!(result.len() <= max + 3, "result too long for max={max}");
        }
        // Specific case: max lands mid-character
        let result = truncate_at_boundary(s, 10);
        assert!(!result.is_empty());
    }

    #[test]
    fn session_new_has_empty_summary() {
        let session = Session::new(
            Uuid::new_v4(),
            "test".into(),
            Provider::ClaudeCode,
            String::new(),
            "/tmp".into(),
        );
        assert_eq!(session.summary, "");
    }

    #[test]
    fn extract_summary_generic_title_falls_through() {
        let result = extract_summary(
            Some("New session - 2026-01-22T01:53:39.024Z"),
            Some("fix auth bug"),
            "opus",
            "proj",
        );
        assert_eq!(result, "fix auth bug");
    }

    #[test]
    fn extract_summary_generic_title_no_user_message() {
        let result = extract_summary(
            Some("New session - 2026-01-22T01:53:39.024Z"),
            None,
            "opus",
            "proj",
        );
        assert_eq!(result, "opus on proj");
    }

    #[test]
    fn merge_parsed_sessions_combines_messages() {
        let sid = Uuid::new_v4();
        let mut session = Session::new(
            sid,
            "test".into(),
            Provider::ClaudeCode,
            String::new(),
            "/tmp".into(),
        );

        let ts1: DateTime<Utc> = "2026-03-12T10:00:00Z".parse().unwrap();
        let ts2: DateTime<Utc> = "2026-03-12T10:01:00Z".parse().unwrap();
        let ts3: DateTime<Utc> = "2026-03-12T10:00:30Z".parse().unwrap();

        let mut primary_msgs = vec![
            Message {
                id: Uuid::new_v4(),
                session_id: sid,
                role: Role::User,
                content: "hello".into(),
                usage: TokenUsage {
                    input_tokens: 500,
                    output_tokens: 0,
                    cache_read_tokens: 0,
                    cache_creation_tokens: 0,
                },
                model: None,
                tool_calls: vec![],
                timestamp: ts1,
            },
            Message {
                id: Uuid::new_v4(),
                session_id: sid,
                role: Role::Assistant,
                content: "hi".into(),
                usage: TokenUsage {
                    input_tokens: 500,
                    output_tokens: 200,
                    cache_read_tokens: 0,
                    cache_creation_tokens: 0,
                },
                model: Some("claude-opus-4-6".into()),
                tool_calls: vec![],
                timestamp: ts2,
            },
        ];

        // Build initial stats
        for msg in &primary_msgs {
            session.add_message(msg);
        }
        session.summary = "original summary".into();

        let secondary_msgs = vec![Message {
            id: Uuid::new_v4(),
            session_id: sid,
            role: Role::Assistant,
            content: "subagent response".into(),
            usage: TokenUsage {
                input_tokens: 300,
                output_tokens: 100,
                cache_read_tokens: 0,
                cache_creation_tokens: 0,
            },
            model: Some("claude-opus-4-6".into()),
            tool_calls: vec![],
            timestamp: ts3,
        }];

        merge_parsed_sessions(&mut session, &mut primary_msgs, secondary_msgs);

        assert_eq!(session.message_count, 3);
        assert_eq!(session.usage.input_tokens, 1300);
        assert_eq!(session.usage.output_tokens, 300);
        // Messages should be sorted by timestamp
        assert!(primary_msgs[0].timestamp <= primary_msgs[1].timestamp);
        assert!(primary_msgs[1].timestamp <= primary_msgs[2].timestamp);
        // Summary should be preserved
        assert_eq!(session.summary, "original summary");
    }
}
