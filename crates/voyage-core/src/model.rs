use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    ClaudeCode,
    OpenCode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    User,
    Assistant,
    System,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

impl Default for TokenUsage {
    fn default() -> Self {
        Self {
            input_tokens: 0,
            output_tokens: 0,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
        }
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
fn cost_rates(model: &str) -> (f64, f64, f64, f64) {
    match model {
        m if m.contains("opus") => (15.0, 75.0, 1.5, 18.75),
        m if m.contains("sonnet") => (3.0, 15.0, 0.3, 3.75),
        m if m.contains("haiku") => (0.8, 4.0, 0.08, 1.0),
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
        }
    }

    pub fn add_message(&mut self, msg: &Message) {
        self.usage += msg.usage.clone();
        self.message_count += 1;
        if msg.role == Role::Assistant {
            self.turn_count += 1;
        }
        if let Some(ref m) = msg.model {
            if self.model.is_empty() {
                self.model = m.clone();
            }
        }
        self.estimated_cost_usd = self.usage.estimated_cost_usd(&self.model);
        match self.ended_at {
            None => self.ended_at = Some(msg.timestamp),
            Some(t) if msg.timestamp > t => self.ended_at = Some(msg.timestamp),
            _ => {}
        }
    }
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
}
