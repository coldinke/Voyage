//! Parser for OpenAI Codex CLI local session data.
//!
//! Codex stores sessions as JSONL rollout files in:
//!   ~/.codex/sessions/YYYY/MM/DD/rollout-<timestamp>-<UUID>.jsonl
//!
//! Each line is `{ "timestamp": "<ISO-8601>", "type": "<type>", "payload": {...} }`
//! Record types: session_meta, response_item, event_msg, turn_context

use std::io::BufRead;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::Deserialize;
use uuid::Uuid;

use voyage_core::model::{Message, Provider, Role, Session, TokenUsage};

use crate::claude_code::ParseError;
use crate::traits::SessionParser;

// ── Raw JSONL types ──────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct RawLine {
    timestamp: String,
    #[serde(rename = "type")]
    record_type: String,
    payload: serde_json::Value,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct SessionMeta {
    id: String,
    timestamp: Option<String>,
    cwd: Option<String>,
    originator: Option<String>,
    cli_version: Option<String>,
    source: Option<String>,
    model_provider: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct TurnContext {
    turn_id: Option<String>,
    model: Option<String>,
    cwd: Option<String>,
    effort: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct ResponseItemPayload {
    #[serde(rename = "type")]
    item_type: String,
    role: Option<String>,
    content: Option<Vec<ContentItem>>,
    #[serde(default)]
    phase: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ContentItem {
    #[serde(rename = "type")]
    content_type: String,
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct EventMsgPayload {
    #[serde(rename = "type")]
    event_type: String,
    info: Option<TokenInfo>,
    message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TokenInfo {
    total_token_usage: Option<TotalTokenUsage>,
}

#[derive(Debug, Deserialize)]
struct TotalTokenUsage {
    input_tokens: Option<u64>,
    cached_input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    reasoning_output_tokens: Option<u64>,
}

// ── Parser ───────────────────────────────────────────────────────────

pub struct CodexParser;

impl CodexParser {
    pub fn new() -> Self {
        Self
    }

    pub fn parse_session(
        &self,
        path: &Path,
    ) -> Result<(Session, Vec<Message>), ParseError> {
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);

        let session_id = extract_uuid_from_filename(path);
        let mut session_meta: Option<SessionMeta> = None;
        let mut model: Option<String> = None;
        let mut messages: Vec<Message> = Vec::new();
        let mut last_total_usage: Option<TotalTokenUsage> = None;

        for (line_num, line) in reader.lines().enumerate() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }

            let raw: RawLine = serde_json::from_str(&line).map_err(|e| ParseError::Json {
                line: line_num + 1,
                source: e,
            })?;

            match raw.record_type.as_str() {
                "session_meta" => {
                    if let Ok(meta) = serde_json::from_value::<SessionMeta>(raw.payload) {
                        session_meta = Some(meta);
                    }
                }
                "turn_context" => {
                    if let Ok(ctx) = serde_json::from_value::<TurnContext>(raw.payload) {
                        if model.is_none() {
                            model = ctx.model;
                        }
                    }
                }
                "response_item" => {
                    if let Ok(item) = serde_json::from_value::<ResponseItemPayload>(raw.payload) {
                        if item.item_type == "message" {
                            if let Some(ref role_str) = item.role {
                                let role = match role_str.as_str() {
                                    "assistant" => Role::Assistant,
                                    "developer" => continue, // skip system/developer messages
                                    _ => Role::User,
                                };

                                let content = item
                                    .content
                                    .as_ref()
                                    .map(|items| {
                                        items
                                            .iter()
                                            .filter_map(|c| c.text.as_deref())
                                            .collect::<Vec<_>>()
                                            .join("\n")
                                    })
                                    .unwrap_or_default();

                                if content.is_empty() {
                                    continue;
                                }

                                let timestamp = DateTime::parse_from_rfc3339(&raw.timestamp)
                                    .map(|t| t.with_timezone(&Utc))
                                    .unwrap_or_else(|_| Utc::now());

                                let tool_calls: Vec<String> = item
                                    .content
                                    .as_ref()
                                    .map(|items| {
                                        items
                                            .iter()
                                            .filter(|c| c.content_type == "tool_call")
                                            .filter_map(|c| c.text.clone())
                                            .collect()
                                    })
                                    .unwrap_or_default();

                                messages.push(Message {
                                    id: Uuid::new_v4(),
                                    session_id,
                                    role,
                                    content,
                                    usage: TokenUsage::default(),
                                    model: model.clone(),
                                    tool_calls,
                                    timestamp,
                                });
                            }
                        }
                    }
                }
                "event_msg" => {
                    if let Ok(evt) = serde_json::from_value::<EventMsgPayload>(raw.payload) {
                        if evt.event_type == "token_count" {
                            if let Some(info) = evt.info {
                                if let Some(usage) = info.total_token_usage {
                                    last_total_usage = Some(usage);
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        // Build session
        let meta = session_meta;
        let cwd = meta
            .as_ref()
            .and_then(|m| m.cwd.clone())
            .unwrap_or_default();
        let project = cwd.clone();
        let model_name = model.unwrap_or_default();

        let started_at = meta
            .as_ref()
            .and_then(|m| m.timestamp.as_ref())
            .and_then(|t| DateTime::parse_from_rfc3339(t).ok())
            .map(|t| t.with_timezone(&Utc))
            .unwrap_or_else(Utc::now);

        let mut session = Session::new(session_id, project, Provider::Codex, model_name, cwd);
        session.started_at = started_at;

        // Apply cumulative token usage from last token_count event
        if let Some(total) = last_total_usage {
            session.usage = TokenUsage {
                input_tokens: total.input_tokens.unwrap_or(0),
                output_tokens: total.output_tokens.unwrap_or(0)
                    + total.reasoning_output_tokens.unwrap_or(0),
                cache_read_tokens: total.cached_input_tokens.unwrap_or(0),
                cache_creation_tokens: 0, // Codex doesn't report cache writes
            };
        }

        // Count messages
        for msg in &messages {
            session.message_count += 1;
            if msg.role == Role::Assistant {
                session.turn_count += 1;
            }
        }

        if let Some(last) = messages.last() {
            session.ended_at = Some(last.timestamp);
        }

        session.estimated_cost_usd = session.usage.estimated_cost_usd(&session.model);

        Ok((session, messages))
    }
}

impl SessionParser for CodexParser {
    fn parse_file(&self, path: &Path) -> Result<Session, ParseError> {
        let (session, _) = self.parse_session(path)?;
        Ok(session)
    }

    fn discover_sessions(&self, base_dir: &Path) -> Result<Vec<PathBuf>, ParseError> {
        let sessions_dir = base_dir.join("sessions");
        let mut results = Vec::new();

        if !sessions_dir.is_dir() {
            return Ok(results);
        }

        collect_jsonl_recursive(&sessions_dir, &mut results)?;
        results.sort();
        Ok(results)
    }
}

fn collect_jsonl_recursive(dir: &Path, results: &mut Vec<PathBuf>) -> Result<(), ParseError> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_jsonl_recursive(&path, results)?;
        } else if path.extension().is_some_and(|e| e == "jsonl") {
            results.push(path);
        }
    }
    Ok(())
}

fn extract_uuid_from_filename(path: &Path) -> Uuid {
    // Filename: rollout-2026-02-27T20-59-06-019c9f2e-7139-7373-89f4-84a04c366ed5.jsonl
    // The UUID is the last 36 chars before .jsonl
    path.file_stem()
        .and_then(|s| s.to_str())
        .and_then(|s| {
            if s.len() >= 36 {
                Uuid::parse_str(&s[s.len() - 36..]).ok()
            } else {
                None
            }
        })
        .unwrap_or_else(|| Uuid::new_v5(&Uuid::NAMESPACE_OID, path.to_string_lossy().as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn write_lines(path: &Path, lines: &[&str]) {
        let mut f = std::fs::File::create(path).unwrap();
        for line in lines {
            writeln!(f, "{}", line).unwrap();
        }
    }

    #[test]
    fn parse_empty_session() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("rollout-2026-01-01T00-00-00-00000000-0000-0000-0000-000000000001.jsonl");
        write_lines(&file, &[
            r#"{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"00000000-0000-0000-0000-000000000001","cwd":"/tmp/test","model_provider":"openai"}}"#,
        ]);

        let parser = CodexParser::new();
        let (session, messages) = parser.parse_session(&file).unwrap();

        assert_eq!(session.provider, Provider::Codex);
        assert_eq!(session.cwd, "/tmp/test");
        assert_eq!(session.message_count, 0);
        assert!(messages.is_empty());
    }

    #[test]
    fn parse_session_with_messages_and_tokens() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("rollout-2026-01-01T00-00-00-00000000-0000-0000-0000-000000000002.jsonl");
        write_lines(&file, &[
            r#"{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"00000000-0000-0000-0000-000000000002","timestamp":"2026-01-01T00:00:00Z","cwd":"/home/user/project","model_provider":"openai"}}"#,
            r#"{"timestamp":"2026-01-01T00:00:01Z","type":"turn_context","payload":{"turn_id":"turn1","model":"gpt-5.3-codex","cwd":"/home/user/project","effort":"medium"}}"#,
            r#"{"timestamp":"2026-01-01T00:00:02Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"Hello"}]}}"#,
            r#"{"timestamp":"2026-01-01T00:00:03Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Hi there!"}]}}"#,
            r#"{"timestamp":"2026-01-01T00:00:04Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":5000,"cached_input_tokens":2000,"output_tokens":300,"reasoning_output_tokens":50,"total_tokens":5300}},"rate_limits":null}}"#,
        ]);

        let parser = CodexParser::new();
        let (session, messages) = parser.parse_session(&file).unwrap();

        assert_eq!(session.provider, Provider::Codex);
        assert_eq!(session.model, "gpt-5.3-codex");
        assert_eq!(session.message_count, 2);
        assert_eq!(session.turn_count, 1);
        assert_eq!(session.usage.input_tokens, 5000);
        assert_eq!(session.usage.output_tokens, 350); // 300 + 50 reasoning
        assert_eq!(session.usage.cache_read_tokens, 2000);
        assert!(session.estimated_cost_usd > 0.0);

        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].role, Role::User);
        assert_eq!(messages[0].content, "Hello");
        assert_eq!(messages[1].role, Role::Assistant);
        assert_eq!(messages[1].content, "Hi there!");
        assert_eq!(messages[1].model.as_deref(), Some("gpt-5.3-codex"));
    }

    #[test]
    fn skips_developer_messages() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("rollout-2026-01-01T00-00-00-00000000-0000-0000-0000-000000000003.jsonl");
        write_lines(&file, &[
            r#"{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"test","cwd":"/tmp"}}"#,
            r#"{"timestamp":"2026-01-01T00:00:01Z","type":"response_item","payload":{"type":"message","role":"developer","content":[{"type":"input_text","text":"system prompt"}]}}"#,
            r#"{"timestamp":"2026-01-01T00:00:02Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}}"#,
        ]);

        let parser = CodexParser::new();
        let (session, messages) = parser.parse_session(&file).unwrap();

        assert_eq!(session.message_count, 1);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].role, Role::User);
    }

    #[test]
    fn skips_reasoning_records() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("rollout-2026-01-01T00-00-00-00000000-0000-0000-0000-000000000004.jsonl");
        write_lines(&file, &[
            r#"{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"test","cwd":"/tmp"}}"#,
            r#"{"timestamp":"2026-01-01T00:00:01Z","type":"response_item","payload":{"type":"reasoning","summary":[],"content":null}}"#,
            r#"{"timestamp":"2026-01-01T00:00:02Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}}"#,
        ]);

        let parser = CodexParser::new();
        let (_, messages) = parser.parse_session(&file).unwrap();

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].content, "answer");
    }

    #[test]
    fn discover_sessions_recursive() {
        let dir = TempDir::new().unwrap();
        let sessions_dir = dir.path().join("sessions/2026/01/01");
        std::fs::create_dir_all(&sessions_dir).unwrap();

        write_lines(
            &sessions_dir.join("rollout-2026-01-01T00-00-00-00000000-0000-0000-0000-000000000001.jsonl"),
            &[r#"{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"test"}}"#],
        );
        write_lines(
            &sessions_dir.join("rollout-2026-01-01T01-00-00-00000000-0000-0000-0000-000000000002.jsonl"),
            &[r#"{"timestamp":"2026-01-01T01:00:00Z","type":"session_meta","payload":{"id":"test2"}}"#],
        );

        let parser = CodexParser::new();
        let found = parser.discover_sessions(dir.path()).unwrap();
        assert_eq!(found.len(), 2);
    }

    #[test]
    fn extract_uuid_from_rollout_filename() {
        let path = Path::new("/home/.codex/sessions/2026/02/27/rollout-2026-02-27T20-59-06-019c9f2e-7139-7373-89f4-84a04c366ed5.jsonl");
        let id = extract_uuid_from_filename(path);
        assert_eq!(
            id.to_string(),
            "019c9f2e-7139-7373-89f4-84a04c366ed5"
        );
    }

    #[test]
    fn uses_last_token_count() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("rollout-2026-01-01T00-00-00-00000000-0000-0000-0000-000000000005.jsonl");
        write_lines(&file, &[
            r#"{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"test","cwd":"/tmp"}}"#,
            r#"{"timestamp":"2026-01-01T00:00:01Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":100,"cached_input_tokens":0,"output_tokens":50,"reasoning_output_tokens":0}},"rate_limits":null}}"#,
            r#"{"timestamp":"2026-01-01T00:00:02Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"more"}]}}"#,
            r#"{"timestamp":"2026-01-01T00:00:03Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":500,"cached_input_tokens":200,"output_tokens":150,"reasoning_output_tokens":30}},"rate_limits":null}}"#,
        ]);

        let parser = CodexParser::new();
        let (session, _) = parser.parse_session(&file).unwrap();

        // Should use the LAST token_count (cumulative)
        assert_eq!(session.usage.input_tokens, 500);
        assert_eq!(session.usage.output_tokens, 180); // 150 + 30
        assert_eq!(session.usage.cache_read_tokens, 200);
    }

    #[test]
    fn token_count_with_null_info_skipped() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("rollout-2026-01-01T00-00-00-00000000-0000-0000-0000-000000000006.jsonl");
        write_lines(&file, &[
            r#"{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"test","cwd":"/tmp"}}"#,
            r#"{"timestamp":"2026-01-01T00:00:01Z","type":"event_msg","payload":{"type":"token_count","info":null,"rate_limits":{"limit_id":"codex"}}}"#,
        ]);

        let parser = CodexParser::new();
        let (session, _) = parser.parse_session(&file).unwrap();
        assert_eq!(session.usage.input_tokens, 0);
    }

    #[test]
    fn real_session_integration() {
        let home = dirs_next::home_dir().unwrap();
        let codex_dir = home.join(".codex");
        if !codex_dir.is_dir() {
            println!("Skipping: no ~/.codex directory");
            return;
        }

        let parser = CodexParser::new();
        let sessions = parser.discover_sessions(&codex_dir).unwrap();
        println!("Found {} Codex session(s)", sessions.len());

        for path in sessions.iter().take(3) {
            match parser.parse_session(path) {
                Ok((session, msgs)) => {
                    println!(
                        "  {} | model={} | msgs={} | tokens={} | cost=${:.4}",
                        &session.id.to_string()[..8],
                        session.model,
                        msgs.len(),
                        session.usage.total(),
                        session.estimated_cost_usd,
                    );
                }
                Err(e) => {
                    eprintln!("  Error parsing {}: {e}", path.display());
                }
            }
        }
    }
}
