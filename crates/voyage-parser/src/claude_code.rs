use std::io::BufRead;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::Deserialize;
use uuid::Uuid;

use voyage_core::model::{Message, Provider, Role, Session, TokenUsage};

use crate::traits::SessionParser;

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON parse error at line {line}: {source}")]
    Json {
        line: usize,
        source: serde_json::Error,
    },
    #[error("Invalid session path: {0}")]
    InvalidPath(String),
}

/// Raw JSONL record as stored by Claude Code
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum RawRecord {
    #[serde(rename = "user")]
    User {
        uuid: Uuid,
        #[serde(rename = "sessionId")]
        session_id: Uuid,
        message: RawUserMessage,
        timestamp: DateTime<Utc>,
        cwd: Option<String>,
        #[serde(rename = "gitBranch")]
        git_branch: Option<String>,
    },
    #[serde(rename = "assistant")]
    Assistant {
        uuid: Uuid,
        #[serde(rename = "sessionId")]
        session_id: Uuid,
        message: RawAssistantMessage,
        timestamp: DateTime<Utc>,
    },
    #[serde(rename = "system")]
    System {
        #[allow(dead_code)]
        subtype: Option<String>,
        #[serde(rename = "durationMs")]
        #[allow(dead_code)]
        duration_ms: Option<u64>,
        #[allow(dead_code)]
        timestamp: DateTime<Utc>,
    },
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
struct RawUserMessage {
    content: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct RawAssistantMessage {
    model: Option<String>,
    content: Option<serde_json::Value>,
    usage: Option<RawUsage>,
}

#[derive(Debug, Deserialize)]
struct RawUsage {
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    cache_read_input_tokens: Option<u64>,
    cache_creation_input_tokens: Option<u64>,
}

pub struct ClaudeCodeParser;

impl Default for ClaudeCodeParser {
    fn default() -> Self {
        Self::new()
    }
}

impl ClaudeCodeParser {
    pub fn new() -> Self {
        Self
    }

    /// Parse a JSONL session file into a Session with Messages
    pub fn parse_session(&self, path: &Path) -> Result<(Session, Vec<Message>), ParseError> {
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);

        let session_id = extract_session_id(path)?;
        let project = extract_project_name(path);

        let mut session = Session::new(
            session_id,
            project,
            Provider::ClaudeCode,
            String::new(),
            String::new(),
        );
        let mut messages = Vec::new();
        let mut first_timestamp: Option<DateTime<Utc>> = None;

        for (line_num, line) in reader.lines().enumerate() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }

            let record: RawRecord = serde_json::from_str(&line).map_err(|e| ParseError::Json {
                line: line_num + 1,
                source: e,
            })?;

            match record {
                RawRecord::User {
                    uuid,
                    session_id: sid,
                    message: raw_msg,
                    timestamp,
                    cwd,
                    git_branch,
                    ..
                } => {
                    // Claude occasionally appends interrupted or sidechain records from a
                    // different session into the same JSONL file. Skip them instead of
                    // poisoning the parsed session or tripping DB foreign keys.
                    if sid != session_id {
                        continue;
                    }
                    if first_timestamp.is_none() {
                        first_timestamp = Some(timestamp);
                        session.started_at = timestamp;
                    }
                    if session.cwd.is_empty()
                        && let Some(ref c) = cwd
                    {
                        session.cwd = c.clone();
                    }
                    if session.git_branch.is_none() {
                        session.git_branch = git_branch;
                    }

                    let content = extract_text_content(&raw_msg.content);
                    let msg = Message {
                        id: uuid,
                        session_id,
                        role: Role::User,
                        content,
                        usage: TokenUsage::default(),
                        model: None,
                        tool_calls: vec![],
                        timestamp,
                    };
                    session.add_message(&msg);
                    messages.push(msg);
                }
                RawRecord::Assistant {
                    uuid,
                    session_id: sid,
                    message: raw_msg,
                    timestamp,
                    ..
                } => {
                    if sid != session_id {
                        continue;
                    }
                    let usage = raw_msg
                        .usage
                        .map(|u| TokenUsage {
                            input_tokens: u.input_tokens.unwrap_or(0),
                            output_tokens: u.output_tokens.unwrap_or(0),
                            cache_read_tokens: u.cache_read_input_tokens.unwrap_or(0),
                            cache_creation_tokens: u.cache_creation_input_tokens.unwrap_or(0),
                        })
                        .unwrap_or_default();

                    let content = raw_msg
                        .content
                        .as_ref()
                        .map(extract_assistant_content)
                        .unwrap_or_default();
                    let tool_calls = raw_msg
                        .content
                        .as_ref()
                        .map(extract_tool_calls)
                        .unwrap_or_default();

                    let msg = Message {
                        id: uuid,
                        session_id,
                        role: Role::Assistant,
                        content,
                        usage,
                        model: raw_msg.model.clone(),
                        tool_calls,
                        timestamp,
                    };
                    session.add_message(&msg);
                    messages.push(msg);
                }
                _ => {}
            }
        }

        Ok((session, messages))
    }
}

impl SessionParser for ClaudeCodeParser {
    fn parse_file(&self, path: &Path) -> Result<Session, ParseError> {
        let (session, _) = self.parse_session(path)?;
        Ok(session)
    }

    fn discover_sessions(&self, base_dir: &Path) -> Result<Vec<PathBuf>, ParseError> {
        let mut sessions = Vec::new();
        if !base_dir.is_dir() {
            return Ok(sessions);
        }
        for project_entry in std::fs::read_dir(base_dir)? {
            let project_entry = project_entry?;
            if !project_entry.path().is_dir() {
                continue;
            }
            for entry in std::fs::read_dir(project_entry.path())? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().is_some_and(|e| e == "jsonl") {
                    sessions.push(path);
                }
            }
        }
        sessions.sort();
        Ok(sessions)
    }
}

fn extract_session_id(path: &Path) -> Result<Uuid, ParseError> {
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| ParseError::InvalidPath(path.display().to_string()))?;
    Uuid::parse_str(stem)
        .map_err(|_| ParseError::InvalidPath(format!("Invalid UUID in filename: {stem}")))
}

fn extract_project_name(path: &Path) -> String {
    path.parent()
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .map(|s| s.replace('-', "/"))
        .unwrap_or_else(|| "unknown".into())
}

fn extract_text_content(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(arr) => arr
            .iter()
            .filter_map(|item| {
                if item.get("type")?.as_str()? == "text" {
                    item.get("text")?.as_str().map(String::from)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

fn extract_assistant_content(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Array(arr) => arr
            .iter()
            .filter_map(|item| {
                let t = item.get("type")?.as_str()?;
                match t {
                    "text" => item.get("text")?.as_str().map(String::from),
                    _ => None,
                }
            })
            .collect::<Vec<_>>()
            .join("\n"),
        serde_json::Value::String(s) => s.clone(),
        _ => String::new(),
    }
}

fn extract_tool_calls(value: &serde_json::Value) -> Vec<String> {
    match value {
        serde_json::Value::Array(arr) => arr
            .iter()
            .filter_map(|item| {
                if item.get("type")?.as_str()? == "tool_use" {
                    item.get("name")?.as_str().map(String::from)
                } else {
                    None
                }
            })
            .collect(),
        _ => vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn make_session_jsonl(lines: &[&str]) -> (TempDir, PathBuf) {
        let dir = TempDir::new().unwrap();
        let project_dir = dir.path().join("-Users-test-project");
        std::fs::create_dir_all(&project_dir).unwrap();
        let session_id = "9550f7c1-2907-414c-8527-eb992e7af55d";
        let path = project_dir.join(format!("{session_id}.jsonl"));
        let mut f = std::fs::File::create(&path).unwrap();
        for line in lines {
            writeln!(f, "{line}").unwrap();
        }
        (dir, path)
    }

    #[test]
    fn parse_empty_file() {
        let (_dir, path) = make_session_jsonl(&[]);
        let parser = ClaudeCodeParser::new();
        let (session, messages) = parser.parse_session(&path).unwrap();
        assert_eq!(session.message_count, 0);
        assert!(messages.is_empty());
    }

    #[test]
    fn parse_user_message() {
        let line = r#"{"parentUuid":null,"isSidechain":false,"type":"user","message":{"role":"user","content":"hello world"},"uuid":"a989fd6e-cc80-4861-a21a-9a96dc1eb1e6","timestamp":"2026-03-12T13:17:35.480Z","userType":"external","cwd":"/Users/test/project","sessionId":"9550f7c1-2907-414c-8527-eb992e7af55d","version":"2.1.74","gitBranch":"main"}"#;
        let (_dir, path) = make_session_jsonl(&[line]);
        let parser = ClaudeCodeParser::new();
        let (session, messages) = parser.parse_session(&path).unwrap();

        assert_eq!(session.message_count, 1);
        assert_eq!(session.turn_count, 0);
        assert_eq!(session.cwd, "/Users/test/project");
        assert_eq!(session.git_branch.as_deref(), Some("main"));
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].role, Role::User);
        assert_eq!(messages[0].content, "hello world");
    }

    #[test]
    fn parse_assistant_message_with_usage() {
        let line = r#"{"parentUuid":"a989fd6e-cc80-4861-a21a-9a96dc1eb1e6","isSidechain":false,"message":{"model":"claude-opus-4-6","id":"msg_01TPZ","type":"message","role":"assistant","content":[{"type":"text","text":"Hi there!"}],"usage":{"input_tokens":100,"output_tokens":200,"cache_read_input_tokens":50,"cache_creation_input_tokens":30}},"type":"assistant","uuid":"1b1eebf7-df53-45b1-922b-5b4cef31f63e","timestamp":"2026-03-12T13:17:39.302Z","userType":"external","cwd":"/Users/test/project","sessionId":"9550f7c1-2907-414c-8527-eb992e7af55d","version":"2.1.74"}"#;
        let (_dir, path) = make_session_jsonl(&[line]);
        let parser = ClaudeCodeParser::new();
        let (session, messages) = parser.parse_session(&path).unwrap();

        assert_eq!(session.message_count, 1);
        assert_eq!(session.turn_count, 1);
        assert_eq!(session.model, "claude-opus-4-6");
        assert_eq!(session.usage.input_tokens, 100);
        assert_eq!(session.usage.output_tokens, 200);
        assert_eq!(session.usage.cache_read_tokens, 50);
        assert_eq!(session.usage.cache_creation_tokens, 30);
        assert!(session.estimated_cost_usd > 0.0);
        assert_eq!(messages[0].content, "Hi there!");
    }

    #[test]
    fn parse_assistant_with_tool_calls() {
        let line = r#"{"parentUuid":"abc","isSidechain":false,"message":{"model":"claude-opus-4-6","id":"msg_02","type":"message","role":"assistant","content":[{"type":"text","text":"Let me check."},{"type":"tool_use","id":"toolu_01","name":"Read","input":{"file_path":"/tmp/test.rs"}}],"usage":{"input_tokens":50,"output_tokens":100}},"type":"assistant","uuid":"2b1eebf7-df53-45b1-922b-5b4cef31f63e","timestamp":"2026-03-12T13:18:00.000Z","userType":"external","cwd":"/tmp","sessionId":"9550f7c1-2907-414c-8527-eb992e7af55d","version":"2.1.74"}"#;
        let (_dir, path) = make_session_jsonl(&[line]);
        let parser = ClaudeCodeParser::new();
        let (_, messages) = parser.parse_session(&path).unwrap();

        assert_eq!(messages[0].tool_calls, vec!["Read"]);
        assert_eq!(messages[0].content, "Let me check.");
    }

    #[test]
    fn parse_skips_unknown_types() {
        let lines = &[
            r#"{"type":"file-history-snapshot","messageId":"abc","snapshot":{"messageId":"abc","trackedFileBackups":{},"timestamp":"2026-03-12T13:17:35.480Z"},"isSnapshotUpdate":false}"#,
            r#"{"parentUuid":"abc","isSidechain":false,"type":"progress","data":{"type":"hook_progress"},"toolUseID":"toolu_01","timestamp":"2026-03-12T13:17:44.604Z","uuid":"cc34f265","userType":"external","cwd":"/tmp","sessionId":"9550f7c1-2907-414c-8527-eb992e7af55d","version":"2.1.74"}"#,
        ];
        let (_dir, path) = make_session_jsonl(lines);
        let parser = ClaudeCodeParser::new();
        let (session, messages) = parser.parse_session(&path).unwrap();

        assert_eq!(session.message_count, 0);
        assert!(messages.is_empty());
    }

    #[test]
    fn parse_skips_records_from_other_session() {
        let foreign_user = r#"{"parentUuid":null,"isSidechain":false,"type":"user","message":{"role":"user","content":"ignore me"},"uuid":"a989fd6e-cc80-4861-a21a-9a96dc1eb1e6","timestamp":"2026-03-12T13:17:35.480Z","userType":"external","cwd":"/Users/test","sessionId":"11111111-1111-1111-1111-111111111111","version":"2.1.74","gitBranch":"main"}"#;
        let local_user = r#"{"parentUuid":"a989fd6e-cc80-4861-a21a-9a96dc1eb1e6","isSidechain":false,"type":"user","message":{"role":"user","content":"keep me"},"uuid":"b989fd6e-cc80-4861-a21a-9a96dc1eb1e7","timestamp":"2026-03-12T13:18:00.000Z","userType":"external","cwd":"/Users/test","sessionId":"9550f7c1-2907-414c-8527-eb992e7af55d","version":"2.1.74","gitBranch":"main"}"#;

        let (_dir, path) = make_session_jsonl(&[foreign_user, local_user]);
        let parser = ClaudeCodeParser::new();
        let (session, messages) = parser.parse_session(&path).unwrap();

        assert_eq!(session.message_count, 1);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].content, "keep me");
        assert_eq!(
            messages[0].session_id.to_string(),
            "9550f7c1-2907-414c-8527-eb992e7af55d"
        );
        assert_eq!(session.started_at, messages[0].timestamp);
    }

    #[test]
    fn parse_multi_message_session() {
        let user_line = r#"{"parentUuid":null,"isSidechain":false,"type":"user","message":{"role":"user","content":"hello"},"uuid":"a989fd6e-cc80-4861-a21a-9a96dc1eb1e6","timestamp":"2026-03-12T13:17:35.480Z","userType":"external","cwd":"/Users/test","sessionId":"9550f7c1-2907-414c-8527-eb992e7af55d","version":"2.1.74","gitBranch":"main"}"#;
        let assistant_line = r#"{"parentUuid":"a989fd6e-cc80-4861-a21a-9a96dc1eb1e6","isSidechain":false,"message":{"model":"claude-opus-4-6","id":"msg_01","type":"message","role":"assistant","content":[{"type":"text","text":"hi"}],"usage":{"input_tokens":100,"output_tokens":50,"cache_read_input_tokens":20,"cache_creation_input_tokens":10}},"type":"assistant","uuid":"1b1eebf7-df53-45b1-922b-5b4cef31f63e","timestamp":"2026-03-12T13:17:39.302Z","userType":"external","cwd":"/Users/test","sessionId":"9550f7c1-2907-414c-8527-eb992e7af55d","version":"2.1.74"}"#;
        let user2_line = r#"{"parentUuid":"1b1eebf7-df53-45b1-922b-5b4cef31f63e","isSidechain":false,"type":"user","message":{"role":"user","content":"thanks"},"uuid":"b989fd6e-cc80-4861-a21a-9a96dc1eb1e7","timestamp":"2026-03-12T13:18:00.000Z","userType":"external","cwd":"/Users/test","sessionId":"9550f7c1-2907-414c-8527-eb992e7af55d","version":"2.1.74","gitBranch":"main"}"#;

        let (_dir, path) = make_session_jsonl(&[user_line, assistant_line, user2_line]);
        let parser = ClaudeCodeParser::new();
        let (session, messages) = parser.parse_session(&path).unwrap();

        assert_eq!(session.message_count, 3);
        assert_eq!(session.turn_count, 1);
        assert_eq!(messages.len(), 3);
        assert_eq!(session.usage.input_tokens, 100);
        assert_eq!(session.usage.output_tokens, 50);
    }

    #[test]
    fn discover_sessions_finds_jsonl_files() {
        let dir = TempDir::new().unwrap();
        let project_dir = dir.path().join("-Users-test-myproject");
        std::fs::create_dir_all(&project_dir).unwrap();

        // Create session files
        std::fs::File::create(project_dir.join("aaaa-bbbb-cccc.jsonl")).unwrap();
        std::fs::File::create(project_dir.join("dddd-eeee-ffff.jsonl")).unwrap();
        // Create subdirectory (should be ignored)
        std::fs::create_dir_all(project_dir.join("aaaa-bbbb-cccc")).unwrap();

        let parser = ClaudeCodeParser::new();
        let sessions = parser.discover_sessions(dir.path()).unwrap();
        assert_eq!(sessions.len(), 2);
    }

    #[test]
    fn discover_sessions_empty_dir() {
        let dir = TempDir::new().unwrap();
        let parser = ClaudeCodeParser::new();
        let sessions = parser.discover_sessions(dir.path()).unwrap();
        assert!(sessions.is_empty());
    }

    #[test]
    fn extract_session_id_from_path() {
        let path =
            PathBuf::from("/tmp/projects/-Users-test/9550f7c1-2907-414c-8527-eb992e7af55d.jsonl");
        let id = extract_session_id(&path).unwrap();
        assert_eq!(id.to_string(), "9550f7c1-2907-414c-8527-eb992e7af55d");
    }

    #[test]
    fn extract_project_name_from_path() {
        let path = PathBuf::from("/tmp/projects/-Users-vinci-lab-Voyage/session.jsonl");
        let name = extract_project_name(&path);
        assert_eq!(name, "/Users/vinci/lab/Voyage");
    }

    #[test]
    fn parse_real_session_file() {
        let real_path = dirs_next::home_dir().unwrap().join(
            ".claude/projects/-Users-vinci-lab-Voyage/9550f7c1-2907-414c-8527-eb992e7af55d.jsonl",
        );
        if !real_path.exists() {
            // Skip if not on the dev machine
            return;
        }
        let parser = ClaudeCodeParser::new();
        let (session, messages) = parser.parse_session(&real_path).unwrap();

        assert!(session.message_count > 0);
        assert!(!messages.is_empty());
        assert_eq!(session.provider, Provider::ClaudeCode);
        assert!(session.usage.total() > 0);
        println!(
            "Real session: {} messages, {} turns, {:.4} USD",
            session.message_count, session.turn_count, session.estimated_cost_usd
        );
    }
}
