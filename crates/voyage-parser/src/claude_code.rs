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
    id: Option<String>,
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

    /// Parse a JSONL session file into a Session with Messages.
    ///
    /// Streaming assistant chunks that share the same API `message.id` are merged
    /// into a single Message with combined content and summed usage.
    pub fn parse_session(&self, path: &Path) -> Result<(Session, Vec<Message>), ParseError> {
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);

        let session_id = extract_session_id(path)?;
        let project = extract_project_name(path);

        let mut messages = Vec::new();
        let mut first_timestamp: Option<DateTime<Utc>> = None;
        let mut cwd = String::new();
        let mut git_branch: Option<String> = None;

        // Track assistant messages by API message.id for merge dedup
        let mut assistant_index: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();

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
                    cwd: record_cwd,
                    git_branch: record_branch,
                    ..
                } => {
                    if sid != session_id {
                        continue;
                    }
                    if first_timestamp.is_none() {
                        first_timestamp = Some(timestamp);
                    }
                    if cwd.is_empty()
                        && let Some(ref c) = record_cwd
                    {
                        cwd = c.clone();
                    }
                    if git_branch.is_none() {
                        git_branch = record_branch;
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

                    // Merge streaming chunks: if this API message.id was already seen,
                    // merge content/usage/tool_calls into the existing entry.
                    if let Some(api_id) = &raw_msg.id {
                        if let Some(&idx) = assistant_index.get(api_id) {
                            let existing = &mut messages[idx];
                            if !content.is_empty() {
                                if existing.content.is_empty() {
                                    existing.content = content;
                                } else {
                                    existing.content.push('\n');
                                    existing.content.push_str(&content);
                                }
                            }
                            existing.usage += usage;
                            for tc in tool_calls {
                                if !existing.tool_calls.contains(&tc) {
                                    existing.tool_calls.push(tc);
                                }
                            }
                            if existing.timestamp < timestamp {
                                existing.timestamp = timestamp;
                            }
                            continue;
                        }
                        assistant_index.insert(api_id.clone(), messages.len());
                    }

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
                    messages.push(msg);
                }
                _ => {}
            }
        }

        // Build session stats from the final message list
        let mut session = Session::new(
            session_id,
            project,
            Provider::ClaudeCode,
            String::new(),
            cwd,
        );
        session.git_branch = git_branch;
        if let Some(ts) = first_timestamp {
            session.started_at = ts;
        }
        for msg in &messages {
            session.add_message(msg);
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
                } else if path.is_dir() {
                    // Check for subagents/ subdirectory
                    let subagents_dir = path.join("subagents");
                    if subagents_dir.is_dir() {
                        for sub_entry in std::fs::read_dir(&subagents_dir)? {
                            let sub_entry = sub_entry?;
                            let sub_path = sub_entry.path();
                            if sub_path.extension().is_some_and(|e| e == "jsonl") {
                                sessions.push(sub_path);
                            }
                        }
                    }
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
    // Try filename stem as UUID first (normal sessions: {uuid}.jsonl)
    if let Ok(id) = Uuid::parse_str(stem) {
        return Ok(id);
    }
    // Fallback: grandparent dir name as UUID (subagent files: {uuid}/subagents/agent-xxx.jsonl)
    if let Some(grandparent) = path.parent().and_then(|p| p.parent())
        && let Some(name) = grandparent.file_name().and_then(|n| n.to_str())
        && let Ok(id) = Uuid::parse_str(name)
    {
        return Ok(id);
    }
    Err(ParseError::InvalidPath(format!(
        "Cannot extract UUID from path: {}",
        path.display()
    )))
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
                let t = item.get("type")?.as_str()?;
                match t {
                    "text" => item.get("text")?.as_str().map(String::from),
                    "tool_result" => {
                        // tool_result content can be a string or a nested array of text blocks
                        let content = item.get("content")?;
                        match content {
                            serde_json::Value::String(s) => Some(s.clone()),
                            serde_json::Value::Array(parts) => {
                                let texts: Vec<String> = parts
                                    .iter()
                                    .filter_map(|p| {
                                        if p.get("type")?.as_str()? == "text" {
                                            p.get("text")?.as_str().map(String::from)
                                        } else {
                                            None
                                        }
                                    })
                                    .collect();
                                if texts.is_empty() {
                                    None
                                } else {
                                    Some(texts.join("\n"))
                                }
                            }
                            _ => None,
                        }
                    }
                    _ => None,
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
                    "thinking" => item.get("thinking")?.as_str().map(String::from),
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
    fn discover_sessions_includes_subagent_files() {
        let dir = TempDir::new().unwrap();
        let project_dir = dir.path().join("-Users-test-myproject");
        std::fs::create_dir_all(&project_dir).unwrap();

        let session_uuid = "9550f7c1-2907-414c-8527-eb992e7af55d";

        // Main session file
        std::fs::File::create(project_dir.join(format!("{session_uuid}.jsonl"))).unwrap();

        // Subagent file
        let subagents_dir = project_dir.join(session_uuid).join("subagents");
        std::fs::create_dir_all(&subagents_dir).unwrap();
        std::fs::File::create(subagents_dir.join("agent-xxx.jsonl")).unwrap();

        let parser = ClaudeCodeParser::new();
        let sessions = parser.discover_sessions(dir.path()).unwrap();
        assert_eq!(sessions.len(), 2);
    }

    #[test]
    fn extract_session_id_from_subagent_path() {
        let uuid = "9550f7c1-2907-414c-8527-eb992e7af55d";
        let path = PathBuf::from(format!(
            "/tmp/projects/-Users-test/{uuid}/subagents/agent-xxx.jsonl"
        ));
        let id = extract_session_id(&path).unwrap();
        assert_eq!(id.to_string(), uuid);
    }

    #[test]
    fn merge_streaming_assistant_chunks() {
        // Two assistant lines with different uuid but same message.id
        let api_msg_id = "msg_01TPZ";
        let user_line = r#"{"parentUuid":null,"isSidechain":false,"type":"user","message":{"role":"user","content":"hello"},"uuid":"a989fd6e-cc80-4861-a21a-9a96dc1eb1e6","timestamp":"2026-03-12T13:17:35.480Z","userType":"external","cwd":"/Users/test","sessionId":"9550f7c1-2907-414c-8527-eb992e7af55d","version":"2.1.74","gitBranch":"main"}"#;
        let assistant1 = format!(
            r#"{{"parentUuid":"a989fd6e","isSidechain":false,"message":{{"model":"claude-opus-4-6","id":"{api_msg_id}","type":"message","role":"assistant","content":[{{"type":"text","text":"Part 1"}}],"usage":{{"input_tokens":100,"output_tokens":50}}}},"type":"assistant","uuid":"1b1eebf7-df53-45b1-922b-5b4cef31f63e","timestamp":"2026-03-12T13:17:39.000Z","userType":"external","cwd":"/tmp","sessionId":"9550f7c1-2907-414c-8527-eb992e7af55d","version":"2.1.74"}}"#
        );
        let assistant2 = format!(
            r#"{{"parentUuid":"a989fd6e","isSidechain":false,"message":{{"model":"claude-opus-4-6","id":"{api_msg_id}","type":"message","role":"assistant","content":[{{"type":"text","text":"Part 2"}},{{"type":"tool_use","id":"toolu_01","name":"Read","input":{{}}}}],"usage":{{"input_tokens":0,"output_tokens":80}}}},"type":"assistant","uuid":"2b1eebf7-df53-45b1-922b-5b4cef31f63e","timestamp":"2026-03-12T13:17:40.000Z","userType":"external","cwd":"/tmp","sessionId":"9550f7c1-2907-414c-8527-eb992e7af55d","version":"2.1.74"}}"#
        );

        let (_dir, path) = make_session_jsonl(&[user_line, &assistant1, &assistant2]);
        let parser = ClaudeCodeParser::new();
        let (session, messages) = parser.parse_session(&path).unwrap();

        // Should produce 2 messages: 1 user + 1 merged assistant
        assert_eq!(messages.len(), 2);
        assert_eq!(session.message_count, 2);
        assert_eq!(session.turn_count, 1);

        let assistant = &messages[1];
        assert_eq!(assistant.content, "Part 1\nPart 2");
        assert_eq!(assistant.usage.input_tokens, 100);
        assert_eq!(assistant.usage.output_tokens, 130); // 50 + 80
        assert_eq!(assistant.tool_calls, vec!["Read"]);
    }

    #[test]
    fn extract_assistant_content_with_thinking() {
        let content = serde_json::json!([
            {"type": "thinking", "thinking": "analysis"},
            {"type": "text", "text": "answer"}
        ]);
        let result = extract_assistant_content(&content);
        assert_eq!(result, "analysis\nanswer");
    }

    #[test]
    fn extract_text_content_with_tool_result_string() {
        let content = serde_json::json!([
            {"type": "tool_result", "tool_use_id": "x", "content": "output"}
        ]);
        let result = extract_text_content(&content);
        assert_eq!(result, "output");
    }

    #[test]
    fn extract_text_content_with_tool_result_array() {
        let content = serde_json::json!([
            {"type": "tool_result", "tool_use_id": "x", "content": [
                {"type": "text", "text": "line1"},
                {"type": "text", "text": "line2"}
            ]}
        ]);
        let result = extract_text_content(&content);
        assert_eq!(result, "line1\nline2");
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
