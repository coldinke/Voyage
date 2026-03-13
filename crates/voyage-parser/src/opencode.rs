//! Parser for OpenCode (opencode.ai) local session data.
//!
//! OpenCode stores sessions as JSON files in:
//!   ~/.local/share/opencode/storage/
//!     session/{projectID}/{sessionID}.json
//!     message/{sessionID}/{messageID}.json
//!     part/{messageID}/{partID}.json

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::Deserialize;
use uuid::Uuid;

use voyage_core::model::{Message, Provider, Role, Session, TokenUsage};

use crate::claude_code::ParseError;
use crate::traits::SessionParser;

/// Raw OpenCode session JSON
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct RawSession {
    id: String,
    directory: Option<String>,
    project_id: Option<String>,
    time: Option<RawSessionTime>,
    title: Option<String>,
    version: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawSessionTime {
    created: Option<f64>,
    updated: Option<f64>,
}

/// Raw OpenCode message JSON (both user and assistant)
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct RawMessage {
    id: String,
    role: String,
    session_id: String,
    #[serde(default)]
    cost: f64,
    model_id: Option<String>,
    provider_id: Option<String>,
    time: Option<RawMessageTime>,
    tokens: Option<RawTokens>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct RawMessageTime {
    created: Option<f64>,
    updated: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct RawTokens {
    input: Option<f64>,
    output: Option<f64>,
    reasoning: Option<f64>,
    cache: Option<RawCacheTokens>,
}

#[derive(Debug, Deserialize)]
struct RawCacheTokens {
    read: Option<f64>,
    write: Option<f64>,
}

/// Raw OpenCode message part JSON
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawPart {
    #[serde(rename = "type")]
    part_type: String,
    text: Option<String>,
    #[serde(default)]
    tool: Option<String>,
    name: Option<String>,
}

pub struct OpenCodeParser;

impl OpenCodeParser {
    pub fn new() -> Self {
        Self
    }

    /// Parse a single session by its directory path.
    /// `session_file` points to `storage/session/{projectID}/{sessionID}.json`
    pub fn parse_session(
        &self,
        session_file: &Path,
        storage_root: &Path,
    ) -> Result<(Session, Vec<Message>), ParseError> {
        let raw: RawSession = read_json(session_file)?;

        let session_id = parse_id_or_generate(&raw.id);
        let started_at = raw
            .time
            .as_ref()
            .and_then(|t| t.created)
            .map(epoch_to_datetime)
            .unwrap_or_else(Utc::now);
        let ended_at = raw
            .time
            .as_ref()
            .and_then(|t| t.updated)
            .map(epoch_to_datetime);

        let project = raw.directory.clone().unwrap_or_else(|| "unknown".into());

        let mut session = Session::new(
            session_id,
            project.clone(),
            Provider::OpenCode,
            String::new(),
            project,
        );
        session.started_at = started_at;
        session.ended_at = ended_at;

        // Load messages from storage/message/{sessionID}/
        let messages_dir = storage_root.join("message").join(&raw.id);
        let mut messages = Vec::new();

        if messages_dir.is_dir() {
            let mut msg_files: Vec<PathBuf> = std::fs::read_dir(&messages_dir)?
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.extension().is_some_and(|e| e == "json"))
                .collect();
            msg_files.sort();

            for msg_file in &msg_files {
                match read_json::<RawMessage>(msg_file) {
                    Ok(raw_msg) => {
                        let msg = self.convert_message(&raw_msg, session_id, storage_root);
                        session.add_message(&msg);
                        messages.push(msg);
                    }
                    Err(e) => {
                        eprintln!(
                            "  Warning: skipping message {}: {e}",
                            msg_file.file_name().unwrap_or_default().to_string_lossy()
                        );
                    }
                }
            }
        }

        session.estimated_cost_usd = session.usage.estimated_cost_usd(&session.model);

        // Set summary from title if available
        if let Some(ref title) = raw.title {
            let title = title.trim();
            if !title.is_empty() {
                session.summary = title.to_string();
            }
        }

        Ok((session, messages))
    }

    fn convert_message(&self, raw: &RawMessage, session_id: Uuid, storage_root: &Path) -> Message {
        let role = match raw.role.as_str() {
            "assistant" => Role::Assistant,
            "system" => Role::System,
            _ => Role::User,
        };

        let usage = raw
            .tokens
            .as_ref()
            .map(|t| TokenUsage {
                input_tokens: t.input.unwrap_or(0.0) as u64,
                output_tokens: t.output.unwrap_or(0.0) as u64 + t.reasoning.unwrap_or(0.0) as u64,
                cache_read_tokens: t.cache.as_ref().and_then(|c| c.read).unwrap_or(0.0) as u64,
                cache_creation_tokens: t.cache.as_ref().and_then(|c| c.write).unwrap_or(0.0) as u64,
            })
            .unwrap_or_default();

        let timestamp = raw
            .time
            .as_ref()
            .and_then(|t| t.created)
            .map(epoch_to_datetime)
            .unwrap_or_else(Utc::now);

        // Load parts to get text content and tool calls
        let (content, tool_calls) = self.load_parts(&raw.id, storage_root);

        Message {
            id: parse_id_or_generate(&raw.id),
            session_id,
            role,
            content,
            usage,
            model: raw.model_id.clone(),
            tool_calls,
            timestamp,
        }
    }

    fn load_parts(&self, message_id: &str, storage_root: &Path) -> (String, Vec<String>) {
        let parts_dir = storage_root.join("part").join(message_id);
        let mut texts = Vec::new();
        let mut tools = Vec::new();

        if !parts_dir.is_dir() {
            return (String::new(), vec![]);
        }

        let mut part_files: Vec<PathBuf> = std::fs::read_dir(&parts_dir)
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|e| e == "json"))
            .collect();
        part_files.sort();

        for pf in &part_files {
            if let Ok(part) = read_json::<RawPart>(pf) {
                match part.part_type.as_str() {
                    "text" => {
                        if let Some(text) = part.text {
                            if !text.is_empty() {
                                texts.push(text);
                            }
                        }
                    }
                    "tool" => {
                        if let Some(name) = part.name.or(part.tool) {
                            if !tools.contains(&name) {
                                tools.push(name);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        (texts.join("\n"), tools)
    }
}

impl SessionParser for OpenCodeParser {
    fn parse_file(&self, path: &Path) -> Result<Session, ParseError> {
        // Determine storage root from session file path
        // session_file: storage/session/{projectID}/{sessionID}.json
        // storage_root: storage/
        let storage_root = path
            .parent() // {projectID}/
            .and_then(|p| p.parent()) // session/
            .and_then(|p| p.parent()) // storage/
            .ok_or_else(|| ParseError::InvalidPath(path.display().to_string()))?;

        let (session, _) = OpenCodeParser::new().parse_session(path, storage_root)?;
        Ok(session)
    }

    fn discover_sessions(&self, base_dir: &Path) -> Result<Vec<PathBuf>, ParseError> {
        let session_dir = base_dir.join("session");
        let mut sessions = Vec::new();

        if !session_dir.is_dir() {
            return Ok(sessions);
        }

        // Iterate project directories under session/
        for project_entry in std::fs::read_dir(&session_dir)? {
            let project_entry = project_entry?;
            if !project_entry.path().is_dir() {
                continue;
            }
            for entry in std::fs::read_dir(project_entry.path())? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().is_some_and(|e| e == "json") {
                    sessions.push(path);
                }
            }
        }

        sessions.sort();
        Ok(sessions)
    }
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, ParseError> {
    let content = std::fs::read_to_string(path)?;
    serde_json::from_str(&content).map_err(|e| ParseError::Json { line: 0, source: e })
}

fn epoch_to_datetime(epoch: f64) -> DateTime<Utc> {
    // Heuristic: if value > 1e12, it's milliseconds (year ~2001+)
    if epoch > 1e12 {
        let ms_secs = (epoch / 1000.0) as i64;
        let ms_nanos = (((epoch / 1000.0) - ms_secs as f64) * 1_000_000_000.0) as u32;
        return DateTime::from_timestamp(ms_secs, ms_nanos).unwrap_or_else(Utc::now);
    }
    let secs = epoch as i64;
    let nanos = ((epoch - secs as f64) * 1_000_000_000.0) as u32;
    DateTime::from_timestamp(secs, nanos).unwrap_or_else(Utc::now)
}

fn parse_id_or_generate(id: &str) -> Uuid {
    // OpenCode uses ULIDs, not UUIDs. Generate a deterministic UUID from the string.
    Uuid::new_v5(&Uuid::NAMESPACE_OID, id.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn setup_storage(dir: &Path) {
        std::fs::create_dir_all(dir.join("session/proj1")).unwrap();
        std::fs::create_dir_all(dir.join("message")).unwrap();
        std::fs::create_dir_all(dir.join("part")).unwrap();
    }

    fn write_json(path: &Path, content: &str) {
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(content.as_bytes()).unwrap();
    }

    #[test]
    fn parse_empty_session() {
        let dir = TempDir::new().unwrap();
        let storage = dir.path();
        setup_storage(storage);

        let session_json = r#"{
            "id": "01JQTEST000001",
            "directory": "/home/user/project",
            "projectId": "proj1",
            "time": {"created": 1741776000.0, "updated": 1741777800.0},
            "title": "Test session"
        }"#;

        let session_file = storage.join("session/proj1/01JQTEST000001.json");
        write_json(&session_file, session_json);

        let parser = OpenCodeParser::new();
        let (session, messages) = parser.parse_session(&session_file, storage).unwrap();

        assert_eq!(session.provider, Provider::OpenCode);
        assert_eq!(session.project, "/home/user/project");
        assert_eq!(session.message_count, 0);
        assert!(messages.is_empty());
    }

    #[test]
    fn parse_session_with_messages() {
        let dir = TempDir::new().unwrap();
        let storage = dir.path();
        setup_storage(storage);

        // Session
        let session_file = storage.join("session/proj1/01JQTEST000002.json");
        write_json(
            &session_file,
            r#"{
                "id": "01JQTEST000002",
                "directory": "/home/user/project",
                "projectId": "proj1",
                "time": {"created": 1741776000.0, "updated": 1741777800.0}
            }"#,
        );

        // Messages dir
        let msg_dir = storage.join("message/01JQTEST000002");
        std::fs::create_dir_all(&msg_dir).unwrap();

        // User message
        write_json(
            &msg_dir.join("01JQMSG000001.json"),
            r#"{
                "id": "01JQMSG000001",
                "role": "user",
                "sessionId": "01JQTEST000002",
                "time": {"created": 1741776000.0}
            }"#,
        );

        // Assistant message with tokens
        write_json(
            &msg_dir.join("01JQMSG000002.json"),
            r#"{
                "id": "01JQMSG000002",
                "role": "assistant",
                "sessionId": "01JQTEST000002",
                "cost": 0.015,
                "modelId": "claude-sonnet-4-20250514",
                "providerId": "anthropic",
                "time": {"created": 1741776010.0},
                "tokens": {
                    "input": 1000,
                    "output": 500,
                    "reasoning": 0,
                    "cache": {"read": 200, "write": 100}
                }
            }"#,
        );

        // Parts for assistant message
        let parts_dir = storage.join("part/01JQMSG000002");
        std::fs::create_dir_all(&parts_dir).unwrap();
        write_json(
            &parts_dir.join("01JQPART000001.json"),
            r#"{"type": "text", "text": "Here is the code."}"#,
        );
        write_json(
            &parts_dir.join("01JQPART000002.json"),
            r#"{"type": "tool", "name": "edit_file", "tool": "edit_file"}"#,
        );

        let parser = OpenCodeParser::new();
        let (session, messages) = parser.parse_session(&session_file, storage).unwrap();

        assert_eq!(session.message_count, 2);
        assert_eq!(session.turn_count, 1);
        assert_eq!(session.usage.input_tokens, 1000);
        assert_eq!(session.usage.output_tokens, 500);
        assert_eq!(session.usage.cache_read_tokens, 200);
        assert_eq!(session.usage.cache_creation_tokens, 100);

        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].role, Role::User);
        assert_eq!(messages[1].role, Role::Assistant);
        assert_eq!(messages[1].content, "Here is the code.");
        assert_eq!(messages[1].tool_calls, vec!["edit_file"]);
        assert_eq!(
            messages[1].model.as_deref(),
            Some("claude-sonnet-4-20250514")
        );
    }

    #[test]
    fn discover_sessions() {
        let dir = TempDir::new().unwrap();
        let storage = dir.path();
        setup_storage(storage);

        // Create multiple session files across projects
        std::fs::create_dir_all(storage.join("session/proj2")).unwrap();
        write_json(&storage.join("session/proj1/sess1.json"), r#"{"id":"s1"}"#);
        write_json(&storage.join("session/proj1/sess2.json"), r#"{"id":"s2"}"#);
        write_json(&storage.join("session/proj2/sess3.json"), r#"{"id":"s3"}"#);

        let parser = OpenCodeParser::new();
        let sessions = parser.discover_sessions(storage).unwrap();
        assert_eq!(sessions.len(), 3);
    }

    #[test]
    fn discover_sessions_empty() {
        let dir = TempDir::new().unwrap();
        let parser = OpenCodeParser::new();
        let sessions = parser.discover_sessions(dir.path()).unwrap();
        assert!(sessions.is_empty());
    }

    #[test]
    fn epoch_to_datetime_seconds() {
        let dt = epoch_to_datetime(1741776000.0);
        assert_eq!(dt.format("%Y-%m-%d").to_string(), "2025-03-12");
    }

    #[test]
    fn epoch_to_datetime_milliseconds() {
        // OpenCode sometimes uses ms
        let dt = epoch_to_datetime(1741776000000.0);
        assert_eq!(dt.format("%Y-%m-%d").to_string(), "2025-03-12");
    }

    #[test]
    fn parse_id_deterministic() {
        let id1 = parse_id_or_generate("01JQTEST000001");
        let id2 = parse_id_or_generate("01JQTEST000001");
        let id3 = parse_id_or_generate("01JQTEST000002");
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn parse_file_trait_impl() {
        let dir = TempDir::new().unwrap();
        let storage = dir.path();
        setup_storage(storage);

        let session_file = storage.join("session/proj1/01JQTEST000003.json");
        write_json(
            &session_file,
            r#"{
                "id": "01JQTEST000003",
                "directory": "/tmp/test",
                "projectId": "proj1",
                "time": {"created": 1741776000.0}
            }"#,
        );

        let parser = OpenCodeParser::new();
        let session = parser.parse_file(&session_file).unwrap();
        assert_eq!(session.provider, Provider::OpenCode);
        assert_eq!(session.project, "/tmp/test");
    }
}
