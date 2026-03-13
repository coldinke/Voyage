use std::path::Path;

use chrono::{DateTime, Utc};
use rusqlite::{Connection, params};
use uuid::Uuid;

use voyage_core::model::{Message, Provider, Role, Session, TokenUsage};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub struct SqliteStore {
    conn: Connection,
}

impl SqliteStore {
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        let conn = Connection::open(path)?;
        let store = Self { conn };
        store.migrate()?;
        Ok(store)
    }

    pub fn open_in_memory() -> Result<Self, StoreError> {
        let conn = Connection::open_in_memory()?;
        let store = Self { conn };
        store.migrate()?;
        Ok(store)
    }

    fn migrate(&self) -> Result<(), StoreError> {
        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY,
                project TEXT NOT NULL,
                provider TEXT NOT NULL,
                model TEXT NOT NULL DEFAULT '',
                started_at TEXT NOT NULL,
                ended_at TEXT,
                cwd TEXT NOT NULL DEFAULT '',
                git_branch TEXT,
                input_tokens INTEGER NOT NULL DEFAULT 0,
                output_tokens INTEGER NOT NULL DEFAULT 0,
                cache_read_tokens INTEGER NOT NULL DEFAULT 0,
                cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
                estimated_cost_usd REAL NOT NULL DEFAULT 0.0,
                message_count INTEGER NOT NULL DEFAULT 0,
                turn_count INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL REFERENCES sessions(id),
                role TEXT NOT NULL,
                content TEXT NOT NULL DEFAULT '',
                input_tokens INTEGER NOT NULL DEFAULT 0,
                output_tokens INTEGER NOT NULL DEFAULT 0,
                cache_read_tokens INTEGER NOT NULL DEFAULT 0,
                cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
                model TEXT,
                tool_calls_json TEXT NOT NULL DEFAULT '[]',
                timestamp TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_messages_session_id ON messages(session_id);
            CREATE INDEX IF NOT EXISTS idx_sessions_project ON sessions(project);
            CREATE INDEX IF NOT EXISTS idx_sessions_started_at ON sessions(started_at);
            ",
        )?;

        // Idempotent migration: add summary column
        let _ = self
            .conn
            .execute_batch("ALTER TABLE sessions ADD COLUMN summary TEXT NOT NULL DEFAULT '';");

        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS daily_stats (
                date TEXT NOT NULL,
                provider TEXT NOT NULL,
                model TEXT NOT NULL,
                project TEXT NOT NULL,
                total_input INTEGER NOT NULL DEFAULT 0,
                total_output INTEGER NOT NULL DEFAULT 0,
                total_cache_read INTEGER NOT NULL DEFAULT 0,
                total_cache_creation INTEGER NOT NULL DEFAULT 0,
                total_cost_usd REAL NOT NULL DEFAULT 0.0,
                session_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (date, provider, model, project)
            );
            ",
        )?;
        Ok(())
    }

    /// Returns (exists, message_count) for a session.
    pub fn session_state(&self, id: &Uuid) -> Result<Option<u32>, StoreError> {
        let mut stmt = self
            .conn
            .prepare("SELECT message_count FROM sessions WHERE id = ?1")?;
        let result = stmt.query_row(params![id.to_string()], |row| {
            Ok(row.get::<_, i64>(0)? as u32)
        });
        match result {
            Ok(count) => Ok(Some(count)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(StoreError::Sqlite(e)),
        }
    }

    pub fn session_exists(&self, id: &Uuid) -> Result<bool, StoreError> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM sessions WHERE id = ?1",
            params![id.to_string()],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    pub fn insert_session(&self, session: &Session) -> Result<(), StoreError> {
        self.conn.execute(
            "INSERT OR REPLACE INTO sessions
             (id, project, provider, model, started_at, ended_at, cwd, git_branch,
              input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens,
              estimated_cost_usd, message_count, turn_count, summary)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            params![
                session.id.to_string(),
                session.project,
                provider_to_str(session.provider),
                session.model,
                session.started_at.to_rfc3339(),
                session.ended_at.map(|t| t.to_rfc3339()),
                session.cwd,
                session.git_branch,
                session.usage.input_tokens as i64,
                session.usage.output_tokens as i64,
                session.usage.cache_read_tokens as i64,
                session.usage.cache_creation_tokens as i64,
                session.estimated_cost_usd,
                session.message_count as i64,
                session.turn_count as i64,
                session.summary,
            ],
        )?;
        Ok(())
    }

    pub fn insert_message(&self, msg: &Message) -> Result<(), StoreError> {
        let tool_calls_json =
            serde_json::to_string(&msg.tool_calls).unwrap_or_else(|_| "[]".into());
        self.conn.execute(
            "INSERT OR REPLACE INTO messages
             (id, session_id, role, content, input_tokens, output_tokens,
              cache_read_tokens, cache_creation_tokens, model, tool_calls_json, timestamp)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                msg.id.to_string(),
                msg.session_id.to_string(),
                role_to_str(msg.role),
                msg.content,
                msg.usage.input_tokens as i64,
                msg.usage.output_tokens as i64,
                msg.usage.cache_read_tokens as i64,
                msg.usage.cache_creation_tokens as i64,
                msg.model,
                tool_calls_json,
                msg.timestamp.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    pub fn insert_session_with_messages(
        &mut self,
        session: &Session,
        messages: &[Message],
    ) -> Result<(), StoreError> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "INSERT OR REPLACE INTO sessions
             (id, project, provider, model, started_at, ended_at, cwd, git_branch,
              input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens,
              estimated_cost_usd, message_count, turn_count, summary)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            params![
                session.id.to_string(),
                session.project,
                provider_to_str(session.provider),
                session.model,
                session.started_at.to_rfc3339(),
                session.ended_at.map(|t| t.to_rfc3339()),
                session.cwd,
                session.git_branch,
                session.usage.input_tokens as i64,
                session.usage.output_tokens as i64,
                session.usage.cache_read_tokens as i64,
                session.usage.cache_creation_tokens as i64,
                session.estimated_cost_usd,
                session.message_count as i64,
                session.turn_count as i64,
                session.summary,
            ],
        )?;

        for msg in messages {
            let tool_calls_json =
                serde_json::to_string(&msg.tool_calls).unwrap_or_else(|_| "[]".into());
            tx.execute(
                "INSERT OR REPLACE INTO messages
                 (id, session_id, role, content, input_tokens, output_tokens,
                  cache_read_tokens, cache_creation_tokens, model, tool_calls_json, timestamp)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    msg.id.to_string(),
                    msg.session_id.to_string(),
                    role_to_str(msg.role),
                    msg.content,
                    msg.usage.input_tokens as i64,
                    msg.usage.output_tokens as i64,
                    msg.usage.cache_read_tokens as i64,
                    msg.usage.cache_creation_tokens as i64,
                    msg.model,
                    tool_calls_json,
                    msg.timestamp.to_rfc3339(),
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn get_session(&self, id: &Uuid) -> Result<Option<Session>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, project, provider, model, started_at, ended_at, cwd, git_branch,
                    input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens,
                    estimated_cost_usd, message_count, turn_count, summary
             FROM sessions WHERE id = ?1",
        )?;

        let result = stmt.query_row(params![id.to_string()], |row| Ok(row_to_session(row)));

        match result {
            Ok(session) => Ok(Some(session)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn list_sessions(
        &self,
        since: Option<DateTime<Utc>>,
        project: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Session>, StoreError> {
        let mut sql = String::from(
            "SELECT id, project, provider, model, started_at, ended_at, cwd, git_branch,
                    input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens,
                    estimated_cost_usd, message_count, turn_count, summary
             FROM sessions WHERE 1=1",
        );
        let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(since) = since {
            sql.push_str(&format!(" AND started_at >= ?{}", param_values.len() + 1));
            param_values.push(Box::new(since.to_rfc3339()));
        }
        if let Some(project) = project {
            sql.push_str(&format!(" AND project = ?{}", param_values.len() + 1));
            param_values.push(Box::new(project.to_string()));
        }

        sql.push_str(&format!(
            " ORDER BY started_at DESC LIMIT ?{}",
            param_values.len() + 1
        ));
        param_values.push(Box::new(limit as i64));

        let params_ref: Vec<&dyn rusqlite::types::ToSql> =
            param_values.iter().map(|p| p.as_ref()).collect();
        let mut stmt = self.conn.prepare(&sql)?;
        let sessions = stmt
            .query_map(params_ref.as_slice(), |row| Ok(row_to_session(row)))?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(sessions)
    }

    pub fn get_stats(
        &self,
        since: Option<DateTime<Utc>>,
        project: Option<&str>,
    ) -> Result<UsageStats, StoreError> {
        let mut sql = String::from(
            "SELECT COALESCE(SUM(input_tokens), 0),
                    COALESCE(SUM(output_tokens), 0),
                    COALESCE(SUM(cache_read_tokens), 0),
                    COALESCE(SUM(cache_creation_tokens), 0),
                    COALESCE(SUM(estimated_cost_usd), 0.0),
                    COUNT(*)
             FROM sessions WHERE 1=1",
        );
        let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(since) = since {
            sql.push_str(&format!(" AND started_at >= ?{}", param_values.len() + 1));
            param_values.push(Box::new(since.to_rfc3339()));
        }
        if let Some(project) = project {
            sql.push_str(&format!(" AND project = ?{}", param_values.len() + 1));
            param_values.push(Box::new(project.to_string()));
        }

        let params_ref: Vec<&dyn rusqlite::types::ToSql> =
            param_values.iter().map(|p| p.as_ref()).collect();
        let stats = self.conn.query_row(&sql, params_ref.as_slice(), |row| {
            Ok(UsageStats {
                input_tokens: row.get::<_, i64>(0)? as u64,
                output_tokens: row.get::<_, i64>(1)? as u64,
                cache_read_tokens: row.get::<_, i64>(2)? as u64,
                cache_creation_tokens: row.get::<_, i64>(3)? as u64,
                total_cost_usd: row.get(4)?,
                session_count: row.get::<_, i64>(5)? as u64,
            })
        })?;
        Ok(stats)
    }

    pub fn get_stats_by_model(
        &self,
        since: Option<DateTime<Utc>>,
    ) -> Result<Vec<ModelStats>, StoreError> {
        let mut sql = String::from(
            "SELECT model,
                    COALESCE(SUM(input_tokens), 0),
                    COALESCE(SUM(output_tokens), 0),
                    COALESCE(SUM(estimated_cost_usd), 0.0),
                    COUNT(*)
             FROM sessions WHERE 1=1",
        );
        let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(since) = since {
            sql.push_str(&format!(" AND started_at >= ?{}", param_values.len() + 1));
            param_values.push(Box::new(since.to_rfc3339()));
        }

        sql.push_str(" GROUP BY model ORDER BY SUM(estimated_cost_usd) DESC");

        let params_ref: Vec<&dyn rusqlite::types::ToSql> =
            param_values.iter().map(|p| p.as_ref()).collect();
        let mut stmt = self.conn.prepare(&sql)?;
        let stats = stmt
            .query_map(params_ref.as_slice(), |row| {
                Ok(ModelStats {
                    model: row.get(0)?,
                    input_tokens: row.get::<_, i64>(1)? as u64,
                    output_tokens: row.get::<_, i64>(2)? as u64,
                    total_cost_usd: row.get(3)?,
                    session_count: row.get::<_, i64>(4)? as u64,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(stats)
    }

    pub fn get_stats_by_provider(
        &self,
        since: Option<DateTime<Utc>>,
    ) -> Result<Vec<ProviderStats>, StoreError> {
        let mut sql = String::from(
            "SELECT provider,
                    COALESCE(SUM(input_tokens + cache_read_tokens + cache_creation_tokens), 0),
                    COALESCE(SUM(output_tokens), 0),
                    COALESCE(SUM(estimated_cost_usd), 0.0),
                    COUNT(*)
             FROM sessions WHERE 1=1",
        );
        let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(since) = since {
            sql.push_str(&format!(" AND started_at >= ?{}", param_values.len() + 1));
            param_values.push(Box::new(since.to_rfc3339()));
        }

        sql.push_str(" GROUP BY provider ORDER BY SUM(estimated_cost_usd) DESC");

        let params_ref: Vec<&dyn rusqlite::types::ToSql> =
            param_values.iter().map(|p| p.as_ref()).collect();
        let mut stmt = self.conn.prepare(&sql)?;
        let stats = stmt
            .query_map(params_ref.as_slice(), |row| {
                Ok(ProviderStats {
                    provider: row.get(0)?,
                    input_tokens: row.get::<_, i64>(1)? as u64,
                    output_tokens: row.get::<_, i64>(2)? as u64,
                    total_cost_usd: row.get(3)?,
                    session_count: row.get::<_, i64>(4)? as u64,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(stats)
    }

    pub fn get_messages_by_session(
        &self,
        session_id: &Uuid,
        limit: usize,
    ) -> Result<Vec<Message>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, session_id, role, content, input_tokens, output_tokens,
                    cache_read_tokens, cache_creation_tokens, model, tool_calls_json, timestamp
             FROM messages WHERE session_id = ?1 ORDER BY timestamp ASC LIMIT ?2",
        )?;
        let messages = stmt
            .query_map(params![session_id.to_string(), limit as i64], |row| {
                let id_str: String = row.get(0)?;
                let sid_str: String = row.get(1)?;
                let role_str: String = row.get(2)?;
                let tool_calls_json: String = row.get(9)?;
                let ts_str: String = row.get(10)?;
                Ok(Message {
                    id: Uuid::parse_str(&id_str).unwrap(),
                    session_id: Uuid::parse_str(&sid_str).unwrap(),
                    role: match role_str.as_str() {
                        "assistant" => Role::Assistant,
                        "system" => Role::System,
                        _ => Role::User,
                    },
                    content: row.get(3)?,
                    usage: TokenUsage {
                        input_tokens: row.get::<_, i64>(4)? as u64,
                        output_tokens: row.get::<_, i64>(5)? as u64,
                        cache_read_tokens: row.get::<_, i64>(6)? as u64,
                        cache_creation_tokens: row.get::<_, i64>(7)? as u64,
                    },
                    model: row.get(8)?,
                    tool_calls: serde_json::from_str(&tool_calls_json).unwrap_or_default(),
                    timestamp: DateTime::parse_from_rfc3339(&ts_str)
                        .unwrap()
                        .with_timezone(&Utc),
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(messages)
    }

    pub fn get_daily_stats(
        &self,
        since: Option<DateTime<Utc>>,
    ) -> Result<Vec<DailyStats>, StoreError> {
        let mut sql = String::from(
            "SELECT DATE(started_at) as day,
                    COALESCE(SUM(input_tokens + cache_read_tokens + cache_creation_tokens), 0),
                    COALESCE(SUM(output_tokens), 0),
                    COALESCE(SUM(estimated_cost_usd), 0.0),
                    COUNT(*),
                    COALESCE(SUM(turn_count), 0)
             FROM sessions WHERE 1=1",
        );
        let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(since) = since {
            sql.push_str(&format!(" AND started_at >= ?{}", param_values.len() + 1));
            param_values.push(Box::new(since.to_rfc3339()));
        }

        sql.push_str(" GROUP BY day ORDER BY day ASC");

        let params_ref: Vec<&dyn rusqlite::types::ToSql> =
            param_values.iter().map(|p| p.as_ref()).collect();
        let mut stmt = self.conn.prepare(&sql)?;
        let stats = stmt
            .query_map(params_ref.as_slice(), |row| {
                Ok(DailyStats {
                    date: row.get(0)?,
                    input_tokens: row.get::<_, i64>(1)? as u64,
                    output_tokens: row.get::<_, i64>(2)? as u64,
                    total_cost_usd: row.get(3)?,
                    session_count: row.get::<_, i64>(4)? as u64,
                    turn_count: row.get::<_, i64>(5)? as u64,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(stats)
    }
}

#[derive(Debug, Clone)]
pub struct UsageStats {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_creation_tokens: u64,
    pub total_cost_usd: f64,
    pub session_count: u64,
}

#[derive(Debug, Clone)]
pub struct ModelStats {
    pub model: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_cost_usd: f64,
    pub session_count: u64,
}

#[derive(Debug, Clone)]
pub struct ProviderStats {
    pub provider: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_cost_usd: f64,
    pub session_count: u64,
}

#[derive(Debug, Clone)]
pub struct DailyStats {
    pub date: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_cost_usd: f64,
    pub session_count: u64,
    pub turn_count: u64,
}

fn provider_to_str(p: Provider) -> &'static str {
    match p {
        Provider::ClaudeCode => "claude_code",
        Provider::OpenCode => "opencode",
        Provider::Codex => "codex",
    }
}

fn str_to_provider(s: &str) -> Provider {
    match s {
        "claude_code" => Provider::ClaudeCode,
        "opencode" => Provider::OpenCode,
        "codex" => Provider::Codex,
        _ => Provider::ClaudeCode,
    }
}

fn role_to_str(r: Role) -> &'static str {
    match r {
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::System => "system",
    }
}

fn row_to_session(row: &rusqlite::Row) -> Session {
    let id_str: String = row.get(0).unwrap();
    let ended_at_str: Option<String> = row.get(5).unwrap();
    Session {
        id: Uuid::parse_str(&id_str).unwrap(),
        project: row.get(1).unwrap(),
        provider: str_to_provider(&row.get::<_, String>(2).unwrap()),
        model: row.get(3).unwrap(),
        started_at: DateTime::parse_from_rfc3339(&row.get::<_, String>(4).unwrap())
            .unwrap()
            .with_timezone(&Utc),
        ended_at: ended_at_str.map(|s| {
            DateTime::parse_from_rfc3339(&s)
                .unwrap()
                .with_timezone(&Utc)
        }),
        cwd: row.get(6).unwrap(),
        git_branch: row.get(7).unwrap(),
        usage: TokenUsage {
            input_tokens: row.get::<_, i64>(8).unwrap() as u64,
            output_tokens: row.get::<_, i64>(9).unwrap() as u64,
            cache_read_tokens: row.get::<_, i64>(10).unwrap() as u64,
            cache_creation_tokens: row.get::<_, i64>(11).unwrap() as u64,
        },
        estimated_cost_usd: row.get(12).unwrap(),
        message_count: row.get::<_, i64>(13).unwrap() as u32,
        turn_count: row.get::<_, i64>(14).unwrap() as u32,
        summary: row.get::<_, String>(15).unwrap_or_default(),
    }
}

// serde_json needed for tool_calls serialization
use serde_json;

#[cfg(test)]
mod tests {
    use super::*;
    use voyage_core::model::{Message, Provider, Role, TokenUsage};

    fn sample_session() -> Session {
        Session {
            id: Uuid::parse_str("9550f7c1-2907-414c-8527-eb992e7af55d").unwrap(),
            project: "/Users/test/project".into(),
            provider: Provider::ClaudeCode,
            model: "claude-opus-4-6".into(),
            started_at: "2026-03-12T10:00:00Z".parse::<DateTime<Utc>>().unwrap(),
            ended_at: Some("2026-03-12T10:30:00Z".parse::<DateTime<Utc>>().unwrap()),
            cwd: "/Users/test/project".into(),
            git_branch: Some("main".into()),
            usage: TokenUsage {
                input_tokens: 5000,
                output_tokens: 10000,
                cache_read_tokens: 2000,
                cache_creation_tokens: 1000,
            },
            estimated_cost_usd: 0.8475,
            message_count: 10,
            turn_count: 5,
            summary: "Test session summary".into(),
        }
    }

    fn sample_message(session_id: Uuid) -> Message {
        Message {
            id: Uuid::new_v4(),
            session_id,
            role: Role::Assistant,
            content: "Hello, I can help with that.".into(),
            usage: TokenUsage {
                input_tokens: 100,
                output_tokens: 50,
                cache_read_tokens: 0,
                cache_creation_tokens: 0,
            },
            model: Some("claude-opus-4-6".into()),
            tool_calls: vec!["Read".into(), "Write".into()],
            timestamp: "2026-03-12T10:00:05Z".parse::<DateTime<Utc>>().unwrap(),
        }
    }

    #[test]
    fn migrate_creates_tables() {
        let store = SqliteStore::open_in_memory().unwrap();
        // Should not error on second call
        store.migrate().unwrap();
    }

    #[test]
    fn insert_and_get_session() {
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sample_session();
        store.insert_session(&session).unwrap();

        let retrieved = store.get_session(&session.id).unwrap().unwrap();
        assert_eq!(retrieved.id, session.id);
        assert_eq!(retrieved.project, session.project);
        assert_eq!(retrieved.model, session.model);
        assert_eq!(retrieved.usage.input_tokens, 5000);
        assert_eq!(retrieved.usage.output_tokens, 10000);
        assert_eq!(retrieved.message_count, 10);
    }

    #[test]
    fn session_exists_check() {
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sample_session();

        assert!(!store.session_exists(&session.id).unwrap());
        store.insert_session(&session).unwrap();
        assert!(store.session_exists(&session.id).unwrap());
    }

    #[test]
    fn insert_session_with_messages_transactional() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        let session = sample_session();
        let msg1 = sample_message(session.id);
        let msg2 = sample_message(session.id);

        store
            .insert_session_with_messages(&session, &[msg1, msg2])
            .unwrap();

        let retrieved = store.get_session(&session.id).unwrap().unwrap();
        assert_eq!(retrieved.id, session.id);
    }

    #[test]
    fn get_nonexistent_session_returns_none() {
        let store = SqliteStore::open_in_memory().unwrap();
        let result = store.get_session(&Uuid::new_v4()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn list_sessions_with_limit() {
        let store = SqliteStore::open_in_memory().unwrap();
        for i in 0..5 {
            let mut s = sample_session();
            s.id = Uuid::new_v4();
            s.started_at = format!("2026-03-{:02}T10:00:00Z", 10 + i)
                .parse::<DateTime<Utc>>()
                .unwrap();
            store.insert_session(&s).unwrap();
        }

        let sessions = store.list_sessions(None, None, 3).unwrap();
        assert_eq!(sessions.len(), 3);
        // Should be ordered by started_at DESC
        assert!(sessions[0].started_at > sessions[1].started_at);
    }

    #[test]
    fn list_sessions_filter_by_project() {
        let store = SqliteStore::open_in_memory().unwrap();

        let mut s1 = sample_session();
        s1.id = Uuid::new_v4();
        s1.project = "project-a".into();
        store.insert_session(&s1).unwrap();

        let mut s2 = sample_session();
        s2.id = Uuid::new_v4();
        s2.project = "project-b".into();
        store.insert_session(&s2).unwrap();

        let sessions = store.list_sessions(None, Some("project-a"), 10).unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].project, "project-a");
    }

    #[test]
    fn list_sessions_filter_by_since() {
        let store = SqliteStore::open_in_memory().unwrap();

        let mut old = sample_session();
        old.id = Uuid::new_v4();
        old.started_at = "2026-03-01T10:00:00Z".parse().unwrap();
        store.insert_session(&old).unwrap();

        let mut recent = sample_session();
        recent.id = Uuid::new_v4();
        recent.started_at = "2026-03-11T10:00:00Z".parse().unwrap();
        store.insert_session(&recent).unwrap();

        let since: DateTime<Utc> = "2026-03-10T00:00:00Z".parse().unwrap();
        let sessions = store.list_sessions(Some(since), None, 10).unwrap();
        assert_eq!(sessions.len(), 1);
    }

    #[test]
    fn get_stats_aggregation() {
        let store = SqliteStore::open_in_memory().unwrap();

        let mut s1 = sample_session();
        s1.id = Uuid::new_v4();
        s1.usage.input_tokens = 1000;
        s1.usage.output_tokens = 2000;
        s1.estimated_cost_usd = 0.5;
        store.insert_session(&s1).unwrap();

        let mut s2 = sample_session();
        s2.id = Uuid::new_v4();
        s2.usage.input_tokens = 3000;
        s2.usage.output_tokens = 4000;
        s2.estimated_cost_usd = 1.0;
        store.insert_session(&s2).unwrap();

        let stats = store.get_stats(None, None).unwrap();
        assert_eq!(stats.input_tokens, 4000);
        assert_eq!(stats.output_tokens, 6000);
        assert!((stats.total_cost_usd - 1.5).abs() < 0.001);
        assert_eq!(stats.session_count, 2);
    }

    #[test]
    fn get_stats_empty_db() {
        let store = SqliteStore::open_in_memory().unwrap();
        let stats = store.get_stats(None, None).unwrap();
        assert_eq!(stats.session_count, 0);
        assert_eq!(stats.input_tokens, 0);
    }

    #[test]
    fn get_stats_by_model() {
        let store = SqliteStore::open_in_memory().unwrap();

        let mut s1 = sample_session();
        s1.id = Uuid::new_v4();
        s1.model = "claude-opus-4-6".into();
        s1.estimated_cost_usd = 2.0;
        store.insert_session(&s1).unwrap();

        let mut s2 = sample_session();
        s2.id = Uuid::new_v4();
        s2.model = "claude-sonnet-4-6".into();
        s2.estimated_cost_usd = 0.5;
        store.insert_session(&s2).unwrap();

        let stats = store.get_stats_by_model(None).unwrap();
        assert_eq!(stats.len(), 2);
        // Ordered by cost DESC
        assert_eq!(stats[0].model, "claude-opus-4-6");
        assert_eq!(stats[1].model, "claude-sonnet-4-6");
    }

    #[test]
    fn upsert_session_replaces() {
        let store = SqliteStore::open_in_memory().unwrap();
        let mut session = sample_session();
        session.message_count = 5;
        store.insert_session(&session).unwrap();

        session.message_count = 15;
        store.insert_session(&session).unwrap();

        let retrieved = store.get_session(&session.id).unwrap().unwrap();
        assert_eq!(retrieved.message_count, 15);
    }
}
