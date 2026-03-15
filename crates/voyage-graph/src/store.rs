use std::collections::HashSet;
use std::path::Path;

use chrono::{DateTime, Utc};
use rusqlite::{Connection, params};
use uuid::Uuid;

use crate::edge::EdgeKind;
use crate::entity::{Entity, EntityKind, EntityMention, MentionRole};

#[derive(Debug, thiserror::Error)]
pub enum GraphError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub struct GraphStore {
    conn: Connection,
}

impl GraphStore {
    pub fn open(path: &Path) -> Result<Self, GraphError> {
        let conn = Connection::open(path)?;
        let store = Self { conn };
        store.migrate()?;
        Ok(store)
    }

    pub fn open_in_memory() -> Result<Self, GraphError> {
        let conn = Connection::open_in_memory()?;
        let store = Self { conn };
        store.migrate()?;
        Ok(store)
    }

    fn migrate(&self) -> Result<(), GraphError> {
        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS entities (
                id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                name TEXT NOT NULL,
                display_name TEXT NOT NULL,
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL,
                mention_count INTEGER DEFAULT 0,
                session_count INTEGER DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_entities_kind ON entities(kind);
            CREATE INDEX IF NOT EXISTS idx_entities_name ON entities(name);

            CREATE TABLE IF NOT EXISTS entity_mentions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_id TEXT NOT NULL REFERENCES entities(id),
                session_id TEXT NOT NULL,
                message_id TEXT,
                timestamp TEXT NOT NULL,
                context TEXT DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_mentions_entity ON entity_mentions(entity_id);
            CREATE INDEX IF NOT EXISTS idx_mentions_session ON entity_mentions(session_id);

            CREATE TABLE IF NOT EXISTS extraction_log (
                session_id TEXT PRIMARY KEY,
                extracted_at TEXT NOT NULL,
                entity_count INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS edges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id TEXT NOT NULL REFERENCES entities(id),
                target_id TEXT NOT NULL REFERENCES entities(id),
                kind TEXT NOT NULL,
                weight REAL DEFAULT 1.0,
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL,
                UNIQUE(source_id, target_id, kind)
            );
            CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source_id);
            CREATE INDEX IF NOT EXISTS idx_edges_target ON edges(target_id);

            CREATE TABLE IF NOT EXISTS session_entities (
                session_id TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                mention_count INTEGER DEFAULT 1,
                PRIMARY KEY (session_id, entity_id)
            );

            CREATE TABLE IF NOT EXISTS entity_aliases (
                alias_name TEXT PRIMARY KEY,
                canonical_id TEXT NOT NULL
            );
            ",
        )?;

        // Incremental migrations: add columns that may not exist yet
        Self::try_add_column(
            &self.conn,
            "entity_mentions",
            "role",
            "TEXT DEFAULT 'unknown'",
        );
        Self::try_add_column(&self.conn, "entities", "pagerank", "REAL DEFAULT 0.0");
        Self::try_add_column(&self.conn, "entities", "community_id", "TEXT");

        Ok(())
    }

    /// Attempt to add a column; silently ignore if it already exists.
    fn try_add_column(conn: &Connection, table: &str, column: &str, col_type: &str) {
        let sql = format!("ALTER TABLE {table} ADD COLUMN {column} {col_type}");
        let _ = conn.execute(&sql, []);
    }

    // ── Entity CRUD ──

    /// Record a mention of an entity. Upserts the entity and inserts the mention.
    pub fn record_mention(
        &self,
        entity: &Entity,
        mention: &EntityMention,
    ) -> Result<(), GraphError> {
        let ts = entity.first_seen.to_rfc3339();

        // Upsert entity
        self.conn.execute(
            "INSERT INTO entities (id, kind, name, display_name, first_seen, last_seen, mention_count, session_count)
             VALUES (?1, ?2, ?3, ?4, ?5, ?5, 1, 0)
             ON CONFLICT(id) DO UPDATE SET
                 last_seen = MAX(last_seen, ?5),
                 mention_count = mention_count + 1",
            params![
                entity.id.to_string(),
                entity.kind.as_str(),
                entity.name,
                entity.display_name,
                ts,
            ],
        )?;

        // Insert mention
        self.conn.execute(
            "INSERT INTO entity_mentions (entity_id, session_id, message_id, timestamp, context, role)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                mention.entity_id.to_string(),
                mention.session_id.to_string(),
                mention.message_id.map(|id| id.to_string()),
                mention.timestamp.to_rfc3339(),
                mention.context,
                mention.role.as_str(),
            ],
        )?;

        // Upsert session_entities
        self.conn.execute(
            "INSERT INTO session_entities (session_id, entity_id, mention_count)
             VALUES (?1, ?2, 1)
             ON CONFLICT(session_id, entity_id) DO UPDATE SET
                 mention_count = mention_count + 1",
            params![mention.session_id.to_string(), entity.id.to_string(),],
        )?;

        Ok(())
    }

    /// Check if a session has already been extracted.
    pub fn session_extracted(&self, session_id: &Uuid) -> Result<bool, GraphError> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM extraction_log WHERE session_id = ?1",
            params![session_id.to_string()],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Delete all mentions for a session (for re-extraction).
    pub fn delete_mentions_for_session(&self, session_id: &Uuid) -> Result<(), GraphError> {
        let sid = session_id.to_string();
        self.conn.execute(
            "DELETE FROM entity_mentions WHERE session_id = ?1",
            params![sid],
        )?;
        self.conn.execute(
            "DELETE FROM session_entities WHERE session_id = ?1",
            params![sid],
        )?;
        self.conn.execute(
            "DELETE FROM extraction_log WHERE session_id = ?1",
            params![sid],
        )?;
        Ok(())
    }

    /// Mark a session as extracted.
    pub fn mark_session_extracted(
        &self,
        session_id: &Uuid,
        entity_count: u32,
    ) -> Result<(), GraphError> {
        self.conn.execute(
            "INSERT OR REPLACE INTO extraction_log (session_id, extracted_at, entity_count)
             VALUES (?1, ?2, ?3)",
            params![
                session_id.to_string(),
                Utc::now().to_rfc3339(),
                entity_count,
            ],
        )?;
        Ok(())
    }

    /// Recalculate session_count for all entities based on session_entities table.
    pub fn refresh_session_counts(&self) -> Result<(), GraphError> {
        self.conn.execute_batch(
            "UPDATE entities SET session_count = (
                SELECT COUNT(DISTINCT session_id) FROM session_entities
                WHERE session_entities.entity_id = entities.id
            )",
        )?;
        Ok(())
    }

    // ── Entity queries ──

    /// Get stats: count of entities by kind.
    pub fn entity_stats(&self) -> Result<Vec<(EntityKind, u32)>, GraphError> {
        let mut stmt = self
            .conn
            .prepare("SELECT kind, COUNT(*) FROM entities GROUP BY kind ORDER BY COUNT(*) DESC")?;
        let rows = stmt.query_map([], |row| {
            let kind_str: String = row.get(0)?;
            let count: u32 = row.get(1)?;
            Ok((kind_str, count))
        })?;
        let mut stats = Vec::new();
        for row in rows {
            let (kind_str, count) = row?;
            if let Ok(kind) = kind_str.parse::<EntityKind>() {
                stats.push((kind, count));
            }
        }
        Ok(stats)
    }

    /// List entities of a given kind, ordered by pagerank then mention_count descending.
    pub fn list_entities(
        &self,
        kind: Option<EntityKind>,
        limit: usize,
    ) -> Result<Vec<Entity>, GraphError> {
        let mut entities = Vec::new();

        match kind {
            Some(k) => {
                let mut stmt = self.conn.prepare(
                    "SELECT id, kind, name, display_name, first_seen, last_seen, mention_count, session_count,
                            COALESCE(pagerank, 0.0), community_id
                     FROM entities WHERE kind = ?1 ORDER BY pagerank DESC, mention_count DESC LIMIT ?2",
                )?;
                let rows =
                    stmt.query_map(params![k.as_str(), limit as u32], Self::row_to_entity)?;
                for row in rows {
                    entities.push(row?);
                }
            }
            None => {
                let mut stmt = self.conn.prepare(
                    "SELECT id, kind, name, display_name, first_seen, last_seen, mention_count, session_count,
                            COALESCE(pagerank, 0.0), community_id
                     FROM entities ORDER BY pagerank DESC, mention_count DESC LIMIT ?1",
                )?;
                let rows = stmt.query_map(params![limit as u32], Self::row_to_entity)?;
                for row in rows {
                    entities.push(row?);
                }
            }
        }

        Ok(entities)
    }

    /// Find an entity by name (exact match, with alias fallback).
    pub fn find_entity_by_name(&self, name: &str) -> Result<Option<Entity>, GraphError> {
        // Direct lookup
        let mut stmt = self.conn.prepare(
            "SELECT id, kind, name, display_name, first_seen, last_seen, mention_count, session_count, pagerank, community_id
             FROM entities WHERE name = ?1",
        )?;
        let mut rows = stmt.query_map(params![name], Self::row_to_entity)?;
        if let Some(row) = rows.next() {
            return Ok(Some(row?));
        }
        drop(rows);
        drop(stmt);

        // Alias fallback
        if let Some(canonical_id) = self.resolve_alias(name)? {
            return self.get_entity_by_id(&canonical_id);
        }

        Ok(None)
    }

    /// Get sessions that mention a given entity, with mention count per session.
    pub fn sessions_for_entity(
        &self,
        name: &str,
    ) -> Result<Vec<(Uuid, DateTime<Utc>, u32)>, GraphError> {
        let mut stmt = self.conn.prepare(
            "SELECT se.session_id, MIN(em.timestamp), se.mention_count
             FROM session_entities se
             JOIN entities e ON se.entity_id = e.id
             JOIN entity_mentions em ON em.entity_id = e.id AND em.session_id = se.session_id
             WHERE e.name = ?1
             GROUP BY se.session_id
             ORDER BY MIN(em.timestamp) DESC",
        )?;
        let rows = stmt.query_map(params![name], |row| {
            let sid_str: String = row.get(0)?;
            let ts_str: String = row.get(1)?;
            let count: u32 = row.get(2)?;
            Ok((sid_str, ts_str, count))
        })?;
        let mut result = Vec::new();
        for row in rows {
            let (sid_str, ts_str, count) = row?;
            if let (Ok(sid), Ok(ts)) = (
                Uuid::parse_str(&sid_str),
                DateTime::parse_from_rfc3339(&ts_str),
            ) {
                result.push((sid, ts.with_timezone(&Utc), count));
            }
        }
        Ok(result)
    }

    /// Get mentions of an entity with context.
    pub fn get_mentions(&self, name: &str, limit: usize) -> Result<Vec<EntityMention>, GraphError> {
        let mut stmt = self.conn.prepare(
            "SELECT em.entity_id, em.session_id, em.message_id, em.timestamp, em.context,
                    COALESCE(em.role, 'unknown')
             FROM entity_mentions em
             JOIN entities e ON em.entity_id = e.id
             WHERE e.name = ?1
             ORDER BY em.timestamp DESC
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![name, limit as u32], |row| {
            let eid_str: String = row.get(0)?;
            let sid_str: String = row.get(1)?;
            let mid_str: Option<String> = row.get(2)?;
            let ts_str: String = row.get(3)?;
            let context: String = row.get(4)?;
            let role_str: String = row.get(5)?;
            Ok((eid_str, sid_str, mid_str, ts_str, context, role_str))
        })?;
        let mut mentions = Vec::new();
        for row in rows {
            let (eid_str, sid_str, mid_str, ts_str, context, role_str) = row?;
            if let (Ok(eid), Ok(sid), Ok(ts)) = (
                Uuid::parse_str(&eid_str),
                Uuid::parse_str(&sid_str),
                DateTime::parse_from_rfc3339(&ts_str),
            ) {
                mentions.push(EntityMention {
                    entity_id: eid,
                    session_id: sid,
                    message_id: mid_str.and_then(|s| Uuid::parse_str(&s).ok()),
                    timestamp: ts.with_timezone(&Utc),
                    context,
                    role: role_str
                        .parse::<MentionRole>()
                        .unwrap_or(MentionRole::Unknown),
                });
            }
        }
        Ok(mentions)
    }

    // ── Edge operations ──

    /// Build edges for a session based on entity co-occurrence and tool usage.
    pub fn build_edges_for_session(&self, session_id: &Uuid) -> Result<u32, GraphError> {
        let sid = session_id.to_string();

        // Get entities in this session (limit to 50 for O(n²) safety)
        let mut stmt = self.conn.prepare(
            "SELECT se.entity_id, e.kind FROM session_entities se
             JOIN entities e ON se.entity_id = e.id
             WHERE se.session_id = ?1
             ORDER BY se.mention_count DESC
             LIMIT 50",
        )?;
        let entity_rows: Vec<(String, String)> = stmt
            .query_map(params![sid], |row| {
                let eid: String = row.get(0)?;
                let kind: String = row.get(1)?;
                Ok((eid, kind))
            })?
            .filter_map(|r| r.ok())
            .collect();

        if entity_rows.len() < 2 {
            return Ok(0);
        }

        let now = Utc::now().to_rfc3339();
        let mut edge_count = 0u32;

        // Get tools and files for this session for targeted edge building
        let tools: HashSet<&str> = entity_rows
            .iter()
            .filter(|(_, k)| k == "tool")
            .map(|(id, _)| id.as_str())
            .collect();
        let files: Vec<&str> = entity_rows
            .iter()
            .filter(|(_, k)| k == "file")
            .map(|(id, _)| id.as_str())
            .collect();
        let _branches: Vec<&str> = entity_rows
            .iter()
            .filter(|(_, k)| k == "git_branch")
            .map(|(id, _)| id.as_str())
            .collect();

        // Check for Write/Edit tools for Modifies edges
        let has_write = self.session_has_tool_named(&sid, "Write")?
            || self.session_has_tool_named(&sid, "Edit")?;

        // Build edges
        for i in 0..entity_rows.len() {
            for j in (i + 1)..entity_rows.len() {
                let (ref id_a, ref kind_a) = entity_rows[i];
                let (ref id_b, ref kind_b) = entity_rows[j];

                // CoOccurs: all entity pairs
                self.upsert_edge(id_a, id_b, EdgeKind::CoOccurs, &now)?;
                edge_count += 1;

                // Modifies: Write/Edit tool present → tool modifies files
                if has_write
                    && kind_a == "file"
                    && kind_b == "tool"
                    && (id_b == "Write" || id_b == "Edit")
                {
                    // We need to look up tool entity by name, not by id
                }

                // BranchContains: branch → file
                if kind_a == "git_branch" && kind_b == "file" {
                    self.upsert_edge(id_a, id_b, EdgeKind::BranchContains, &now)?;
                    edge_count += 1;
                } else if kind_b == "git_branch" && kind_a == "file" {
                    self.upsert_edge(id_b, id_a, EdgeKind::BranchContains, &now)?;
                    edge_count += 1;
                }
            }
        }

        // Modifies edges: if Write/Edit tool was used, connect it to all files
        if has_write {
            let write_tool_id = self.find_entity_id_by_name("Write")?;
            let edit_tool_id = self.find_entity_id_by_name("Edit")?;

            for file_id in &files {
                if let Some(ref wid) = write_tool_id
                    && tools.contains(wid.as_str())
                {
                    self.upsert_edge(wid, file_id, EdgeKind::Modifies, &now)?;
                    edge_count += 1;
                }
                if let Some(ref eid) = edit_tool_id
                    && tools.contains(eid.as_str())
                {
                    self.upsert_edge(eid, file_id, EdgeKind::Modifies, &now)?;
                    edge_count += 1;
                }
            }
        }

        Ok(edge_count)
    }

    fn session_has_tool_named(
        &self,
        session_id: &str,
        tool_name: &str,
    ) -> Result<bool, GraphError> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM session_entities se
             JOIN entities e ON se.entity_id = e.id
             WHERE se.session_id = ?1 AND e.kind = 'tool' AND e.name = ?2",
            params![session_id, tool_name],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    fn find_entity_id_by_name(&self, name: &str) -> Result<Option<String>, GraphError> {
        let mut stmt = self
            .conn
            .prepare("SELECT id FROM entities WHERE name = ?1 LIMIT 1")?;
        let mut rows = stmt.query_map(params![name], |row| {
            let id: String = row.get(0)?;
            Ok(id)
        })?;
        match rows.next() {
            Some(r) => Ok(Some(r?)),
            None => Ok(None),
        }
    }

    fn upsert_edge(
        &self,
        source_id: &str,
        target_id: &str,
        kind: EdgeKind,
        timestamp: &str,
    ) -> Result<(), GraphError> {
        self.conn.execute(
            "INSERT INTO edges (source_id, target_id, kind, weight, first_seen, last_seen)
             VALUES (?1, ?2, ?3, 1.0, ?4, ?4)
             ON CONFLICT(source_id, target_id, kind) DO UPDATE SET
                 weight = weight + 1.0,
                 last_seen = MAX(last_seen, ?4)",
            params![source_id, target_id, kind.as_str(), timestamp],
        )?;
        Ok(())
    }

    /// Delete edges for a session (used during re-extraction).
    /// Since edges are aggregated, we rebuild all edges after re-extraction.
    pub fn delete_edges_for_session(&self, _session_id: &Uuid) -> Result<(), GraphError> {
        // Edges are session-independent aggregates; full rebuild is done via rebuild_all_edges
        Ok(())
    }

    /// Rebuild all edges from session_entities data.
    pub fn rebuild_all_edges(&self) -> Result<u32, GraphError> {
        self.conn.execute("DELETE FROM edges", [])?;

        let mut stmt = self
            .conn
            .prepare("SELECT DISTINCT session_id FROM session_entities")?;
        let session_ids: Vec<String> = stmt
            .query_map([], |row| {
                let sid: String = row.get(0)?;
                Ok(sid)
            })?
            .filter_map(|r| r.ok())
            .collect();

        let mut total = 0u32;
        for sid_str in &session_ids {
            if let Ok(sid) = Uuid::parse_str(sid_str) {
                total += self.build_edges_for_session(&sid)?;
            }
        }
        Ok(total)
    }

    // ── Graph queries (Phase 2) ──

    /// Get entities related to a given entity via edges, sorted by raw weight.
    pub fn related_entities(
        &self,
        name: &str,
        limit: usize,
    ) -> Result<Vec<(Entity, EdgeKind, f64)>, GraphError> {
        let entity = match self.find_entity_by_name(name)? {
            Some(e) => e,
            None => return Ok(Vec::new()),
        };
        let eid = entity.id.to_string();

        let mut stmt = self.conn.prepare(
            "SELECT
                CASE WHEN source_id = ?1 THEN target_id ELSE source_id END as neighbor_id,
                kind, weight
             FROM edges
             WHERE source_id = ?1 OR target_id = ?1
             ORDER BY weight DESC
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![eid, limit as u32], |row| {
            let nid: String = row.get(0)?;
            let kind_str: String = row.get(1)?;
            let weight: f64 = row.get(2)?;
            Ok((nid, kind_str, weight))
        })?;

        let mut result = Vec::new();
        for row in rows {
            let (nid, kind_str, weight) = row?;
            if let (Ok(kind), Some(neighbor)) =
                (kind_str.parse::<EdgeKind>(), self.get_entity_by_id(&nid)?)
            {
                result.push((neighbor, kind, weight));
            }
        }
        Ok(result)
    }

    /// Get related entities using Pointwise Mutual Information (PMI) to filter noise.
    /// Only returns pairs with positive PMI (co-occur more than expected by chance).
    pub fn related_entities_pmi(
        &self,
        name: &str,
        limit: usize,
    ) -> Result<Vec<(Entity, f64)>, GraphError> {
        let entity = match self.find_entity_by_name(name)? {
            Some(e) => e,
            None => return Ok(Vec::new()),
        };
        let eid = entity.id.to_string();

        // Total number of sessions
        let total_sessions: f64 = self.conn.query_row(
            "SELECT COUNT(DISTINCT session_id) FROM session_entities",
            [],
            |row| row.get(0),
        )?;
        if total_sessions == 0.0 {
            return Ok(Vec::new());
        }

        // count_a: sessions containing entity a
        let count_a: f64 = self.conn.query_row(
            "SELECT COUNT(DISTINCT session_id) FROM session_entities WHERE entity_id = ?1",
            params![eid],
            |row| row.get(0),
        )?;
        if count_a == 0.0 {
            return Ok(Vec::new());
        }

        // Get co-occurring entities with their co-occurrence count
        let mut stmt = self.conn.prepare(
            "SELECT
                CASE WHEN source_id = ?1 THEN target_id ELSE source_id END as neighbor_id,
                weight
             FROM edges
             WHERE (source_id = ?1 OR target_id = ?1) AND kind = 'co_occurs'",
        )?;
        let rows = stmt.query_map(params![eid], |row| {
            let nid: String = row.get(0)?;
            let weight: f64 = row.get(1)?;
            Ok((nid, weight))
        })?;

        let mut candidates: Vec<(String, f64)> = Vec::new();
        for row in rows {
            let (nid, co_occur) = row?;
            // count_b: sessions containing entity b
            let count_b: f64 = self.conn.query_row(
                "SELECT COUNT(DISTINCT session_id) FROM session_entities WHERE entity_id = ?1",
                params![nid],
                |row| row.get(0),
            )?;
            if count_b == 0.0 {
                continue;
            }

            // PMI = log2(co_occur * total / (count_a * count_b))
            let pmi = (co_occur * total_sessions / (count_a * count_b)).log2();
            if pmi > 0.0 {
                candidates.push((nid, pmi));
            }
        }

        // Sort by PMI descending
        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        candidates.truncate(limit);

        let mut result = Vec::new();
        for (nid, pmi) in candidates {
            if let Some(neighbor) = self.get_entity_by_id(&nid)? {
                result.push((neighbor, pmi));
            }
        }
        Ok(result)
    }

    /// Get the timeline of an entity: mentions grouped by date.
    pub fn entity_timeline(&self, name: &str) -> Result<Vec<(String, u32)>, GraphError> {
        let mut stmt = self.conn.prepare(
            "SELECT DATE(em.timestamp) as day, COUNT(*) as cnt
             FROM entity_mentions em
             JOIN entities e ON em.entity_id = e.id
             WHERE e.name = ?1
             GROUP BY day
             ORDER BY day",
        )?;
        let rows = stmt.query_map(params![name], |row| {
            let day: String = row.get(0)?;
            let count: u32 = row.get(1)?;
            Ok((day, count))
        })?;
        let mut timeline = Vec::new();
        for row in rows {
            timeline.push(row?);
        }
        Ok(timeline)
    }

    /// Get session IDs that mention a given entity (for cross-db cost lookup).
    pub fn session_ids_for_entity(&self, name: &str) -> Result<Vec<Uuid>, GraphError> {
        let mut stmt = self.conn.prepare(
            "SELECT DISTINCT se.session_id
             FROM session_entities se
             JOIN entities e ON se.entity_id = e.id
             WHERE e.name = ?1",
        )?;
        let rows = stmt.query_map(params![name], |row| {
            let sid: String = row.get(0)?;
            Ok(sid)
        })?;
        let mut ids = Vec::new();
        for row in rows {
            if let Ok(sid) = Uuid::parse_str(&row?) {
                ids.push(sid);
            }
        }
        Ok(ids)
    }

    /// Get total entity count.
    pub fn entity_count(&self) -> Result<u32, GraphError> {
        let count: u32 = self
            .conn
            .query_row("SELECT COUNT(*) FROM entities", [], |row| row.get(0))?;
        Ok(count)
    }

    /// Get total edge count.
    pub fn edge_count(&self) -> Result<u32, GraphError> {
        let count: u32 = self
            .conn
            .query_row("SELECT COUNT(*) FROM edges", [], |row| row.get(0))?;
        Ok(count)
    }

    /// Get total mention count.
    pub fn mention_count(&self) -> Result<u32, GraphError> {
        let count: u32 =
            self.conn
                .query_row("SELECT COUNT(*) FROM entity_mentions", [], |row| row.get(0))?;
        Ok(count)
    }

    /// Get extracted session count.
    pub fn extracted_session_count(&self) -> Result<u32, GraphError> {
        let count: u32 = self
            .conn
            .query_row("SELECT COUNT(*) FROM extraction_log", [], |row| row.get(0))?;
        Ok(count)
    }

    /// Get all extracted session IDs (for the extract command).
    pub fn all_extracted_session_ids(&self) -> Result<HashSet<Uuid>, GraphError> {
        let mut stmt = self.conn.prepare("SELECT session_id FROM extraction_log")?;
        let rows = stmt.query_map([], |row| {
            let sid: String = row.get(0)?;
            Ok(sid)
        })?;
        let mut ids = HashSet::new();
        for row in rows {
            if let Ok(sid) = Uuid::parse_str(&row?) {
                ids.insert(sid);
            }
        }
        Ok(ids)
    }

    /// Remove entities that fail validation, along with their mentions and edges.
    /// Returns the number of entities removed.
    pub fn cleanup_invalid_entities(&self) -> Result<u32, GraphError> {
        use crate::extract::is_valid_entity_name;

        let mut stmt = self.conn.prepare("SELECT id, name FROM entities")?;
        let invalid_ids: Vec<String> = stmt
            .query_map([], |row| {
                let id: String = row.get(0)?;
                let name: String = row.get(1)?;
                Ok((id, name))
            })?
            .filter_map(|r| r.ok())
            .filter(|(_, name)| !is_valid_entity_name(name))
            .map(|(id, _)| id)
            .collect();

        let count = invalid_ids.len() as u32;
        for id in &invalid_ids {
            self.conn.execute(
                "DELETE FROM entity_mentions WHERE entity_id = ?1",
                params![id],
            )?;
            self.conn.execute(
                "DELETE FROM session_entities WHERE entity_id = ?1",
                params![id],
            )?;
            self.conn.execute(
                "DELETE FROM edges WHERE source_id = ?1 OR target_id = ?1",
                params![id],
            )?;
            self.conn
                .execute("DELETE FROM entities WHERE id = ?1", params![id])?;
        }

        Ok(count)
    }

    /// Clear all graph data (for re-extraction).
    pub fn clear_all(&self) -> Result<(), GraphError> {
        self.conn.execute_batch(
            "DELETE FROM entity_mentions;
             DELETE FROM session_entities;
             DELETE FROM edges;
             DELETE FROM entities;
             DELETE FROM extraction_log;",
        )?;
        Ok(())
    }

    // ── Helpers ──

    fn get_entity_by_id(&self, id: &str) -> Result<Option<Entity>, GraphError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, kind, name, display_name, first_seen, last_seen, mention_count, session_count,
                    COALESCE(pagerank, 0.0), community_id
             FROM entities WHERE id = ?1",
        )?;
        let mut rows = stmt.query_map(params![id], Self::row_to_entity)?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    fn row_to_entity(row: &rusqlite::Row) -> rusqlite::Result<Entity> {
        let id_str: String = row.get(0)?;
        let kind_str: String = row.get(1)?;
        let name: String = row.get(2)?;
        let display_name: String = row.get(3)?;
        let first_seen_str: String = row.get(4)?;
        let last_seen_str: String = row.get(5)?;
        let mention_count: u32 = row.get(6)?;
        let session_count: u32 = row.get(7)?;
        let pagerank: f64 = row.get::<_, f64>(8).unwrap_or(0.0);
        let community_id: Option<String> = row.get::<_, Option<String>>(9).unwrap_or(None);

        let id = Uuid::parse_str(&id_str).unwrap_or_else(|_| Uuid::nil());
        let kind = kind_str
            .parse::<EntityKind>()
            .unwrap_or(EntityKind::Concept);
        let first_seen = DateTime::parse_from_rfc3339(&first_seen_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());
        let last_seen = DateTime::parse_from_rfc3339(&last_seen_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());

        Ok(Entity {
            id,
            kind,
            name,
            display_name,
            first_seen,
            last_seen,
            mention_count,
            session_count,
            pagerank,
            community_id,
        })
    }

    // ── Alias operations ──

    /// Register an alias that maps to a canonical entity ID.
    pub fn register_alias(&self, alias_name: &str, canonical_id: &str) -> Result<(), GraphError> {
        self.conn.execute(
            "INSERT OR REPLACE INTO entity_aliases (alias_name, canonical_id)
             VALUES (?1, ?2)",
            params![alias_name, canonical_id],
        )?;
        Ok(())
    }

    /// Resolve an alias to a canonical entity ID, if one exists.
    pub fn resolve_alias(&self, alias_name: &str) -> Result<Option<String>, GraphError> {
        let mut stmt = self
            .conn
            .prepare("SELECT canonical_id FROM entity_aliases WHERE alias_name = ?1")?;
        let mut rows = stmt.query_map(params![alias_name], |row| {
            let cid: String = row.get(0)?;
            Ok(cid)
        })?;
        match rows.next() {
            Some(r) => Ok(Some(r?)),
            None => Ok(None),
        }
    }

    // ── Edge decay ──

    /// Apply temporal decay to all edge weights.
    /// Each call multiplies all weights by the given factor (e.g. 0.95).
    pub fn apply_edge_decay(&self, factor: f64) -> Result<(), GraphError> {
        self.conn
            .execute("UPDATE edges SET weight = weight * ?1", params![factor])?;
        Ok(())
    }

    // ── PageRank ──

    /// Compute PageRank for all entities and write back to the entities table.
    pub fn compute_pagerank(&self) -> Result<(), GraphError> {
        let damping = 0.85f64;
        let max_iter = 20;
        let epsilon = 1e-6;

        // Load all entity IDs
        let mut stmt = self.conn.prepare("SELECT id FROM entities")?;
        let entity_ids: Vec<String> = stmt
            .query_map([], |row| {
                let id: String = row.get(0)?;
                Ok(id)
            })?
            .filter_map(|r| r.ok())
            .collect();

        let n = entity_ids.len();
        if n == 0 {
            return Ok(());
        }

        // Map entity ID → index
        let id_to_idx: std::collections::HashMap<&str, usize> = entity_ids
            .iter()
            .enumerate()
            .map(|(i, id)| (id.as_str(), i))
            .collect();

        // Build adjacency: for each node, collect outgoing neighbors with weights
        let mut out_edges: Vec<Vec<(usize, f64)>> = vec![Vec::new(); n];
        let mut out_weight: Vec<f64> = vec![0.0; n];

        let mut edge_stmt = self
            .conn
            .prepare("SELECT source_id, target_id, weight FROM edges")?;
        let edges: Vec<(String, String, f64)> = edge_stmt
            .query_map([], |row| {
                let s: String = row.get(0)?;
                let t: String = row.get(1)?;
                let w: f64 = row.get(2)?;
                Ok((s, t, w))
            })?
            .filter_map(|r| r.ok())
            .collect();

        for (src, tgt, w) in &edges {
            if let (Some(&si), Some(&ti)) =
                (id_to_idx.get(src.as_str()), id_to_idx.get(tgt.as_str()))
            {
                out_edges[si].push((ti, *w));
                out_weight[si] += w;
                // Treat as undirected for co-occurrence graph
                out_edges[ti].push((si, *w));
                out_weight[ti] += w;
            }
        }

        // Initialize PageRank uniformly
        let init = 1.0 / n as f64;
        let mut pr = vec![init; n];
        let mut new_pr = vec![0.0; n];

        for _ in 0..max_iter {
            let base = (1.0 - damping) / n as f64;
            for val in new_pr.iter_mut() {
                *val = base;
            }

            for i in 0..n {
                if out_weight[i] > 0.0 {
                    for &(j, w) in &out_edges[i] {
                        new_pr[j] += damping * pr[i] * w / out_weight[i];
                    }
                } else {
                    // Dangling node: distribute evenly
                    let share = damping * pr[i] / n as f64;
                    for val in new_pr.iter_mut() {
                        *val += share;
                    }
                }
            }

            // Check convergence
            let diff: f64 = pr
                .iter()
                .zip(new_pr.iter())
                .map(|(a, b)| (a - b).abs())
                .sum();
            std::mem::swap(&mut pr, &mut new_pr);
            if diff < epsilon {
                break;
            }
        }

        // Write back to DB
        let tx = self.conn.unchecked_transaction()?;
        {
            let mut update = tx.prepare("UPDATE entities SET pagerank = ?1 WHERE id = ?2")?;
            for (i, id) in entity_ids.iter().enumerate() {
                update.execute(params![pr[i], id])?;
            }
        }
        tx.commit()?;

        Ok(())
    }

    // ── Community detection (Label Propagation) ──

    /// Compute communities using weighted Label Propagation and write to entities.
    pub fn compute_communities(&self) -> Result<(), GraphError> {
        let max_iter = 10;

        // Load all entity IDs
        let mut stmt = self.conn.prepare("SELECT id FROM entities")?;
        let entity_ids: Vec<String> = stmt
            .query_map([], |row| {
                let id: String = row.get(0)?;
                Ok(id)
            })?
            .filter_map(|r| r.ok())
            .collect();

        let n = entity_ids.len();
        if n == 0 {
            return Ok(());
        }

        let id_to_idx: std::collections::HashMap<&str, usize> = entity_ids
            .iter()
            .enumerate()
            .map(|(i, id)| (id.as_str(), i))
            .collect();

        // Build weighted adjacency list
        let mut neighbors: Vec<Vec<(usize, f64)>> = vec![Vec::new(); n];

        let mut edge_stmt = self
            .conn
            .prepare("SELECT source_id, target_id, weight FROM edges")?;
        let edges: Vec<(String, String, f64)> = edge_stmt
            .query_map([], |row| {
                let s: String = row.get(0)?;
                let t: String = row.get(1)?;
                let w: f64 = row.get(2)?;
                Ok((s, t, w))
            })?
            .filter_map(|r| r.ok())
            .collect();

        for (src, tgt, w) in &edges {
            if let (Some(&si), Some(&ti)) =
                (id_to_idx.get(src.as_str()), id_to_idx.get(tgt.as_str()))
            {
                neighbors[si].push((ti, *w));
                neighbors[ti].push((si, *w));
            }
        }

        // Initialize: each node in its own community (labeled by index)
        let mut labels: Vec<usize> = (0..n).collect();

        for _ in 0..max_iter {
            let mut changed = false;

            for i in 0..n {
                if neighbors[i].is_empty() {
                    continue;
                }

                // Weighted vote: sum weights per neighbor label
                let mut votes: std::collections::HashMap<usize, f64> =
                    std::collections::HashMap::new();
                for &(j, w) in &neighbors[i] {
                    *votes.entry(labels[j]).or_insert(0.0) += w;
                }

                // Pick label with highest weight
                let best_label = votes
                    .into_iter()
                    .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
                    .map(|(label, _)| label)
                    .unwrap_or(labels[i]);

                if best_label != labels[i] {
                    labels[i] = best_label;
                    changed = true;
                }
            }

            if !changed {
                break;
            }
        }

        // Remap labels to community IDs (use the entity ID of the label's node)
        let tx = self.conn.unchecked_transaction()?;
        {
            let mut update = tx.prepare("UPDATE entities SET community_id = ?1 WHERE id = ?2")?;
            for (i, id) in entity_ids.iter().enumerate() {
                let community = &entity_ids[labels[i]];
                update.execute(params![community, id])?;
            }
        }
        tx.commit()?;

        Ok(())
    }

    /// Get entities associated with a session, ordered by mention_count descending.
    pub fn entities_for_session(
        &self,
        session_id: &Uuid,
        limit: usize,
    ) -> Result<Vec<(Entity, u32)>, GraphError> {
        let mut stmt = self.conn.prepare(
            "SELECT e.id, e.kind, e.name, e.display_name, e.first_seen, e.last_seen,
                    e.mention_count, e.session_count, COALESCE(e.pagerank, 0.0), e.community_id,
                    se.mention_count
             FROM session_entities se
             JOIN entities e ON se.entity_id = e.id
             WHERE se.session_id = ?1
             ORDER BY se.mention_count DESC
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![session_id.to_string(), limit as u32], |row| {
            let entity = Self::row_to_entity(row)?;
            let session_mention_count: u32 = row.get(10)?;
            Ok((entity, session_mention_count))
        })?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    /// List communities with their members, sorted by size descending.
    pub fn list_communities(&self) -> Result<Vec<(String, Vec<Entity>)>, GraphError> {
        let mut stmt = self.conn.prepare(
            "SELECT community_id, COUNT(*) as cnt
             FROM entities
             WHERE community_id IS NOT NULL
             GROUP BY community_id
             ORDER BY cnt DESC",
        )?;
        let community_ids: Vec<String> = stmt
            .query_map([], |row| {
                let cid: String = row.get(0)?;
                Ok(cid)
            })?
            .filter_map(|r| r.ok())
            .collect();

        let mut result = Vec::new();
        for cid in community_ids {
            let mut member_stmt = self.conn.prepare(
                "SELECT id, kind, name, display_name, first_seen, last_seen, mention_count, session_count,
                        COALESCE(pagerank, 0.0), community_id
                 FROM entities
                 WHERE community_id = ?1
                 ORDER BY pagerank DESC, mention_count DESC",
            )?;
            let members: Vec<Entity> = member_stmt
                .query_map(params![cid], Self::row_to_entity)?
                .filter_map(|r| r.ok())
                .collect();
            if members.len() >= 2 {
                result.push((cid, members));
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn make_entity(kind: EntityKind, name: &str) -> Entity {
        Entity::new(kind, name.to_string(), name.to_string(), Utc::now())
    }

    fn make_mention(entity: &Entity, session_id: Uuid) -> EntityMention {
        EntityMention {
            entity_id: entity.id,
            session_id,
            message_id: None,
            timestamp: Utc::now(),
            context: "test context".to_string(),
            role: MentionRole::Unknown,
        }
    }

    #[test]
    fn record_and_query_entity() {
        let store = GraphStore::open_in_memory().unwrap();
        let session_id = Uuid::new_v4();

        let entity = make_entity(EntityKind::File, "src/main.rs");
        let mention = make_mention(&entity, session_id);

        store.record_mention(&entity, &mention).unwrap();

        let found = store.find_entity_by_name("src/main.rs").unwrap();
        assert!(found.is_some());
        let found = found.unwrap();
        assert_eq!(found.kind, EntityKind::File);
        assert_eq!(found.mention_count, 1);
    }

    #[test]
    fn multiple_mentions_increment_count() {
        let store = GraphStore::open_in_memory().unwrap();
        let session_id = Uuid::new_v4();

        let entity = make_entity(EntityKind::File, "src/lib.rs");
        let mention1 = make_mention(&entity, session_id);
        let mention2 = make_mention(&entity, session_id);

        store.record_mention(&entity, &mention1).unwrap();
        store.record_mention(&entity, &mention2).unwrap();

        let found = store.find_entity_by_name("src/lib.rs").unwrap().unwrap();
        assert_eq!(found.mention_count, 2);
    }

    #[test]
    fn entity_stats() {
        let store = GraphStore::open_in_memory().unwrap();
        let sid = Uuid::new_v4();

        let file1 = make_entity(EntityKind::File, "src/a.rs");
        let file2 = make_entity(EntityKind::File, "src/b.rs");
        let tool = make_entity(EntityKind::Tool, "Read");

        store
            .record_mention(&file1, &make_mention(&file1, sid))
            .unwrap();
        store
            .record_mention(&file2, &make_mention(&file2, sid))
            .unwrap();
        store
            .record_mention(&tool, &make_mention(&tool, sid))
            .unwrap();

        let stats = store.entity_stats().unwrap();
        let file_count = stats
            .iter()
            .find(|(k, _)| *k == EntityKind::File)
            .unwrap()
            .1;
        let tool_count = stats
            .iter()
            .find(|(k, _)| *k == EntityKind::Tool)
            .unwrap()
            .1;
        assert_eq!(file_count, 2);
        assert_eq!(tool_count, 1);
    }

    #[test]
    fn sessions_for_entity_returns_correct_data() {
        let store = GraphStore::open_in_memory().unwrap();
        let sid1 = Uuid::new_v4();
        let sid2 = Uuid::new_v4();

        let entity = make_entity(EntityKind::File, "src/main.rs");
        store
            .record_mention(&entity, &make_mention(&entity, sid1))
            .unwrap();
        store
            .record_mention(&entity, &make_mention(&entity, sid2))
            .unwrap();

        let sessions = store.sessions_for_entity("src/main.rs").unwrap();
        assert_eq!(sessions.len(), 2);
    }

    #[test]
    fn delete_mentions_for_session() {
        let store = GraphStore::open_in_memory().unwrap();
        let sid = Uuid::new_v4();

        let entity = make_entity(EntityKind::File, "src/main.rs");
        store
            .record_mention(&entity, &make_mention(&entity, sid))
            .unwrap();
        store.mark_session_extracted(&sid, 1).unwrap();

        assert!(store.session_extracted(&sid).unwrap());

        store.delete_mentions_for_session(&sid).unwrap();

        assert!(!store.session_extracted(&sid).unwrap());
        let mentions = store.get_mentions("src/main.rs", 10).unwrap();
        assert!(mentions.is_empty());
    }

    #[test]
    fn list_entities_by_kind() {
        let store = GraphStore::open_in_memory().unwrap();
        let sid = Uuid::new_v4();

        let file = make_entity(EntityKind::File, "src/main.rs");
        let tool = make_entity(EntityKind::Tool, "Read");
        store
            .record_mention(&file, &make_mention(&file, sid))
            .unwrap();
        store
            .record_mention(&tool, &make_mention(&tool, sid))
            .unwrap();

        let files = store.list_entities(Some(EntityKind::File), 10).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].kind, EntityKind::File);
    }

    #[test]
    fn edge_building_and_related_query() {
        let store = GraphStore::open_in_memory().unwrap();
        let sid = Uuid::new_v4();

        let file1 = make_entity(EntityKind::File, "src/a.rs");
        let file2 = make_entity(EntityKind::File, "src/b.rs");
        let tool = make_entity(EntityKind::Tool, "Read");

        store
            .record_mention(&file1, &make_mention(&file1, sid))
            .unwrap();
        store
            .record_mention(&file2, &make_mention(&file2, sid))
            .unwrap();
        store
            .record_mention(&tool, &make_mention(&tool, sid))
            .unwrap();

        let edges = store.build_edges_for_session(&sid).unwrap();
        assert!(edges > 0, "should build edges");

        let related = store.related_entities("src/a.rs", 10).unwrap();
        assert!(!related.is_empty(), "should have related entities");
        let names: Vec<_> = related.iter().map(|(e, _, _)| e.name.as_str()).collect();
        assert!(names.contains(&"src/b.rs"), "names: {names:?}");
    }

    #[test]
    fn entity_timeline() {
        let store = GraphStore::open_in_memory().unwrap();
        let sid = Uuid::new_v4();

        let entity = make_entity(EntityKind::File, "src/main.rs");
        store
            .record_mention(&entity, &make_mention(&entity, sid))
            .unwrap();

        let timeline = store.entity_timeline("src/main.rs").unwrap();
        assert_eq!(timeline.len(), 1);
        assert_eq!(timeline[0].1, 1);
    }

    #[test]
    fn clear_all_removes_everything() {
        let store = GraphStore::open_in_memory().unwrap();
        let sid = Uuid::new_v4();

        let entity = make_entity(EntityKind::File, "src/main.rs");
        store
            .record_mention(&entity, &make_mention(&entity, sid))
            .unwrap();
        store.mark_session_extracted(&sid, 1).unwrap();

        store.clear_all().unwrap();

        assert_eq!(store.entity_count().unwrap(), 0);
        assert_eq!(store.mention_count().unwrap(), 0);
        assert_eq!(store.extracted_session_count().unwrap(), 0);
    }

    #[test]
    fn mention_role_stored_and_retrieved() {
        let store = GraphStore::open_in_memory().unwrap();
        let sid = Uuid::new_v4();

        let entity = make_entity(EntityKind::File, "src/main.rs");
        let mut mention = make_mention(&entity, sid);
        mention.role = MentionRole::Definition;

        store.record_mention(&entity, &mention).unwrap();

        let mentions = store.get_mentions("src/main.rs", 10).unwrap();
        assert_eq!(mentions.len(), 1);
        assert_eq!(mentions[0].role, MentionRole::Definition);
    }

    #[test]
    fn alias_registration_and_resolution() {
        let store = GraphStore::open_in_memory().unwrap();
        let sid = Uuid::new_v4();

        let entity = make_entity(EntityKind::File, "src/main.rs");
        store
            .record_mention(&entity, &make_mention(&entity, sid))
            .unwrap();

        // Register alias
        store
            .register_alias("main.rs", &entity.id.to_string())
            .unwrap();

        // Resolve by alias
        let found = store.find_entity_by_name("main.rs").unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "src/main.rs");
    }

    #[test]
    fn edge_decay_reduces_weights() {
        let store = GraphStore::open_in_memory().unwrap();
        let sid = Uuid::new_v4();

        let file1 = make_entity(EntityKind::File, "src/a.rs");
        let file2 = make_entity(EntityKind::File, "src/b.rs");
        store
            .record_mention(&file1, &make_mention(&file1, sid))
            .unwrap();
        store
            .record_mention(&file2, &make_mention(&file2, sid))
            .unwrap();
        store.build_edges_for_session(&sid).unwrap();

        let before = store.related_entities("src/a.rs", 10).unwrap();
        let weight_before = before[0].2;

        store.apply_edge_decay(0.5).unwrap();

        let after = store.related_entities("src/a.rs", 10).unwrap();
        let weight_after = after[0].2;

        assert!((weight_after - weight_before * 0.5).abs() < 0.01);
    }

    #[test]
    fn pagerank_computes_without_error() {
        let store = GraphStore::open_in_memory().unwrap();
        let sid = Uuid::new_v4();

        let file1 = make_entity(EntityKind::File, "src/a.rs");
        let file2 = make_entity(EntityKind::File, "src/b.rs");
        let file3 = make_entity(EntityKind::File, "src/c.rs");
        store
            .record_mention(&file1, &make_mention(&file1, sid))
            .unwrap();
        store
            .record_mention(&file2, &make_mention(&file2, sid))
            .unwrap();
        store
            .record_mention(&file3, &make_mention(&file3, sid))
            .unwrap();
        store.build_edges_for_session(&sid).unwrap();

        store.compute_pagerank().unwrap();

        let entities = store.list_entities(None, 10).unwrap();
        assert!(!entities.is_empty());
        // All should have non-zero pagerank
        for e in &entities {
            assert!(e.pagerank > 0.0, "entity {} has zero pagerank", e.name);
        }
    }

    #[test]
    fn community_detection_assigns_communities() {
        let store = GraphStore::open_in_memory().unwrap();
        let sid = Uuid::new_v4();

        let file1 = make_entity(EntityKind::File, "src/a.rs");
        let file2 = make_entity(EntityKind::File, "src/b.rs");
        store
            .record_mention(&file1, &make_mention(&file1, sid))
            .unwrap();
        store
            .record_mention(&file2, &make_mention(&file2, sid))
            .unwrap();
        store.build_edges_for_session(&sid).unwrap();

        store.compute_communities().unwrap();

        let e1 = store.find_entity_by_name("src/a.rs").unwrap().unwrap();
        let e2 = store.find_entity_by_name("src/b.rs").unwrap().unwrap();
        assert!(e1.community_id.is_some());
        assert!(e2.community_id.is_some());
        // Connected entities should be in the same community
        assert_eq!(e1.community_id, e2.community_id);
    }

    #[test]
    fn entities_for_session_empty() {
        let store = GraphStore::open_in_memory().unwrap();
        let sid = Uuid::new_v4();
        let result = store.entities_for_session(&sid, 10).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn entities_for_session_returns_ordered() {
        let store = GraphStore::open_in_memory().unwrap();
        let sid = Uuid::new_v4();

        let file = make_entity(EntityKind::File, "src/auth.rs");
        let func = make_entity(EntityKind::Function, "validate_token");
        let concept = make_entity(EntityKind::Concept, "authentication");

        // Record different numbers of mentions per entity in same session
        store
            .record_mention(&file, &make_mention(&file, sid))
            .unwrap();
        store
            .record_mention(&file, &make_mention(&file, sid))
            .unwrap();
        store
            .record_mention(&file, &make_mention(&file, sid))
            .unwrap(); // 3

        store
            .record_mention(&func, &make_mention(&func, sid))
            .unwrap(); // 1

        store
            .record_mention(&concept, &make_mention(&concept, sid))
            .unwrap();
        store
            .record_mention(&concept, &make_mention(&concept, sid))
            .unwrap(); // 2

        let result = store.entities_for_session(&sid, 10).unwrap();
        assert_eq!(result.len(), 3);
        // Ordered by session mention_count DESC: file(3), concept(2), func(1)
        assert_eq!(result[0].0.name, "src/auth.rs");
        assert_eq!(result[0].1, 3);
        assert_eq!(result[1].0.name, "authentication");
        assert_eq!(result[1].1, 2);
        assert_eq!(result[2].0.name, "validate_token");
        assert_eq!(result[2].1, 1);
    }

    #[test]
    fn entities_for_session_respects_limit() {
        let store = GraphStore::open_in_memory().unwrap();
        let sid = Uuid::new_v4();

        let file = make_entity(EntityKind::File, "src/auth.rs");
        let func = make_entity(EntityKind::Function, "validate_token");
        let concept = make_entity(EntityKind::Concept, "authentication");

        store
            .record_mention(&file, &make_mention(&file, sid))
            .unwrap();
        store
            .record_mention(&func, &make_mention(&func, sid))
            .unwrap();
        store
            .record_mention(&concept, &make_mention(&concept, sid))
            .unwrap();

        let result = store.entities_for_session(&sid, 2).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn pmi_filters_noise() {
        let store = GraphStore::open_in_memory().unwrap();
        let sid1 = Uuid::new_v4();
        let sid2 = Uuid::new_v4();
        let sid3 = Uuid::new_v4();

        let file_a = make_entity(EntityKind::File, "src/a.rs");
        let file_b = make_entity(EntityKind::File, "src/b.rs");
        // tool_read appears in all 3 sessions (noise)
        let tool = make_entity(EntityKind::Tool, "Read");
        let file_c = make_entity(EntityKind::File, "src/c.rs");

        // Session 1: a + b + Read
        store
            .record_mention(&file_a, &make_mention(&file_a, sid1))
            .unwrap();
        store
            .record_mention(&file_b, &make_mention(&file_b, sid1))
            .unwrap();
        store
            .record_mention(&tool, &make_mention(&tool, sid1))
            .unwrap();
        store.build_edges_for_session(&sid1).unwrap();

        // Session 2: a + b + Read
        store
            .record_mention(&file_a, &make_mention(&file_a, sid2))
            .unwrap();
        store
            .record_mention(&file_b, &make_mention(&file_b, sid2))
            .unwrap();
        store
            .record_mention(&tool, &make_mention(&tool, sid2))
            .unwrap();
        store.build_edges_for_session(&sid2).unwrap();

        // Session 3: c + Read (without a or b → Read is ubiquitous noise)
        store
            .record_mention(&file_c, &make_mention(&file_c, sid3))
            .unwrap();
        store
            .record_mention(&tool, &make_mention(&tool, sid3))
            .unwrap();
        store.build_edges_for_session(&sid3).unwrap();

        let pmi_results = store.related_entities_pmi("src/a.rs", 10).unwrap();
        assert!(!pmi_results.is_empty());
        // file_b should rank higher than Read tool due to PMI
        let names: Vec<_> = pmi_results.iter().map(|(e, _)| e.name.as_str()).collect();
        assert!(names.contains(&"src/b.rs"), "names: {names:?}");
        if names.contains(&"Read") {
            // If Read is present, file_b should rank higher (higher PMI)
            let b_idx = names.iter().position(|n| *n == "src/b.rs").unwrap();
            let read_idx = names.iter().position(|n| *n == "Read").unwrap();
            assert!(
                b_idx < read_idx,
                "file_b should rank higher than Read via PMI"
            );
        }
    }
}
