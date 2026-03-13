use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Namespace UUID for deterministic v5 IDs: `voyage-graph-entity`
const ENTITY_NAMESPACE: Uuid = Uuid::from_bytes([
    0x6b, 0x9f, 0x2a, 0x1e, 0x3c, 0x7d, 0x4f, 0x8a, 0xb5, 0xd1, 0xe2, 0xf4, 0x06, 0x18, 0x2a, 0x3c,
]);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MentionRole {
    Definition,
    Reference,
    Modification,
    Unknown,
}

impl MentionRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Definition => "definition",
            Self::Reference => "reference",
            Self::Modification => "modification",
            Self::Unknown => "unknown",
        }
    }
}

impl std::str::FromStr for MentionRole {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "definition" => Self::Definition,
            "reference" => Self::Reference,
            "modification" => Self::Modification,
            _ => Self::Unknown,
        })
    }
}

impl std::fmt::Display for MentionRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityKind {
    File,
    Function,
    Module,
    Concept,
    Tool,
    Error,
    Dependency,
    GitBranch,
}

impl EntityKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Function => "function",
            Self::Module => "module",
            Self::Concept => "concept",
            Self::Tool => "tool",
            Self::Error => "error",
            Self::Dependency => "dependency",
            Self::GitBranch => "git_branch",
        }
    }

    pub fn all() -> &'static [EntityKind] {
        &[
            Self::File,
            Self::Function,
            Self::Module,
            Self::Concept,
            Self::Tool,
            Self::Error,
            Self::Dependency,
            Self::GitBranch,
        ]
    }
}

impl std::fmt::Display for EntityKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for EntityKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "file" => Ok(Self::File),
            "function" => Ok(Self::Function),
            "module" => Ok(Self::Module),
            "concept" => Ok(Self::Concept),
            "tool" => Ok(Self::Tool),
            "error" => Ok(Self::Error),
            "dependency" => Ok(Self::Dependency),
            "git_branch" => Ok(Self::GitBranch),
            _ => Err(format!("Unknown entity kind: {s}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entity {
    pub id: Uuid,
    pub kind: EntityKind,
    pub name: String,
    pub display_name: String,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub mention_count: u32,
    pub session_count: u32,
    pub pagerank: f64,
    pub community_id: Option<String>,
}

impl Entity {
    pub fn new(
        kind: EntityKind,
        name: String,
        display_name: String,
        timestamp: DateTime<Utc>,
    ) -> Self {
        Self {
            id: deterministic_id(kind, &name),
            kind,
            name,
            display_name,
            first_seen: timestamp,
            last_seen: timestamp,
            mention_count: 0,
            session_count: 0,
            pagerank: 0.0,
            community_id: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EntityMention {
    pub entity_id: Uuid,
    pub session_id: Uuid,
    pub message_id: Option<Uuid>,
    pub timestamp: DateTime<Utc>,
    pub context: String,
    pub role: MentionRole,
}

/// Generate a deterministic UUID v5 from kind + canonical name.
/// Same entity across sessions always gets the same ID.
pub fn deterministic_id(kind: EntityKind, name: &str) -> Uuid {
    let input = format!("{}:{}", kind.as_str(), name);
    Uuid::new_v5(&ENTITY_NAMESPACE, input.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_id_is_stable() {
        let id1 = deterministic_id(EntityKind::File, "src/main.rs");
        let id2 = deterministic_id(EntityKind::File, "src/main.rs");
        assert_eq!(id1, id2);
    }

    #[test]
    fn deterministic_id_differs_by_kind() {
        let file_id = deterministic_id(EntityKind::File, "auth");
        let concept_id = deterministic_id(EntityKind::Concept, "auth");
        assert_ne!(file_id, concept_id);
    }

    #[test]
    fn entity_kind_roundtrip() {
        for kind in EntityKind::all() {
            let s = kind.as_str();
            let parsed: EntityKind = s.parse().unwrap();
            assert_eq!(*kind, parsed);
        }
    }
}
