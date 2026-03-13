use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeKind {
    /// Two entities appear in the same session
    CoOccurs,
    /// A Write/Edit tool was used on a file
    Modifies,
    /// A file was accessed via a tool
    UsesTool,
    /// File depends on a module/crate
    DependsOn,
    /// A git branch contains references to a file
    BranchContains,
}

impl EdgeKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::CoOccurs => "co_occurs",
            Self::Modifies => "modifies",
            Self::UsesTool => "uses_tool",
            Self::DependsOn => "depends_on",
            Self::BranchContains => "branch_contains",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "co_occurs" => Some(Self::CoOccurs),
            "modifies" => Some(Self::Modifies),
            "uses_tool" => Some(Self::UsesTool),
            "depends_on" => Some(Self::DependsOn),
            "branch_contains" => Some(Self::BranchContains),
            _ => None,
        }
    }
}

impl std::fmt::Display for EdgeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone)]
pub struct Edge {
    pub id: i64,
    pub source_id: Uuid,
    pub target_id: Uuid,
    pub kind: EdgeKind,
    pub weight: f64,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
}
