use std::path::Path;

use voyage_core::model::Session;

use crate::claude_code::ParseError;

pub trait SessionParser {
    fn parse_file(&self, path: &Path) -> Result<Session, ParseError>;
    fn discover_sessions(&self, base_dir: &Path) -> Result<Vec<std::path::PathBuf>, ParseError>;
}
