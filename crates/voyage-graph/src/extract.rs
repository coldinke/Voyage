use chrono::{DateTime, Utc};
use regex::Regex;
use uuid::Uuid;

use crate::entity::{Entity, EntityKind, EntityMention, MentionRole};

/// Context needed for entity extraction from a single message.
pub struct ExtractionContext {
    pub session_id: Uuid,
    pub message_id: Option<Uuid>,
    pub timestamp: DateTime<Utc>,
    pub cwd: String,
    pub git_branch: Option<String>,
    pub tool_calls: Vec<String>,
    pub is_user_message: bool,
}

/// Result of extracting entities from a message.
pub struct Extraction {
    pub entities: Vec<(Entity, EntityMention)>,
}

/// Software concepts whitelist for concept extraction (user messages only).
const CONCEPTS: &[&str] = &[
    "authentication",
    "authorization",
    "caching",
    "database",
    "testing",
    "deployment",
    "refactoring",
    "debugging",
    "logging",
    "monitoring",
    "migration",
    "security",
    "performance",
    "optimization",
    "serialization",
    "deserialization",
    "concurrency",
    "parallelism",
    "networking",
    "routing",
    "middleware",
    "validation",
    "pagination",
    "indexing",
    "encryption",
    "hashing",
    "compression",
    "streaming",
    "websocket",
    "graphql",
    "rest api",
    "grpc",
    "docker",
    "kubernetes",
    "ci/cd",
    "linting",
    "formatting",
    "bundling",
    "transpiling",
    "compilation",
    "linking",
    "packaging",
    "versioning",
    "rate limiting",
    "load balancing",
    "error handling",
    "state management",
    "dependency injection",
    "configuration",
];

/// File extensions we recognize as valid source files.
const VALID_EXTENSIONS: &[&str] = &[
    "rs", "py", "js", "ts", "tsx", "jsx", "go", "java", "c", "cpp", "h", "hpp", "cs", "rb",
    "php", "swift", "kt", "scala", "zig", "lua", "sh", "bash", "zsh", "fish", "yaml", "yml",
    "toml", "json", "xml", "html", "css", "scss", "sass", "less", "sql", "md", "txt", "cfg",
    "ini", "env", "lock", "mod", "sum", "gradle", "cmake", "make", "dockerfile", "proto",
    "graphql", "svelte", "vue",
];

/// Paths that indicate system/non-project files.
const EXCLUDED_PATH_PREFIXES: &[&str] = &[
    "/usr/", "/proc/", "/sys/", "/etc/", "/var/", "/dev/", "/tmp/", "/opt/", "/bin/", "/sbin/",
    "http://", "https://", "ftp://",
];

/// Extract all entities from a message's content given context.
pub fn extract_entities(content: &str, ctx: &ExtractionContext) -> Extraction {
    let mut entities = Vec::new();

    extract_file_paths(content, ctx, &mut entities);
    extract_tools(ctx, &mut entities);
    extract_git_branch(ctx, &mut entities);

    // Derive file_context from the last extracted file entity for function qualification
    let file_context: Option<String> = entities
        .iter()
        .rev()
        .find(|(e, _)| e.kind == EntityKind::File)
        .map(|(e, _)| e.display_name.clone());

    extract_functions(content, ctx, file_context.as_deref(), &mut entities);
    extract_dependencies(content, ctx, &mut entities);
    extract_errors(content, ctx, &mut entities);

    if ctx.is_user_message {
        extract_concepts(content, ctx, &mut entities);
    }

    Extraction { entities }
}

/// Extract context snippet around a match position.
fn context_snippet(content: &str, start: usize, end: usize) -> String {
    let ctx_start = content[..start]
        .char_indices()
        .rev()
        .nth(50)
        .map(|(i, _)| i)
        .unwrap_or(0);
    let ctx_end = content[end..]
        .char_indices()
        .nth(50)
        .map(|(i, _)| end + i)
        .unwrap_or(content.len());
    let snippet = &content[ctx_start..ctx_end];
    snippet.replace('\n', " ").trim().to_string()
}

fn make_mention(entity: &Entity, ctx: &ExtractionContext, context: String, role: MentionRole) -> EntityMention {
    EntityMention {
        entity_id: entity.id,
        session_id: ctx.session_id,
        message_id: ctx.message_id,
        timestamp: ctx.timestamp,
        context,
        role,
    }
}

fn extract_file_paths(
    content: &str,
    ctx: &ExtractionContext,
    out: &mut Vec<(Entity, EntityMention)>,
) {
    let re = Regex::new(
        r#"(?:[\s"'`(\[,=:]|^)((?:\.{0,2}/)?(?:[\w.@+\-]+/)*[\w.@+\-]+\.[a-zA-Z]{1,10})(?:[\s"'`):,\]\n]|$)"#,
    )
    .unwrap();

    let mut seen = std::collections::HashSet::new();

    for cap in re.captures_iter(content) {
        let m = cap.get(1).unwrap();
        let raw_path = m.as_str();

        // Filter out excluded prefixes
        if EXCLUDED_PATH_PREFIXES.iter().any(|p| raw_path.starts_with(p)) {
            continue;
        }

        // Must have a valid extension
        let ext = raw_path.rsplit('.').next().unwrap_or("").to_lowercase();
        if !VALID_EXTENSIONS.contains(&ext.as_str()) {
            continue;
        }

        // Filter out obvious non-paths (version numbers like v1.0, single-component dot files)
        if raw_path.starts_with("v") && raw_path[1..].chars().next().is_some_and(|c| c.is_ascii_digit()) {
            continue;
        }

        // Normalize path relative to cwd
        let normalized = normalize_path(raw_path, &ctx.cwd);

        if !seen.insert(normalized.clone()) {
            continue;
        }

        let display = display_name_from_path(&normalized);
        let entity = Entity::new(EntityKind::File, normalized, display, ctx.timestamp);
        let role = if ctx.tool_calls.iter().any(|t| t == "Write" || t == "Edit") {
            MentionRole::Modification
        } else {
            MentionRole::Reference
        };
        let mention = make_mention(&entity, ctx, context_snippet(content, m.start(), m.end()), role);
        out.push((entity, mention));
    }
}

fn extract_tools(ctx: &ExtractionContext, out: &mut Vec<(Entity, EntityMention)>) {
    for tool in &ctx.tool_calls {
        let entity = Entity::new(
            EntityKind::Tool,
            tool.clone(),
            tool.clone(),
            ctx.timestamp,
        );
        let mention = make_mention(&entity, ctx, format!("tool_call: {tool}"), MentionRole::Reference);
        out.push((entity, mention));
    }
}

fn extract_git_branch(ctx: &ExtractionContext, out: &mut Vec<(Entity, EntityMention)>) {
    if let Some(branch) = &ctx.git_branch {
        if !branch.is_empty() {
            let entity = Entity::new(
                EntityKind::GitBranch,
                branch.clone(),
                branch.clone(),
                ctx.timestamp,
            );
            let mention = make_mention(&entity, ctx, format!("branch: {branch}"), MentionRole::Reference);
            out.push((entity, mention));
        }
    }
}

fn extract_functions(
    content: &str,
    ctx: &ExtractionContext,
    file_context: Option<&str>,
    out: &mut Vec<(Entity, EntityMention)>,
) {
    let patterns = [
        // Rust
        r"\bfn\s+(\w+)",
        r"\bstruct\s+(\w+)",
        r"\benum\s+(\w+)",
        r"\btrait\s+(\w+)",
        r"\bimpl\s+(\w+)",
        // Python
        r"\bdef\s+(\w+)",
        r"\bclass\s+(\w+)",
        // JS/TS
        r"\bfunction\s+(\w+)",
        // Go
        r"\bfunc\s+(?:\([^)]+\)\s+)?(\w+)",
    ];

    let mut seen = std::collections::HashSet::new();

    for pattern in &patterns {
        let re = Regex::new(pattern).unwrap();
        for cap in re.captures_iter(content) {
            let bare_name = cap.get(1).unwrap().as_str();

            // Skip common noise: single-char names, all-caps constants, test helpers
            if bare_name.len() <= 1 || bare_name == "self" || bare_name == "Self" || bare_name == "test" {
                continue;
            }

            if !seen.insert(bare_name.to_string()) {
                continue;
            }

            // Qualify with file context: e.g. "auth.rs::validate_token"
            let qualified_name = match file_context {
                Some(fc) => format!("{fc}::{bare_name}"),
                None => bare_name.to_string(),
            };

            let m = cap.get(0).unwrap();
            let entity = Entity::new(
                EntityKind::Function,
                qualified_name,
                bare_name.to_string(),
                ctx.timestamp,
            );
            let mention =
                make_mention(&entity, ctx, context_snippet(content, m.start(), m.end()), MentionRole::Definition);
            out.push((entity, mention));
        }
    }
}

fn extract_dependencies(
    content: &str,
    ctx: &ExtractionContext,
    out: &mut Vec<(Entity, EntityMention)>,
) {
    let patterns = [
        // Rust: use crate::foo or use serde::Deserialize
        (r"\buse\s+([\w:]+)", true),
        // Python: import foo, from foo import bar
        (r"\bimport\s+([\w.]+)", false),
        (r"\bfrom\s+([\w.]+)\s+import\b", false),
        // JS/TS: from 'package' or from "package"
        (r#"\bfrom\s+['"]([@\w/.-]+)['"]"#, false),
        // JS/TS: require('package')
        (r#"\brequire\s*\(\s*['"]([@\w/.-]+)['"]\s*\)"#, false),
    ];

    let mut seen = std::collections::HashSet::new();

    for (pattern, is_rust_use) in &patterns {
        let re = Regex::new(pattern).unwrap();
        for cap in re.captures_iter(content) {
            let raw = cap.get(1).unwrap().as_str();

            // For Rust `use`, extract the top-level crate name
            let name = if *is_rust_use {
                // Skip `use crate::` and `use self::` and `use super::`
                if raw.starts_with("crate::") || raw.starts_with("self::") || raw.starts_with("super::") {
                    // Extract as module reference instead
                    let module = raw.split("::").take(2).collect::<Vec<_>>().join("::");
                    module
                } else {
                    raw.split("::").next().unwrap_or(raw).to_string()
                }
            } else {
                // For Python, take top-level package
                raw.split('.').next().unwrap_or(raw).to_string()
            };

            // Skip std/builtin
            if name == "std" || name == "core" || name == "alloc" || name == "os" || name == "sys" {
                continue;
            }

            if !seen.insert(name.clone()) {
                continue;
            }

            let m = cap.get(0).unwrap();
            let entity = Entity::new(
                EntityKind::Dependency,
                name.clone(),
                name,
                ctx.timestamp,
            );
            let mention =
                make_mention(&entity, ctx, context_snippet(content, m.start(), m.end()), MentionRole::Reference);
            out.push((entity, mention));
        }
    }
}

fn extract_errors(
    content: &str,
    ctx: &ExtractionContext,
    out: &mut Vec<(Entity, EntityMention)>,
) {
    let patterns = [
        r"error\[E(\d{4})\]",
        r"panicked at '([^']+)'",
        r"(?i)\b(FAILED|FAILURE)\b",
        r"(?:Exception|Error):\s*(.+?)(?:\n|$)",
    ];

    let mut seen = std::collections::HashSet::new();

    for pattern in &patterns {
        let re = Regex::new(pattern).unwrap();
        for cap in re.captures_iter(content) {
            let error_text = if let Some(g) = cap.get(1) {
                if pattern.contains("E(") {
                    format!("E{}", g.as_str())
                } else {
                    g.as_str().to_string()
                }
            } else {
                cap.get(0).unwrap().as_str().to_string()
            };

            // Truncate long error messages
            let name = if error_text.len() > 80 {
                format!("{}...", &error_text[..77])
            } else {
                error_text
            };

            if !seen.insert(name.clone()) {
                continue;
            }

            let m = cap.get(0).unwrap();
            let entity = Entity::new(
                EntityKind::Error,
                name.clone(),
                name,
                ctx.timestamp,
            );
            let mention =
                make_mention(&entity, ctx, context_snippet(content, m.start(), m.end()), MentionRole::Unknown);
            out.push((entity, mention));
        }
    }
}

fn extract_concepts(
    content: &str,
    ctx: &ExtractionContext,
    out: &mut Vec<(Entity, EntityMention)>,
) {
    let lower = content.to_lowercase();
    for concept in CONCEPTS {
        if lower.contains(concept) {
            // Find position for context
            if let Some(pos) = lower.find(concept) {
                let entity = Entity::new(
                    EntityKind::Concept,
                    concept.to_string(),
                    concept.to_string(),
                    ctx.timestamp,
                );
                let mention = make_mention(
                    &entity,
                    ctx,
                    context_snippet(content, pos, pos + concept.len()),
                    MentionRole::Unknown,
                );
                out.push((entity, mention));
            }
        }
    }
}

/// Normalize a file path relative to the session's working directory.
/// Uses a component stack to resolve `..` and collapses `//`.
fn normalize_path(raw: &str, cwd: &str) -> String {
    let mut path = raw.to_string();

    // Step 1: Try to strip cwd prefix for absolute paths
    if path.starts_with('/') && !cwd.is_empty() {
        let cwd_prefix = if cwd.ends_with('/') {
            cwd.to_string()
        } else {
            format!("{cwd}/")
        };
        if let Some(relative) = path.strip_prefix(&cwd_prefix) {
            path = relative.to_string();
        }
    }

    // Step 2: Component stack normalization
    // Split by '/', filter empty segments (collapses //), resolve . and ..
    let mut stack: Vec<&str> = Vec::new();
    for component in path.split('/') {
        match component {
            "" | "." => continue,
            ".." => {
                // Pop stack but don't go above project root
                stack.pop();
            }
            c => stack.push(c),
        }
    }

    if stack.is_empty() {
        raw.to_string()
    } else {
        stack.join("/")
    }
}

/// Extract a display-friendly name from a path.
fn display_name_from_path(path: &str) -> String {
    path.rsplit('/').next().unwrap_or(path).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ctx() -> ExtractionContext {
        ExtractionContext {
            session_id: Uuid::new_v4(),
            message_id: Some(Uuid::new_v4()),
            timestamp: Utc::now(),
            cwd: "/home/user/project".to_string(),
            git_branch: Some("main".to_string()),
            tool_calls: vec!["Read".to_string(), "Write".to_string()],
            is_user_message: true,
        }
    }

    #[test]
    fn extracts_file_paths() {
        let ctx = test_ctx();
        let content = "I modified src/auth.rs and tests/test_auth.rs";
        let result = extract_entities(content, &ctx);
        let files: Vec<_> = result
            .entities
            .iter()
            .filter(|(e, _)| e.kind == EntityKind::File)
            .map(|(e, _)| e.name.as_str())
            .collect();
        assert!(files.contains(&"src/auth.rs"), "files: {files:?}");
        assert!(files.contains(&"tests/test_auth.rs"), "files: {files:?}");
    }

    #[test]
    fn normalizes_absolute_paths() {
        let mut ctx = test_ctx();
        ctx.cwd = "/home/user/project".to_string();
        let content = "reading /home/user/project/src/main.rs now";
        let result = extract_entities(content, &ctx);
        let files: Vec<_> = result
            .entities
            .iter()
            .filter(|(e, _)| e.kind == EntityKind::File)
            .map(|(e, _)| e.name.as_str())
            .collect();
        assert!(files.contains(&"src/main.rs"), "files: {files:?}");
    }

    #[test]
    fn extracts_tools() {
        let ctx = test_ctx();
        let result = extract_entities("", &ctx);
        let tools: Vec<_> = result
            .entities
            .iter()
            .filter(|(e, _)| e.kind == EntityKind::Tool)
            .map(|(e, _)| e.name.as_str())
            .collect();
        assert!(tools.contains(&"Read"));
        assert!(tools.contains(&"Write"));
    }

    #[test]
    fn extracts_git_branch() {
        let ctx = test_ctx();
        let result = extract_entities("", &ctx);
        let branches: Vec<_> = result
            .entities
            .iter()
            .filter(|(e, _)| e.kind == EntityKind::GitBranch)
            .map(|(e, _)| e.name.as_str())
            .collect();
        assert!(branches.contains(&"main"));
    }

    #[test]
    fn extracts_rust_functions() {
        let ctx = test_ctx();
        // The test_ctx has tool_calls with Write, so file_paths will be extracted with Modification role.
        // But we need a file entity in content for file_context to work.
        let content = "Editing src/auth.rs now\nfn process_request() { ... }\nstruct Config { }";
        let result = extract_entities(content, &ctx);
        let fns: Vec<_> = result
            .entities
            .iter()
            .filter(|(e, _)| e.kind == EntityKind::Function)
            .map(|(e, _)| e.name.as_str())
            .collect();
        // Functions are now qualified with file context: auth.rs::process_request
        assert!(fns.contains(&"auth.rs::process_request"), "fns: {fns:?}");
        assert!(fns.contains(&"auth.rs::Config"), "fns: {fns:?}");
    }

    #[test]
    fn function_without_file_context_uses_bare_name() {
        let mut ctx = test_ctx();
        ctx.tool_calls = vec![];
        let content = "fn standalone_func() { }";
        let result = extract_entities(content, &ctx);
        let fns: Vec<_> = result
            .entities
            .iter()
            .filter(|(e, _)| e.kind == EntityKind::Function)
            .map(|(e, _)| e.name.as_str())
            .collect();
        assert!(fns.contains(&"standalone_func"), "fns: {fns:?}");
    }

    #[test]
    fn extracts_concepts_from_user_messages() {
        let ctx = test_ctx();
        let content = "Help me fix the authentication logic and add caching";
        let result = extract_entities(content, &ctx);
        let concepts: Vec<_> = result
            .entities
            .iter()
            .filter(|(e, _)| e.kind == EntityKind::Concept)
            .map(|(e, _)| e.name.as_str())
            .collect();
        assert!(concepts.contains(&"authentication"), "concepts: {concepts:?}");
        assert!(concepts.contains(&"caching"), "concepts: {concepts:?}");
    }

    #[test]
    fn no_concepts_from_assistant_messages() {
        let mut ctx = test_ctx();
        ctx.is_user_message = false;
        let content = "I'll help with the authentication logic";
        let result = extract_entities(content, &ctx);
        let concepts: Vec<_> = result
            .entities
            .iter()
            .filter(|(e, _)| e.kind == EntityKind::Concept)
            .collect();
        assert!(concepts.is_empty());
    }

    #[test]
    fn extracts_rust_dependencies() {
        let ctx = test_ctx();
        let content = "use serde::Deserialize;\nuse chrono::Utc;";
        let result = extract_entities(content, &ctx);
        let deps: Vec<_> = result
            .entities
            .iter()
            .filter(|(e, _)| e.kind == EntityKind::Dependency)
            .map(|(e, _)| e.name.as_str())
            .collect();
        assert!(deps.contains(&"serde"), "deps: {deps:?}");
        assert!(deps.contains(&"chrono"), "deps: {deps:?}");
    }

    #[test]
    fn extracts_error_codes() {
        let ctx = test_ctx();
        let content = "error[E0308]: mismatched types";
        let result = extract_entities(content, &ctx);
        let errors: Vec<_> = result
            .entities
            .iter()
            .filter(|(e, _)| e.kind == EntityKind::Error)
            .map(|(e, _)| e.name.as_str())
            .collect();
        assert!(errors.contains(&"E0308"), "errors: {errors:?}");
    }

    #[test]
    fn filters_system_paths() {
        let ctx = test_ctx();
        let content = "check /usr/lib/libssl.so and /proc/cpuinfo";
        let result = extract_entities(content, &ctx);
        let files: Vec<_> = result
            .entities
            .iter()
            .filter(|(e, _)| e.kind == EntityKind::File)
            .collect();
        assert!(files.is_empty(), "should filter system paths");
    }

    #[test]
    fn filters_urls() {
        let ctx = test_ctx();
        let content = "visit https://example.com/path.html for docs";
        let result = extract_entities(content, &ctx);
        let files: Vec<_> = result
            .entities
            .iter()
            .filter(|(e, _)| e.kind == EntityKind::File)
            .collect();
        assert!(files.is_empty(), "should filter URLs");
    }

    #[test]
    fn normalize_path_strips_cwd() {
        assert_eq!(
            normalize_path("/home/user/project/src/main.rs", "/home/user/project"),
            "src/main.rs"
        );
    }

    #[test]
    fn normalize_path_strips_dot_slash() {
        assert_eq!(normalize_path("./src/main.rs", ""), "src/main.rs");
    }

    #[test]
    fn normalize_path_keeps_relative() {
        assert_eq!(normalize_path("src/main.rs", ""), "src/main.rs");
    }

    #[test]
    fn normalize_path_resolves_dotdot() {
        assert_eq!(normalize_path("src/../lib/foo.rs", ""), "lib/foo.rs");
    }

    #[test]
    fn normalize_path_collapses_double_slash() {
        assert_eq!(normalize_path("src//main.rs", ""), "src/main.rs");
    }

    #[test]
    fn normalize_path_complex_dotdot() {
        assert_eq!(
            normalize_path("src/auth/../utils/../models/user.rs", ""),
            "src/models/user.rs"
        );
    }

    #[test]
    fn mention_role_on_file_with_write() {
        let ctx = test_ctx(); // has Write in tool_calls
        let content = "modified src/auth.rs";
        let result = extract_entities(content, &ctx);
        let file_mentions: Vec<_> = result
            .entities
            .iter()
            .filter(|(e, _)| e.kind == EntityKind::File)
            .collect();
        assert!(!file_mentions.is_empty());
        assert_eq!(file_mentions[0].1.role, MentionRole::Modification);
    }

    #[test]
    fn mention_role_on_file_without_write() {
        let mut ctx = test_ctx();
        ctx.tool_calls = vec!["Read".to_string()];
        let content = "reading src/auth.rs";
        let result = extract_entities(content, &ctx);
        let file_mentions: Vec<_> = result
            .entities
            .iter()
            .filter(|(e, _)| e.kind == EntityKind::File)
            .collect();
        assert!(!file_mentions.is_empty());
        assert_eq!(file_mentions[0].1.role, MentionRole::Reference);
    }

    #[test]
    fn function_definition_role() {
        let ctx = test_ctx();
        let content = "Reading src/lib.rs\nfn my_func() {}";
        let result = extract_entities(content, &ctx);
        let fn_mentions: Vec<_> = result
            .entities
            .iter()
            .filter(|(e, _)| e.kind == EntityKind::Function)
            .collect();
        assert!(!fn_mentions.is_empty());
        assert_eq!(fn_mentions[0].1.role, MentionRole::Definition);
    }
}
