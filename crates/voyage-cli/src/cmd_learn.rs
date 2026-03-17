use std::path::Path;

use voyage_core::model::KnowledgeKind;
use voyage_store::sqlite::SqliteStore;

/// `voyage learn` — generate Claude Code rules from knowledge bank.
///
/// Reads error→resolution pairs, user preferences, and project context
/// from the knowledge bank, then generates `.claude/rules/voyage/` files
/// that Claude Code will auto-load.
pub fn run(
    db_path: &Path,
    output_dir: Option<&Path>,
    dry_run: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let store = SqliteStore::open(db_path)?;

    // Determine output directory
    let rules_dir = match output_dir {
        Some(dir) => dir.to_path_buf(),
        None => find_rules_dir()?,
    };

    let mut generated = Vec::new();

    // ── Error Rules ──
    let errors = store.list_knowledge_items(Some(KnowledgeKind::Experience), Some(2), 100)?;
    let error_rules: Vec<_> = errors
        .iter()
        .filter(|e| e.title.starts_with("Error:") && e.content.contains("Resolution context:"))
        .collect();

    if !error_rules.is_empty() {
        let mut content = String::from("# Known Errors & Resolutions\n\n");
        content.push_str("These error patterns have been seen before. Apply the known resolution.\n\n");

        for item in &error_rules {
            let err = item.title.strip_prefix("Error: ").unwrap_or(&item.title);
            content.push_str(&format!("## {err}\n\n"));

            // Extract resolution from content
            if let Some(res_start) = item.content.find("Resolution context:") {
                let resolution = &item.content[res_start + "Resolution context:".len()..];
                content.push_str(&format!(
                    "Resolution: {}\n",
                    resolution.trim()
                ));
            }
            content.push_str(&format!("Seen {} times.\n\n", item.mention_count));
        }

        generated.push(("errors.md", content));
    }

    // ── Preference Rules ──
    let opinions = store.list_knowledge_items(Some(KnowledgeKind::Opinion), Some(2), 50)?;
    if !opinions.is_empty() {
        let mut content = String::from("# User Preferences\n\n");
        content.push_str("Follow these preferences when working in this project.\n\n");

        for item in &opinions {
            let pref = item.title.strip_prefix("Preference: ").unwrap_or(&item.title);
            content.push_str(&format!("- {pref}\n"));
        }
        content.push('\n');

        generated.push(("preferences.md", content));
    }

    // ── Project Context Rules ──
    let world = store.list_knowledge_items(Some(KnowledgeKind::World), Some(2), 100)?;
    let tech: Vec<&str> = world
        .iter()
        .filter(|w| w.mention_count >= 2)
        .filter_map(|w| w.title.strip_prefix("Uses "))
        .collect();

    let projects: Vec<_> = world
        .iter()
        .filter(|w| w.title.starts_with("Project:") && w.mention_count >= 2)
        .collect();

    if !tech.is_empty() || !projects.is_empty() {
        let mut content = String::from("# Project Context\n\n");

        if !tech.is_empty() {
            let mut sorted = tech;
            sorted.sort();
            content.push_str(&format!("Tech stack: {}\n\n", sorted.join(", ")));
        }

        if !projects.is_empty() {
            content.push_str("Active projects:\n");
            for p in &projects {
                let name = p.title.strip_prefix("Project: ").unwrap_or(&p.title);
                content.push_str(&format!("- {name}\n"));
            }
            content.push('\n');
        }

        generated.push(("project-context.md", content));
    }

    // ── Output ──
    if generated.is_empty() {
        println!("No rules to generate. Run `voyage distill --promote` first to build up the knowledge bank.");
        return Ok(());
    }

    if dry_run {
        println!("Dry run — would generate {} rule file(s):\n", generated.len());
        for (filename, content) in &generated {
            println!("=== {filename} ===");
            // Show first 20 lines
            for line in content.lines().take(20) {
                println!("  {line}");
            }
            let total = content.lines().count();
            if total > 20 {
                println!("  ... ({} more lines)", total - 20);
            }
            println!();
        }
        println!("Target: {}", rules_dir.display());
    } else {
        std::fs::create_dir_all(&rules_dir)?;
        for (filename, content) in &generated {
            let path = rules_dir.join(filename);
            std::fs::write(&path, content)?;
            println!("  wrote {}", path.display());
        }
        println!(
            "\n{} rule file(s) generated in {}",
            generated.len(),
            rules_dir.display()
        );
    }

    Ok(())
}

/// Find the `.claude/rules/voyage/` directory by walking up to the project root.
fn find_rules_dir() -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let cwd = std::env::current_dir()?;
    let mut dir = cwd.as_path();
    loop {
        if dir.join(".git").exists() || dir.join(".claude").exists() {
            return Ok(dir.join(".claude/rules/voyage"));
        }
        match dir.parent() {
            Some(parent) => dir = parent,
            None => return Ok(cwd.join(".claude/rules/voyage")),
        }
    }
}
