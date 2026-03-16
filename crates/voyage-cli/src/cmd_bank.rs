use std::path::Path;

use voyage_core::model::KnowledgeKind;
use voyage_store::sqlite::SqliteStore;

pub fn run_overview(db_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let store = SqliteStore::open(db_path)?;
    let counts = store.knowledge_counts()?;

    if counts.is_empty() {
        println!("Knowledge bank is empty. Run `voyage distill` first.");
        return Ok(());
    }

    println!("Knowledge Bank Overview\n");
    println!("{:<15} {:<8} {:<8}", "Kind", "Level", "Count");
    println!("{}", "-".repeat(35));

    let mut total = 0i64;
    for (kind, level, count) in &counts {
        let level_label = match level {
            1 => "L1",
            2 => "L2",
            3 => "L3",
            _ => "?",
        };
        println!("{:<15} {:<8} {:<8}", kind, level_label, count);
        total += count;
    }
    println!("{}", "-".repeat(35));
    println!("{:<15} {:<8} {:<8}", "Total", "", total);

    Ok(())
}

pub fn run_list(
    db_path: &Path,
    kind: Option<&str>,
    limit: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let store = SqliteStore::open(db_path)?;
    let kind_filter = kind.map(KnowledgeKind::parse);
    let items = store.list_knowledge_items(kind_filter, None, limit)?;

    if items.is_empty() {
        println!("No knowledge items found.");
        return Ok(());
    }

    for item in &items {
        let level = format!("L{}", item.level);
        let mentions = if item.mention_count > 1 {
            format!(" (x{})", item.mention_count)
        } else {
            String::new()
        };
        println!(
            "  [{:<12}] {:<4} {}{} — {}",
            item.kind.as_str(),
            level,
            truncate(&item.title, 50),
            mentions,
            &item.id,
        );
    }
    println!("\n{} items", items.len());

    Ok(())
}

pub fn run_show(db_path: &Path, id_prefix: &str) -> Result<(), Box<dyn std::error::Error>> {
    let store = SqliteStore::open(db_path)?;
    let item = store.get_knowledge_item(id_prefix)?;

    match item {
        Some(item) => {
            println!("ID:         {}", item.id);
            println!("Kind:       {}", item.kind.as_str());
            println!("Level:      {}", item.level);
            println!("Title:      {}", item.title);
            println!("Confidence: {:.2}", item.confidence);
            println!("Mentions:   {}", item.mention_count);
            println!("Source:     {}", item.source_date);
            if let Some(ref sid) = item.source_session_id {
                println!("Session:    {}", &sid.to_string()[..8]);
            }
            if let Some(ref sup) = item.superseded_by {
                println!("Superseded: {}", sup);
            }
            println!("Created:    {}", item.created_at);
            println!("Updated:    {}", item.updated_at);
            println!("\n{}", item.content);
        }
        None => println!("No knowledge item matching '{id_prefix}'"),
    }

    Ok(())
}

pub fn run_search(
    db_path: &Path,
    query: &str,
    limit: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let store = SqliteStore::open(db_path)?;
    let items = store.search_knowledge_fts(query, limit)?;

    if items.is_empty() {
        println!("No results for '{query}'");
        return Ok(());
    }

    for item in &items {
        println!(
            "  [{:<12}] L{} {} — {}",
            item.kind.as_str(),
            item.level,
            truncate(&item.title, 60),
            &item.id,
        );
    }
    println!("\n{} results", items.len());

    Ok(())
}

pub fn run_memory(db_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let store = SqliteStore::open(db_path)?;

    println!("# Knowledge Memory\n");

    for (kind_str, label) in &[
        ("world", "World Knowledge"),
        ("experience", "Experience & Lessons"),
        ("opinion", "Preferences & Opinions"),
    ] {
        let kind = KnowledgeKind::parse(kind_str);
        let items = store.list_knowledge_items(Some(kind), Some(2), 10)?;

        if items.is_empty() {
            // Fallback to L1
            let l1_items = store.list_knowledge_items(Some(kind), Some(1), 5)?;
            if l1_items.is_empty() {
                continue;
            }
            println!("## {label}\n");
            for item in &l1_items {
                println!("- {}", item.title);
            }
            println!();
            continue;
        }

        println!("## {label}\n");
        for item in &items {
            println!("- {} (confidence: {:.1})", item.title, item.confidence);
        }
        println!();
    }

    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let mut end = max;
        while !s.is_char_boundary(end) && end > 0 {
            end -= 1;
        }
        format!("{}...", &s[..end])
    }
}
