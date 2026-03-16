use std::path::Path;

use voyage_store::knowledge::{extract_knowledge, promote_items};
use voyage_store::sqlite::SqliteStore;

pub fn run(
    db_path: &Path,
    reprocess: bool,
    promote: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let store = SqliteStore::open(db_path)?;

    let session_ids = if reprocess {
        // Clear distillation log and reprocess all
        store
            .conn()
            .execute("DELETE FROM distillation_log", [])?;
        store.undistilled_sessions()?
    } else {
        store.undistilled_sessions()?
    };

    if session_ids.is_empty() {
        println!("No undistilled sessions found.");
        if promote {
            let promoted = promote_items(&store)?;
            println!("Promoted {promoted} items to Level 2.");
        }
        return Ok(());
    }

    println!("Distilling {} session(s)...", session_ids.len());
    let mut total_items = 0u32;

    // Load existing knowledge items for dedup
    let existing = store.list_knowledge_items(None, None, 100_000)?;

    for sid in &session_ids {
        let session = match store.get_session(sid)? {
            Some(s) => s,
            None => continue,
        };
        let messages = store.get_messages_by_session(sid, 10_000)?;

        let items = extract_knowledge(&session, &messages, &existing);
        let count = items.len() as u32;

        for item in &items {
            store.upsert_knowledge_item(item)?;
        }

        store.mark_distilled(sid, count)?;

        if count > 0 {
            println!(
                "  {} ({} items): {}",
                &sid.to_string()[..8],
                count,
                truncate(&session.summary, 50),
            );
        }
        total_items += count;
    }

    println!(
        "\nDistilled: {} sessions → {} knowledge items",
        session_ids.len(),
        total_items
    );

    if promote {
        let promoted = promote_items(&store)?;
        println!("Promoted {promoted} items to Level 2.");
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
