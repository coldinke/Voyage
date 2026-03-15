use std::path::Path;

use voyage_store::sqlite::SqliteStore;

pub fn run(
    db_path: &Path,
    session_id_prefix: &str,
    rating: u8,
    tags: Option<Vec<String>>,
) -> Result<(), Box<dyn std::error::Error>> {
    if !(1..=5).contains(&rating) {
        return Err("Rating must be between 1 and 5".into());
    }

    let store = SqliteStore::open(db_path)?;

    let session = store
        .find_session_by_prefix(session_id_prefix)?
        .ok_or_else(|| format!("No session found matching prefix: {session_id_prefix}"))?;

    store.set_rating(&session.id, rating)?;

    if let Some(tags) = &tags {
        store.set_tags(&session.id, tags)?;
    }

    let summary = if session.summary.is_empty() {
        "(no summary)"
    } else {
        &session.summary
    };

    println!(
        "Rated session {} ({}) -> {rating}/5",
        &session.id.to_string()[..8],
        summary
    );

    if let Some(tags) = &tags {
        println!("Tags: {}", tags.join(", "));
    }

    Ok(())
}
