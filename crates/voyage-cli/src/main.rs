mod cmd_index;
mod cmd_ingest;
mod cmd_report;
mod cmd_search;
mod cmd_session;
mod cmd_stats;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "voyage", about = "LLM token analytics & knowledge platform")]
struct Cli {
    /// Path to the data directory (default: ~/.voyage)
    #[arg(long, global = true)]
    data_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Ingest session files into the local database
    Ingest {
        /// Source directory (defaults depend on provider)
        #[arg(long)]
        source: Option<PathBuf>,
        /// Provider: claude-code or opencode (default: ingest all)
        #[arg(long)]
        provider: Option<String>,
    },
    /// Show token usage statistics
    Stats {
        /// Show stats for the last N days
        #[arg(long, default_value = "1")]
        days: u32,
        /// Filter by project name
        #[arg(long)]
        project: Option<String>,
        /// Break down by model
        #[arg(long)]
        by_model: bool,
    },
    /// List and inspect sessions
    Session {
        #[command(subcommand)]
        action: SessionAction,
    },
    /// Generate an HTML visual report
    Report {
        /// Report period in days
        #[arg(long, default_value = "30")]
        days: u32,
        /// Output file path (default: ./voyage-report.html)
        #[arg(long)]
        output: Option<PathBuf>,
        /// Open report in browser after generation
        #[arg(long)]
        open: bool,
    },
    /// Build vector index from ingested sessions
    Index {
        /// Delete all existing embeddings and re-embed with richer text
        #[arg(long)]
        reindex: bool,
    },
    /// Semantic search across sessions
    Search {
        /// Search query
        query: String,
        /// Maximum number of results
        #[arg(long, default_value = "10")]
        limit: usize,
    },
}

#[derive(Subcommand)]
enum SessionAction {
    /// List recent sessions
    List {
        /// Show sessions from the last N days
        #[arg(long, default_value = "7")]
        days: u32,
        /// Maximum number of sessions to show
        #[arg(long, default_value = "20")]
        limit: usize,
        /// Filter by project
        #[arg(long)]
        project: Option<String>,
    },
}

fn default_data_dir() -> PathBuf {
    dirs_next::home_dir()
        .expect("Cannot determine home directory")
        .join(".voyage")
}


fn main() {
    let cli = Cli::parse();
    let data_dir = cli.data_dir.unwrap_or_else(default_data_dir);

    if let Err(e) = std::fs::create_dir_all(&data_dir) {
        eprintln!("Error creating data directory: {e}");
        std::process::exit(1);
    }

    let db_path = data_dir.join("voyage.db");

    let result = match cli.command {
        Commands::Ingest { source, provider } => {
            cmd_ingest::run(&db_path, source, provider.as_deref())
        }
        Commands::Stats {
            days,
            project,
            by_model,
        } => cmd_stats::run(&db_path, days, project.as_deref(), by_model),
        Commands::Session { action } => match action {
            SessionAction::List {
                days,
                limit,
                project,
            } => cmd_session::run_list(&db_path, days, limit, project.as_deref()),
        },
        Commands::Report { days, output, open } => {
            cmd_report::run(&db_path, days, output.as_deref(), open)
        }
        Commands::Index { reindex } => cmd_index::run(&data_dir, reindex),
        Commands::Search { query, limit } => cmd_search::run(&data_dir, &query, limit),
    };

    if let Err(e) = result {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
