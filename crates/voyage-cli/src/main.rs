mod cmd_analytics;
mod cmd_graph;
mod cmd_index;
mod cmd_ingest;
mod cmd_rate;
mod cmd_report;
mod cmd_search;
mod cmd_session;
mod cmd_stats;
mod cmd_suggest;

use std::path::PathBuf;

use chrono::{Duration, Utc};
use clap::{Parser, Subcommand};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Human,
    Machine,
    Context,
}

#[derive(Parser)]
#[command(name = "voyage", about = "LLM token analytics & knowledge platform")]
struct Cli {
    /// Path to the data directory (default: ~/.voyage)
    #[arg(long, global = true)]
    data_dir: Option<PathBuf>,

    /// Output machine-readable JSON instead of human-friendly text
    #[arg(long, global = true)]
    machine: bool,

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
        /// Show all-time stats
        #[arg(long, conflicts_with = "days")]
        all: bool,
        /// Filter by project name
        #[arg(long)]
        project: Option<String>,
        /// Break down by model
        #[arg(long, conflicts_with_all = ["daily", "blocks", "by_provider"])]
        by_model: bool,
        /// Show daily usage trend
        #[arg(long, conflicts_with_all = ["by_model", "blocks", "by_provider"])]
        daily: bool,
        /// Show 5-hour billing window stats
        #[arg(long, conflicts_with_all = ["by_model", "daily", "by_provider"])]
        blocks: bool,
        /// Break down by provider
        #[arg(long, conflicts_with_all = ["by_model", "daily", "blocks"])]
        by_provider: bool,
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
        /// Show all-time report
        #[arg(long, conflicts_with = "days")]
        all: bool,
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
        /// Embedding model: mini (AllMiniLmL6V2, default) or multi (MultilingualE5Small)
        #[arg(long, default_value = "mini")]
        model: String,
    },
    /// Semantic search across sessions
    Search {
        /// Search query
        query: String,
        /// Maximum number of results
        #[arg(long, default_value = "10")]
        limit: usize,
        /// Filter by project name
        #[arg(long)]
        project: Option<String>,
        /// Filter by time (e.g. 7d, 30d, 2026-01-01)
        #[arg(long)]
        since: Option<String>,
        /// Filter by entity name
        #[arg(long)]
        entity: Option<String>,
        /// Output LLM-optimized context format
        #[arg(long)]
        context: bool,
    },
    /// Rate a session (1-5) with optional tags
    Rate {
        /// Session ID prefix
        id: String,
        /// Rating (1-5)
        rating: u8,
        /// Tags (can be repeated)
        #[arg(long)]
        tag: Vec<String>,
    },
    /// Show usage analytics and cost anomalies
    Analytics,
    /// Suggest context from past sessions
    Suggest {
        /// Find sessions that touched this file
        #[arg(long)]
        file: Option<String>,
        /// Find sessions about this entity/concept
        #[arg(long)]
        entity: Option<String>,
        /// Estimate cost for a task description
        #[arg(long)]
        cost: Option<String>,
    },
    /// Knowledge graph: entity extraction and relationship queries
    Graph {
        #[command(subcommand)]
        action: GraphAction,
    },
}

#[derive(Subcommand)]
enum GraphAction {
    /// Show entity overview statistics
    Stats,
    /// List entities, optionally filtered by kind
    List {
        /// Filter by entity kind: file, function, module, concept, tool, error, dependency, git_branch
        #[arg(long)]
        kind: Option<String>,
        /// Maximum number of entities to show
        #[arg(long, default_value = "30")]
        limit: usize,
    },
    /// Show sessions that mention a specific entity
    Mentions {
        /// Entity name (e.g. src/auth.rs)
        name: String,
        /// Maximum number of sessions to show
        #[arg(long, default_value = "20")]
        limit: usize,
    },
    /// Extract entities from ingested sessions
    Extract {
        /// Re-extract all sessions (clear existing graph data)
        #[arg(long)]
        reextract: bool,
    },
    /// Show entities related to a given entity (PMI-scored)
    Related {
        /// Entity name
        name: String,
        /// Maximum number of results
        #[arg(long, default_value = "20")]
        limit: usize,
    },
    /// Show total cost associated with an entity
    Cost {
        /// Entity name
        name: String,
    },
    /// Show activity timeline for an entity
    Timeline {
        /// Entity name
        name: String,
    },
    /// Show top entities by PageRank
    Rank {
        /// Maximum number of entities to show
        #[arg(long, default_value = "30")]
        limit: usize,
    },
    /// Detect and show entity communities
    Communities {
        /// Maximum number of communities to show
        #[arg(long, default_value = "20")]
        limit: usize,
    },
    /// Remove invalid entities (stopwords, empty names, noise)
    Cleanup,
}

#[derive(Subcommand)]
enum SessionAction {
    /// List recent sessions
    List {
        /// Show sessions from the last N days
        #[arg(long, default_value = "7")]
        days: u32,
        /// Show all-time sessions
        #[arg(long, conflicts_with = "days")]
        all: bool,
        /// Maximum number of sessions to show
        #[arg(long, default_value = "20")]
        limit: usize,
        /// Filter by project
        #[arg(long)]
        project: Option<String>,
    },
    /// Show detailed message-level cost breakdown for a session
    Show {
        /// Session ID (full UUID or prefix)
        id: String,
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
    let format = if cli.machine {
        OutputFormat::Machine
    } else {
        OutputFormat::Human
    };

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
            all,
            project,
            by_model,
            daily,
            blocks,
            by_provider,
        } => {
            let since = if all {
                None
            } else {
                Some(Utc::now() - Duration::days(days as i64))
            };
            cmd_stats::run(&cmd_stats::StatsOptions {
                db_path: &db_path,
                since,
                days,
                project: project.as_deref(),
                by_model,
                daily,
                blocks,
                by_provider,
            })
        }
        Commands::Session { action } => match action {
            SessionAction::List {
                days,
                all,
                limit,
                project,
            } => {
                let since = if all {
                    None
                } else {
                    Some(Utc::now() - Duration::days(days as i64))
                };
                cmd_session::run_list(&db_path, since, days, limit, project.as_deref())
            }
            SessionAction::Show { id } => cmd_session::run_show(&db_path, &id),
        },
        Commands::Report {
            days,
            all,
            output,
            open,
        } => {
            let since = if all {
                None
            } else {
                Some(Utc::now() - Duration::days(days as i64))
            };
            cmd_report::run(&db_path, since, days, output.as_deref(), open)
        }
        Commands::Rate { id, rating, tag } => {
            let tags = if tag.is_empty() { None } else { Some(tag) };
            cmd_rate::run(&db_path, &id, rating, tags)
        }
        Commands::Analytics => cmd_analytics::run(&db_path, format),
        Commands::Suggest { file, entity, cost } => cmd_suggest::run(
            &data_dir,
            file.as_deref(),
            entity.as_deref(),
            cost.as_deref(),
        ),
        Commands::Index { reindex, model } => cmd_index::run(&data_dir, reindex, &model),
        Commands::Search {
            query,
            limit,
            project,
            since,
            entity: _entity,
            context,
        } => cmd_search::run(
            &data_dir,
            &query,
            limit,
            if context {
                OutputFormat::Context
            } else {
                format
            },
            project.as_deref(),
            since.as_deref(),
        ),
        Commands::Graph { action } => {
            let graph_path = data_dir.join("graph.db");
            match action {
                GraphAction::Stats => cmd_graph::run_stats(&graph_path),
                GraphAction::List { kind, limit } => {
                    cmd_graph::run_list(&graph_path, kind.as_deref(), limit)
                }
                GraphAction::Mentions { name, limit } => {
                    cmd_graph::run_mentions(&graph_path, &db_path, &name, limit)
                }
                GraphAction::Extract { reextract } => {
                    cmd_graph::run_extract(&graph_path, &db_path, reextract)
                }
                GraphAction::Related { name, limit } => {
                    cmd_graph::run_related(&graph_path, &name, limit)
                }
                GraphAction::Cost { name } => cmd_graph::run_cost(&graph_path, &db_path, &name),
                GraphAction::Timeline { name } => cmd_graph::run_timeline(&graph_path, &name),
                GraphAction::Rank { limit } => cmd_graph::run_rank(&graph_path, limit),
                GraphAction::Communities { limit } => {
                    cmd_graph::run_communities(&graph_path, limit)
                }
                GraphAction::Cleanup => cmd_graph::run_cleanup(&graph_path),
            }
        }
    };

    if let Err(e) = result {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
