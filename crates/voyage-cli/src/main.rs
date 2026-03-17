mod cmd_analytics;
mod cmd_bank;
mod cmd_context;
mod cmd_discover;
mod cmd_distill;
mod cmd_graph;
mod cmd_index;
mod cmd_ingest;
mod cmd_learn;
mod cmd_profile;
mod cmd_rate;
mod cmd_report;
mod cmd_search;
mod cmd_session;
mod cmd_stats;
mod cmd_suggest;
mod cmd_sync;

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
#[command(name = "voyage", about = "LLM session analytics & knowledge platform")]
struct Cli {
    /// Path to the data directory (default: ~/.voyage)
    #[arg(long, global = true)]
    data_dir: Option<PathBuf>,

    /// Output machine-readable JSON instead of human-friendly text
    #[arg(long, short = 'j', global = true)]
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
        /// Provider: claude-code, opencode, or codex (default: ingest all)
        #[arg(long)]
        provider: Option<String>,
        /// Recalculate summaries for all sessions
        #[arg(long)]
        resummarize: bool,
    },
    /// Show token usage statistics and analytics
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
        #[arg(long, conflicts_with_all = ["daily", "blocks", "by_provider", "anomalies"])]
        by_model: bool,
        /// Show daily usage trend
        #[arg(long, conflicts_with_all = ["by_model", "blocks", "by_provider", "anomalies"])]
        daily: bool,
        /// Show 5-hour billing window stats
        #[arg(long, conflicts_with_all = ["by_model", "daily", "by_provider", "anomalies"])]
        blocks: bool,
        /// Break down by provider
        #[arg(long, conflicts_with_all = ["by_model", "daily", "blocks", "anomalies"])]
        by_provider: bool,
        /// Show tool usage and cost anomalies
        #[arg(long, conflicts_with_all = ["by_model", "daily", "blocks", "by_provider"])]
        anomalies: bool,
    },
    /// List and inspect sessions
    Session {
        #[command(subcommand)]
        action: SessionAction,
    },
    /// Show details of a session or knowledge item (auto-detect by ID)
    Show {
        /// Session or knowledge item ID (full UUID, prefix, or knowledge ID)
        id: String,
    },
    /// Search across sessions and knowledge
    Search {
        /// Search query
        query: String,
        /// Maximum number of results
        #[arg(long, short = 'n', default_value = "10")]
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
        /// Search knowledge bank instead of sessions
        #[arg(long)]
        bank: bool,
        /// Find sessions that touched this file
        #[arg(long)]
        file: Option<String>,
        /// Estimate cost for a task description
        #[arg(long)]
        cost_estimate: bool,
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
    /// Extract knowledge from sessions into the knowledge bank
    Distill {
        /// Reprocess all sessions (clear distillation log)
        #[arg(long)]
        reprocess: bool,
        /// Run promotion after extraction (Level 1 → Level 2)
        #[arg(long)]
        promote: bool,
    },
    /// Browse and search the knowledge bank
    Bank {
        #[command(subcommand)]
        action: Option<BankAction>,
    },
    /// Show or refresh the user profile
    Profile {
        /// Recompute the profile from all data
        #[arg(long)]
        refresh: bool,
    },
    /// Run full pipeline: ingest → extract → distill → promote, with optional CC injection
    Sync {
        /// Auto-inject into Claude Code without prompting
        #[arg(long)]
        inject: bool,
    },
    /// Output project context for Claude Code integration
    Context {
        /// Optional query for targeted context retrieval
        query: Option<String>,
        /// Maximum search results to include when query is provided
        #[arg(long, default_value = "5")]
        limit: usize,
    },
    /// Discover knowledge gaps and missed opportunities
    Discover {
        /// Scan sessions from the last N days
        #[arg(long, default_value = "30")]
        days: u32,
    },
    /// Generate Claude Code rules from knowledge bank
    Learn {
        /// Preview without writing files
        #[arg(long)]
        dry_run: bool,
        /// Output directory (default: .claude/rules/voyage/)
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Knowledge graph: entity extraction and relationship queries
    Graph {
        #[command(subcommand)]
        action: Option<GraphAction>,
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
    /// Show entity details: mentions, related entities, cost, and timeline
    Show {
        /// Entity name (e.g. src/auth.rs, authentication)
        name: String,
        /// Maximum related entities / mentions to show
        #[arg(long, default_value = "10")]
        limit: usize,
    },
    /// Extract entities from ingested sessions
    Extract {
        /// Re-extract all sessions (clear existing graph data)
        #[arg(long)]
        reextract: bool,
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
enum BankAction {
    /// List knowledge items
    List {
        /// Filter by kind: world, experience, opinion, entity_page
        #[arg(long)]
        kind: Option<String>,
        /// Maximum number of items
        #[arg(long, default_value = "30")]
        limit: usize,
    },
    /// Show full details of a knowledge item
    Show {
        /// Knowledge item ID (or prefix)
        id: String,
    },
    /// Search knowledge items
    Search {
        /// Search query
        query: String,
        /// Maximum results
        #[arg(long, default_value = "20")]
        limit: usize,
    },
    /// Output a MEMORY.md-style summary of top knowledge
    Memory,
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
        // ── Data Pipeline ──
        Commands::Ingest {
            source,
            provider,
            resummarize,
        } => {
            if resummarize {
                cmd_ingest::run_resummarize(&db_path)
            } else {
                cmd_ingest::run(&db_path, source, provider.as_deref())
            }
        }
        Commands::Index { reindex, model } => cmd_index::run(&data_dir, reindex, &model),
        Commands::Distill {
            reprocess,
            promote,
        } => cmd_distill::run(&db_path, reprocess, promote),

        // ── Stats (merged: stats + analytics) ──
        Commands::Stats {
            days,
            all,
            project,
            by_model,
            daily,
            blocks,
            by_provider,
            anomalies,
        } => {
            if anomalies {
                cmd_analytics::run(&db_path, format)
            } else {
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
        }

        // ── Session ──
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

        // ── Show (top-level shortcut: auto-detect session vs knowledge item) ──
        Commands::Show { id } => run_show(&db_path, &id),

        // ── Search (merged: search + suggest) ──
        Commands::Search {
            query,
            limit,
            project,
            since,
            entity,
            context,
            bank,
            file,
            cost_estimate,
        } => {
            // --file: suggest by file (delegates to suggest logic)
            if let Some(file_name) = file {
                return exit_result(cmd_suggest::run(
                    &data_dir,
                    Some(&file_name),
                    None,
                    None,
                ));
            }
            // --cost-estimate: suggest cost
            if cost_estimate {
                return exit_result(cmd_suggest::run(&data_dir, None, None, Some(&query)));
            }
            // --entity: suggest by entity
            if let Some(ref entity_name) = entity {
                return exit_result(cmd_suggest::run(
                    &data_dir,
                    None,
                    Some(entity_name),
                    None,
                ));
            }
            // --bank: search knowledge bank via FTS
            if bank {
                return exit_result(cmd_bank::run_search(&db_path, &query, limit));
            }
            // Default: hybrid semantic + FTS search
            cmd_search::run(
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
            )
        }

        // ── Rate ──
        Commands::Rate { id, rating, tag } => {
            let tags = if tag.is_empty() { None } else { Some(tag) };
            cmd_rate::run(&db_path, &id, rating, tags)
        }

        // ── Report ──
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

        // ── Knowledge Bank ──
        Commands::Bank { action } => match action {
            None => cmd_bank::run_overview(&db_path),
            Some(BankAction::List { kind, limit }) => {
                cmd_bank::run_list(&db_path, kind.as_deref(), limit)
            }
            Some(BankAction::Show { id }) => cmd_bank::run_show(&db_path, &id),
            Some(BankAction::Search { query, limit }) => {
                cmd_bank::run_search(&db_path, &query, limit)
            }
            Some(BankAction::Memory) => cmd_bank::run_memory(&db_path),
        },

        // ── Profile ──
        Commands::Profile { refresh } => cmd_profile::run(&db_path, refresh, format),

        // ── Sync (full pipeline + CC injection) ──
        Commands::Sync { inject } => cmd_sync::run(&data_dir, inject),

        // ── Context (CC integration) ──
        Commands::Context { query, limit } => {
            cmd_context::run(&data_dir, query.as_deref(), limit)
        }

        // ── Discover (gap detection) ──
        Commands::Discover { days } => cmd_discover::run(&data_dir, days),

        // ── Learn (generate rules) ──
        Commands::Learn { dry_run, output } => {
            cmd_learn::run(&db_path, output.as_deref(), dry_run)
        }

        // ── Graph (merged: stats default, show combines mentions+related+cost+timeline) ──
        Commands::Graph { action } => {
            let graph_path = data_dir.join("graph.db");
            match action {
                None => cmd_graph::run_stats(&graph_path),
                Some(GraphAction::Stats) => cmd_graph::run_stats(&graph_path),
                Some(GraphAction::List { kind, limit }) => {
                    cmd_graph::run_list(&graph_path, kind.as_deref(), limit)
                }
                Some(GraphAction::Show { name, limit }) => {
                    cmd_graph::run_show(&graph_path, &db_path, &name, limit)
                }
                Some(GraphAction::Extract { reextract }) => {
                    cmd_graph::run_extract(&graph_path, &db_path, reextract)
                }
                Some(GraphAction::Rank { limit }) => cmd_graph::run_rank(&graph_path, limit),
                Some(GraphAction::Communities { limit }) => {
                    cmd_graph::run_communities(&graph_path, limit)
                }
                Some(GraphAction::Cleanup) => cmd_graph::run_cleanup(&graph_path),
            }
        }
    };

    exit_result(result);
}

/// `voyage show <id>` — auto-detect session ID vs knowledge item ID.
fn run_show(db_path: &std::path::Path, id: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Knowledge item IDs have a kind prefix (e.g. "world-", "experience-")
    if id.starts_with("world-")
        || id.starts_with("experience-")
        || id.starts_with("opinion-")
        || id.starts_with("entity_page-")
    {
        return cmd_bank::run_show(db_path, id);
    }
    // Otherwise treat as session ID
    cmd_session::run_show(db_path, id)
}

fn exit_result(result: Result<(), Box<dyn std::error::Error>>) {
    if let Err(e) = result {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
