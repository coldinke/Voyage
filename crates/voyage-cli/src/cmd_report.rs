use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use voyage_store::sqlite::SqliteStore;

pub fn run(
    db_path: &Path,
    since: Option<DateTime<Utc>>,
    days: u32,
    output: Option<&Path>,
    open_browser: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if !db_path.exists() {
        println!("No data yet. Run `voyage ingest` first.");
        return Ok(());
    }

    let store = SqliteStore::open(db_path)?;

    let overall = store.get_stats(since, None)?;
    let by_model = store.get_stats_by_model(since)?;
    let by_provider = store.get_stats_by_provider(since)?;
    let daily = store.get_daily_stats(since)?;
    let sessions = store.list_sessions(since, None, 100)?;
    let tool_stats = store.get_tool_stats(since)?;

    let period_label = if since.is_none() {
        "all time".to_string()
    } else {
        format!("last {days} day(s)")
    };

    if overall.session_count == 0 {
        println!("No usage data for {period_label}.");
        return Ok(());
    }

    // Per-project aggregation
    let mut project_map: std::collections::HashMap<String, (u64, u64, f64, u64, u32)> =
        std::collections::HashMap::new();
    for s in &sessions {
        let entry = project_map.entry(s.project.clone()).or_default();
        entry.0 += s.usage.input_tokens + s.usage.cache_read_tokens + s.usage.cache_creation_tokens;
        entry.1 += s.usage.output_tokens;
        entry.2 += s.estimated_cost_usd;
        entry.3 += 1;
        entry.4 += s.turn_count;
    }
    let mut projects: Vec<_> = project_map.into_iter().collect();
    projects.sort_by(|a, b| b.1.2.partial_cmp(&a.1.2).unwrap());

    let total_tokens = overall.input_tokens
        + overall.output_tokens
        + overall.cache_read_tokens
        + overall.cache_creation_tokens;

    let cache_tokens = overall.cache_read_tokens + overall.cache_creation_tokens;
    let cache_hit_rate = if overall.input_tokens + cache_tokens > 0 {
        overall.cache_read_tokens as f64 / (overall.input_tokens + cache_tokens) as f64 * 100.0
    } else {
        0.0
    };

    let avg_cost = if overall.session_count > 0 {
        overall.total_cost_usd / overall.session_count as f64
    } else {
        0.0
    };

    // Chart data serialization
    let daily_labels: String = daily
        .iter()
        .map(|d| format!("\"{}\"", &d.date[5..]))
        .collect::<Vec<_>>()
        .join(",");
    let daily_costs: String = daily
        .iter()
        .map(|d| format!("{:.4}", d.total_cost_usd))
        .collect::<Vec<_>>()
        .join(",");
    let daily_sessions: String = daily
        .iter()
        .map(|d| d.session_count.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let daily_input: String = daily
        .iter()
        .map(|d| (d.input_tokens / 1000).to_string())
        .collect::<Vec<_>>()
        .join(",");
    let daily_output: String = daily
        .iter()
        .map(|d| (d.output_tokens / 1000).to_string())
        .collect::<Vec<_>>()
        .join(",");
    let daily_turns: String = daily
        .iter()
        .map(|d| d.turn_count.to_string())
        .collect::<Vec<_>>()
        .join(",");

    let model_labels: String = by_model
        .iter()
        .map(|m| format!("\"{}\"", m.model))
        .collect::<Vec<_>>()
        .join(",");
    let model_costs: String = by_model
        .iter()
        .map(|m| format!("{:.4}", m.total_cost_usd))
        .collect::<Vec<_>>()
        .join(",");

    let provider_labels: String = by_provider
        .iter()
        .map(|p| format!("\"{}\"", p.provider))
        .collect::<Vec<_>>()
        .join(",");
    let provider_costs: String = by_provider
        .iter()
        .map(|p| format!("{:.4}", p.total_cost_usd))
        .collect::<Vec<_>>()
        .join(",");

    let top_projects = &projects[..projects.len().min(10)];
    let proj_labels: String = top_projects
        .iter()
        .map(|(name, _)| {
            let short = shorten_path(name, 35);
            format!("\"{short}\"")
        })
        .collect::<Vec<_>>()
        .join(",");
    let proj_costs: String = top_projects
        .iter()
        .map(|(_, s)| format!("{:.4}", s.2))
        .collect::<Vec<_>>()
        .join(",");

    let token_breakdown_labels = r#""Input","Output","Cache Read","Cache Write""#;
    let token_breakdown_data = format!(
        "{},{},{},{}",
        overall.input_tokens,
        overall.output_tokens,
        overall.cache_read_tokens,
        overall.cache_creation_tokens
    );

    // Session table rows
    let session_rows: String = sessions.iter().map(|s| {
        let total = s.usage.total();
        let tokens_str = format_tokens(total);
        let project_short = shorten_path(&s.project, 40);
        let provider_badge = match s.provider {
            voyage_core::model::Provider::ClaudeCode => r#"<span class="badge badge-claude">Claude</span>"#,
            voyage_core::model::Provider::OpenCode => r#"<span class="badge badge-opencode">OpenCode</span>"#,
            voyage_core::model::Provider::Codex => r#"<span class="badge badge-codex">Codex</span>"#,
        };
        let summary_short = truncate_chars(&s.summary, 60);
        let summary_escaped = html_escape(&s.summary);
        let summary_short_escaped = html_escape(&summary_short);
        let model_short = truncate_chars(&s.model, 20);
        let input_pct = if total > 0 { (s.usage.input_tokens as f64 / total as f64 * 100.0) as u32 } else { 0 };
        let output_pct = if total > 0 { (s.usage.output_tokens as f64 / total as f64 * 100.0) as u32 } else { 0 };
        let cache_pct = 100u32.saturating_sub(input_pct).saturating_sub(output_pct);

        format!(
            r#"<tr>
              <td><code>{id}</code></td>
              <td>{provider}</td>
              <td title="{project_full}">{project}</td>
              <td class="summary-cell" title="{summary_full}">{summary}</td>
              <td><code>{model}</code></td>
              <td>{date}</td>
              <td>{msgs}</td>
              <td>{turns}</td>
              <td class="num">{tokens}</td>
              <td><div class="comp-bar"><span class="seg-in" style="width:{input_pct}%"></span><span class="seg-out" style="width:{output_pct}%"></span><span class="seg-cache" style="width:{cache_pct}%"></span></div></td>
              <td class="num cost-val">${cost:.4}</td>
            </tr>"#,
            id = &s.id.to_string()[..8],
            provider = provider_badge,
            project_full = html_escape(&s.project),
            project = project_short,
            summary_full = summary_escaped,
            summary = summary_short_escaped,
            model = model_short,
            date = s.started_at.format("%m-%d %H:%M"),
            msgs = s.message_count,
            turns = s.turn_count,
            tokens = tokens_str,
            input_pct = input_pct,
            output_pct = output_pct,
            cache_pct = cache_pct,
            cost = s.estimated_cost_usd,
        )
    }).collect::<Vec<_>>().join("\n");

    let project_detail_rows: String = projects.iter().map(|(name, stats)| {
        let total = stats.0 + stats.1;
        format!(
            r#"<tr><td title="{name}">{short}</td><td class="num">{sessions}</td><td class="num">{turns}</td><td class="num">{tokens}</td><td class="num cost-val">${cost:.4}</td><td class="num">${avg:.4}</td></tr>"#,
            name = html_escape(name),
            short = shorten_path(name, 50),
            sessions = stats.3,
            turns = stats.4,
            tokens = format_tokens(total),
            cost = stats.2,
            avg = if stats.3 > 0 { stats.2 / stats.3 as f64 } else { 0.0 },
        )
    }).collect::<Vec<_>>().join("\n");

    // ── Tool usage aggregation ──────────────────────────────────
    let total_tool_calls: u64 = tool_stats.iter().map(|t| t.count).sum();

    fn tool_category(name: &str) -> &'static str {
        match name {
            "Read" | "Write" | "Edit" | "Glob" | "Grep" | "NotebookEdit" => "File Operations",
            "Bash" | "Task" | "TaskCreate" | "TaskUpdate" | "TaskGet" | "TaskList" | "TaskStop"
            | "TaskOutput" => "Execution & Tasks",
            "Agent" | "Skill" | "ToolSearch" => "AI & Agents",
            "WebFetch" | "WebSearch" => "Web",
            _ => "Other",
        }
    }

    // Category aggregation
    let mut category_map: std::collections::HashMap<&str, u64> = std::collections::HashMap::new();
    for t in &tool_stats {
        *category_map.entry(tool_category(&t.tool)).or_default() += t.count;
    }
    let mut categories: Vec<_> = category_map.into_iter().collect();
    categories.sort_by(|a, b| b.1.cmp(&a.1));

    let cat_labels: String = categories
        .iter()
        .map(|(name, _)| format!("\"{name}\""))
        .collect::<Vec<_>>()
        .join(",");
    let cat_data: String = categories
        .iter()
        .map(|(_, count)| count.to_string())
        .collect::<Vec<_>>()
        .join(",");

    // Top tools chart (max 12)
    let top_tools = &tool_stats[..tool_stats.len().min(12)];
    let top_tool_labels: String = top_tools
        .iter()
        .map(|t| format!("\"{}\"", t.tool))
        .collect::<Vec<_>>()
        .join(",");
    let top_tool_data: String = top_tools
        .iter()
        .map(|t| t.count.to_string())
        .collect::<Vec<_>>()
        .join(",");

    // Tool table rows (grouped by category)
    let mut tools_by_cat: std::collections::HashMap<&str, Vec<(&str, u64)>> =
        std::collections::HashMap::new();
    for t in &tool_stats {
        tools_by_cat
            .entry(tool_category(&t.tool))
            .or_default()
            .push((&t.tool, t.count));
    }
    let tool_table_rows: String = {
        let mut rows = Vec::new();
        let mut sorted_cats: Vec<_> = tools_by_cat.into_iter().collect();
        sorted_cats.sort_by(|a, b| {
            let sum_a: u64 = a.1.iter().map(|x| x.1).sum();
            let sum_b: u64 = b.1.iter().map(|x| x.1).sum();
            sum_b.cmp(&sum_a)
        });
        for (cat, mut tools) in sorted_cats {
            tools.sort_by(|a, b| b.1.cmp(&a.1));
            let cat_total: u64 = tools.iter().map(|x| x.1).sum();
            let pct = if total_tool_calls > 0 {
                cat_total as f64 / total_tool_calls as f64 * 100.0
            } else {
                0.0
            };
            rows.push(format!(
                r#"<tr class="cat-row"><td colspan="2"><strong>{cat}</strong></td><td class="num"><strong>{total}</strong></td><td class="num"><strong>{pct:.1}%</strong></td></tr>"#,
                cat = cat,
                total = format_tokens(cat_total),
                pct = pct,
            ));
            for (tool, count) in &tools {
                let tool_pct = if total_tool_calls > 0 {
                    *count as f64 / total_tool_calls as f64 * 100.0
                } else {
                    0.0
                };
                rows.push(format!(
                    r#"<tr><td></td><td><code>{tool}</code></td><td class="num">{count}</td><td class="num">{pct:.1}%</td></tr>"#,
                    tool = tool,
                    count = format_tokens(*count),
                    pct = tool_pct,
                ));
            }
        }
        rows.join("\n")
    };

    let html = format!(
        r##"<!DOCTYPE html>
<html lang="en" data-theme="light">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Voyage — Analytics</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@400;500;600;700;800&family=DM+Sans:opsz,wght@9..40,400;9..40,500;9..40,600&display=swap" rel="stylesheet">
<style>
:root, [data-theme="light"] {{
  --bg:         oklch(97% 0.005 75);
  --surface:    oklch(100% 0.002 75);
  --border:     oklch(87% 0.012 75);
  --border-sub: oklch(91% 0.008 75);
  --text-1:     oklch(18% 0.01 50);
  --text-2:     oklch(42% 0.015 50);
  --text-3:     oklch(58% 0.015 50);
  --accent:     oklch(55% 0.14 40);
  --accent-s:   oklch(55% 0.14 40 / 0.08);
  --success:    oklch(48% 0.12 155);
  --success-s:  oklch(48% 0.12 155 / 0.08);
  --warn:       oklch(56% 0.13 80);
  --warn-s:     oklch(56% 0.13 80 / 0.08);
  --error:      oklch(52% 0.16 25);
  --muted:      oklch(55% 0.06 260);
  --code-bg:    oklch(94% 0.006 75);
  --hover:      oklch(55% 0.14 40 / 0.04);
  color-scheme: light;
}}
[data-theme="dark"] {{
  --bg:         oklch(14% 0.007 50);
  --surface:    oklch(18% 0.007 50);
  --border:     oklch(26% 0.01 50);
  --border-sub: oklch(22% 0.008 50);
  --text-1:     oklch(90% 0.008 50);
  --text-2:     oklch(62% 0.015 50);
  --text-3:     oklch(48% 0.015 50);
  --accent:     oklch(70% 0.13 40);
  --accent-s:   oklch(70% 0.13 40 / 0.10);
  --success:    oklch(65% 0.12 155);
  --success-s:  oklch(65% 0.12 155 / 0.10);
  --warn:       oklch(72% 0.12 80);
  --warn-s:     oklch(72% 0.12 80 / 0.10);
  --error:      oklch(65% 0.15 25);
  --muted:      oklch(62% 0.06 260);
  --code-bg:    oklch(19% 0.007 50);
  --hover:      oklch(70% 0.13 40 / 0.06);
  color-scheme: dark;
}}

*, *::before, *::after {{ margin:0; padding:0; box-sizing:border-box; }}
body {{
  background: var(--bg); color: var(--text-1);
  font-family: 'DM Sans', system-ui, sans-serif;
  font-size: 14px; line-height: 1.55;
  -webkit-font-smoothing: antialiased;
}}

.page {{ max-width: 1320px; margin: 0 auto; padding: 48px 36px 32px; }}

/* Header */
.header {{ display:flex; align-items:flex-start; justify-content:space-between; margin-bottom:44px; }}
.header h1 {{ font-family:'Outfit',system-ui; font-size:1.8rem; font-weight:800; letter-spacing:-0.04em; line-height:1; }}
.subtitle {{ font-size:0.78rem; color:var(--text-3); margin-top:6px; }}
.theme-btn {{
  appearance:none; border:1px solid var(--border-sub); background:transparent;
  border-radius:20px; width:34px; height:34px; cursor:pointer; font-size:0.9rem;
  color:var(--text-3); display:grid; place-items:center;
  transition: border-color 0.2s ease-out;
}}
.theme-btn:hover {{ border-color:var(--text-2); }}

/* Metrics */
.metrics {{
  display:flex; align-items:flex-start; gap:0;
  padding-bottom:36px; border-bottom:1px solid var(--border);
  margin-bottom:48px; flex-wrap:wrap;
}}
.metric {{ flex:1; min-width:120px; padding:0 22px; }}
.metric:first-child {{ padding-left:0; }}
.metric-sep {{ width:1px; align-self:stretch; background:var(--border); flex-shrink:0; }}
.metric-val {{
  font-family:'Outfit',system-ui; font-size:1.65rem; font-weight:700;
  letter-spacing:-0.03em; line-height:1.1;
}}
.metric-lbl {{
  font-size:0.65rem; font-weight:600; color:var(--text-3);
  text-transform:uppercase; letter-spacing:0.06em; margin-top:5px;
}}
.metric-sub {{ font-size:0.72rem; color:var(--text-2); margin-top:2px; }}

/* Sections */
.section {{ margin-bottom:48px; }}
.section > h2 {{
  font-family:'Outfit',system-ui; font-size:1.05rem; font-weight:700;
  letter-spacing:-0.02em; margin-bottom:20px; padding-bottom:8px;
  border-bottom:2px solid var(--accent); display:inline-block;
}}
.section > h2 .ct {{ font-weight:400; color:var(--text-3); }}

/* Chart containers */
.g21 {{ display:grid; grid-template-columns:2fr 1fr; gap:24px; }}
.g3  {{ display:grid; grid-template-columns:1fr 1fr 1fr; gap:24px; }}
.g12 {{ display:grid; grid-template-columns:1fr 2fr; gap:24px; }}
.cb {{
  background:var(--surface); border-radius:10px; padding:20px;
  border:1px solid var(--border-sub);
}}
.cb h3 {{
  font-family:'Outfit',system-ui; font-size:0.7rem; font-weight:600;
  color:var(--text-3); text-transform:uppercase; letter-spacing:0.05em;
  margin-bottom:14px;
}}

/* Tables */
.tw {{ overflow-x:auto; margin-top:16px; }}
table {{ width:100%; border-collapse:collapse; font-size:0.78rem; white-space:nowrap; table-layout:fixed; }}
th {{
  text-align:left; padding:8px 12px;
  font-family:'Outfit',system-ui; font-size:0.63rem; font-weight:600;
  color:var(--text-3); text-transform:uppercase; letter-spacing:0.05em;
  border-bottom:2px solid var(--border); overflow:hidden; text-overflow:ellipsis;
}}
td {{ padding:7px 12px; border-bottom:1px solid var(--border-sub); overflow:hidden; text-overflow:ellipsis; }}
tr:hover td {{ background:var(--hover); }}
.num {{ text-align:right; font-variant-numeric:tabular-nums; }}
.cost-val {{ color:var(--success); font-weight:600; }}
code {{
  background:var(--code-bg); padding:2px 6px; border-radius:4px;
  font-size:0.72rem; font-family:'Menlo','Consolas',monospace;
}}

/* Badges */
.badge {{
  display:inline-block; padding:2px 8px; border-radius:4px;
  font-size:0.6rem; font-weight:600; text-transform:uppercase; letter-spacing:0.04em;
}}
.badge-claude  {{ background:var(--accent-s); color:var(--accent); }}
.badge-opencode {{ background:var(--success-s); color:var(--success); }}
.badge-codex   {{ background:var(--warn-s); color:var(--warn); }}

.summary-cell {{ max-width:280px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; color:var(--text-2); }}
.cat-row td {{ background:var(--surface); }}

.comp-bar {{ display:flex; height:7px; border-radius:4px; overflow:hidden; min-width:56px; background:var(--border-sub); }}
.seg-in    {{ background:var(--accent); }}
.seg-out   {{ background:var(--warn); }}
.seg-cache {{ background:var(--muted); opacity:0.6; }}

footer {{
  color:var(--text-3); font-size:0.72rem; padding:24px 0 8px;
  border-top:1px solid var(--border-sub);
}}
footer a {{ color:var(--accent); text-decoration:none; }}
footer a:hover {{ text-decoration:underline; }}

/* Animations */
@keyframes rise {{
  from {{ opacity:0; transform:translateY(10px); }}
  to {{ opacity:1; transform:translateY(0); }}
}}
.metric {{ animation: rise 0.45s cubic-bezier(0.16,1,0.3,1) both; }}
.metric:nth-child(1)  {{ animation-delay:0s; }}
.metric:nth-child(3)  {{ animation-delay:0.04s; }}
.metric:nth-child(5)  {{ animation-delay:0.08s; }}
.metric:nth-child(7)  {{ animation-delay:0.12s; }}
.metric:nth-child(9)  {{ animation-delay:0.16s; }}
.metric:nth-child(11) {{ animation-delay:0.2s; }}
.section {{ animation: rise 0.5s cubic-bezier(0.16,1,0.3,1) both; animation-delay:0.15s; }}

@media (prefers-reduced-motion:reduce) {{
  *, *::before, *::after {{ animation-duration:0.01ms !important; transition-duration:0.01ms !important; }}
}}
@media (max-width:1100px) {{
  .g21,.g12 {{ grid-template-columns:1fr; }}
  .g3 {{ grid-template-columns:1fr 1fr; }}
}}
@media (max-width:768px) {{
  .page {{ padding:24px 16px; }}
  .header {{ flex-direction:column; gap:12px; }}
  .metrics {{ flex-direction:column; gap:20px; padding-bottom:24px; margin-bottom:32px; }}
  .metric {{ padding:0; }}
  .metric-sep {{ width:100%; height:1px; }}
  .g3 {{ grid-template-columns:1fr; }}
  .section {{ margin-bottom:32px; }}
}}
</style>
</head>
<body>
<div class="page">

  <header class="header">
    <div>
      <h1>Voyage</h1>
      <p class="subtitle">{period_tag} &middot; {now} &middot; {provider_count} provider(s) &middot; {project_count} project(s)</p>
    </div>
    <button class="theme-btn" onclick="toggleTheme()" id="themeBtn" aria-label="Toggle theme">&#9790;</button>
  </header>

  <div class="metrics">
    <div class="metric"><div class="metric-val">${cost:.2}</div><div class="metric-lbl">Total Cost</div><div class="metric-sub">{cost_per_day}/day</div></div>
    <div class="metric-sep"></div>
    <div class="metric"><div class="metric-val">{total_tokens_fmt}</div><div class="metric-lbl">Tokens</div><div class="metric-sub">{input_fmt} in &middot; {output_fmt} out</div></div>
    <div class="metric-sep"></div>
    <div class="metric"><div class="metric-val">{session_count}</div><div class="metric-lbl">Sessions</div><div class="metric-sub">{total_turns} turns</div></div>
    <div class="metric-sep"></div>
    <div class="metric"><div class="metric-val">${avg_cost:.4}</div><div class="metric-lbl">Avg / Session</div><div class="metric-sub">{avg_tokens} tokens</div></div>
    <div class="metric-sep"></div>
    <div class="metric"><div class="metric-val">{cache_hit_rate:.1}%</div><div class="metric-lbl">Cache Hit Rate</div><div class="metric-sub">{cache_read_fmt} read &middot; {cache_write_fmt} write</div></div>
    <div class="metric-sep"></div>
    <div class="metric"><div class="metric-val">{model_count}</div><div class="metric-lbl">Models</div><div class="metric-sub">{top_model}</div></div>
  </div>

  <section class="section">
    <h2>Daily Overview</h2>
    <div class="g21">
      <div class="cb"><canvas id="cDaily" height="110"></canvas></div>
      <div class="cb"><h3>Token Composition</h3><canvas id="cTokens"></canvas></div>
    </div>
  </section>

  <section class="section">
    <h2>Cost Breakdown</h2>
    <div class="g3">
      <div class="cb"><h3>By Model</h3><canvas id="cModel"></canvas></div>
      <div class="cb"><h3>By Provider</h3><canvas id="cProvider"></canvas></div>
      <div class="cb"><h3>By Project</h3><canvas id="cProject"></canvas></div>
    </div>
  </section>

  <section class="section">
    <h2>Token Volume</h2>
    <div class="cb"><canvas id="cVolume" height="80"></canvas></div>
  </section>

  <section class="section">
    <h2>Tool Usage</h2>
    <div class="g12" style="margin-bottom:20px">
      <div class="cb"><h3>By Category</h3><canvas id="cToolCat"></canvas></div>
      <div class="cb"><h3>Top Tools ({total_tool_calls} calls)</h3><canvas id="cTopTools" height="100"></canvas></div>
    </div>
    <div class="tw">
      <table><colgroup><col style="width:10%"><col style="width:50%"><col style="width:20%"><col style="width:20%"></colgroup>
      <thead><tr><th></th><th>Tool</th><th class="num">Calls</th><th class="num">Share</th></tr></thead>
      <tbody>{tool_table_rows}</tbody></table>
    </div>
  </section>

  <section class="section">
    <h2>Projects</h2>
    <div class="tw">
      <table><colgroup><col style="width:40%"><col style="width:12%"><col style="width:12%"><col style="width:12%"><col style="width:12%"><col style="width:12%"></colgroup>
      <thead><tr><th>Project</th><th class="num">Sessions</th><th class="num">Turns</th><th class="num">Tokens</th><th class="num">Cost</th><th class="num">Avg/Sess</th></tr></thead>
      <tbody>{project_detail_rows}</tbody></table>
    </div>
  </section>

  <section class="section">
    <h2>Sessions <span class="ct">({session_count})</span></h2>
    <div class="tw">
      <table><colgroup><col style="width:6%"><col style="width:7%"><col style="width:16%"><col style="width:22%"><col style="width:11%"><col style="width:8%"><col style="width:5%"><col style="width:5%"><col style="width:7%"><col style="width:5%"><col style="width:8%"></colgroup>
      <thead><tr><th>ID</th><th>Provider</th><th>Project</th><th>Summary</th><th>Model</th><th>Date</th><th class="num">Msgs</th><th class="num">Turns</th><th class="num">Tokens</th><th>Mix</th><th class="num">Cost</th></tr></thead>
      <tbody>{session_table}</tbody></table>
    </div>
  </section>

  <footer>Generated by <a href="https://github.com/coldinke/Voyage">Voyage</a></footer>
</div>

<script>
function getTheme() {{ return document.documentElement.getAttribute('data-theme') || 'light'; }}
function setTheme(t) {{
  document.documentElement.setAttribute('data-theme', t);
  document.getElementById('themeBtn').innerHTML = t === 'dark' ? '&#9788;' : '&#9790;';
  try {{ localStorage.setItem('voyage-theme', t); }} catch(e) {{}}
  refreshCharts();
}}
function toggleTheme() {{ setTheme(getTheme() === 'dark' ? 'light' : 'dark'); }}
try {{
  const saved = localStorage.getItem('voyage-theme');
  if (saved) setTheme(saved);
  else if (window.matchMedia('(prefers-color-scheme: dark)').matches) setTheme('dark');
}} catch(e) {{}}

function P() {{
  const dk = getTheme() === 'dark';
  return {{
    accent:  dk ? '#c97a55' : '#a35a38',
    success: dk ? '#5aaa74' : '#358a5a',
    warn:    dk ? '#c5a44a' : '#9a7c28',
    error:   dk ? '#c06858' : '#a04040',
    muted:   dk ? '#7a88aa' : '#646c8a',
    teal:    dk ? '#55a89a' : '#2a8a7a',
    grid:    dk ? 'rgba(255,255,255,0.05)' : 'rgba(0,0,0,0.05)',
    text:    dk ? '#8a7a70' : '#7a6a60',
    border:  dk ? 'rgba(255,255,255,0.04)' : 'rgba(0,0,0,0.04)',
  }};
}}

let charts = [];
function destroyCharts() {{ charts.forEach(c => c.destroy()); charts = []; }}

function refreshCharts() {{
  destroyCharts();
  const p = P();
  Chart.defaults.color = p.text;
  Chart.defaults.borderColor = p.border;
  Chart.defaults.font.family = "'DM Sans','Outfit',system-ui";
  Chart.defaults.font.size = 11;
  const donut = {{ cutout:'65%', plugins:{{ legend:{{ position:'bottom', labels:{{ padding:12, usePointStyle:true, pointStyle:'circle', font:{{ size:11 }} }} }} }} }};

  charts.push(new Chart(document.getElementById('cDaily'), {{
    type:'bar',
    data:{{
      labels:[{daily_labels}],
      datasets:[
        {{ label:'Cost ($)', data:[{daily_costs}], backgroundColor:p.success+'aa', borderRadius:4, yAxisID:'y', order:2 }},
        {{ label:'Sessions', data:[{daily_sessions}], type:'line', borderColor:p.accent, backgroundColor:p.accent+'18', pointRadius:2.5, pointBackgroundColor:p.accent, tension:0.4, yAxisID:'y1', order:1, fill:true }},
        {{ label:'Turns', data:[{daily_turns}], type:'line', borderColor:p.muted, borderDash:[4,4], pointRadius:0, tension:0.4, yAxisID:'y1', order:0 }}
      ]
    }},
    options:{{
      interaction:{{ mode:'index', intersect:false }},
      scales:{{
        y:{{ position:'left', grid:{{ color:p.grid }}, ticks:{{ callback:v=>'$'+v }} }},
        y1:{{ position:'right', grid:{{ display:false }}, ticks:{{ stepSize:1 }} }},
        x:{{ grid:{{ display:false }} }}
      }},
      plugins:{{ legend:{{ labels:{{ usePointStyle:true, pointStyle:'circle', padding:12 }} }} }}
    }}
  }}));

  charts.push(new Chart(document.getElementById('cTokens'), {{
    type:'doughnut',
    data:{{ labels:[{token_breakdown_labels}], datasets:[{{ data:[{token_breakdown_data}], backgroundColor:[p.accent,p.warn,p.muted,p.teal], borderWidth:0 }}] }},
    options:donut
  }}));

  charts.push(new Chart(document.getElementById('cModel'), {{
    type:'doughnut',
    data:{{ labels:[{model_labels}], datasets:[{{ data:[{model_costs}], backgroundColor:[p.accent,p.success,p.warn,p.error,p.muted,p.teal], borderWidth:0 }}] }},
    options:donut
  }}));

  charts.push(new Chart(document.getElementById('cProvider'), {{
    type:'doughnut',
    data:{{ labels:[{provider_labels}], datasets:[{{ data:[{provider_costs}], backgroundColor:[p.accent,p.success,p.warn,p.error,p.muted], borderWidth:0 }}] }},
    options:donut
  }}));

  charts.push(new Chart(document.getElementById('cProject'), {{
    type:'bar',
    data:{{ labels:[{proj_labels}], datasets:[{{ label:'Cost ($)', data:[{proj_costs}], backgroundColor:p.accent+'bb', borderRadius:4 }}] }},
    options:{{ indexAxis:'y', plugins:{{ legend:{{ display:false }} }}, scales:{{ x:{{ grid:{{ color:p.grid }}, ticks:{{ callback:v=>'$'+v }} }}, y:{{ grid:{{ display:false }} }} }} }}
  }}));

  charts.push(new Chart(document.getElementById('cVolume'), {{
    type:'bar',
    data:{{
      labels:[{daily_labels}],
      datasets:[
        {{ label:'Input (K)', data:[{daily_input}], backgroundColor:p.accent+'88', borderRadius:3 }},
        {{ label:'Output (K)', data:[{daily_output}], backgroundColor:p.warn+'88', borderRadius:3 }}
      ]
    }},
    options:{{
      scales:{{ x:{{ stacked:true, grid:{{ display:false }} }}, y:{{ stacked:true, grid:{{ color:p.grid }}, ticks:{{ callback:v=>v+'K' }} }} }},
      plugins:{{ legend:{{ labels:{{ usePointStyle:true, pointStyle:'circle', padding:12 }} }} }}
    }}
  }}));

  charts.push(new Chart(document.getElementById('cToolCat'), {{
    type:'doughnut',
    data:{{ labels:[{cat_labels}], datasets:[{{ data:[{cat_data}], backgroundColor:[p.accent,p.success,p.warn,p.error,p.muted,p.teal], borderWidth:0 }}] }},
    options:donut
  }}));

  charts.push(new Chart(document.getElementById('cTopTools'), {{
    type:'bar',
    data:{{ labels:[{top_tool_labels}], datasets:[{{ label:'Calls', data:[{top_tool_data}], backgroundColor:p.accent+'bb', borderRadius:4 }}] }},
    options:{{ indexAxis:'y', plugins:{{ legend:{{ display:false }} }}, scales:{{ x:{{ grid:{{ color:p.grid }} }}, y:{{ grid:{{ display:false }} }} }} }}
  }}));
}}

refreshCharts();
</script>
</body>
</html>"##,
        period_tag = if since.is_none() {
            "All time".to_string()
        } else {
            format!("Last {days} day(s)")
        },
        now = Utc::now().format("%Y-%m-%d %H:%M UTC"),
        provider_count = by_provider.len(),
        project_count = projects.len(),
        cost = overall.total_cost_usd,
        cost_per_day = if days > 0 {
            format!("${:.2}", overall.total_cost_usd / days as f64)
        } else {
            "$0".into()
        },
        total_tokens_fmt = format_tokens(total_tokens),
        input_fmt = format_tokens(overall.input_tokens),
        output_fmt = format_tokens(overall.output_tokens),
        session_count = overall.session_count,
        total_turns = sessions.iter().map(|s| s.turn_count as u64).sum::<u64>(),
        avg_cost = avg_cost,
        avg_tokens = format_tokens(if overall.session_count > 0 {
            total_tokens / overall.session_count
        } else {
            0
        }),
        cache_read_fmt = format_tokens(overall.cache_read_tokens),
        cache_write_fmt = format_tokens(overall.cache_creation_tokens),
        cache_hit_rate = cache_hit_rate,
        model_count = by_model.len(),
        top_model = by_model.first().map(|m| m.model.as_str()).unwrap_or("-"),
        daily_labels = daily_labels,
        daily_costs = daily_costs,
        daily_sessions = daily_sessions,
        daily_input = daily_input,
        daily_output = daily_output,
        daily_turns = daily_turns,
        token_breakdown_labels = token_breakdown_labels,
        token_breakdown_data = token_breakdown_data,
        model_labels = model_labels,
        model_costs = model_costs,
        provider_labels = provider_labels,
        provider_costs = provider_costs,
        proj_labels = proj_labels,
        proj_costs = proj_costs,
        project_detail_rows = project_detail_rows,
        session_table = session_rows,
        total_tool_calls = format_tokens(total_tool_calls),
        tool_table_rows = tool_table_rows,
        cat_labels = cat_labels,
        cat_data = cat_data,
        top_tool_labels = top_tool_labels,
        top_tool_data = top_tool_data,
    );

    let output_path = output.map(|p| p.to_path_buf()).unwrap_or_else(|| {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join("voyage-report.html")
    });

    std::fs::write(&output_path, &html)?;
    println!("Report generated: {}", output_path.display());

    if open_browser {
        #[cfg(target_os = "macos")]
        {
            let _ = std::process::Command::new("open").arg(&output_path).spawn();
        }
        #[cfg(target_os = "linux")]
        {
            let _ = std::process::Command::new("xdg-open")
                .arg(&output_path)
                .spawn();
        }
    }

    Ok(())
}

fn format_tokens(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn truncate_chars(s: &str, max_chars: usize) -> String {
    let len = s.chars().count();
    if len <= max_chars {
        return s.to_string();
    }
    if max_chars <= 3 {
        return s.chars().take(max_chars).collect();
    }
    let prefix: String = s.chars().take(max_chars - 3).collect();
    format!("{prefix}...")
}

fn shorten_path(s: &str, max: usize) -> String {
    let len = s.chars().count();
    if len <= max {
        s.to_string()
    } else if max <= 3 {
        s.chars().take(max).collect()
    } else {
        let tail: String = s.chars().skip(len - (max - 3)).collect();
        format!("...{tail}")
    }
}
