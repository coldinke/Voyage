use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use voyage_core::model::cost_rates;
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
    let daily = store.get_daily_stats(since)?;
    let sessions = store.list_sessions(since, None, 200)?;
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

    // ── Computed insights ────────────────────────────────────────
    let total_tokens = overall.input_tokens
        + overall.output_tokens
        + overall.cache_read_tokens
        + overall.cache_creation_tokens;

    let total_turns: u64 = sessions.iter().map(|s| s.turn_count as u64).sum();

    let cost_per_turn = if total_turns > 0 {
        overall.total_cost_usd / total_turns as f64
    } else {
        0.0
    };

    let active_days = daily.len() as u32;

    // Cache metrics
    let cache_tokens = overall.cache_read_tokens + overall.cache_creation_tokens;
    let cache_hit_rate = if overall.input_tokens + cache_tokens > 0 {
        overall.cache_read_tokens as f64 / (overall.input_tokens + cache_tokens) as f64 * 100.0
    } else {
        0.0
    };

    let cache_by_model = store.get_cache_read_by_model(since)?;
    let cache_savings: f64 = cache_by_model
        .iter()
        .map(|(model, cache_read)| {
            let (input_rate, _, cache_read_rate, _) = cost_rates(model);
            *cache_read as f64 * (input_rate - cache_read_rate) / 1_000_000.0
        })
        .sum();

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

    // ── Chart data ───────────────────────────────────────────────
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

    // Filter model chart data: exclude empty/synthetic models
    let chart_models: Vec<_> = by_model
        .iter()
        .filter(|m| !m.model.is_empty() && m.model != "<synthetic>" && m.total_cost_usd > 0.005)
        .collect();
    let model_labels: String = chart_models
        .iter()
        .map(|m| format!("\"{}\"", short_model_name(&m.model)))
        .collect::<Vec<_>>()
        .join(",");
    let model_costs: String = chart_models
        .iter()
        .map(|m| format!("{:.4}", m.total_cost_usd))
        .collect::<Vec<_>>()
        .join(",");

    let top_projects = &projects[..projects.len().min(5)];
    let proj_labels: String = top_projects
        .iter()
        .map(|(name, _)| {
            let short = project_display_name(name);
            let short = truncate_chars(&short, 16);
            format!("\"{short}\"")
        })
        .collect::<Vec<_>>()
        .join(",");
    let proj_costs: String = top_projects
        .iter()
        .map(|(_, s)| format!("{:.4}", s.2))
        .collect::<Vec<_>>()
        .join(",");

    let top_tools = &tool_stats[..tool_stats.len().min(6)];
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
    let total_tool_calls: u64 = tool_stats.iter().map(|t| t.count).sum();

    // Heatmap data
    let heatmap_data: String = daily
        .iter()
        .map(|d| format!("[\"{}\",{}]", d.date, d.session_count))
        .collect::<Vec<_>>()
        .join(",");

    // ── Session rows ─────────────────────────────────────────────
    let mut sorted_sessions = sessions.clone();
    sorted_sessions.sort_by(|a, b| {
        b.estimated_cost_usd
            .partial_cmp(&a.estimated_cost_usd)
            .unwrap()
    });

    let is_trivial = |s: &voyage_core::model::Session| -> bool {
        s.turn_count == 0 || (s.estimated_cost_usd < 0.005 && s.message_count <= 2)
    };

    let meaningful_count = sorted_sessions.iter().filter(|s| !is_trivial(s)).count();
    let trivial_count = sorted_sessions.iter().filter(|s| is_trivial(s)).count();

    let render_session_row = |s: &voyage_core::model::Session| -> String {
        let total = s.usage.total();
        let tokens_str = format_tokens(total);
        let proj_name = project_display_name(&s.project);
        let provider_badge = match s.provider {
            voyage_core::model::Provider::ClaudeCode => {
                r#"<span class="badge badge-claude">CC</span>"#
            }
            voyage_core::model::Provider::OpenCode => {
                r#"<span class="badge badge-opencode">OC</span>"#
            }
            voyage_core::model::Provider::Codex => r#"<span class="badge badge-codex">CX</span>"#,
        };
        let clean_summary = clean_session_summary(&s.summary);
        let summary_short = truncate_chars(&clean_summary, 72);
        let summary_escaped = html_escape(&clean_summary);
        let summary_short_escaped = html_escape(&summary_short);
        let model_short = truncate_chars(&s.model, 22);
        let token_detail = format!(
            "In: {} / Out: {} / Cache: {}",
            format_tokens(s.usage.input_tokens),
            format_tokens(s.usage.output_tokens),
            format_tokens(s.usage.cache_read_tokens + s.usage.cache_creation_tokens),
        );

        format!(
            r#"<tr>
              <td>{provider} {project}</td>
              <td class="summary-cell" title="{summary_full}">{summary}</td>
              <td><code>{model}</code></td>
              <td>{date}</td>
              <td class="num" title="{token_detail}">{tokens} <span class="sub">/ {turns}t</span></td>
              <td class="num cost-val">{cost}</td>
            </tr>"#,
            provider = provider_badge,
            project = html_escape(&proj_name),
            summary_full = summary_escaped,
            summary = summary_short_escaped,
            model = model_short,
            date = s.started_at.format("%m-%d %H:%M"),
            tokens = tokens_str,
            token_detail = token_detail,
            turns = s.turn_count,
            cost = format_cost(s.estimated_cost_usd),
        )
    };

    let session_rows: String = sorted_sessions
        .iter()
        .filter(|s| !is_trivial(s))
        .map(render_session_row)
        .collect::<Vec<_>>()
        .join("\n");

    let trivial_rows: String = sorted_sessions
        .iter()
        .filter(|s| is_trivial(s))
        .map(render_session_row)
        .collect::<Vec<_>>()
        .join("\n");

    // ── Project table rows ───────────────────────────────────────
    let project_detail_rows: String = projects
        .iter()
        .filter(|(_, stats)| stats.2 > 0.005 || stats.4 > 0) // filter noise
        .map(|(name, stats)| {
            let total = stats.0 + stats.1;
            let cost_per_t = if stats.4 > 0 {
                stats.2 / stats.4 as f64
            } else {
                0.0
            };
            format!(
                r#"<tr><td title="{name}">{short}</td><td class="num">{sessions}</td><td class="num">{turns}</td><td class="num">{tokens}</td><td class="num cost-val">{cost}</td><td class="num">{cpt}</td></tr>"#,
                name = html_escape(name),
                short = project_display_name(name),
                sessions = stats.3,
                turns = stats.4,
                tokens = format_tokens(total),
                cost = format_cost(stats.2),
                cpt = format_cost(cost_per_t),
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    // ── HTML ─────────────────────────────────────────────────────
    let html = format!(
        r##"<!DOCTYPE html>
<html lang="en" data-theme="light">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Voyage — Usage Report</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,300;9..144,500;9..144,700;9..144,900&family=Figtree:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
:root, [data-theme="light"] {{
  --bg: oklch(97% 0.004 80);
  --surface: oklch(99.5% 0.002 80);
  --rule: oklch(86% 0.008 80);
  --rule-light: oklch(92% 0.004 80);
  --ink: oklch(18% 0.008 50);
  --ink-2: oklch(38% 0.012 50);
  --ink-3: oklch(55% 0.008 50);
  --accent: oklch(46% 0.14 155);
  --accent-dim: oklch(46% 0.14 155 / 0.08);
  --red: oklch(52% 0.14 25);
  --amber: oklch(58% 0.12 75);
  --teal: oklch(46% 0.09 195);
  --blue: oklch(48% 0.09 260);
  --slate: oklch(52% 0.025 265);
  --code-bg: oklch(94% 0.003 80);
  --hover: oklch(95% 0.006 80);
  color-scheme: light;
}}
[data-theme="dark"] {{
  --bg: oklch(14% 0.005 60);
  --surface: oklch(18% 0.005 60);
  --rule: oklch(26% 0.008 60);
  --rule-light: oklch(21% 0.005 60);
  --ink: oklch(91% 0.004 60);
  --ink-2: oklch(70% 0.008 60);
  --ink-3: oklch(50% 0.006 60);
  --accent: oklch(62% 0.12 155);
  --accent-dim: oklch(62% 0.12 155 / 0.08);
  --red: oklch(68% 0.12 25);
  --amber: oklch(74% 0.12 75);
  --teal: oklch(64% 0.09 195);
  --blue: oklch(68% 0.09 260);
  --slate: oklch(60% 0.025 265);
  --code-bg: oklch(19% 0.005 60);
  --hover: oklch(20% 0.008 60);
  color-scheme: dark;
}}

*, *::before, *::after {{ margin:0; padding:0; box-sizing:border-box; }}
body {{
  background: var(--bg); color: var(--ink);
  font-family: 'Figtree', system-ui, sans-serif;
  font-size: 14px; line-height: 1.55;
  -webkit-font-smoothing: antialiased;
}}

.page {{ max-width: 1200px; margin: 0 auto; padding: 48px 40px 32px; }}

/* ── Header ── */
.masthead {{ display:flex; align-items:baseline; justify-content:space-between; margin-bottom: 32px; }}
.logo {{
  font-family: 'Fraunces', Georgia, serif; font-size: 1.1rem; font-weight: 700;
  letter-spacing: -0.02em; color: var(--ink);
}}
.logo-sub {{ font-family: 'Figtree', system-ui; font-size: 0.72rem; font-weight: 400; color: var(--ink-3); margin-left: 10px; }}
.theme-toggle {{
  appearance: none; background: none; border: 1px solid var(--rule);
  width: 28px; height: 28px; border-radius: 50%; cursor: pointer;
  color: var(--ink-3); font-size: 0.8rem; display: grid; place-items: center;
  transition: border-color 0.15s;
}}
.theme-toggle:hover {{ border-color: var(--ink-2); }}

/* ── Summary block ── */
.summary {{ margin-bottom: 40px; display: grid; grid-template-columns: 1fr auto; gap: 24px; align-items: end; }}
.cost-big {{
  font-family: 'Fraunces', Georgia, serif; font-optical-sizing: auto;
  font-size: clamp(2.8rem, 5vw, 4rem); font-weight: 900;
  letter-spacing: -0.04em; line-height: 1; color: var(--ink);
}}
.cost-big .sub-period {{
  font-family: 'Figtree', system-ui; font-size: 0.82rem; font-weight: 500;
  color: var(--ink-3); letter-spacing: 0; vertical-align: baseline; margin-left: 12px;
}}
.summary-line {{
  display: flex; flex-wrap: wrap; gap: 6px 20px;
  font-size: 0.82rem; color: var(--ink-2); margin-top: 8px; line-height: 1.6;
}}
.summary-line strong {{ color: var(--ink); font-weight: 600; }}
.summary-line .accent {{ color: var(--accent); font-weight: 600; }}

/* ── Section ── */
section {{ margin-bottom: 48px; }}
.sec-head {{
  font-family: 'Figtree', system-ui; font-size: 0.88rem; font-weight: 600;
  color: var(--ink); margin-bottom: 14px; letter-spacing: -0.01em;
}}
.sec-head .count {{ font-weight: 400; color: var(--ink-3); }}

/* ── Grid ── */
.g-trio {{ display:grid; grid-template-columns: 1fr 1fr 1fr; gap:16px; height:260px; }}
.g-trio .panel {{ display:flex; flex-direction:column; overflow:hidden; }}
.g-trio .chart-wrap {{ flex:1; position:relative; min-height:0; }}

/* ── Panels ── */
.panel {{
  background: var(--surface); padding: 16px; border-radius: 6px;
  position: relative;
}}
.chart-wrap {{ position: relative; }}
.panel-lbl {{
  font-size: 0.72rem; font-weight: 500;
  letter-spacing: 0; color: var(--ink-3); margin-bottom: 10px;
}}

/* ── Tables ── */
.tw {{ overflow-x: auto; }}
table {{ width:100%; border-collapse:collapse; font-size: 0.8rem; white-space:nowrap; }}
th {{
  text-align:left; padding: 8px 10px;
  font-size: 0.66rem; font-weight: 600; text-transform: uppercase;
  letter-spacing: 0.04em; color: var(--ink-3);
  border-bottom: 1px solid var(--rule);
}}
td {{ padding: 7px 10px; border-bottom: 1px solid var(--rule-light); }}
tr:hover td {{ background: var(--hover); }}
.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
.cost-val {{ color: var(--accent); font-weight: 600; }}
code {{
  background: var(--code-bg); padding: 1px 5px; border-radius: 3px;
  font-size: 0.72rem; font-family: 'SF Mono','Menlo','Consolas',monospace;
}}

/* ── Badges ── */
.badge {{
  display: inline-block; padding: 1px 4px; border-radius: 3px;
  font-size: 0.58rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.02em;
  vertical-align: 1px;
}}
.badge-claude  {{ background: oklch(52% 0.14 25 / 0.1); color: var(--red); }}
.badge-opencode {{ background: var(--accent-dim); color: var(--accent); }}
.badge-codex   {{ background: oklch(58% 0.12 75 / 0.1); color: var(--amber); }}

.summary-cell {{ max-width: 340px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; color: var(--ink-2); }}
.sub {{ color: var(--ink-3); font-size: 0.70rem; }}

/* ── Trivial toggle ── */
.trivial-toggle {{
  display: inline-flex; align-items: center; gap: 6px; cursor: pointer;
  font-size: 0.74rem; color: var(--ink-3); padding: 8px 0; user-select: none;
}}
.trivial-toggle:hover {{ color: var(--ink-2); }}
.trivial-toggle .arrow {{ display: inline-block; transition: transform 0.2s ease; font-size: 0.6rem; }}
.trivial-toggle.open .arrow {{ transform: rotate(90deg); }}
.trivial-body[hidden] {{ display: none; }}
.trivial-body table {{ opacity: 0.45; }}
.trivial-body tr:hover td {{ opacity: 1; }}

/* ── Heatmap ── */
.hm-wrap {{ display:flex; margin-bottom: 4px; }}
.hm-days {{
  display:flex; flex-direction:column; gap:3px; margin-right:5px;
  font-size:0.52rem; color:var(--ink-3);
}}
.hm-days span {{ height:13px; line-height:13px; }}
.hm-scroll {{ overflow-x:auto; }}
.heatmap {{ display:flex; gap:3px; }}
.heatmap-col {{ display:flex; flex-direction:column; gap:3px; }}
.hm-cell {{
  width:13px; height:13px; border-radius:2px;
  background: var(--rule-light);
}}
.hm-cell[data-level="1"] {{ background:oklch(65% 0.08 155); }}
.hm-cell[data-level="2"] {{ background:oklch(52% 0.11 155); }}
.hm-cell[data-level="3"] {{ background:oklch(42% 0.14 155); }}
.hm-cell[data-level="4"] {{ background:oklch(34% 0.16 155); }}
[data-theme="dark"] .hm-cell {{ background:oklch(20% 0.005 155); }}
[data-theme="dark"] .hm-cell[data-level="1"] {{ background:oklch(28% 0.06 155); }}
[data-theme="dark"] .hm-cell[data-level="2"] {{ background:oklch(38% 0.10 155); }}
[data-theme="dark"] .hm-cell[data-level="3"] {{ background:oklch(48% 0.13 155); }}
[data-theme="dark"] .hm-cell[data-level="4"] {{ background:oklch(60% 0.15 155); }}
.hm-legend {{
  display:flex; align-items:center; gap:3px;
  font-size:0.52rem; color:var(--ink-3);
}}
.hm-legend .hm-cell {{ width:10px; height:10px; }}

/* ── Footer ── */
footer {{
  font-size: 0.72rem; color: var(--ink-3); padding: 20px 0 8px;
  border-top: 1px solid var(--rule-light);
}}
footer a {{ color: var(--accent); text-decoration:none; }}
footer a:hover {{ text-decoration: underline; }}

/* ── Motion ── */
@keyframes enter {{
  from {{ opacity:0; transform:translateY(6px); }}
  to {{ opacity:1; transform:translateY(0); }}
}}
.summary {{ animation: enter 0.4s cubic-bezier(0.16,1,0.3,1) both; }}
section:nth-of-type(1) {{ animation: enter 0.4s cubic-bezier(0.16,1,0.3,1) 0.04s both; }}
section:nth-of-type(2) {{ animation: enter 0.4s cubic-bezier(0.16,1,0.3,1) 0.08s both; }}
section:nth-of-type(3) {{ animation: enter 0.4s cubic-bezier(0.16,1,0.3,1) 0.12s both; }}

@media (prefers-reduced-motion:reduce) {{
  *, *::before, *::after {{ animation-duration:0.01ms !important; transition-duration:0.01ms !important; }}
}}
@media (max-width:960px) {{
  .g-trio {{ grid-template-columns:1fr; }}
  .summary {{ grid-template-columns:1fr; }}
}}
@media (max-width:600px) {{
  .page {{ padding:24px 16px; }}
  .cost-big {{ font-size:2.2rem; }}
  .summary-line {{ flex-direction: column; gap: 4px; }}
}}
</style>
</head>
<body>
<div class="page">

  <div class="masthead">
    <div><span class="logo">Voyage</span><span class="logo-sub">{period_tag} &middot; {now}</span></div>
    <button class="theme-toggle" onclick="toggleTheme()" id="themeBtn" aria-label="Toggle theme">&#9790;</button>
  </div>

  <div class="summary">
    <div>
      <div class="cost-big">${cost:.2}<span class="sub-period">{active_days} days</span></div>
      <div class="summary-line">
        <span><strong>{session_count}</strong> sessions</span>
        <span><strong>{total_tokens_fmt}</strong> tokens</span>
        <span><strong>{total_turns}</strong> turns at {format_cost_per_turn}/turn</span>
        <span>cache <strong>{cache_hit_rate:.0}%</strong> hit, <span class="accent">{format_cost_savings} saved</span></span>
      </div>
    </div>
    <div>
      <div class="hm-wrap">
        <div class="hm-days">
          <span></span><span>M</span><span></span><span>W</span><span></span><span>F</span><span></span>
        </div>
        <div class="hm-scroll"><div class="heatmap" id="heatmap"></div></div>
      </div>
      <div class="hm-legend">
        <span>Less</span>
        <div class="hm-cell" data-level="0"></div>
        <div class="hm-cell" data-level="1"></div>
        <div class="hm-cell" data-level="2"></div>
        <div class="hm-cell" data-level="3"></div>
        <div class="hm-cell" data-level="4"></div>
        <span>More</span>
      </div>
    </div>
  </div>

  <section>
    <div class="panel" style="margin-bottom:16px"><div class="panel-lbl">Daily Trend</div><div class="chart-wrap"><canvas id="cDaily" height="100"></canvas></div></div>
    <div class="g-trio">
      <div class="panel"><div class="panel-lbl">By Model</div><div class="chart-wrap"><canvas id="cModel"></canvas></div></div>
      <div class="panel"><div class="panel-lbl">By Project</div><div class="chart-wrap"><canvas id="cProject"></canvas></div></div>
      <div class="panel"><div class="panel-lbl">Top Tools <span class="count">({total_tool_calls} calls)</span></div><div class="chart-wrap"><canvas id="cTopTools"></canvas></div></div>
    </div>
  </section>

  <section>
    <div class="sec-head">Projects</div>
    <div class="tw">
      <table>
      <thead><tr><th>Project</th><th class="num">Sess</th><th class="num">Turns</th><th class="num">Tokens</th><th class="num">Cost</th><th class="num">$/Turn</th></tr></thead>
      <tbody>{project_detail_rows}</tbody></table>
    </div>
  </section>

  <section>
    <div class="sec-head">Sessions <span class="count">({meaningful_count})</span></div>
    <div class="tw">
      <table>
      <thead><tr><th>Project</th><th>Summary</th><th>Model</th><th>Date</th><th class="num">Tokens</th><th class="num">Cost</th></tr></thead>
      <tbody>{session_table}</tbody></table>
    </div>
    <div>
      <span class="trivial-toggle" onclick="toggleTrivial()" id="trivialBtn"><span class="arrow">&#9654;</span> {trivial_count} trivial</span>
      <div class="trivial-body" id="trivialBody" hidden>
        <div class="tw">
          <table>
          <thead><tr><th>Project</th><th>Summary</th><th>Model</th><th>Date</th><th class="num">Tokens</th><th class="num">Cost</th></tr></thead>
          <tbody>{trivial_table}</tbody></table>
        </div>
      </div>
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
    accent: dk ? '#50b880' : '#2a7a50',
    red:    dk ? '#d07060' : '#a34030',
    amber:  dk ? '#d0a850' : '#9a7a28',
    teal:   dk ? '#50b0a0' : '#2a7a75',
    blue:   dk ? '#7a9ac0' : '#3a5a90',
    slate:  dk ? '#8a8a9a' : '#6a6a80',
    grid:   dk ? 'rgba(255,255,255,0.05)' : 'rgba(0,0,0,0.05)',
    text:   dk ? '#887868' : '#685848',
    border: dk ? 'rgba(255,255,255,0.02)' : 'rgba(0,0,0,0.02)',
  }};
}}

let charts = [];
function destroyCharts() {{ charts.forEach(c => c.destroy()); charts = []; }}

function refreshCharts() {{
  destroyCharts();
  const p = P();
  Chart.defaults.color = p.text;
  Chart.defaults.borderColor = p.border;
  Chart.defaults.font.family = "'Figtree',system-ui";
  Chart.defaults.font.size = 11;
  const pal = [p.accent, p.red, p.amber, p.teal, p.blue, p.slate];

  charts.push(new Chart(document.getElementById('cDaily'), {{
    type:'bar',
    data:{{
      labels:[{daily_labels}],
      datasets:[
        {{ label:'Cost ($)', data:[{daily_costs}], backgroundColor:p.accent+'88', borderRadius:2, yAxisID:'y', order:2 }},
        {{ label:'Sessions', data:[{daily_sessions}], type:'line', borderColor:p.red, backgroundColor:p.red+'10', pointRadius:1.5, pointBackgroundColor:p.red, tension:0.4, yAxisID:'y1', order:1, fill:true, borderWidth:1.5 }}
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

  charts.push(new Chart(document.getElementById('cModel'), {{
    type:'doughnut',
    data:{{ labels:[{model_labels}], datasets:[{{ data:[{model_costs}], backgroundColor:pal, borderWidth:0 }}] }},
    options:{{ cutout:'62%', maintainAspectRatio:false, plugins:{{ legend:{{ position:'bottom', labels:{{ padding:6, usePointStyle:true, pointStyle:'circle', font:{{ size:9 }} }} }} }} }}
  }}));

  charts.push(new Chart(document.getElementById('cProject'), {{
    type:'bar',
    data:{{ labels:[{proj_labels}], datasets:[{{ data:[{proj_costs}], backgroundColor:p.accent+'aa', borderRadius:3 }}] }},
    options:{{ indexAxis:'y', maintainAspectRatio:false, plugins:{{ legend:{{ display:false }} }}, scales:{{ x:{{ grid:{{ color:p.grid }}, ticks:{{ callback:v=>'$'+v, maxTicksLimit:4 }} }}, y:{{ grid:{{ display:false }}, ticks:{{ font:{{ size:10 }} }} }} }} }}
  }}));

  charts.push(new Chart(document.getElementById('cTopTools'), {{
    type:'bar',
    data:{{ labels:[{top_tool_labels}], datasets:[{{ data:[{top_tool_data}], backgroundColor:p.slate+'88', borderRadius:3 }}] }},
    options:{{ indexAxis:'y', maintainAspectRatio:false, plugins:{{ legend:{{ display:false }} }}, scales:{{ x:{{ grid:{{ color:p.grid }}, ticks:{{ maxTicksLimit:4 }} }}, y:{{ grid:{{ display:false }}, ticks:{{ font:{{ size:10 }} }} }} }} }}
  }}));
}}

refreshCharts();

(function() {{
  const data = [{heatmap_data}];
  const map = new Map(data);
  const container = document.getElementById('heatmap');
  if (!container) return;
  // Show at least 90 days so the heatmap has visual density
  const hmDays = Math.max({heatmap_days}, 90);
  const endDate = new Date(); endDate.setHours(0,0,0,0);
  const startDate = new Date(endDate);
  startDate.setDate(startDate.getDate() - hmDays + 1);
  const start = new Date(startDate);
  start.setDate(start.getDate() - start.getDay());
  const end = new Date(endDate);
  end.setDate(end.getDate() + (6 - end.getDay()));
  const cur = new Date(start);
  let col = null;
  while (cur <= end) {{
    if (cur.getDay() === 0) {{
      col = document.createElement('div');
      col.className = 'heatmap-col';
      container.appendChild(col);
    }}
    const ds = cur.toISOString().slice(0, 10);
    const count = map.get(ds) || 0;
    const cell = document.createElement('div');
    cell.className = 'hm-cell';
    const level = count === 0 ? 0 : count <= 2 ? 1 : count <= 4 ? 2 : count <= 6 ? 3 : 4;
    cell.setAttribute('data-level', level);
    cell.title = ds + ': ' + count + ' session(s)';
    col.appendChild(cell);
    cur.setDate(cur.getDate() + 1);
  }}
}})();

function toggleTrivial() {{
  const btn = document.getElementById('trivialBtn');
  const body = document.getElementById('trivialBody');
  const open = body.hidden;
  body.hidden = !open;
  btn.classList.toggle('open', open);
}}
</script>
</body>
</html>"##,
        period_tag = if since.is_none() {
            "All time".to_string()
        } else {
            format!("Last {} days", days)
        },
        now = Utc::now().format("%Y-%m-%d"),
        cost = overall.total_cost_usd,
        session_count = overall.session_count,
        total_tokens_fmt = format_tokens(total_tokens),
        total_turns = total_turns,
        active_days = active_days,
        format_cost_per_turn = format_cost(cost_per_turn),
        format_cost_savings = format_cost(cache_savings),
        cache_hit_rate = cache_hit_rate,
        daily_labels = daily_labels,
        daily_costs = daily_costs,
        daily_sessions = daily_sessions,
        model_labels = model_labels,
        model_costs = model_costs,
        proj_labels = proj_labels,
        proj_costs = proj_costs,
        project_detail_rows = project_detail_rows,
        session_table = session_rows,
        meaningful_count = meaningful_count,
        trivial_count = trivial_count,
        trivial_table = trivial_rows,
        total_tool_calls = format_tokens(total_tool_calls),
        top_tool_labels = top_tool_labels,
        top_tool_data = top_tool_data,
        heatmap_data = heatmap_data,
        heatmap_days = days,
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

fn format_cost(v: f64) -> String {
    if v == 0.0 {
        "$0".to_string()
    } else if v < 0.005 {
        "<$0.01".to_string()
    } else if v < 10.0 {
        format!("${:.2}", v)
    } else {
        format!("${:.0}", v)
    }
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Clean noisy session summaries (e.g. system-prompt boilerplate).
fn clean_session_summary(s: &str) -> String {
    let s = s.trim();
    if s.starts_with("# AGENTS.md instructions")
        || s.starts_with("# agents.md instructions")
        || s.starts_with("<INSTRUCTION>")
        || s.starts_with("<instruction>")
    {
        "(auto-start)".to_string()
    } else if s.is_empty() {
        "(no summary)".to_string()
    } else {
        s.to_string()
    }
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

/// Extract a human-readable project name from a path.
/// "/Users/vinci/lab/center" → "center"
/// "/Users/vinci/lab/center/backend" → "center/backend"
/// "subagents" → "subagents"
fn project_display_name(s: &str) -> String {
    let path = std::path::Path::new(s);
    let components: Vec<_> = path
        .components()
        .filter_map(|c| {
            if let std::path::Component::Normal(os) = c {
                os.to_str()
            } else {
                None
            }
        })
        .collect();

    if components.len() <= 1 {
        return s.to_string();
    }

    // Find index after known prefixes like "Users/xxx/lab" or "Users/xxx"
    let skip = if components.len() >= 3
        && components[0] == "Users"
        && (components.get(2) == Some(&"lab")
            || components.get(2) == Some(&"opens")
            || components.get(2) == Some(&"work"))
    {
        3
    } else if components.len() >= 2 && components[0] == "Users" {
        2
    } else {
        // Keep last 2 components at most
        components.len().saturating_sub(2)
    };

    let tail: Vec<_> = components[skip..].to_vec();
    if tail.is_empty() {
        components.last().unwrap_or(&s).to_string()
    } else {
        tail.join("/")
    }
}

/// Shorten model names for chart legends.
/// "claude-sonnet-4-5-20250929" → "sonnet-4.5"
/// "claude-opus-4-6" → "opus-4.6"
/// "gpt-5.3-codex" → "gpt-5.3"
/// "big-pickle" → "big-pickle"
fn short_model_name(s: &str) -> String {
    let s = s.trim();
    // Strip date suffix: "-20250929", "-20251001" etc.
    let base = if s.len() > 9 {
        let tail = &s[s.len() - 9..];
        if tail.starts_with('-') && tail[1..].chars().all(|c| c.is_ascii_digit()) && tail.len() == 9
        {
            &s[..s.len() - 9]
        } else {
            s
        }
    } else {
        s
    };
    // Strip "claude-" prefix
    let base = base.strip_prefix("claude-").unwrap_or(base);
    // Normalize version separators: "4-5" → "4.5", "4-6" → "4.6"
    // Match pattern: word-digit-digit at the end
    let re_version = |b: &str| -> String {
        let bytes = b.as_bytes();
        let len = bytes.len();
        if len >= 3
            && bytes[len - 1].is_ascii_digit()
            && bytes[len - 2] == b'-'
            && bytes[len - 3].is_ascii_digit()
        {
            format!("{}.{}", &b[..len - 2], &b[len - 1..])
        } else {
            b.to_string()
        }
    };
    re_version(base)
}
