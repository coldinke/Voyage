use std::path::{Path, PathBuf};

use chrono::{Duration, Utc};
use voyage_store::sqlite::SqliteStore;

pub fn run(
    db_path: &Path,
    days: u32,
    output: Option<&Path>,
    open_browser: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if !db_path.exists() {
        println!("No data yet. Run `voyage ingest` first.");
        return Ok(());
    }

    let store = SqliteStore::open(db_path)?;
    let since = Utc::now() - Duration::days(days as i64);

    let overall = store.get_stats(Some(since), None)?;
    let by_model = store.get_stats_by_model(Some(since))?;
    let by_provider = store.get_stats_by_provider(Some(since))?;
    let daily = store.get_daily_stats(Some(since))?;
    let sessions = store.list_sessions(Some(since), None, 100)?;

    if overall.session_count == 0 {
        println!("No usage data for the last {days} day(s).");
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

    let html = format!(
        r##"<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Voyage — Token Analytics</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Instrument+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<style>
/* ── Semantic tokens: dark theme (default) ─────────────────── */
:root, [data-theme="dark"] {{
  --bg:          oklch(12% 0.01 250);
  --surface-1:   oklch(16% 0.01 250);
  --surface-2:   oklch(21% 0.01 250);
  --border:      oklch(30% 0.01 250);
  --border-sub:  oklch(22% 0.01 250);

  --text-1:      oklch(93% 0.01 250);
  --text-2:      oklch(65% 0.02 250);
  --text-3:      oklch(50% 0.02 250);

  --accent:      oklch(65% 0.14 220);
  --accent-dim:  oklch(65% 0.14 220 / 0.12);
  --success:     oklch(68% 0.16 155);
  --success-dim: oklch(68% 0.16 155 / 0.12);
  --warn:        oklch(75% 0.15 75);
  --warn-dim:    oklch(75% 0.15 75 / 0.12);
  --error:       oklch(65% 0.2 25);
  --muted:       oklch(60% 0.1 280);
  --muted-dim:   oklch(60% 0.1 280 / 0.12);

  --chart-grid:  oklch(22% 0.01 250);
  --chart-text:  oklch(50% 0.02 250);

  --body-weight: 400;
  --code-bg:     oklch(18% 0.01 250);
  --hover-bg:    oklch(20% 0.02 220 / 0.4);
  --toggle-bg:   oklch(22% 0.01 250);
  color-scheme: dark;
}}

/* ── Semantic tokens: light theme ──────────────────────────── */
[data-theme="light"] {{
  --bg:          oklch(97% 0.008 60);
  --surface-1:   oklch(100% 0.003 60);
  --surface-2:   oklch(95% 0.008 60);
  --border:      oklch(85% 0.01 60);
  --border-sub:  oklch(90% 0.008 60);

  --text-1:      oklch(18% 0.01 60);
  --text-2:      oklch(40% 0.02 60);
  --text-3:      oklch(55% 0.02 60);

  --accent:      oklch(50% 0.16 220);
  --accent-dim:  oklch(50% 0.16 220 / 0.08);
  --success:     oklch(48% 0.14 155);
  --success-dim: oklch(48% 0.14 155 / 0.08);
  --warn:        oklch(55% 0.14 75);
  --warn-dim:    oklch(55% 0.14 75 / 0.08);
  --error:       oklch(52% 0.18 25);
  --muted:       oklch(48% 0.08 280);
  --muted-dim:   oklch(48% 0.08 280 / 0.08);

  --chart-grid:  oklch(90% 0.008 60);
  --chart-text:  oklch(50% 0.02 60);

  --body-weight: 420;
  --code-bg:     oklch(94% 0.008 60);
  --hover-bg:    oklch(50% 0.16 220 / 0.04);
  --toggle-bg:   oklch(92% 0.008 60);
  color-scheme: light;
}}

/* ── Reset & base ──────────────────────────────────────────── */
*, *::before, *::after {{ margin:0; padding:0; box-sizing:border-box; }}
body {{
  background: var(--bg);
  color: var(--text-1);
  font-family: 'Instrument Sans', system-ui, sans-serif;
  font-weight: var(--body-weight);
  line-height: 1.5;
  -webkit-font-smoothing: antialiased;
}}

/* ── Layout ────────────────────────────────────────────────── */
.dash {{ max-width: 1400px; margin: 0 auto; padding: 32px 28px; }}

/* ── Header ────────────────────────────────────────────────── */
.hdr {{ display:flex; align-items:center; justify-content:space-between; margin-bottom:6px; }}
.hdr-left {{ display:flex; align-items:baseline; gap:14px; }}
.hdr h1 {{ font-size:1.4rem; font-weight:700; letter-spacing:-0.03em; }}
.hdr .tag {{
  font-family:'JetBrains Mono',monospace; font-size:0.72rem; font-weight:500;
  color:var(--text-3); background:var(--surface-1); padding:3px 10px; border-radius:5px;
  border:1px solid var(--border-sub);
}}
.meta {{ font-size:0.75rem; color:var(--text-3); margin-bottom:32px; }}

/* ── Theme toggle ──────────────────────────────────────────── */
.theme-toggle {{
  appearance:none; border:none; cursor:pointer;
  background:var(--toggle-bg); border-radius:6px; padding:6px 10px;
  font-family:'Instrument Sans',system-ui; font-size:0.75rem; font-weight:500;
  color:var(--text-2); border:1px solid var(--border-sub);
  transition: background 0.2s, color 0.2s;
}}
.theme-toggle:hover {{ color:var(--text-1); }}

/* ── KPI row ───────────────────────────────────────────────── */
.kpis {{ display:grid; grid-template-columns:repeat(7,1fr); gap:10px; margin-bottom:28px; }}
.kpi {{
  background:var(--surface-1); border:1px solid var(--border-sub); border-radius:8px;
  padding:14px 16px; border-left:3px solid transparent;
}}
.kpi.k-cost   {{ border-left-color:var(--success); }}
.kpi.k-tok    {{ border-left-color:var(--accent); }}
.kpi.k-sess   {{ border-left-color:var(--warn); }}
.kpi.k-avg    {{ border-left-color:var(--success); }}
.kpi.k-cache  {{ border-left-color:var(--muted); }}
.kpi.k-model  {{ border-left-color:var(--accent); }}
.kpi .lbl {{ font-size:0.65rem; font-weight:600; color:var(--text-3); text-transform:uppercase; letter-spacing:0.06em; margin-bottom:4px; }}
.kpi .val {{ font-family:'JetBrains Mono',monospace; font-size:1.25rem; font-weight:600; }}
.kpi .sub {{ font-family:'JetBrains Mono',monospace; font-size:0.65rem; color:var(--text-3); margin-top:3px; }}

/* ── Chart panels ──────────────────────────────────────────── */
.grid {{ display:grid; grid-template-columns:1fr 1fr 1fr; gap:14px; margin-bottom:14px; }}
.panel {{
  background:var(--surface-1); border:1px solid var(--border-sub);
  border-radius:8px; padding:16px;
}}
.panel h3 {{ font-size:0.7rem; font-weight:600; color:var(--text-3); text-transform:uppercase; letter-spacing:0.06em; margin-bottom:12px; }}
.c2 {{ grid-column:span 2; }}
.c3 {{ grid-column:span 3; }}

/* ── Tables ────────────────────────────────────────────────── */
.tbl-wrap {{ background:var(--surface-1); border:1px solid var(--border-sub); border-radius:8px; padding:16px; margin-bottom:14px; }}
.tbl-wrap h3 {{ font-size:0.7rem; font-weight:600; color:var(--text-3); text-transform:uppercase; letter-spacing:0.06em; margin-bottom:12px; }}
.scroll {{ overflow-x:auto; }}
table {{ width:100%; border-collapse:collapse; font-size:0.75rem; white-space:nowrap; }}
th {{
  text-align:left; padding:7px 10px; border-bottom:1px solid var(--border);
  font-size:0.62rem; font-weight:600; color:var(--text-3);
  text-transform:uppercase; letter-spacing:0.04em;
}}
td {{ padding:6px 10px; border-bottom:1px solid var(--border-sub); }}
tr:hover td {{ background:var(--hover-bg); }}
.num {{ text-align:right; font-variant-numeric:tabular-nums; font-family:'JetBrains Mono',monospace; }}
.cost-val {{ color:var(--success); font-weight:500; font-family:'JetBrains Mono',monospace; }}
code {{
  background:var(--code-bg); padding:2px 6px; border-radius:3px;
  font-size:0.68rem; font-family:'JetBrains Mono',monospace;
}}

/* ── Badges ────────────────────────────────────────────────── */
.badge {{
  display:inline-block; padding:1px 7px; border-radius:3px;
  font-size:0.6rem; font-weight:600; text-transform:uppercase; letter-spacing:0.03em;
}}
.badge-claude  {{ background:var(--accent-dim); color:var(--accent); }}
.badge-opencode {{ background:var(--success-dim); color:var(--success); }}
.badge-codex   {{ background:var(--warn-dim); color:var(--warn); }}

/* ── Summary cell ─────────────────────────────────────────── */
.summary-cell {{ max-width:280px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; font-size:0.7rem; color:var(--text-2); }}

/* ── Token composition bar ─────────────────────────────────── */
.comp-bar {{ display:flex; height:6px; border-radius:3px; overflow:hidden; min-width:56px; background:var(--surface-2); }}
.seg-in    {{ background:var(--accent); }}
.seg-out   {{ background:var(--warn); }}
.seg-cache {{ background:var(--muted); opacity:0.65; }}

/* ── Footer ────────────────────────────────────────────────── */
.foot {{ text-align:center; color:var(--text-3); font-size:0.68rem; padding:20px 0 6px; }}
.foot a {{ color:var(--accent); text-decoration:none; }}

/* ── Responsive ────────────────────────────────────────────── */
@media (max-width:1100px) {{
  .kpis {{ grid-template-columns:repeat(4,1fr); }}
  .grid {{ grid-template-columns:1fr; }}
  .c2,.c3 {{ grid-column:span 1; }}
}}
@media (max-width:640px) {{
  .kpis {{ grid-template-columns:repeat(2,1fr); }}
  .dash {{ padding:16px 12px; }}
}}
</style>
</head>
<body>
<div class="dash">

  <div class="hdr">
    <div class="hdr-left">
      <h1>Voyage</h1>
      <span class="tag">{days}d</span>
    </div>
    <button class="theme-toggle" onclick="toggleTheme()" id="themeBtn" aria-label="Toggle theme">Light</button>
  </div>
  <div class="meta">{now} &middot; {provider_count} provider(s) &middot; {project_count} project(s)</div>

  <div class="kpis">
    <div class="kpi k-cost"><div class="lbl">Total Cost</div><div class="val">${cost:.2}</div><div class="sub">{cost_per_day}/day</div></div>
    <div class="kpi k-tok"><div class="lbl">Total Tokens</div><div class="val">{total_tokens_fmt}</div><div class="sub">{input_fmt} in / {output_fmt} out</div></div>
    <div class="kpi k-sess"><div class="lbl">Sessions</div><div class="val">{session_count}</div><div class="sub">{total_turns} turns</div></div>
    <div class="kpi k-avg"><div class="lbl">Avg / Session</div><div class="val">${avg_cost:.4}</div><div class="sub">{avg_tokens} tokens</div></div>
    <div class="kpi k-cache"><div class="lbl">Cache Read</div><div class="val">{cache_read_fmt}</div><div class="sub">{cache_hit_rate:.1}% hit rate</div></div>
    <div class="kpi k-cache"><div class="lbl">Cache Write</div><div class="val">{cache_write_fmt}</div></div>
    <div class="kpi k-model"><div class="lbl">Models</div><div class="val">{model_count}</div><div class="sub">{top_model}</div></div>
  </div>

  <div class="grid">
    <div class="panel c2"><h3>Daily Cost &amp; Sessions</h3><canvas id="cDaily" height="100"></canvas></div>
    <div class="panel"><h3>Token Composition</h3><canvas id="cTokens"></canvas></div>
  </div>
  <div class="grid">
    <div class="panel"><h3>Cost by Model</h3><canvas id="cModel"></canvas></div>
    <div class="panel"><h3>Cost by Provider</h3><canvas id="cProvider"></canvas></div>
    <div class="panel"><h3>Cost by Project</h3><canvas id="cProject"></canvas></div>
  </div>
  <div class="grid">
    <div class="panel c3"><h3>Daily Token Volume (K)</h3><canvas id="cVolume" height="70"></canvas></div>
  </div>

  <div class="tbl-wrap"><h3>Projects</h3><div class="scroll">
    <table><thead><tr><th>Project</th><th>Sessions</th><th>Turns</th><th>Tokens</th><th>Cost</th><th>Avg/Sess</th></tr></thead>
    <tbody>{project_detail_rows}</tbody></table>
  </div></div>

  <div class="tbl-wrap"><h3>Sessions ({session_count})</h3><div class="scroll">
    <table><thead><tr><th>ID</th><th>Provider</th><th>Project</th><th>Summary</th><th>Model</th><th>Date</th><th>Msgs</th><th>Turns</th><th>Tokens</th><th>Mix</th><th>Cost</th></tr></thead>
    <tbody>{session_table}</tbody></table>
  </div></div>

  <div class="foot">Generated by <a href="https://github.com/coldinke/Voyage">Voyage</a> v0.1.0</div>
</div>

<script>
// ── Theme management ─────────────────────────────────────────
function getTheme() {{ return document.documentElement.getAttribute('data-theme') || 'dark'; }}
function setTheme(t) {{
  document.documentElement.setAttribute('data-theme', t);
  document.getElementById('themeBtn').textContent = t === 'dark' ? 'Light' : 'Dark';
  try {{ localStorage.setItem('voyage-theme', t); }} catch(e) {{}}
  refreshCharts();
}}
function toggleTheme() {{ setTheme(getTheme() === 'dark' ? 'light' : 'dark'); }}
// Restore saved preference
try {{
  const saved = localStorage.getItem('voyage-theme');
  if (saved) setTheme(saved);
  else if (window.matchMedia('(prefers-color-scheme: light)').matches) setTheme('light');
}} catch(e) {{}}

// ── Palette (theme-aware) ────────────────────────────────────
function P() {{
  const dark = getTheme() === 'dark';
  return {{
    accent:   dark ? '#5ba3d9' : '#2e7ab8',
    success:  dark ? '#4cb87a' : '#2d8a56',
    warn:     dark ? '#c9a84c' : '#9a7c2e',
    error:    dark ? '#c85a5a' : '#a33a3a',
    muted:    dark ? '#8a7cc8' : '#6a5caa',
    teal:     dark ? '#49b8a8' : '#2d8a7a',
    grid:     dark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)',
    text:     dark ? '#7a8599' : '#6b7280',
    border:   dark ? 'rgba(255,255,255,0.05)' : 'rgba(0,0,0,0.05)',
  }};
}}

// ── Chart instances (for re-creation on theme switch) ────────
let charts = [];
function destroyCharts() {{ charts.forEach(c => c.destroy()); charts = []; }}

function refreshCharts() {{
  destroyCharts();
  const p = P();
  Chart.defaults.color = p.text;
  Chart.defaults.borderColor = p.border;
  Chart.defaults.font.family = "'JetBrains Mono','Instrument Sans',system-ui";
  Chart.defaults.font.size = 11;

  const donut = {{ cutout:'62%', plugins:{{ legend:{{ position:'bottom', labels:{{ padding:10, usePointStyle:true, pointStyle:'circle' }} }} }} }};

  // Daily Cost + Sessions
  charts.push(new Chart(document.getElementById('cDaily'), {{
    type:'bar',
    data:{{
      labels:[{daily_labels}],
      datasets:[
        {{ label:'Cost ($)', data:[{daily_costs}], backgroundColor:p.success+'aa', borderRadius:3, yAxisID:'y', order:2 }},
        {{ label:'Sessions', data:[{daily_sessions}], type:'line', borderColor:p.warn, backgroundColor:p.warn+'22', pointRadius:2.5, pointBackgroundColor:p.warn, tension:0.35, yAxisID:'y1', order:1, fill:true }},
        {{ label:'Turns', data:[{daily_turns}], type:'line', borderColor:p.teal, borderDash:[3,3], pointRadius:0, tension:0.35, yAxisID:'y1', order:0 }}
      ]
    }},
    options:{{
      interaction:{{ mode:'index', intersect:false }},
      scales:{{
        y:{{ position:'left', grid:{{ color:p.grid }}, ticks:{{ callback:v=>'$'+v }} }},
        y1:{{ position:'right', grid:{{ display:false }}, ticks:{{ stepSize:1 }} }},
        x:{{ grid:{{ display:false }} }}
      }},
      plugins:{{ legend:{{ labels:{{ usePointStyle:true, pointStyle:'circle', padding:10 }} }} }}
    }}
  }}));

  // Token breakdown
  charts.push(new Chart(document.getElementById('cTokens'), {{
    type:'doughnut',
    data:{{
      labels:[{token_breakdown_labels}],
      datasets:[{{ data:[{token_breakdown_data}], backgroundColor:[p.accent,p.warn,p.muted,p.teal], borderWidth:0 }}]
    }},
    options:donut
  }}));

  // Model
  charts.push(new Chart(document.getElementById('cModel'), {{
    type:'doughnut',
    data:{{
      labels:[{model_labels}],
      datasets:[{{ data:[{model_costs}], backgroundColor:[p.accent,p.success,p.warn,p.error,p.muted,p.teal], borderWidth:0 }}]
    }},
    options:donut
  }}));

  // Provider
  charts.push(new Chart(document.getElementById('cProvider'), {{
    type:'doughnut',
    data:{{
      labels:[{provider_labels}],
      datasets:[{{ data:[{provider_costs}], backgroundColor:[p.accent,p.success,p.warn,p.error,p.muted], borderWidth:0 }}]
    }},
    options:donut
  }}));

  // Project bar
  charts.push(new Chart(document.getElementById('cProject'), {{
    type:'bar',
    data:{{
      labels:[{proj_labels}],
      datasets:[{{ label:'Cost ($)', data:[{proj_costs}], backgroundColor:p.accent+'bb', borderRadius:3 }}]
    }},
    options:{{
      indexAxis:'y',
      plugins:{{ legend:{{ display:false }} }},
      scales:{{ x:{{ grid:{{ color:p.grid }}, ticks:{{ callback:v=>'$'+v }} }}, y:{{ grid:{{ display:false }} }} }}
    }}
  }}));

  // Daily token volume
  charts.push(new Chart(document.getElementById('cVolume'), {{
    type:'bar',
    data:{{
      labels:[{daily_labels}],
      datasets:[
        {{ label:'Input (K)', data:[{daily_input}], backgroundColor:p.accent+'99', borderRadius:2 }},
        {{ label:'Output (K)', data:[{daily_output}], backgroundColor:p.warn+'99', borderRadius:2 }}
      ]
    }},
    options:{{
      scales:{{
        x:{{ stacked:true, grid:{{ display:false }} }},
        y:{{ stacked:true, grid:{{ color:p.grid }}, ticks:{{ callback:v=>v+'K' }} }}
      }},
      plugins:{{ legend:{{ labels:{{ usePointStyle:true, pointStyle:'circle', padding:10 }} }} }}
    }}
  }}));
}}

// Initial render
refreshCharts();
</script>
</body>
</html>"##,
        days = days,
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
