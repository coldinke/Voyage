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
    projects.sort_by(|a, b| b.1 .2.partial_cmp(&a.1 .2).unwrap());

    let total_tokens = overall.input_tokens
        + overall.output_tokens
        + overall.cache_read_tokens
        + overall.cache_creation_tokens;

    // Cache efficiency
    let cache_tokens = overall.cache_read_tokens + overall.cache_creation_tokens;
    let cache_hit_rate = if overall.input_tokens + cache_tokens > 0 {
        overall.cache_read_tokens as f64 / (overall.input_tokens + cache_tokens) as f64 * 100.0
    } else {
        0.0
    };

    // Avg cost per session
    let avg_cost = if overall.session_count > 0 {
        overall.total_cost_usd / overall.session_count as f64
    } else {
        0.0
    };

    // Daily chart data
    let daily_labels: String = daily.iter().map(|d| format!("\"{}\"", &d.date[5..])).collect::<Vec<_>>().join(",");
    let daily_costs: String = daily.iter().map(|d| format!("{:.4}", d.total_cost_usd)).collect::<Vec<_>>().join(",");
    let daily_sessions: String = daily.iter().map(|d| d.session_count.to_string()).collect::<Vec<_>>().join(",");
    let daily_input: String = daily.iter().map(|d| (d.input_tokens / 1000).to_string()).collect::<Vec<_>>().join(",");
    let daily_output: String = daily.iter().map(|d| (d.output_tokens / 1000).to_string()).collect::<Vec<_>>().join(",");
    let daily_turns: String = daily.iter().map(|d| d.turn_count.to_string()).collect::<Vec<_>>().join(",");

    // Model chart data
    let model_labels: String = by_model.iter().map(|m| format!("\"{}\"", m.model)).collect::<Vec<_>>().join(",");
    let model_costs: String = by_model.iter().map(|m| format!("{:.4}", m.total_cost_usd)).collect::<Vec<_>>().join(",");

    // Provider chart data
    let provider_labels: String = by_provider.iter().map(|p| format!("\"{}\"", p.provider)).collect::<Vec<_>>().join(",");
    let provider_costs: String = by_provider.iter().map(|p| format!("{:.4}", p.total_cost_usd)).collect::<Vec<_>>().join(",");

    // Project chart data (top 10)
    let top_projects = &projects[..projects.len().min(10)];
    let proj_labels: String = top_projects.iter().map(|(name, _)| {
        let short = if name.len() > 35 { format!("...{}", &name[name.len() - 32..]) } else { name.clone() };
        format!("\"{short}\"")
    }).collect::<Vec<_>>().join(",");
    let proj_costs: String = top_projects.iter().map(|(_, s)| format!("{:.4}", s.2)).collect::<Vec<_>>().join(",");

    // Token breakdown for doughnut
    let token_breakdown_labels = r#""Input","Output","Cache Read","Cache Write""#;
    let token_breakdown_data = format!(
        "{},{},{},{}",
        overall.input_tokens, overall.output_tokens,
        overall.cache_read_tokens, overall.cache_creation_tokens
    );

    // Session table rows
    let session_rows: String = sessions.iter().map(|s| {
        let total = s.usage.total();
        let tokens_str = format_tokens(total);
        let project_short = shorten_path(&s.project, 40);
        let provider_badge = match s.provider {
            voyage_core::model::Provider::ClaudeCode => r#"<span class="badge claude">Claude</span>"#,
            voyage_core::model::Provider::OpenCode => r#"<span class="badge opencode">OpenCode</span>"#,
            voyage_core::model::Provider::Codex => r#"<span class="badge codex">Codex</span>"#,
        };
        let model_short = if s.model.len() > 20 {
            format!("{}...", &s.model[..17])
        } else {
            s.model.clone()
        };
        let input_pct = if total > 0 { (s.usage.input_tokens as f64 / total as f64 * 100.0) as u32 } else { 0 };
        let output_pct = if total > 0 { (s.usage.output_tokens as f64 / total as f64 * 100.0) as u32 } else { 0 };
        let cache_pct = 100u32.saturating_sub(input_pct).saturating_sub(output_pct);

        format!(
            r#"<tr>
              <td><code>{id}</code></td>
              <td>{provider}</td>
              <td title="{project_full}">{project}</td>
              <td><code>{model}</code></td>
              <td>{date}</td>
              <td>{msgs}</td>
              <td>{turns}</td>
              <td class="num">{tokens}</td>
              <td><div class="token-bar"><span class="bar-in" style="width:{input_pct}%"></span><span class="bar-out" style="width:{output_pct}%"></span><span class="bar-cache" style="width:{cache_pct}%"></span></div></td>
              <td class="num cost-cell">${cost:.4}</td>
            </tr>"#,
            id = &s.id.to_string()[..8],
            provider = provider_badge,
            project_full = s.project,
            project = project_short,
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

    // Project detail rows
    let project_detail_rows: String = projects.iter().map(|(name, stats)| {
        let total = stats.0 + stats.1;
        format!(
            r#"<tr><td title="{name}">{short}</td><td class="num">{sessions}</td><td class="num">{turns}</td><td class="num">{tokens}</td><td class="num cost-cell">${cost:.4}</td><td class="num">${avg:.4}</td></tr>"#,
            name = name,
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
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Voyage — Token Analytics</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&family=Inter:wght@400;500;600;700&display=swap');

:root {{
  --bg: #09090b; --surface: #18181b; --surface-2: #27272a;
  --border: #3f3f46; --border-subtle: #27272a;
  --text: #fafafa; --text-2: #a1a1aa; --text-3: #71717a;
  --blue: #60a5fa; --green: #4ade80; --amber: #fbbf24;
  --red: #f87171; --purple: #c084fc; --cyan: #22d3ee;
  --teal: #2dd4bf; --pink: #f472b6; --orange: #fb923c;
  --radius: 10px;
}}

* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ background:var(--bg); color:var(--text); font-family:'Inter',system-ui,sans-serif; }}

.dashboard {{ max-width:1440px; margin:0 auto; padding:28px 32px; }}

/* Header */
.header {{ display:flex; align-items:baseline; gap:16px; margin-bottom:8px; }}
.header h1 {{ font-size:24px; font-weight:700; letter-spacing:-0.5px; }}
.header .period {{ font-family:'JetBrains Mono',monospace; font-size:13px; color:var(--text-3); background:var(--surface); padding:4px 10px; border-radius:6px; }}
.generated {{ font-size:12px; color:var(--text-3); margin-bottom:28px; }}

/* KPI strip */
.kpi-strip {{ display:grid; grid-template-columns:repeat(7, 1fr); gap:12px; margin-bottom:28px; }}
.kpi {{ background:var(--surface); border:1px solid var(--border-subtle); border-radius:var(--radius); padding:16px 18px; position:relative; overflow:hidden; }}
.kpi::before {{ content:''; position:absolute; top:0; left:0; right:0; height:2px; }}
.kpi.cost::before {{ background:linear-gradient(90deg, var(--green), var(--teal)); }}
.kpi.tokens::before {{ background:linear-gradient(90deg, var(--blue), var(--cyan)); }}
.kpi.sessions::before {{ background:linear-gradient(90deg, var(--amber), var(--orange)); }}
.kpi.cache::before {{ background:linear-gradient(90deg, var(--purple), var(--pink)); }}
.kpi.avg::before {{ background:linear-gradient(90deg, var(--teal), var(--green)); }}
.kpi .label {{ font-size:11px; font-weight:500; color:var(--text-3); text-transform:uppercase; letter-spacing:0.8px; margin-bottom:6px; }}
.kpi .value {{ font-family:'JetBrains Mono',monospace; font-size:22px; font-weight:600; }}
.kpi .sub {{ font-size:11px; color:var(--text-3); margin-top:4px; font-family:'JetBrains Mono',monospace; }}

/* Chart grid */
.chart-grid {{ display:grid; grid-template-columns:1fr 1fr 1fr; gap:16px; margin-bottom:16px; }}
.chart-panel {{ background:var(--surface); border:1px solid var(--border-subtle); border-radius:var(--radius); padding:18px; }}
.chart-panel h3 {{ font-size:12px; font-weight:600; color:var(--text-3); text-transform:uppercase; letter-spacing:0.8px; margin-bottom:14px; }}
.span-2 {{ grid-column: span 2; }}
.span-3 {{ grid-column: span 3; }}

/* Tables */
.table-panel {{ background:var(--surface); border:1px solid var(--border-subtle); border-radius:var(--radius); padding:18px; margin-bottom:16px; }}
.table-panel h3 {{ font-size:12px; font-weight:600; color:var(--text-3); text-transform:uppercase; letter-spacing:0.8px; margin-bottom:14px; }}
.scroll-x {{ overflow-x:auto; }}
table {{ width:100%; border-collapse:collapse; font-size:12px; font-family:'JetBrains Mono',monospace; white-space:nowrap; }}
th {{ text-align:left; padding:8px 10px; border-bottom:1px solid var(--border); color:var(--text-3); font-size:10px; font-weight:600; text-transform:uppercase; letter-spacing:0.5px; }}
td {{ padding:7px 10px; border-bottom:1px solid var(--border-subtle); }}
tr:hover td {{ background:rgba(96,165,250,0.04); }}
td.num {{ text-align:right; font-variant-numeric:tabular-nums; }}
td.cost-cell {{ color:var(--green); font-weight:500; }}
code {{ background:var(--surface-2); padding:2px 6px; border-radius:4px; font-size:11px; font-family:'JetBrains Mono',monospace; }}

/* Provider badges */
.badge {{ display:inline-block; padding:2px 8px; border-radius:4px; font-size:10px; font-weight:600; text-transform:uppercase; letter-spacing:0.3px; }}
.badge.claude {{ background:rgba(96,165,250,0.15); color:var(--blue); }}
.badge.opencode {{ background:rgba(74,222,128,0.15); color:var(--green); }}
.badge.codex {{ background:rgba(251,191,36,0.15); color:var(--amber); }}

/* Token composition bar */
.token-bar {{ display:flex; height:8px; border-radius:4px; overflow:hidden; min-width:60px; background:var(--surface-2); }}
.bar-in {{ background:var(--blue); }}
.bar-out {{ background:var(--amber); }}
.bar-cache {{ background:var(--purple); opacity:0.6; }}

/* Footer */
.footer {{ text-align:center; color:var(--text-3); font-size:11px; padding:20px 0 8px; }}
.footer a {{ color:var(--blue); text-decoration:none; }}

@media (max-width:1024px) {{
  .kpi-strip {{ grid-template-columns:repeat(4, 1fr); }}
  .chart-grid {{ grid-template-columns:1fr; }}
  .span-2, .span-3 {{ grid-column:span 1; }}
}}
@media (max-width:640px) {{
  .kpi-strip {{ grid-template-columns:repeat(2, 1fr); }}
  .dashboard {{ padding:16px; }}
}}
</style>
</head>
<body>
<div class="dashboard">

  <div class="header">
    <h1>Voyage</h1>
    <span class="period">{days}d window</span>
  </div>
  <div class="generated">{now} &middot; {provider_count} provider(s) &middot; {project_count} project(s)</div>

  <!-- KPI Strip -->
  <div class="kpi-strip">
    <div class="kpi cost">
      <div class="label">Total Cost</div>
      <div class="value">${cost:.2}</div>
      <div class="sub">{cost_per_day}/day</div>
    </div>
    <div class="kpi tokens">
      <div class="label">Total Tokens</div>
      <div class="value">{total_tokens_fmt}</div>
      <div class="sub">{input_fmt} in / {output_fmt} out</div>
    </div>
    <div class="kpi sessions">
      <div class="label">Sessions</div>
      <div class="value">{session_count}</div>
      <div class="sub">{total_turns} turns</div>
    </div>
    <div class="kpi avg">
      <div class="label">Avg/Session</div>
      <div class="value">${avg_cost:.4}</div>
      <div class="sub">{avg_tokens}/session</div>
    </div>
    <div class="kpi cache">
      <div class="label">Cache Read</div>
      <div class="value">{cache_read_fmt}</div>
      <div class="sub">{cache_hit_rate:.1}% hit rate</div>
    </div>
    <div class="kpi cache">
      <div class="label">Cache Write</div>
      <div class="value">{cache_write_fmt}</div>
    </div>
    <div class="kpi tokens">
      <div class="label">Models</div>
      <div class="value">{model_count}</div>
      <div class="sub">{top_model}</div>
    </div>
  </div>

  <!-- Charts Row 1: Daily trends -->
  <div class="chart-grid">
    <div class="chart-panel span-2">
      <h3>Daily Cost &amp; Sessions</h3>
      <canvas id="dailyCostChart" height="100"></canvas>
    </div>
    <div class="chart-panel">
      <h3>Token Composition</h3>
      <canvas id="tokenBreakdownChart"></canvas>
    </div>
  </div>

  <!-- Charts Row 2: Breakdowns -->
  <div class="chart-grid">
    <div class="chart-panel">
      <h3>Cost by Model</h3>
      <canvas id="modelCostChart"></canvas>
    </div>
    <div class="chart-panel">
      <h3>Cost by Provider</h3>
      <canvas id="providerChart"></canvas>
    </div>
    <div class="chart-panel">
      <h3>Cost by Project (Top 10)</h3>
      <canvas id="projectChart"></canvas>
    </div>
  </div>

  <!-- Charts Row 3: Token volume -->
  <div class="chart-grid">
    <div class="chart-panel span-3">
      <h3>Daily Token Volume (K) &mdash; Input vs Output</h3>
      <canvas id="dailyTokenChart" height="70"></canvas>
    </div>
  </div>

  <!-- Project detail table -->
  <div class="table-panel">
    <h3>Projects</h3>
    <div class="scroll-x">
    <table>
      <thead><tr><th>Project</th><th>Sessions</th><th>Turns</th><th>Tokens</th><th>Cost</th><th>Avg/Session</th></tr></thead>
      <tbody>{project_detail_rows}</tbody>
    </table>
    </div>
  </div>

  <!-- Session table -->
  <div class="table-panel">
    <h3>Sessions ({session_count})</h3>
    <div class="scroll-x">
    <table>
      <thead><tr><th>ID</th><th>Provider</th><th>Project</th><th>Model</th><th>Date</th><th>Msgs</th><th>Turns</th><th>Tokens</th><th>Composition</th><th>Cost</th></tr></thead>
      <tbody>{session_table}</tbody>
    </table>
    </div>
  </div>

  <div class="footer">Generated by <a href="https://github.com/coldinke/Voyage">Voyage</a> v0.1.0</div>
</div>

<script>
const C = {{
  blue:'#60a5fa', green:'#4ade80', amber:'#fbbf24', red:'#f87171',
  purple:'#c084fc', cyan:'#22d3ee', teal:'#2dd4bf', pink:'#f472b6', orange:'#fb923c',
  surface:'#27272a', text3:'#71717a', border:'#3f3f46'
}};
Chart.defaults.color = C.text3;
Chart.defaults.borderColor = '#27272a';
Chart.defaults.font.family = "'JetBrains Mono','Inter',system-ui";
Chart.defaults.font.size = 11;
const doughnutOpts = {{ cutout:'65%', plugins:{{ legend:{{ position:'bottom', labels:{{ padding:12, usePointStyle:true, pointStyle:'circle' }} }} }} }};

// Daily Cost & Sessions (dual axis)
new Chart(document.getElementById('dailyCostChart'), {{
  type:'bar',
  data:{{
    labels:[{daily_labels}],
    datasets:[
      {{ label:'Cost ($)', data:[{daily_costs}], backgroundColor:C.green+'99', borderRadius:4, yAxisID:'y', order:2 }},
      {{ label:'Sessions', data:[{daily_sessions}], type:'line', borderColor:C.amber, backgroundColor:C.amber+'33', pointRadius:3, pointBackgroundColor:C.amber, tension:0.3, yAxisID:'y1', order:1, fill:true }},
      {{ label:'Turns', data:[{daily_turns}], type:'line', borderColor:C.cyan, borderDash:[4,4], pointRadius:0, tension:0.3, yAxisID:'y1', order:0 }}
    ]
  }},
  options:{{
    interaction:{{ mode:'index', intersect:false }},
    scales:{{
      y:{{ position:'left', grid:{{ color:'#27272a' }}, ticks:{{ callback:v=>'$'+v }} }},
      y1:{{ position:'right', grid:{{ display:false }}, ticks:{{ stepSize:1 }} }},
      x:{{ grid:{{ display:false }} }}
    }},
    plugins:{{ legend:{{ labels:{{ usePointStyle:true, pointStyle:'circle', padding:12 }} }} }}
  }}
}});

// Token Composition (doughnut)
new Chart(document.getElementById('tokenBreakdownChart'), {{
  type:'doughnut',
  data:{{
    labels:[{token_breakdown_labels}],
    datasets:[{{ data:[{token_breakdown_data}], backgroundColor:[C.blue,C.amber,C.purple,C.pink], borderWidth:0 }}]
  }},
  options:doughnutOpts
}});

// Cost by Model (doughnut)
new Chart(document.getElementById('modelCostChart'), {{
  type:'doughnut',
  data:{{
    labels:[{model_labels}],
    datasets:[{{ data:[{model_costs}], backgroundColor:[C.blue,C.green,C.amber,C.red,C.purple,C.cyan,C.teal,C.pink,C.orange], borderWidth:0 }}]
  }},
  options:doughnutOpts
}});

// Cost by Provider (doughnut)
new Chart(document.getElementById('providerChart'), {{
  type:'doughnut',
  data:{{
    labels:[{provider_labels}],
    datasets:[{{ data:[{provider_costs}], backgroundColor:[C.blue,C.green,C.amber,C.red,C.purple], borderWidth:0 }}]
  }},
  options:doughnutOpts
}});

// Cost by Project (horizontal bar)
new Chart(document.getElementById('projectChart'), {{
  type:'bar',
  data:{{
    labels:[{proj_labels}],
    datasets:[{{ label:'Cost ($)', data:[{proj_costs}], backgroundColor:C.blue+'bb', borderRadius:4 }}]
  }},
  options:{{
    indexAxis:'y',
    plugins:{{ legend:{{ display:false }} }},
    scales:{{ x:{{ grid:{{ color:'#27272a' }}, ticks:{{ callback:v=>'$'+v }} }}, y:{{ grid:{{ display:false }} }} }}
  }}
}});

// Daily Token Volume (stacked bar)
new Chart(document.getElementById('dailyTokenChart'), {{
  type:'bar',
  data:{{
    labels:[{daily_labels}],
    datasets:[
      {{ label:'Input (K)', data:[{daily_input}], backgroundColor:C.blue+'99', borderRadius:2 }},
      {{ label:'Output (K)', data:[{daily_output}], backgroundColor:C.amber+'99', borderRadius:2 }}
    ]
  }},
  options:{{
    scales:{{
      x:{{ stacked:true, grid:{{ display:false }} }},
      y:{{ stacked:true, grid:{{ color:'#27272a' }}, ticks:{{ callback:v=>v+'K' }} }}
    }},
    plugins:{{ legend:{{ labels:{{ usePointStyle:true, pointStyle:'circle', padding:12 }} }} }}
  }}
}});
</script>
</body>
</html>"##,
        days = days,
        now = Utc::now().format("%Y-%m-%d %H:%M UTC"),
        provider_count = by_provider.len(),
        project_count = projects.len(),
        cost = overall.total_cost_usd,
        cost_per_day = if days > 0 { format!("${:.2}", overall.total_cost_usd / days as f64) } else { "$0".into() },
        total_tokens_fmt = format_tokens(total_tokens),
        input_fmt = format_tokens(overall.input_tokens),
        output_fmt = format_tokens(overall.output_tokens),
        session_count = overall.session_count,
        total_turns = sessions.iter().map(|s| s.turn_count as u64).sum::<u64>(),
        avg_cost = avg_cost,
        avg_tokens = format_tokens(if overall.session_count > 0 { total_tokens / overall.session_count } else { 0 }),
        cache_read_fmt = format_tokens(overall.cache_read_tokens),
        cache_write_fmt = format_tokens(overall.cache_creation_tokens),
        cache_hit_rate = cache_hit_rate,
        model_count = by_model.len(),
        top_model = by_model.first().map(|m| m.model.as_str()).unwrap_or("-"),
        // Daily
        daily_labels = daily_labels,
        daily_costs = daily_costs,
        daily_sessions = daily_sessions,
        daily_input = daily_input,
        daily_output = daily_output,
        daily_turns = daily_turns,
        // Token breakdown
        token_breakdown_labels = token_breakdown_labels,
        token_breakdown_data = token_breakdown_data,
        // Model
        model_labels = model_labels,
        model_costs = model_costs,
        // Provider
        provider_labels = provider_labels,
        provider_costs = provider_costs,
        // Project
        proj_labels = proj_labels,
        proj_costs = proj_costs,
        // Tables
        project_detail_rows = project_detail_rows,
        session_table = session_rows,
    );

    let output_path = output
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| {
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
            let _ = std::process::Command::new("xdg-open").arg(&output_path).spawn();
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

fn shorten_path(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("...{}", &s[s.len() - (max - 3)..])
    }
}
