use std::path::Path;

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
    let sessions = store.list_sessions(Some(since), None, 50)?;

    if overall.session_count == 0 {
        println!("No usage data for the last {days} day(s).");
        return Ok(());
    }

    // Build per-project stats
    let mut project_map: std::collections::HashMap<String, (u64, u64, f64, u64)> =
        std::collections::HashMap::new();
    for s in &sessions {
        let entry = project_map.entry(s.project.clone()).or_default();
        entry.0 += s.usage.input_tokens + s.usage.cache_read_tokens + s.usage.cache_creation_tokens;
        entry.1 += s.usage.output_tokens;
        entry.2 += s.estimated_cost_usd;
        entry.3 += 1;
    }
    let mut projects: Vec<_> = project_map.into_iter().collect();
    projects.sort_by(|a, b| b.1 .2.partial_cmp(&a.1 .2).unwrap());

    // Model chart data
    let model_labels: Vec<String> = by_model.iter().map(|m| format!("\"{}\"", m.model)).collect();
    let model_costs: Vec<String> = by_model
        .iter()
        .map(|m| format!("{:.4}", m.total_cost_usd))
        .collect();
    // Project chart data
    let proj_labels: Vec<String> = projects
        .iter()
        .map(|(name, _)| {
            let short = if name.len() > 30 {
                format!("...{}", &name[name.len() - 27..])
            } else {
                name.clone()
            };
            format!("\"{short}\"")
        })
        .collect();
    let proj_costs: Vec<String> = projects
        .iter()
        .map(|(_, stats)| format!("{:.4}", stats.2))
        .collect();

    // Session table rows
    let session_rows: Vec<String> = sessions
        .iter()
        .map(|s| {
            let total = s.usage.total();
            let tokens = if total >= 1_000_000 {
                format!("{:.1}M", total as f64 / 1_000_000.0)
            } else if total >= 1_000 {
                format!("{:.1}K", total as f64 / 1_000.0)
            } else {
                total.to_string()
            };
            let project_short = if s.project.len() > 30 {
                format!("...{}", &s.project[s.project.len() - 27..])
            } else {
                s.project.clone()
            };
            format!(
                "<tr><td><code>{}</code></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>${:.4}</td></tr>",
                &s.id.to_string()[..8],
                project_short,
                s.model,
                s.started_at.format("%Y-%m-%d %H:%M"),
                tokens,
                s.estimated_cost_usd,
            )
        })
        .collect();

    let total_tokens = overall.input_tokens
        + overall.output_tokens
        + overall.cache_read_tokens
        + overall.cache_creation_tokens;

    let html = format!(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Voyage — Token Usage Report</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
  :root {{
    --bg: #0d1117; --surface: #161b22; --border: #30363d;
    --text: #e6edf3; --muted: #8b949e; --accent: #58a6ff;
    --green: #3fb950; --orange: #d29922; --red: #f85149;
  }}
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ background:var(--bg); color:var(--text); font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif; padding:24px; }}
  .container {{ max-width:1200px; margin:0 auto; }}
  h1 {{ font-size:28px; margin-bottom:4px; }}
  .subtitle {{ color:var(--muted); margin-bottom:32px; font-size:14px; }}
  .cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(200px,1fr)); gap:16px; margin-bottom:32px; }}
  .card {{ background:var(--surface); border:1px solid var(--border); border-radius:12px; padding:20px; }}
  .card .label {{ color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:8px; }}
  .card .value {{ font-size:28px; font-weight:600; }}
  .card .value.cost {{ color:var(--green); }}
  .card .value.tokens {{ color:var(--accent); }}
  .card .value.sessions {{ color:var(--orange); }}
  .charts {{ display:grid; grid-template-columns:1fr 1fr; gap:24px; margin-bottom:32px; }}
  .chart-box {{ background:var(--surface); border:1px solid var(--border); border-radius:12px; padding:20px; }}
  .chart-box h3 {{ font-size:14px; color:var(--muted); margin-bottom:16px; text-transform:uppercase; letter-spacing:0.5px; }}
  .full-width {{ grid-column: 1 / -1; }}
  table {{ width:100%; border-collapse:collapse; font-size:13px; }}
  th {{ text-align:left; padding:10px 12px; border-bottom:2px solid var(--border); color:var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:0.5px; }}
  td {{ padding:8px 12px; border-bottom:1px solid var(--border); }}
  tr:hover td {{ background:rgba(88,166,255,0.04); }}
  code {{ background:rgba(88,166,255,0.1); padding:2px 6px; border-radius:4px; font-size:12px; }}
  .footer {{ text-align:center; color:var(--muted); font-size:12px; margin-top:32px; padding-top:16px; border-top:1px solid var(--border); }}
  @media (max-width:768px) {{ .charts {{ grid-template-columns:1fr; }} }}
</style>
</head>
<body>
<div class="container">
  <h1>Voyage</h1>
  <div class="subtitle">Token Usage Report — Last {days} day(s) — Generated {now}</div>

  <div class="cards">
    <div class="card"><div class="label">Total Cost</div><div class="value cost">${cost:.4}</div></div>
    <div class="card"><div class="label">Total Tokens</div><div class="value tokens">{total_tokens_fmt}</div></div>
    <div class="card"><div class="label">Sessions</div><div class="value sessions">{session_count}</div></div>
    <div class="card"><div class="label">Input Tokens</div><div class="value">{input_fmt}</div></div>
    <div class="card"><div class="label">Output Tokens</div><div class="value">{output_fmt}</div></div>
    <div class="card"><div class="label">Cache Tokens</div><div class="value">{cache_fmt}</div></div>
  </div>

  <div class="charts">
    <div class="chart-box">
      <h3>Cost by Model</h3>
      <canvas id="modelCostChart"></canvas>
    </div>
    <div class="chart-box">
      <h3>Cost by Project</h3>
      <canvas id="projectCostChart"></canvas>
    </div>
    <div class="chart-box full-width">
      <h3>Session Cost Timeline</h3>
      <canvas id="timelineChart" height="80"></canvas>
    </div>
  </div>

  <div class="chart-box full-width">
    <h3>Sessions</h3>
    <table>
      <thead><tr><th>ID</th><th>Project</th><th>Model</th><th>Started</th><th>Tokens</th><th>Cost</th></tr></thead>
      <tbody>{session_table}</tbody>
    </table>
  </div>

  <div class="footer">Generated by Voyage v0.1.0</div>
</div>

<script>
const colors = ['#58a6ff','#3fb950','#d29922','#f85149','#bc8cff','#f78166','#56d4dd'];
Chart.defaults.color = '#8b949e';
Chart.defaults.borderColor = '#30363d';

new Chart(document.getElementById('modelCostChart'), {{
  type: 'doughnut',
  data: {{
    labels: [{model_labels}],
    datasets: [{{ data: [{model_costs}], backgroundColor: colors, borderWidth: 0 }}]
  }},
  options: {{ plugins: {{ legend: {{ position: 'bottom' }} }} }}
}});

new Chart(document.getElementById('projectCostChart'), {{
  type: 'bar',
  data: {{
    labels: [{proj_labels}],
    datasets: [{{ label: 'Cost ($)', data: [{proj_costs}], backgroundColor: '#58a6ff', borderRadius: 6 }}]
  }},
  options: {{ indexAxis: 'y', plugins: {{ legend: {{ display: false }} }}, scales: {{ x: {{ grid: {{ display: false }} }} }} }}
}});

new Chart(document.getElementById('timelineChart'), {{
  type: 'bar',
  data: {{
    labels: [{timeline_labels}],
    datasets: [{{ label: 'Session Cost ($)', data: [{timeline_data}], backgroundColor: '#3fb950', borderRadius: 4 }}]
  }},
  options: {{ plugins: {{ legend: {{ display: false }} }}, scales: {{ x: {{ grid: {{ display: false }} }}, y: {{ beginAtZero: true }} }} }}
}});
</script>
</body>
</html>"##,
        days = days,
        now = Utc::now().format("%Y-%m-%d %H:%M UTC"),
        cost = overall.total_cost_usd,
        total_tokens_fmt = format_tokens(total_tokens),
        session_count = overall.session_count,
        input_fmt = format_tokens(overall.input_tokens),
        output_fmt = format_tokens(overall.output_tokens),
        cache_fmt = format_tokens(overall.cache_read_tokens + overall.cache_creation_tokens),
        model_labels = model_labels.join(","),
        model_costs = model_costs.join(","),
        proj_labels = proj_labels.join(","),
        proj_costs = proj_costs.join(","),
        timeline_labels = sessions
            .iter()
            .map(|s| format!("\"{}\"", s.started_at.format("%m-%d %H:%M")))
            .collect::<Vec<_>>()
            .join(","),
        timeline_data = sessions
            .iter()
            .map(|s| format!("{:.4}", s.estimated_cost_usd))
            .collect::<Vec<_>>()
            .join(","),
        session_table = session_rows.join("\n      "),
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
            let _ = std::process::Command::new("open")
                .arg(&output_path)
                .spawn();
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

use std::path::PathBuf;
