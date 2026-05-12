import html
import os
from datetime import datetime

import pandas as pd


def ensure_output_layout(output_dir):
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(os.path.join(output_dir, "events"), exist_ok=True)


def write_csv(df, path):
    df.to_csv(path, index=False)


def append_csv(df, path):
    header = not os.path.exists(path)
    df.to_csv(path, index=False, mode="a", header=header)


def load_csv(path):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def dataframe_to_table(df, title, max_rows=10):
    if df.empty:
        return (
            f"<section class='panel'><h2>{html.escape(title)}</h2>"
            "<p class='empty'>No data available yet.</p></section>"
        )

    subset = df.head(max_rows)
    header_html = "".join(f"<th>{html.escape(str(col))}</th>" for col in subset.columns)
    rows_html = []
    for _, row in subset.iterrows():
        cells = "".join(f"<td>{html.escape(str(value))}</td>" for value in row.tolist())
        rows_html.append(f"<tr>{cells}</tr>")

    return (
        f"<section class='panel'><h2>{html.escape(title)}</h2>"
        "<div class='table-wrap'><table>"
        f"<thead><tr>{header_html}</tr></thead>"
        f"<tbody>{''.join(rows_html)}</tbody>"
        "</table></div></section>"
    )


def bar_rows(df, label_col, value_col, title, formatter=None):
    if df.empty or label_col not in df.columns or value_col not in df.columns:
        return (
            f"<section class='panel'><h2>{html.escape(title)}</h2>"
            "<p class='empty'>No chart data available yet.</p></section>"
        )

    chart_df = df[[label_col, value_col]].copy().head(8)
    max_value = max(float(chart_df[value_col].max()), 1.0)
    rows = []
    for _, row in chart_df.iterrows():
        label = html.escape(str(row[label_col]))
        raw_value = row[value_col]
        value = formatter(raw_value) if formatter else raw_value
        width = max(6.0, float(raw_value) / max_value * 100.0)
        rows.append(
            "<div class='bar-row'>"
            f"<div class='bar-label'>{label}</div>"
            "<div class='bar-track'>"
            f"<div class='bar-fill' style='width:{width:.1f}%'></div>"
            "</div>"
            f"<div class='bar-value'>{html.escape(str(value))}</div>"
            "</div>"
        )

    return (
        f"<section class='panel'><h2>{html.escape(title)}</h2>"
        f"<div class='bars'>{''.join(rows)}</div></section>"
    )


def write_summary(output_dir):
    history_df = load_csv(os.path.join(output_dir, "kpi_history.csv"))
    city_df = load_csv(os.path.join(output_dir, "city_summary_latest.csv"))
    membership_df = load_csv(os.path.join(output_dir, "membership_summary_latest.csv"))
    risk_df = load_csv(os.path.join(output_dir, "at_risk_customers_latest.csv"))

    latest = history_df.iloc[-1].to_dict() if not history_df.empty else {}
    summary_path = os.path.join(output_dir, "summary.md")

    with open(summary_path, "w", encoding="utf-8") as handle:
        handle.write("# Realtime Analysis Summary\n\n")
        handle.write(
            f"- Generated at: {datetime.now().isoformat(timespec='seconds')}\n"
        )
        if latest:
            handle.write(f"- Processed batches: {int(latest['processed_batches'])}\n")
            handle.write(
                f"- Total streamed events: {int(latest['cumulative_event_count'])}\n"
            )
            handle.write(
                f"- Latest batch revenue: {float(latest['total_revenue']):.2f}\n"
            )
            handle.write(f"- Latest batch avg spend: {float(latest['avg_spend']):.2f}\n")
            handle.write(
                f"- Latest batch at-risk customers: {int(latest['at_risk_count'])}\n"
            )
        handle.write(f"- Cities in latest batch: {len(city_df)}\n")
        handle.write(f"- Membership groups in latest batch: {len(membership_df)}\n")
        handle.write(f"- At-risk records in latest batch: {len(risk_df)}\n\n")
        handle.write("## Files\n\n")
        handle.write("- `kpi_history.csv`: batch-level KPI history for the stream\n")
        handle.write("- `stream_events.csv`: all streamed events captured during the run\n")
        handle.write("- `latest_events.csv`: latest micro-batch events\n")
        handle.write("- `city_summary_latest.csv`: city-level KPI snapshot\n")
        handle.write("- `membership_summary_latest.csv`: membership-level KPI snapshot\n")
        handle.write("- `at_risk_customers_latest.csv`: customers flagged as at risk\n")
        handle.write("- `dashboard.html`: saved HTML dashboard\n")


def render_dashboard(output_dir):
    ensure_output_layout(output_dir)

    history_df = load_csv(os.path.join(output_dir, "kpi_history.csv"))
    latest_events_df = load_csv(os.path.join(output_dir, "latest_events.csv"))
    city_df = load_csv(os.path.join(output_dir, "city_summary_latest.csv"))
    membership_df = load_csv(os.path.join(output_dir, "membership_summary_latest.csv"))
    risk_df = load_csv(os.path.join(output_dir, "at_risk_customers_latest.csv"))

    latest = history_df.iloc[-1].to_dict() if not history_df.empty else {}
    cards = {
        "Processed Batches": int(latest.get("processed_batches", 0)),
        "Total Events": int(latest.get("cumulative_event_count", 0)),
        "Latest Revenue": f"{float(latest.get('total_revenue', 0.0)):.2f}",
        "At-Risk Customers": int(latest.get("at_risk_count", 0)),
    }

    cards_html = "".join(
        "<div class='card'>"
        f"<div class='card-label'>{html.escape(label)}</div>"
        f"<div class='card-value'>{html.escape(str(value))}</div>"
        "</div>"
        for label, value in cards.items()
    )

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Realtime E-commerce Dashboard</title>
  <style>
    :root {{
      --bg: #f4efe6;
      --panel: #fffdf8;
      --ink: #1e1f23;
      --muted: #646970;
      --accent: #0f766e;
      --accent-soft: #d6f4ef;
      --border: #ddd4c7;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", "PingFang SC", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top right, #d8f3dc 0, transparent 28%),
        linear-gradient(180deg, #f9f4ea 0%, var(--bg) 100%);
    }}
    .container {{
      max-width: 1200px;
      margin: 0 auto;
      padding: 32px 20px 48px;
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: 34px;
    }}
    .subtitle {{
      margin: 0 0 24px;
      color: var(--muted);
    }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
      gap: 16px;
      margin-bottom: 20px;
    }}
    .card, .panel {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 18px;
      box-shadow: 0 10px 30px rgba(40, 44, 52, 0.05);
    }}
    .card {{
      padding: 18px;
    }}
    .card-label {{
      color: var(--muted);
      font-size: 14px;
      margin-bottom: 8px;
    }}
    .card-value {{
      font-size: 28px;
      font-weight: 700;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 16px;
    }}
    .panel {{
      padding: 18px;
    }}
    .panel h2 {{
      margin: 0 0 14px;
      font-size: 20px;
    }}
    .empty {{
      color: var(--muted);
      margin: 0;
    }}
    .bars {{
      display: grid;
      gap: 12px;
    }}
    .bar-row {{
      display: grid;
      grid-template-columns: 120px 1fr 90px;
      gap: 12px;
      align-items: center;
    }}
    .bar-label, .bar-value {{
      font-size: 14px;
    }}
    .bar-track {{
      height: 12px;
      background: #ece5d9;
      border-radius: 999px;
      overflow: hidden;
    }}
    .bar-fill {{
      height: 100%;
      background: linear-gradient(90deg, var(--accent) 0%, #14b8a6 100%);
      border-radius: 999px;
    }}
    .table-wrap {{
      overflow-x: auto;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }}
    th, td {{
      text-align: left;
      padding: 10px 8px;
      border-bottom: 1px solid #efe7dc;
      white-space: nowrap;
    }}
    th {{
      color: var(--muted);
      font-weight: 600;
      background: #faf6ef;
    }}
    .footer {{
      margin-top: 20px;
      color: var(--muted);
      font-size: 13px;
    }}
  </style>
</head>
<body>
  <div class="container">
    <h1>Realtime E-commerce Dashboard</h1>
    <p class="subtitle">Saved at {html.escape(datetime.now().isoformat(timespec='seconds'))}</p>
    <section class="cards">{cards_html}</section>
    <section class="grid">
      {bar_rows(city_df, "city", "avg_total_spend", "Average Spend by City", lambda value: f"{float(value):.2f}")}
      {bar_rows(membership_df, "membership_type", "event_count", "Events by Membership Type", lambda value: int(value))}
      {dataframe_to_table(history_df.tail(8).iloc[::-1], "Batch KPI History", max_rows=8)}
      {dataframe_to_table(risk_df, "At-Risk Customers", max_rows=10)}
      {dataframe_to_table(latest_events_df, "Latest Stream Events", max_rows=10)}
      {dataframe_to_table(membership_df, "Membership Snapshot", max_rows=10)}
    </section>
    <p class="footer">This dashboard is generated from files under Reports/realtime.</p>
  </div>
</body>
</html>
"""

    with open(os.path.join(output_dir, "dashboard.html"), "w", encoding="utf-8") as handle:
        handle.write(page)

    write_summary(output_dir)
