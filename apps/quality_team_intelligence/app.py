"""Quality Team Intelligence — Streamlit demo app.

Single-page tabbed flow that tells the cleaning story end-to-end:

  📥 Input        — browse the messy source xlsx files in SharePoint /input
  🧹 Cleaning     — what got fixed (419 logged DQ events, by rule)
  🤖 AI Mapping   — column-name decisions made by the LLM/synonym matcher
  📤 Output       — both cleaned views (tidy + same-format) with downloads
  📊 Analytics    — pass-rate / spec-violation / trend charts over gold

Reads from quality_de.{bronze,silver,gold}.* via the workspace SQL Warehouse
and from /Volumes/quality_de/bronze/{sharepoint_input,sharepoint_output}/
directly. Triggers the deployed pipeline job via the Databricks SDK.
"""

from __future__ import annotations

import io
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from openpyxl import load_workbook


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CATALOG = os.environ.get("QDE_CATALOG", "quality_de")
VOLUME_INPUT = os.environ.get("QDE_VOLUME_INPUT", "sharepoint_input")
VOLUME_OUTPUT = os.environ.get("QDE_VOLUME_OUTPUT", "sharepoint_output")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "2de6a251cf2870eb")
JOB_NAME_HINT = os.environ.get("QDE_JOB_NAME_HINT", "quality_de")

INPUT_VOLUME_PATH = Path(f"/Volumes/{CATALOG}/bronze/{VOLUME_INPUT}")
OUTPUT_VOLUME_PATH = Path(f"/Volumes/{CATALOG}/bronze/{VOLUME_OUTPUT}")
CLEANED_VOLUME_PATH = OUTPUT_VOLUME_PATH / "cleaned"

# Visual constants
PRIMARY = "#0B6BCB"
ACCENT_GREEN = "#2E7D32"
ACCENT_RED = "#C62828"
ACCENT_AMBER = "#EF6C00"


st.set_page_config(
    page_title="Quality Team Intelligence",
    page_icon="🧪",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Trim Streamlit chrome
st.markdown(
    """
    <style>
        .block-container { padding-top: 1.6rem; padding-bottom: 2rem; }
        h1 { padding-top: 0; }
        [data-testid="stMetricValue"] { font-size: 1.8rem; }
        [data-testid="stMetricLabel"] { font-size: 0.85rem; opacity: 0.75; }
        .stTabs [data-baseweb="tab-list"] { gap: 1.5rem; }
        .stTabs [data-baseweb="tab"] { font-size: 1rem; padding: 0.6rem 0.2rem; }
        .badge {
            display: inline-block;
            padding: 0.18rem 0.55rem;
            border-radius: 999px;
            font-size: 0.78rem;
            font-weight: 600;
            font-family: ui-monospace, SFMono-Regular, monospace;
        }
        .badge-ok   { background: #E6F4EA; color: #1E6F30; }
        .badge-fail { background: #FCE8E6; color: #B3261E; }
        .badge-warn { background: #FEF7E0; color: #9A6700; }
    </style>
    """,
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# Connections — cached
# ---------------------------------------------------------------------------

@st.cache_resource(show_spinner=False)
def _config() -> Config:
    return Config()


@st.cache_resource(show_spinner=False)
def _workspace_client() -> WorkspaceClient:
    return WorkspaceClient()


def _sql_connection():
    cfg = _config()
    host = cfg.host or os.environ.get("DATABRICKS_HOST", "")
    host = host.replace("https://", "").replace("http://", "")
    return sql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        credentials_provider=lambda: cfg.authenticate,
    )


@st.cache_data(ttl=30, show_spinner=False)
def run_query(query: str) -> pd.DataFrame:
    with _sql_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            cols = [c[0] for c in cur.description]
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


# ---------------------------------------------------------------------------
# Job lookup + trigger
# ---------------------------------------------------------------------------

@st.cache_data(ttl=120, show_spinner=False)
def _find_pipeline_job() -> dict | None:
    w = _workspace_client()
    for j in w.jobs.list():
        name = (j.settings.name or "") if j.settings else ""
        if JOB_NAME_HINT in name.lower():
            return {"job_id": j.job_id, "name": name}
    return None


@st.cache_data(ttl=30, show_spinner=False)
def _last_run_info(job_id: int) -> dict | None:
    w = _workspace_client()
    runs = list(w.jobs.list_runs(job_id=job_id, limit=1))
    if not runs:
        return None
    r = runs[0]
    state = r.state
    end = r.end_time or 0
    when = datetime.fromtimestamp(end / 1000, tz=timezone.utc) if end else None
    return {
        "run_id": r.run_id,
        "state": (state.life_cycle_state.value if state and state.life_cycle_state else "?"),
        "result": (state.result_state.value if state and state.result_state else "?"),
        "ended_at": when,
        "duration_s": (r.run_duration / 1000) if r.run_duration else None,
        "url": r.run_page_url,
    }


def _trigger_job(job_id: int) -> int:
    w = _workspace_client()
    handle = w.jobs.run_now(job_id=job_id)
    return handle.run_id


# ---------------------------------------------------------------------------
# Volume file helpers
# ---------------------------------------------------------------------------

def _list_volume_xlsx(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out = []
    for p in sorted(path.glob("*.xlsx")):
        st_ = p.stat()
        out.append({
            "name": p.name,
            "path": str(p),
            "size_bytes": st_.st_size,
            "modified": datetime.fromtimestamp(st_.st_mtime),
        })
    return out


def _read_sheet_preview(path: Path, sheet_name: str, max_rows: int = 25) -> pd.DataFrame:
    wb = load_workbook(path, read_only=True, data_only=True)
    ws = wb[sheet_name]
    rows = []
    for i, row in enumerate(ws.iter_rows(values_only=True)):
        if i >= max_rows:
            break
        rows.append(row)
    if not rows:
        return pd.DataFrame()
    width = max(len(r) for r in rows)
    cols = [f"col_{chr(65 + c) if c < 26 else c+1}" for c in range(width)]
    padded = [list(r) + [None] * (width - len(r)) for r in rows]
    df = pd.DataFrame(padded, columns=cols)
    return df


def _list_sheets(path: Path) -> list[str]:
    wb = load_workbook(path, read_only=True, data_only=True)
    return list(wb.sheetnames)


def _file_bytes(path: Path) -> bytes:
    return path.read_bytes()


# ---------------------------------------------------------------------------
# Header — title bar + status
# ---------------------------------------------------------------------------

st.markdown(
    f"""
    <h1 style="margin-bottom: 0.2rem;">Quality Team Intelligence</h1>
    <div style="opacity: 0.75; font-size: 1rem; margin-bottom: 1.2rem;">
        Self-contained ADF mock · AI-driven cleaning · SharePoint round-trip
        &nbsp;·&nbsp; catalog <code>{CATALOG}</code>
    </div>
    """,
    unsafe_allow_html=True,
)


# ---- Top bar: KPIs + status ----
try:
    summary = run_query(f"""
        SELECT
          (SELECT COUNT(*) FROM {CATALOG}.bronze.raw_workbooks)                AS files,
          (SELECT COUNT(*) FROM {CATALOG}.silver.observations_long)            AS observations,
          (SELECT COUNT(*) FROM {CATALOG}.silver.dq_issues)                    AS fixes,
          (SELECT COUNT(*) FROM {CATALOG}.silver.column_mapping_log)           AS mappings,
          (SELECT COUNT(*) FROM {CATALOG}.gold.fact_observation
             WHERE pass = false)                                              AS violations,
          (SELECT ROUND(AVG(CASE WHEN pass=true THEN 100.0
                                 WHEN pass=false THEN 0.0 END), 1)
             FROM {CATALOG}.gold.fact_observation)                            AS pass_rate
    """).iloc[0]
except Exception as e:
    st.error(
        f"Couldn't query {CATALOG}. Has the pipeline been deployed and run? "
        f"Use `./deploy.sh` from the repo root. Details: `{e}`"
    )
    st.stop()


k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Files ingested", f"{int(summary.files):,}")
k2.metric("Observations", f"{int(summary.observations):,}")
k3.metric("DQ fixes", f"{int(summary.fixes):,}")
k4.metric("AI mappings", f"{int(summary.mappings):,}")
k5.metric("Spec violations", f"{int(summary.violations):,}")
k6.metric("Pass rate", f"{summary.pass_rate}%")


# ---- Pipeline status row ----
job = _find_pipeline_job()
ps_left, ps_mid, ps_right = st.columns([3, 2, 1])

with ps_left:
    if job:
        st.markdown(
            f"**Pipeline job** &nbsp; `{job['name']}`",
            unsafe_allow_html=True,
        )
    else:
        st.warning("No deployed pipeline job found in this workspace.")

with ps_mid:
    if job:
        last = _last_run_info(job["job_id"])
        if last:
            badge_class = "badge-ok" if last["result"] == "SUCCESS" else (
                "badge-fail" if last["result"] == "FAILED" else "badge-warn"
            )
            ts = last["ended_at"].strftime("%Y-%m-%d %H:%M UTC") if last["ended_at"] else "—"
            dur = f" · {int(last['duration_s'])}s" if last["duration_s"] else ""
            st.markdown(
                f"<div style='padding-top:0.4rem;'>Last run: "
                f"<span class='badge {badge_class}'>{last['result']}</span>"
                f" &nbsp; <span style='opacity:0.7'>{ts}{dur}</span></div>",
                unsafe_allow_html=True,
            )

with ps_right:
    if job and st.button("▶ Re-run pipeline", use_container_width=True, type="primary"):
        with st.spinner("Triggering pipeline run..."):
            run_id = _trigger_job(job["job_id"])
        st.success(f"Triggered run {run_id}. Refresh in ~5 min to see new data.")
        run_query.clear()
        _last_run_info.clear()


st.divider()


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab_in, tab_clean, tab_ai, tab_out, tab_analytics = st.tabs([
    "📥 Input — the mess",
    "🧹 Cleaning — every fix",
    "🤖 AI Mapping",
    "📤 Output — round-trip",
    "📊 Analytics",
])


# ===========================================================================
# Tab 1 — Input
# ===========================================================================
with tab_in:
    st.markdown(
        "**What the Quality team uploads to SharePoint.** Multi-tab Excel with "
        "merged headers, RT/RRT/Spec metadata bands, free-text appearance fields. "
        "Each workbook below is one source file in the `/input/` volume."
    )
    files = _list_volume_xlsx(INPUT_VOLUME_PATH)
    if not files:
        st.warning(
            f"No xlsx files found in `{INPUT_VOLUME_PATH}`. "
            "Run the setup task to generate synthetic input files."
        )
    else:
        # File cards
        cols = st.columns(len(files))
        for col, f in zip(cols, files):
            with col:
                st.markdown(
                    f"""
                    <div style="border:1px solid #E0E0E0; border-radius:8px;
                               padding:0.9rem 1.1rem; background:#FAFAFA;">
                       <div style="font-weight:600; font-size:0.95rem; margin-bottom:0.4rem;">
                           {f['name']}
                       </div>
                       <div style="opacity:0.7; font-size:0.85rem;">
                           {f['size_bytes']:,} bytes &nbsp;·&nbsp;
                           {f['modified'].strftime('%Y-%m-%d %H:%M')}
                       </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

        st.markdown("&nbsp;", unsafe_allow_html=True)

        # File + sheet selectors
        names = [f["name"] for f in files]
        sel_file = st.selectbox("Open a workbook", names, key="in_file")
        sel_path = INPUT_VOLUME_PATH / sel_file
        try:
            sheets = _list_sheets(sel_path)
        except Exception as e:
            st.error(f"Could not open {sel_file}: {e}")
            sheets = []

        if sheets:
            sel_sheet = st.selectbox("Sheet", sheets, key="in_sheet",
                                     help="Each batch lives on its own tab")
            df = _read_sheet_preview(sel_path, sel_sheet, max_rows=20)

            # Look up DQ issue count for this sheet to motivate the demo
            try:
                dirt_count = run_query(f"""
                    SELECT COUNT(*) AS n FROM {CATALOG}.silver.dq_issues
                    WHERE workbook = '{sel_file}' AND sheet = '{sel_sheet}'
                """).iloc[0]["n"]
            except Exception:
                dirt_count = None

            head_l, head_r = st.columns([3, 1])
            with head_l:
                st.markdown(f"**Preview** — `{sel_sheet}`, first 20 rows")
            with head_r:
                if dirt_count is not None:
                    st.markdown(
                        f"<div style='text-align:right;padding-top:0.3rem'>"
                        f"<span class='badge badge-warn'>{int(dirt_count)} dirty cells fixed by pipeline</span>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )

            st.dataframe(df, use_container_width=True, height=520, hide_index=False)

            with st.expander("What you're looking at"):
                st.markdown(
                    """
                    - **Rows 3–11** form the merged-cell header band (process label, RT, RRT,
                      Unit, Specification, Internal SRF Specification).
                    - **Row 13 onwards** is the actual data — batch number, sample/report time,
                      appearance, plus the 5–9 impurity columns.
                    - Look for `??:??` malformed times, `NULL`/`?` sentinels in the appearance
                      columns, leading whitespace, lowercase variants, and occasional negative
                      impurity values. The cleaner catches all of them — see the next tab.
                    """
                )


# ===========================================================================
# Tab 2 — Cleaning
# ===========================================================================
with tab_clean:
    st.markdown(
        "**Every cleaning rule that fired during silver, with full audit trail.** "
        "Each repair is logged so any reviewer can trace `raw → repaired` back "
        "to the source row."
    )

    breakdown = run_query(f"""
        SELECT rule, severity, COUNT(*) AS issues
        FROM {CATALOG}.silver.dq_issues
        GROUP BY rule, severity
        ORDER BY issues DESC
    """)

    chart_col, table_col = st.columns([3, 2])

    with chart_col:
        st.markdown("**Fixes by rule**")
        if breakdown.empty:
            st.info("No DQ events recorded yet — run the pipeline.")
        else:
            chart_df = breakdown.copy()
            chart_df["rule"] = chart_df["rule"].astype(str)
            chart_df = chart_df.set_index("rule")
            st.bar_chart(chart_df["issues"], height=320)

    with table_col:
        st.markdown("**Total**")
        st.metric("Issues logged", f"{int(breakdown['issues'].sum()):,}" if not breakdown.empty else "0")
        st.dataframe(
            breakdown.style.format({"issues": "{:,}"}),
            use_container_width=True, hide_index=True,
        )

    st.markdown("&nbsp;")
    st.markdown("**Sample fixes** — filter by rule to see specific repairs")
    rules = ["(all)"] + sorted(breakdown["rule"].unique().tolist()) if not breakdown.empty else ["(all)"]
    f_rule = st.selectbox("Rule", rules, key="dq_rule")
    where = "" if f_rule == "(all)" else f"WHERE rule = '{f_rule}'"
    fixes = run_query(f"""
        SELECT sheet, row_seq, column, rule, severity, raw_value, repaired_value, note
        FROM {CATALOG}.silver.dq_issues {where}
        ORDER BY rule, sheet, row_seq
        LIMIT 200
    """)
    st.dataframe(fixes, use_container_width=True, hide_index=True, height=420)


# ===========================================================================
# Tab 3 — AI mapping
# ===========================================================================
with tab_ai:
    st.markdown(
        "**Column-name decisions made by the LLM column mapper** (with a "
        "deterministic synonym fallback when the LLM endpoint isn't reachable). "
        "Every decision carries a confidence score and rationale."
    )

    src_breakdown = run_query(f"""
        SELECT source, COUNT(*) AS n
        FROM {CATALOG}.silver.column_mapping_log
        WHERE role <> 'ignored'
        GROUP BY source
    """)

    s1, s2, s3 = st.columns(3)
    s1.metric("Total decisions",
              f"{int(src_breakdown['n'].sum()):,}" if not src_breakdown.empty else "0")
    s2.metric(
        "Mode",
        src_breakdown.iloc[0]["source"] if not src_breakdown.empty else "—",
        help="`llm` if the foundation model fired; `mock_synonyms` if it fell back",
    )
    quarantine = run_query(f"""
        SELECT COUNT(*) AS n FROM {CATALOG}.silver.column_mapping_log
        WHERE role <> 'ignored' AND confidence < 0.5
    """).iloc[0]["n"]
    s3.metric("Flagged for review", f"{int(quarantine):,}",
              delta="confidence < 0.5", delta_color="off")

    st.markdown("&nbsp;")
    min_conf = st.slider("Show mappings with confidence ≥", 0.0, 1.0, 0.0, step=0.05)
    mappings = run_query(f"""
        SELECT sheet, raw_label, role, canonical, ROUND(confidence, 2) AS confidence,
               source, rationale
        FROM {CATALOG}.silver.column_mapping_log
        WHERE role <> 'ignored' AND confidence >= {min_conf}
        ORDER BY confidence ASC, sheet, raw_label
    """)
    st.dataframe(mappings, use_container_width=True, hide_index=True, height=480)


# ===========================================================================
# Tab 4 — Output
# ===========================================================================
with tab_out:
    st.markdown(
        "**Two cleaned views, each in its own SharePoint /output subfolder.** "
        "The same-format view drops back into the Quality team's familiar "
        "Excel template; the tidy long-form view feeds analytics and dashboards."
    )

    out_l, out_r = st.columns(2)

    with out_l:
        st.markdown("### 📂 `/output/cleaned/` — same format")
        st.caption(
            "Same 7 tabs, same merged headers, same column layout — only data "
            "cells rewritten with cleaned values."
        )
        for f in _list_volume_xlsx(CLEANED_VOLUME_PATH):
            with st.container(border=True):
                a, b = st.columns([3, 1])
                with a:
                    st.markdown(f"**{f['name']}**")
                    st.caption(
                        f"{f['size_bytes']:,} bytes · "
                        f"{f['modified'].strftime('%Y-%m-%d %H:%M')}"
                    )
                with b:
                    st.download_button(
                        "Download",
                        data=_file_bytes(Path(f["path"])),
                        file_name=f["name"],
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                        key=f"dl_clean_{f['name']}",
                    )

    with out_r:
        st.markdown("### 📂 `/output/` — tidy long-form")
        st.caption(
            "Three sheets per workbook (`observations`, `dq_issues`, "
            "`column_mapping_log`) — analyst- and dashboard-friendly."
        )
        for f in _list_volume_xlsx(OUTPUT_VOLUME_PATH):
            with st.container(border=True):
                a, b = st.columns([3, 1])
                with a:
                    st.markdown(f"**{f['name']}**")
                    st.caption(
                        f"{f['size_bytes']:,} bytes · "
                        f"{f['modified'].strftime('%Y-%m-%d %H:%M')}"
                    )
                with b:
                    st.download_button(
                        "Download",
                        data=_file_bytes(Path(f["path"])),
                        file_name=f["name"],
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                        key=f"dl_tidy_{f['name']}",
                    )

    st.markdown("&nbsp;")
    st.markdown("### Same-format preview")
    same_fmt_files = _list_volume_xlsx(CLEANED_VOLUME_PATH)
    if same_fmt_files:
        sel = st.selectbox(
            "Pick a cleaned workbook", [f["name"] for f in same_fmt_files], key="out_file"
        )
        sel_path = CLEANED_VOLUME_PATH / sel
        sheets = _list_sheets(sel_path)
        sheet = st.selectbox("Sheet", sheets, key="out_sheet")
        st.dataframe(
            _read_sheet_preview(sel_path, sheet, max_rows=20),
            use_container_width=True, height=500,
        )


# ===========================================================================
# Tab 5 — Analytics
# ===========================================================================
with tab_analytics:
    st.markdown(
        "**Compliance and trend views over the cleaned gold layer.** "
        "These are the same queries a downstream Lakeview dashboard would use."
    )

    # --- Pass-rate by batch ---
    st.markdown("#### Pass rate by batch")
    pass_rate = run_query(f"""
        SELECT batch_no, sheet, observations, passing, failing,
               ROUND(pass_rate * 100, 1) AS pass_rate_pct
        FROM {CATALOG}.gold.mv_batch_pass_rate
        ORDER BY pass_rate ASC
        LIMIT 15
    """)
    if not pass_rate.empty:
        chart_l, chart_r = st.columns([2, 3])
        with chart_l:
            st.dataframe(pass_rate, use_container_width=True, hide_index=True, height=380)
        with chart_r:
            chart_df = pass_rate.set_index("batch_no")[["pass_rate_pct"]]
            st.bar_chart(chart_df, height=360, color=PRIMARY)

    st.markdown("&nbsp;")

    # --- Spec violations by analyte ---
    st.markdown("#### Spec violations by analyte")
    violations = run_query(f"""
        SELECT analyte_canonical, SUM(violation_count) AS violations,
               ROUND(AVG(avg_overshoot), 4) AS avg_overshoot
        FROM {CATALOG}.gold.mv_spec_violations
        GROUP BY analyte_canonical
        ORDER BY violations DESC
    """)
    if not violations.empty:
        v_l, v_r = st.columns([3, 2])
        with v_l:
            st.bar_chart(
                violations.set_index("analyte_canonical")["violations"],
                height=320, color=ACCENT_RED,
            )
        with v_r:
            st.dataframe(violations, use_container_width=True, hide_index=True, height=320)

    st.markdown("&nbsp;")

    # --- Impurity trend over time ---
    st.markdown("#### Impurity trend (avg value over time)")
    trend = run_query(f"""
        SELECT analyte_canonical, sample_date, avg_value, spec_max
        FROM {CATALOG}.gold.mv_impurity_trend
        ORDER BY sample_date
    """)
    if not trend.empty:
        # Pivot for line chart
        pivot = trend.pivot_table(
            index="sample_date", columns="analyte_canonical",
            values="avg_value", aggfunc="mean",
        )
        st.line_chart(pivot, height=380)


# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------

st.divider()
st.caption(
    f"**Quality Team Intelligence** · catalog `{CATALOG}` · "
    f"warehouse `{WAREHOUSE_ID[:6]}…` · source: "
    "[github.com/AbhinavJFT/primeinsurance-poc](https://github.com/AbhinavJFT/primeinsurance-poc)"
)
