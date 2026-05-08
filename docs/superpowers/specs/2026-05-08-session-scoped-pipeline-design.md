# Session-Scoped Pipeline — Design

**Branch:** `feat/session-scoped-pipeline`
**Date:** 2026-05-08
**Status:** Draft, awaiting user review

---

## Problem

Today the Quality Team Intelligence Streamlit app cleans Excel files **in-process inside its own container** (`quality_core.process_workbook` running on /tmp), and the deployed Databricks medallion job (`quality_de_pipeline`) runs over the entire `/Volumes/quality_de/bronze/sharepoint_input/` folder in `overwrite` mode against shared bronze/silver/gold tables. The two are independent — the app demo doesn't exercise the lakehouse architecture, and the medallion job can't be safely run against just a subset of files.

We want the app to become the front-end for the lakehouse pipeline: the user generates N synthetic files, the app uploads them to a session-scoped subfolder in the input volume, triggers the medallion job scoped to that session, streams live status, then renders the cleaned outputs back from the session-scoped output subfolder. All Delta tables track which session each row came from.

## Goals

1. **Session isolation**: each app run lives in its own subfolder under both input and output volumes; rows in bronze/silver/gold are tagged with `session_id`.
2. **Live progress**: while the Databricks job runs, the app shows per-task state (5 tasks: setup → bronze → silver → gold → export) updating every ~3s.
3. **Rate-limit resilience**: 429s from the Foundation Model endpoint don't kill the run; the cleaner falls back to the deterministic synonym matcher per-sheet.
4. **No regressions**: History view and Dashboard continue to work, just sourced from the new session-scoped tables instead of `/tmp/qde_runs/`.

## Non-goals

- Run-level concurrency locking (single-user POC; bundle queue is enabled but no UC semaphore).
- Reattaching to a running job after the user closes the browser (History view is the recovery path).
- Auto-cleanup of orphaned session subfolders (manual via the Discard button or workspace UI).
- Real ADF — the existing connectors mock layer remains in place.

---

## Architecture

### Data flow (per app session)

```
[App container]                 [Unity Catalog]                  [Databricks Job]
   │                                  │                                │
 1 ├── slider N=20 ──── Generate ──┐  │                                │
   │   build N xlsx in /tmp/qde_   │  │                                │
   │   session/<sid>/              │  │                                │
   │                               │  │                                │
 2 ├── files.upload(...) ─────────►├─►/Volumes/.../sharepoint_         │
   │                               │  input/sessions/<sid>/{N files}   │
   │   stage="staged"              │                                   │
   │                               │                                   │
 3 ├── jobs.run_now(notebook_      │                                   │
   │     params={session_id:<sid>})┼──────────────────────────────────►│
   │                               │                                   │
 4 ├── while not done: ◄───────────┼─── jobs.get_run(run_id) ──────────│
   │     poll every 3s             │   tasks[].state.life_cycle_state  │
   │     update st.empty()         │                                   │
   │   stage="running"             │                                   │
   │                               │                                   │
   │   Tasks during step 4:                                            │
   │     setup → bronze_ingest → silver_ai_cleaning →                  │
   │     gold_curated → export_sharepoint                              │
   │                               │                                   │
   │   Each task writes APPEND-mode rows tagged with session_id        │
   │   into bronze/silver/gold tables; export writes outputs to        │
   │   /output/sessions/<sid>/{cleaned,transformed}/                   │
   │                               │                                   │
 5 ├── read outputs ◄──────────────│                                   │
   │   render 4 tabs filtered      │                                   │
   │   WHERE session_id=<sid>      │                                   │
   │   stage="results"             │                                   │
```

### Session ID

Format: `YYYY-MM-DD-hhmmss-<6hex>` (e.g. `2026-05-08-141532-a3f9b1`). Human-readable, lexicographically sortable, collision-resistant within a one-second window per app instance. Generated client-side at the moment the user clicks **Generate**.

### Volume layout

```
/Volumes/quality_de/bronze/sharepoint_input/
├── (legacy main /input/ files — untouched)
└── sessions/
    └── 2026-05-08-141532-a3f9b1/
        ├── file_001.xlsx
        └── ... (N files)

/Volumes/quality_de/bronze/sharepoint_output/
└── sessions/
    └── 2026-05-08-141532-a3f9b1/
        ├── cleaned/                ← same-format (input-shape preserved)
        │   └── file_001.xlsx
        └── transformed/            ← tidy long-form (3 sheets per file)
            └── file_001_CLEAN.xlsx
```

### Schema changes

All five Delta tables get a non-null `session_id STRING` column, switch to `append` mode with `mergeSchema=true`, and partition by `session_id`:

| Table | New column | Partition | Mode |
|---|---|---|---|
| `quality_de.bronze.raw_workbooks` | `session_id` | `session_id` | append |
| `quality_de.silver.observations_long` | `session_id` | `session_id` | append |
| `quality_de.silver.dq_issues` | `session_id` | `session_id` | append |
| `quality_de.silver.column_mapping_log` | `session_id` | `session_id` | append |
| `quality_de.gold.fact_observation` | `session_id` | `session_id` | append |
| `gold.mv_*` materialized views | column propagates | n/a | rebuild from sources |

**One-time reset**: a new notebook `00a_reset_tables.py` drops all five tables. They're recreated by the next pipeline run with the new schema. The 103-file run currently in flight is invalidated by this; we have nothing else worth keeping in those tables yet.

### Pipeline parameter wiring

`databricks.yml`: every task in `quality_de_pipeline` gains `base_parameters.session_id` (default value `"legacy_main_pipeline"`). When the app calls `jobs.run_now(notebook_params={"session_id": "<sid>"})`, that override propagates to `dbutils.widgets.get("session_id")` in every notebook. Default behavior (no override) means existing scheduled or manual runs keep working as today, all rows landing under the legacy session_id.

`session_id` is intentionally **not** a job-level parameter — task-level base_parameters lets `notebook_params` override cleanly (verified per Databricks docs: job-level params would otherwise win).

### Rate-limit mitigation

Three layers:

1. **Graceful fallback in `quality_core.mapping`**: catch `databricks.sdk` and `openai` rate-limit / 429 exceptions in addition to existing connection errors, fall back to the deterministic `mock_synonyms` matcher for that sheet. The run completes; affected mappings carry `source = "mock_synonyms"` and lower confidence — visible in the Column Resolution tab.

2. **Slider ceiling = 25**: keeps the per-minute LLM call volume below the workspace's pay-per-token throughput cap for `databricks-gpt-oss-20b`. Lower than the discussed 50; based on observed failure of the 103-file run.

3. **Provisioned-throughput option**: `databricks.yml` has the `llm_endpoint` variable. We add a commented `llm_endpoint_provisioned` line so a one-line edit in the bundle config swaps to a provisioned endpoint when demos need to scale.

---

## App UX (the four stages)

State machine on `st.session_state.stage`:

| Stage | Trigger to enter | Screen content | Primary actions |
|---|---|---|---|
| `empty` | Initial load, or "Run another" from results | Title, slider (1–25, default 20), "Generate" button | Generate |
| `staged` | After successful upload of N files to volume | "Session `<id>` ready. N files in volume." + file list preview + 2 buttons | **Run pipeline** (primary), **Discard session** (secondary) |
| `running` | After `jobs.run_now` returns a run_id | Live status: 5-task list + per-task state markers + elapsed time, total elapsed | (read-only while polling) |
| `results` | When run state = `TERMINATED + SUCCESS` | 4 existing tabs (Deliverables / DQ Audit / Column Resolution / Compliance Metrics), session-scoped | "Run another" |

### Live status detail (stage `running`)

```
Session 2026-05-08-141532-a3f9b1 · pipeline run 0123456 · 02:34 elapsed

✓  setup                    SUCCESS    23s
✓  bronze_ingest            SUCCESS    1m 14s
●  silver_ai_cleaning       RUNNING    47s elapsed
○  gold_curated             PENDING
○  export_sharepoint        PENDING
```

State markers (text only, per existing no-emoji preference):
- `○` queued / not yet started
- `●` running
- `✓` succeeded
- `✗` failed

Polling cadence: 3s. Updates rendered into a single `st.empty()` container so the page doesn't flicker.

### Discard session

Click **Discard session** in stage `staged` (or from the failure path):
1. Delete the session subfolder from input volume (`/sessions/<sid>/`)
2. Delete rows from all 5 tables `WHERE session_id = <sid>` (only relevant if a previous Run pipeline call already wrote some rows)
3. Reset to stage `empty`

### Retry on failure

Polling detects `result_state = FAILED` for any task → freeze that task with `✗` + show task `state_message`. Two buttons:
- **Retry this session** — deletes existing rows for `session_id` from all 5 tables (so we don't get duplicates), re-triggers `jobs.run_now` with the same `session_id`, returns to `running`. Files in the input subfolder stay.
- **Discard and start over** — same as Discard session above; back to `empty`.

### Browser closed mid-run

Job keeps running on Databricks (independent of the app). When the user reloads, they're back at `empty`. Their session's outputs become visible in the History view once the run terminates. No reattach logic for the POC.

---

## File-level change list

### Bundle / config
- `databricks.yml` — add `base_parameters.session_id = "legacy_main_pipeline"` to all 5 tasks; add commented `llm_endpoint_provisioned` variable
- `apps/quality_team_intelligence/app.yaml` — add `WRITE_VOLUME` on `bronze.sharepoint_input`, add `CAN_MANAGE_RUN` on the pipeline job

### Notebooks
- `notebooks/00a_reset_tables.py` (NEW) — drop 5 tables, idempotent
- `notebooks/00_setup.py` — accept `session_id`, create new tables with `session_id STRING NOT NULL` + `PARTITIONED BY (session_id)`
- `notebooks/01_bronze_sharepoint_ingest.py` — accept `session_id`; if non-default, list files from `<input_volume>/sessions/<session_id>/`; tag every bronze row with `session_id`; append + mergeSchema
- `notebooks/02_silver_ai_cleaning.py` — accept `session_id`; filter bronze WHERE session_id=<sid>; propagate to silver writes; append + mergeSchema; rate-limit fallback covered by quality_core change below
- `notebooks/03_gold_curated.py` — accept `session_id`; propagate to gold writes; rebuild MVs across all sessions
- `notebooks/04_export_sharepoint.py` — accept `session_id` + `output_subfolder`; if non-default, write to `sessions/<sid>/{cleaned,transformed}/` instead of root
- `notebooks/05_demo_walkthrough.py` — minor: queries gain optional session_id filter

### Library
- `quality_core/mapping.py` — extend except clause to catch `databricks.sdk`-style RateLimitError (HTTP 429) and OpenAI-style rate-limit exceptions, fall back to `mock_synonyms` for that sheet. No signature change.

### App
- `apps/quality_team_intelligence/app.py` — substantial rewrite:
  - Remove uploader entirely
  - Replace stages: `empty/files_loaded/processing/results` → `empty/staged/running/results`
  - Add Databricks SDK helpers: `_mint_session_id()`, `_upload_session_files()`, `_trigger_pipeline()`, `_poll_run()`, `_discard_session()`
  - New stage renderers: `_render_home_empty` (slider + Generate), `_render_home_staged` (preview + Run pipeline + Discard), `_render_home_running` (live status), `_render_home_results` (4 tabs, session-scoped)
  - Update History view: query `gold.fact_observation` for distinct session_ids (replaces /tmp/qde_runs/ enumeration)
  - Update Dashboard view: Section A queries new session-scoped tables instead of /tmp; Section B largely unchanged

### App vendored deps
- `apps/quality_team_intelligence/quality_core/mapping.py` — refreshed by `vendor.sh` from repo root (no manual edit needed)

---

## Failure modes (summary)

| Scenario | Behavior |
|---|---|
| LLM endpoint 429 | `quality_core.mapping` falls back to mock_synonyms for that sheet; run completes |
| Pipeline task fails for any other reason | App freezes at the failed task with state_message; Retry / Discard options shown |
| Upload fails partway | App reports per-file success/fail; user retries failed uploads or Discards |
| User closes browser mid-run | Job continues; user finds outputs via History view on next visit |
| Two users hit Run pipeline simultaneously | Bundle queue serializes them; each writes only to its own session_id rows; no collision |
| User clicks Generate but never Run pipeline | Files orphaned in `/sessions/<sid>/`. Discard button or manual cleanup |
| `databricks-gpt-oss-20b` swap to provisioned-throughput needed | One-line config change in `databricks.yml` |

---

## Implementation sequence (proposed PR / commit chunks)

1. **Notebook reset + schema migration** — `00a_reset_tables.py`, `00_setup.py` rewrite. Self-contained; deployable independently.
2. **Bundle param plumbing** — `databricks.yml` changes; touch all 5 notebooks to add the widget. No semantic change yet (default value preserves current behavior).
3. **Cleaner rate-limit fallback** — `quality_core/mapping.py`. Defensive, can ship anytime.
4. **App permission updates** — `app.yaml`. Tiny, ship before app changes need them.
5. **App UI rewrite** — the bulk of the work. Largest risk surface; one focused commit, then deploy and manually test.
6. **Final deploy + test** — vendor.sh, deploy bundle, run reset notebook, deploy app, click through happy path + retry path + discard path.

Each step keeps the workspace functional. Steps 1–4 don't change the app's user-visible behavior. Step 5 flips the experience.

---

## Open questions

None blocking — all design decisions confirmed by user as of 2026-05-08.

## Test plan (high-level)

- **Schema**: after step 1, verify `DESCRIBE TABLE` shows `session_id STRING NOT NULL` partitioned by `session_id`.
- **Bundle params**: trigger job manually with `notebook_params={"session_id": "test_001"}`; verify bronze gets a row tagged `test_001`.
- **Rate-limit fallback**: temporarily swap `llm_endpoint` to a non-existent endpoint; trigger run; confirm `mock_synonyms` fallback fires and run completes.
- **App happy path**: Generate 5 → Run pipeline → wait → see 4 tabs populated with session-scoped data; outputs accessible in volume.
- **Retry path**: kill `databricks-gpt-oss-20b` mid-run (or use a known-bad endpoint); confirm UI shows failure; click Retry; succeeds on second attempt.
- **Discard path**: Generate → Discard before Run; confirm session subfolder deleted from volume.
- **Concurrency smoke**: open app in two browser tabs; Generate + Run in both; confirm both runs complete with their own session_id rows in tables.
