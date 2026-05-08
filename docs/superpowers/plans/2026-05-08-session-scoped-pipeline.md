# Session-Scoped Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert the Quality Team Intelligence Streamlit app from in-process cleaning to session-scoped Databricks medallion pipeline orchestration: app generates N files, uploads to UC volume subfolder, triggers job, streams live status, renders session-scoped outputs.

**Architecture:** Five Delta tables get a `session_id` column, switch from `overwrite` to `append` + partitioned by session_id. Bundle job tasks gain `session_id` base_parameter. App calls `WorkspaceClient.files.upload(...)` for files and `WorkspaceClient.jobs.run_now(notebook_params=...)` to trigger, polls `jobs.get_run(...)` every 3s for live UI. Outputs land in `/Volumes/.../output/sessions/<sid>/{cleaned,transformed}/`. `quality_core.mapping` falls back to deterministic synonyms on FM rate limits.

**Tech Stack:** Python 3.11, Streamlit, Databricks Apps, databricks-sdk, openpyxl, Delta Lake, PySpark, Unity Catalog.

**Spec:** `docs/superpowers/specs/2026-05-08-session-scoped-pipeline-design.md`

**Verification approach:** This codebase has no automated test suite. Verification per task is: (1) syntax-check edited files, (2) visual diff review, (3) bundle/app deploy validation when applicable, (4) manual smoke test of the deployed app at the end of Phase 5. Frequent small commits prevent large-blast-radius mistakes.

---

## Phase 1 — Schema reset + new schema setup

### Task 1.1: Create reset notebook

**Files:**
- Create: `notebooks/00a_reset_tables.py`

- [ ] **Step 1: Create the reset notebook**

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # 00a — Reset session-scoped tables (one-time migration)
# MAGIC
# MAGIC Drops the five core tables so they get recreated by 00_setup.py with
# MAGIC the new schema (`session_id STRING NOT NULL`, `PARTITIONED BY (session_id)`,
# MAGIC append-mode writes). Idempotent — safe to re-run.
# MAGIC
# MAGIC Run this notebook ONCE before the first deploy of the new pipeline.
# MAGIC The 103-file demo run currently in the tables is wiped.

# COMMAND ----------

dbutils.widgets.text("catalog", "quality_de")
CATALOG = dbutils.widgets.get("catalog")

# COMMAND ----------

TABLES = [
    f"{CATALOG}.bronze.raw_workbooks",
    f"{CATALOG}.silver.observations_long",
    f"{CATALOG}.silver.dq_issues",
    f"{CATALOG}.silver.column_mapping_log",
    f"{CATALOG}.gold.fact_observation",
]

for t in TABLES:
    print(f"Dropping {t} ...")
    spark.sql(f"DROP TABLE IF EXISTS {t}")

print("\nDone. Re-run 00_setup followed by the full pipeline to recreate.")
```

- [ ] **Step 2: Syntax check**

Run:
```bash
python3 -c "import ast; ast.parse(open('/home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/notebooks/00a_reset_tables.py').read()); print('OK')"
```
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add notebooks/00a_reset_tables.py
git commit -m "Add 00a_reset_tables notebook for one-time schema migration"
```

---

### Task 1.2: Update 00_setup to create new-schema tables

**Files:**
- Modify: `notebooks/00_setup.py`

- [ ] **Step 1: Read current 00_setup.py to find the table-creation logic**

Run:
```bash
grep -n "CREATE\|CATALOG\|SCHEMA\|VOLUME\|widget" /home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/notebooks/00_setup.py
```

- [ ] **Step 2: Add `session_id` column + `PARTITIONED BY (session_id)` to every CREATE TABLE statement**

For each `CREATE TABLE IF NOT EXISTS` for `bronze.raw_workbooks`, `silver.observations_long`, `silver.dq_issues`, `silver.column_mapping_log`, `gold.fact_observation`:
- Add `session_id STRING NOT NULL` as the **last** column
- Add `PARTITIONED BY (session_id)` clause
- Leave existing columns and types unchanged

Show only the modified `CREATE TABLE` for `bronze.raw_workbooks` as a template; apply same pattern to the other four:

```sql
CREATE TABLE IF NOT EXISTS quality_de.bronze.raw_workbooks (
  workbook         STRING NOT NULL,
  source_path      STRING,
  size_bytes       BIGINT,
  ingested_at      TIMESTAMP,
  -- ... any other existing columns preserved as-is ...
  session_id       STRING NOT NULL
) USING DELTA
PARTITIONED BY (session_id)
```

- [ ] **Step 3: Syntax check the modified notebook**

Run:
```bash
python3 -c "import ast; ast.parse(open('/home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/notebooks/00_setup.py').read()); print('OK')"
```
Expected: `OK`

- [ ] **Step 4: Commit**

```bash
git add notebooks/00_setup.py
git commit -m "Add session_id column + partition to setup notebook table creates"
```

---

## Phase 2 — Bundle parameter plumbing

### Task 2.1: Add session_id base_parameter to every task in databricks.yml

**Files:**
- Modify: `databricks.yml`

- [ ] **Step 1: Read databricks.yml to locate the 5 task definitions**

Run:
```bash
grep -n "task_key\|notebook_task\|base_parameters\|catalog\|llm_endpoint" /home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/databricks.yml
```

- [ ] **Step 2: Add `session_id: "legacy_main_pipeline"` to `base_parameters` of every task**

For each of `setup`, `bronze_ingest`, `silver_ai_cleaning`, `gold_curated`, `export_sharepoint` tasks, add the line under `base_parameters`. Example (apply to all 5):

```yaml
- task_key: setup
  notebook_task:
    notebook_path: notebooks/00_setup.py
    base_parameters:
      catalog: ${var.catalog}
      volume_input: ${var.volume_input}
      volume_output: ${var.volume_output}
      session_id: "legacy_main_pipeline"
  environment_key: default
```

- [ ] **Step 3: Add commented `llm_endpoint_provisioned` variable**

Under the `variables:` block, after the existing `llm_endpoint:` definition, add:

```yaml
  llm_endpoint:
    description: Foundation Model serving endpoint for column mapping
    default: databricks-gpt-oss-20b
  # Swap llm_endpoint default to a provisioned-throughput endpoint to bypass
  # workspace pay-per-token rate limits at scale (sessions over ~25 files).
  # llm_endpoint_provisioned:
  #   description: Provisioned-throughput Foundation Model endpoint
  #   default: <your-provisioned-endpoint-name>
```

- [ ] **Step 4: Validate the bundle**

Run:
```bash
cd /home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc
/usr/local/bin/databricks bundle validate --target dev -p newaccount 2>&1 | tail -10
```
Expected: `Validation OK!`

- [ ] **Step 5: Commit**

```bash
git add databricks.yml
git commit -m "Add session_id base_parameter to all pipeline tasks"
```

---

### Task 2.2: Notebook 01 — bronze ingest scoped to session subfolder

**Files:**
- Modify: `notebooks/01_bronze_sharepoint_ingest.py`

- [ ] **Step 1: Read the notebook**

Run:
```bash
cat /home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/notebooks/01_bronze_sharepoint_ingest.py
```

- [ ] **Step 2: Add `session_id` widget reading + conditional subfolder logic**

Replace the widget setup block (currently sets `catalog` and `volume_input` widgets) with:

```python
dbutils.widgets.text("catalog", "quality_de")
dbutils.widgets.text("volume_input", "sharepoint_input")
dbutils.widgets.text("session_id", "legacy_main_pipeline")
CATALOG = dbutils.widgets.get("catalog")
VOL_IN = dbutils.widgets.get("volume_input")
SESSION_ID = dbutils.widgets.get("session_id")

# When invoked for a session, scope the input to that session's subfolder.
# When the legacy default is in effect, scope to the volume root (back-compat).
INPUT_SUBPATH = f"sessions/{SESSION_ID}" if SESSION_ID != "legacy_main_pipeline" else ""
```

- [ ] **Step 3: Pass session_id + input_subpath to the ADF runner**

Update the `run_pipeline` call to include both new parameters:

```python
result = run_pipeline(
    "pl_ingest_sp_to_bronze",
    parameters={
        "catalog": CATALOG,
        "volume": VOL_IN,
        "site": "QualityTeam",
        "session_id": SESSION_ID,
        "input_subpath": INPUT_SUBPATH,
    },
    spark=spark,
)
```

- [ ] **Step 4: Update the ingest pipeline JSON to declare the new parameters**

Modify `adf/pipelines/pl_ingest_sp_to_bronze.json`:
- Under `properties.parameters`, add:

```json
"session_id":    { "type": "String", "defaultValue": "legacy_main_pipeline" },
"input_subpath": { "type": "String", "defaultValue": "" }
```

- Update the dataset reference to honor `input_subpath` (look for `ds_sp_xlsx_input` reference) and add to its parameters:

```json
"subpath": "@pipeline().parameters.input_subpath"
```

- [ ] **Step 5: Update `connectors/adf/linked_services.py` to honor `subpath` in `list_files`**

In `SharePointBackend.list_files`, change folder resolution to support an optional subpath:

```python
def list_files(self, folder: str, suffix: str | None = None, subpath: str = "") -> list[dict[str, Any]]:
    folder = self._resolve_folder(folder)
    full_folder = f"{folder}/{subpath}" if subpath else folder
    return [
        {"name": f.name, "type": "File", "size": f.size_bytes, "path": f.path}
        for f in self.client.list_files(full_folder, suffix=suffix)
    ]
```

Apply same `subpath` extension to `read_bytes`.

- [ ] **Step 6: Update `connectors/adf/activities.py` `GetMetadata` activity to pass subpath through**

Locate the `execute_get_metadata` (or similarly-named) activity that calls `backend.list_files`. Add the dataset's `subpath` parameter to the call.

Run:
```bash
grep -n "list_files\|read_bytes\|subpath\|ds_sp_xlsx" /home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/connectors/adf/activities.py | head -20
```

Then locate the call site and pass `subpath=ds_params.get("subpath", "")` from the dataset parameters.

- [ ] **Step 7: Tag bronze rows with session_id in `connectors/adf/linked_services.py` `DeltaBackend`**

In `register_workbooks` (or whichever method writes `bronze.raw_workbooks`), accept a `session_id: str` parameter and add it as a column on every row. Change the write mode to `append` + `mergeSchema`:

```python
def register_workbooks(self, catalog: str, manifest_rows: list[dict], session_id: str = "legacy_main_pipeline") -> str:
    if self.is_databricks():
        for row in manifest_rows:
            row["session_id"] = session_id
        df = self.spark.createDataFrame(manifest_rows)
        (df.write
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("session_id")
            .saveAsTable(f"{catalog}.bronze.raw_workbooks"))
        return f"{catalog}.bronze.raw_workbooks"
    # local fallback (unchanged)
    ...
```

- [ ] **Step 8: Pipeline JSON: pass session_id to the manifest activity**

In `pl_ingest_sp_to_bronze.json`, find the `BuildManifest` (or equivalent) activity that writes the bronze manifest. Add `session_id` to its parameter passthrough:

```json
"_mock": {
  "handler": "register_workbooks",
  "params": { "session_id": "@pipeline().parameters.session_id" }
}
```

(Adjust to match the existing param-passing convention in the JSON.)

- [ ] **Step 9: Syntax-check the notebook + JSON**

Run:
```bash
python3 -c "import ast; ast.parse(open('/home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/notebooks/01_bronze_sharepoint_ingest.py').read()); print('notebook OK')"
python3 -c "import json; json.load(open('/home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/adf/pipelines/pl_ingest_sp_to_bronze.json')); print('JSON OK')"
```

- [ ] **Step 10: Bundle validate**

Run:
```bash
cd /home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc
/usr/local/bin/databricks bundle validate --target dev -p newaccount 2>&1 | tail -5
```
Expected: `Validation OK!`

- [ ] **Step 11: Commit**

```bash
git add notebooks/01_bronze_sharepoint_ingest.py adf/pipelines/pl_ingest_sp_to_bronze.json connectors/adf/linked_services.py connectors/adf/activities.py
git commit -m "Bronze ingest: scope to session subfolder, tag rows with session_id"
```

---

### Task 2.3: Notebook 02 — silver cleaning scoped to session

**Files:**
- Modify: `notebooks/02_silver_ai_cleaning.py`

- [ ] **Step 1: Read the notebook to locate widget setup, bronze-read query, silver writes**

Run:
```bash
grep -n "widget\|raw_workbooks\|saveAsTable\|process_workbook\|observations_long\|dq_issues\|column_mapping_log" /home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/notebooks/02_silver_ai_cleaning.py | head -30
```

- [ ] **Step 2: Add `session_id` widget**

After existing widgets, add:

```python
dbutils.widgets.text("session_id", "legacy_main_pipeline")
SESSION_ID = dbutils.widgets.get("session_id")
```

- [ ] **Step 3: Filter bronze read to this session**

Replace any `spark.table(f"{CATALOG}.bronze.raw_workbooks")` reads with:

```python
bronze = spark.table(f"{CATALOG}.bronze.raw_workbooks").filter(f"session_id = '{SESSION_ID}'")
```

- [ ] **Step 4: Tag silver writes with `session_id` and switch to append + mergeSchema**

Find every `saveAsTable` to silver (`observations_long`, `dq_issues`, `column_mapping_log`). For each, before the write, ensure the DataFrame has a `session_id` column populated with `SESSION_ID`:

```python
obs_df = obs_df.withColumn("session_id", lit(SESSION_ID))

(obs_df.write
    .mode("append")
    .option("mergeSchema", "true")
    .partitionBy("session_id")
    .saveAsTable(f"{CATALOG}.silver.observations_long"))
```

(Apply same pattern for `dq_issues` and `column_mapping_log`. Add `from pyspark.sql.functions import lit` at the top of the notebook if not already imported.)

- [ ] **Step 5: Syntax check**

Run:
```bash
python3 -c "import ast; ast.parse(open('/home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/notebooks/02_silver_ai_cleaning.py').read()); print('OK')"
```
Expected: `OK`

- [ ] **Step 6: Commit**

```bash
git add notebooks/02_silver_ai_cleaning.py
git commit -m "Silver cleaning: scope to session, tag writes, append mode"
```

---

### Task 2.4: Notebook 03 — gold curation scoped to session

**Files:**
- Modify: `notebooks/03_gold_curated.py`

- [ ] **Step 1: Read the notebook**

Run:
```bash
grep -n "widget\|silver\|saveAsTable\|fact_observation\|mv_" /home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/notebooks/03_gold_curated.py | head -30
```

- [ ] **Step 2: Add session_id widget + filter silver reads**

```python
dbutils.widgets.text("session_id", "legacy_main_pipeline")
SESSION_ID = dbutils.widgets.get("session_id")
```

Update silver-table reads:
```python
obs = spark.table(f"{CATALOG}.silver.observations_long").filter(f"session_id = '{SESSION_ID}'")
```

- [ ] **Step 3: Tag fact_observation write + append + partition**

```python
fact_df = fact_df.withColumn("session_id", lit(SESSION_ID))

(fact_df.write
    .mode("append")
    .option("mergeSchema", "true")
    .partitionBy("session_id")
    .saveAsTable(f"{CATALOG}.gold.fact_observation"))
```

- [ ] **Step 4: Refresh materialized views from full table (cross-session)**

The MV definitions stay aggregating across all sessions. Verify the existing `CREATE OR REPLACE` MV statements don't need changes — they read from `fact_observation` which now contains rows from all sessions, which is correct.

If MVs use streaming source: ensure they pick up the appended rows. For `MATERIALIZED VIEW`, a `REFRESH MATERIALIZED VIEW <name>` call after the gold write is sufficient. Add at end of notebook 03:

```python
for mv in ["mv_spec_violations", "mv_impurity_trend"]:
    try:
        spark.sql(f"REFRESH MATERIALIZED VIEW {CATALOG}.gold.{mv}")
    except Exception as e:
        print(f"  (MV {mv} refresh skipped: {e})")
```

- [ ] **Step 5: Syntax check**

Run:
```bash
python3 -c "import ast; ast.parse(open('/home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/notebooks/03_gold_curated.py').read()); print('OK')"
```

- [ ] **Step 6: Commit**

```bash
git add notebooks/03_gold_curated.py
git commit -m "Gold curation: scope to session, tag writes, refresh MVs"
```

---

### Task 2.5: Notebook 04 — export to session-scoped output subfolders

**Files:**
- Modify: `notebooks/04_export_sharepoint.py`
- Modify: `adf/pipelines/pl_export_gold_to_sp.json`
- Modify: `adf/pipelines/pl_export_clean_to_sp.json`

- [ ] **Step 1: Read notebook 04**

Run:
```bash
cat /home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/notebooks/04_export_sharepoint.py
```

- [ ] **Step 2: Add `session_id` widget + replace export pipeline params**

Replace existing widget block + run_pipeline calls with:

```python
dbutils.widgets.text("catalog", "quality_de")
dbutils.widgets.text("volume_input", "sharepoint_input")
dbutils.widgets.text("volume_output", "sharepoint_output")
dbutils.widgets.text("session_id", "legacy_main_pipeline")
CATALOG = dbutils.widgets.get("catalog")
VOL_IN = dbutils.widgets.get("volume_input")
VOL_OUT = dbutils.widgets.get("volume_output")
SESSION_ID = dbutils.widgets.get("session_id")

# Session-scoped output: writes go to /output/sessions/<sid>/transformed
# (was /output/_CLEAN.xlsx) and /output/sessions/<sid>/cleaned
# (was /output/cleaned/). Legacy session_id falls back to root behavior.
if SESSION_ID == "legacy_main_pipeline":
    TIDY_SUBFOLDER = ""
    CLEAN_SUBFOLDER = "cleaned"
else:
    TIDY_SUBFOLDER = f"sessions/{SESSION_ID}/transformed"
    CLEAN_SUBFOLDER = f"sessions/{SESSION_ID}/cleaned"
```

Replace `run_pipeline` calls with session_id + subfolder params:

```python
result_tidy = run_pipeline(
    "pl_export_gold_to_sp",
    parameters={
        "catalog": CATALOG, "site": "QualityTeam",
        "session_id": SESSION_ID,
        "subfolder": TIDY_SUBFOLDER,
    },
    spark=spark,
)

result_clean = run_pipeline(
    "pl_export_clean_to_sp",
    parameters={
        "catalog": CATALOG, "site": "QualityTeam",
        "session_id": SESSION_ID,
        "subfolder": CLEAN_SUBFOLDER,
    },
    spark=spark,
)
```

- [ ] **Step 3: Update pl_export_gold_to_sp.json — add session_id parameter + filter**

In `parameters` block, add:
```json
"session_id": { "type": "String", "defaultValue": "legacy_main_pipeline" },
"subfolder":  { "type": "String", "defaultValue": "" }
```

Update the `LookupDistinctWorkbooks` query to filter by session_id:

```json
"query": "SELECT DISTINCT workbook FROM @{pipeline().parameters.catalog}.gold.fact_observation WHERE session_id = '@{pipeline().parameters.session_id}'"
```

Update the export workbook query similarly:
```json
"query": "SELECT * FROM @{pipeline().parameters.catalog}.gold.fact_observation WHERE workbook = '@{item().workbook}' AND session_id = '@{pipeline().parameters.session_id}'"
```

Update the output dataset's filename pattern to include the subfolder. Find the `outputs` block referencing `ds_sp_xlsx_output`. Add to its parameters:

```json
"subfolder": "@pipeline().parameters.subfolder"
```

- [ ] **Step 4: Same updates for pl_export_clean_to_sp.json**

Apply identical changes (parameters, filter, subfolder) to `pl_export_clean_to_sp.json`. Note that this pipeline already has a `subfolder` parameter — ensure the new version is consistent and reads from `pipeline().parameters.subfolder`.

- [ ] **Step 5: Update output dataset (`ds_sp_xlsx_output`) to honor subfolder**

Locate `adf/datasets/ds_sp_xlsx_output.json`. Add `subfolder` parameter and use it in the path:

```json
"parameters": {
  "fileName": { "type": "String" },
  "subfolder": { "type": "String", "defaultValue": "" }
},
"typeProperties": {
  "location": {
    "type": "HttpServerLocation",
    "relativeUrl": "@concat(if(empty(dataset().subfolder), '', concat(dataset().subfolder, '/')), dataset().fileName)"
  }
}
```

(Adapt to match the existing JSON structure exactly.)

- [ ] **Step 6: Update `connectors/adf/activities.py` `Copy` activity to honor subfolder in upload**

Locate where the Copy activity's output `subfolder` parameter is used to write via `backend.upload_file` or `backend.write_bytes`. Pass `subfolder` to compose the destination path:

```python
out_folder = ds_params.get("folderPath", "output")
subfolder = ds_params.get("subfolder", "")
file_name = ds_params.get("fileName")
target_path = f"{subfolder}/{file_name}" if subfolder else file_name
backend.write_bytes(folder=out_folder, name=target_path, payload=blob)
```

- [ ] **Step 7: Syntax + JSON check**

Run:
```bash
python3 -c "import ast, json; ast.parse(open('/home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/notebooks/04_export_sharepoint.py').read()); json.load(open('/home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/adf/pipelines/pl_export_gold_to_sp.json')); json.load(open('/home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/adf/pipelines/pl_export_clean_to_sp.json')); print('OK')"
```

- [ ] **Step 8: Bundle validate**

Run:
```bash
cd /home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc
/usr/local/bin/databricks bundle validate --target dev -p newaccount 2>&1 | tail -5
```
Expected: `Validation OK!`

- [ ] **Step 9: Commit**

```bash
git add notebooks/04_export_sharepoint.py adf/pipelines/pl_export_gold_to_sp.json adf/pipelines/pl_export_clean_to_sp.json adf/datasets/ds_sp_xlsx_output.json connectors/adf/activities.py
git commit -m "Export: scope to session, write outputs to /sessions/<sid>/{cleaned,transformed}/"
```

---

## Phase 3 — Cleaner rate-limit fallback

### Task 3.1: Catch 429s in quality_core.mapping

**Files:**
- Modify: `quality_core/mapping.py`

- [ ] **Step 1: Read mapping.py to locate the LLM call site**

Run:
```bash
grep -n "client\|chat\|except\|RateLimitError\|databricks\|openai\|fall.*back\|mock_synonyms" /home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/quality_core/mapping.py | head -20
```

- [ ] **Step 2: Add explicit 429 / rate-limit catch**

Locate the `try/except` block that wraps the LLM `chat.completions.create(...)` call. Extend the `except` to catch rate-limit exceptions explicitly. Add at the top of the file:

```python
try:
    from openai import RateLimitError as _OpenAIRateLimitError
except ImportError:
    _OpenAIRateLimitError = ()
```

Then in the LLM-call try/except, change:

```python
except Exception as e:
    # existing connection-error fallback
    ...
```

to:

```python
except _OpenAIRateLimitError as e:
    print(f"  [mapping] 429 rate limit on FM endpoint — falling back to synonyms for sheet {sheet_name!r}: {e}")
    return _best_meta_match(raw_label, synonyms)  # or appropriate fallback call
except Exception as e:
    # existing connection-error fallback (unchanged)
    ...
```

(Adjust the fallback call to match the existing one — preserve behavior; just add the explicit 429 path so we get a clear log message.)

- [ ] **Step 3: Syntax check**

Run:
```bash
python3 -c "import ast; ast.parse(open('/home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/quality_core/mapping.py').read()); print('OK')"
```

- [ ] **Step 4: Commit**

```bash
git add quality_core/mapping.py
git commit -m "Mapping: explicit fallback to synonyms on FM rate-limit (429)"
```

---

## Phase 4 — App permission updates

### Task 4.1: Update app.yaml for WRITE_VOLUME and CAN_MANAGE_RUN

**Files:**
- Modify: `apps/quality_team_intelligence/app.yaml`

- [ ] **Step 1: Read current app.yaml resources block**

Run:
```bash
grep -n "resources\|volume\|warehouse\|permission\|securable" /home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/apps/quality_team_intelligence/app.yaml
```

- [ ] **Step 2: Change input volume permission from READ_VOLUME to WRITE_VOLUME**

Find the resource block for `bronze_volume` (input). Change:
```yaml
permission: "READ_VOLUME"
```
to:
```yaml
permission: "WRITE_VOLUME"
```

(`WRITE_VOLUME` implies `READ_VOLUME` per Databricks ACL rules.)

- [ ] **Step 3: Add a job resource for the medallion pipeline**

After the existing volume resources, append:

```yaml
  - name: pipeline_job
    description: |
      Permission to trigger the medallion pipeline job (quality_de_pipeline)
      via WorkspaceClient.jobs.run_now from the app. The app's identity
      needs CAN_MANAGE_RUN to start runs and observe their state.
    job:
      id: "${resources.jobs.quality_de_pipeline.id}"
      permission: "CAN_MANAGE_RUN"
```

- [ ] **Step 4: Bundle validate**

Run:
```bash
cd /home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc
/usr/local/bin/databricks bundle validate --target dev -p newaccount 2>&1 | tail -10
```
Expected: `Validation OK!`

- [ ] **Step 5: Commit**

```bash
git add apps/quality_team_intelligence/app.yaml
git commit -m "App: WRITE_VOLUME on input + CAN_MANAGE_RUN on pipeline job"
```

---

## Phase 5 — App UI rewrite

### Task 5.1: Add SDK helpers (mint, upload, trigger, poll, discard)

**Files:**
- Modify: `apps/quality_team_intelligence/app.py`

- [ ] **Step 1: Read the helper section to find a good insertion point**

Run:
```bash
grep -n "def _ensure_dirs\|def _save_uploaded\|def _generate_demo_files\|# Helpers\|WORKING_DIR\|RUNS_DIR" /home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/apps/quality_team_intelligence/app.py | head -15
```

- [ ] **Step 2: Add SDK + session helpers after the existing file-management helpers**

Locate the comment block "# Helpers — file management" and the end of `_list_available_files`. After the last function in that block, insert:

```python
# ===========================================================================
# Helpers — Databricks session orchestration (new in session-scoped flow)
# ===========================================================================

INPUT_VOLUME_BASE = f"/Volumes/{CATALOG}/bronze/sharepoint_input/sessions"
OUTPUT_VOLUME_BASE = f"/Volumes/{CATALOG}/bronze/sharepoint_output/sessions"
PIPELINE_JOB_NAME = "quality_de_pipeline"


def _mint_session_id() -> str:
    """Generate a human-readable session_id: YYYY-MM-DD-hhmmss-<6hex>."""
    import secrets
    now = datetime.now(timezone.utc)
    suffix = secrets.token_hex(3)  # 6 hex chars
    return f"{now.strftime('%Y-%m-%d-%H%M%S')}-{suffix}"


def _resolve_pipeline_job_id() -> int:
    """Look up the pipeline job ID by name from the workspace."""
    w = _workspace_client()
    for j in w.jobs.list(name=PIPELINE_JOB_NAME):
        return j.job_id
    raise RuntimeError(f"Job {PIPELINE_JOB_NAME!r} not found in workspace")


def _upload_session_files(session_id: str, local_files: list[Path]) -> list[str]:
    """Upload local files to the session subfolder in the input volume.
    Returns the list of remote paths."""
    w = _workspace_client()
    target_dir = f"{INPUT_VOLUME_BASE}/{session_id}"
    uploaded: list[str] = []
    for f in local_files:
        remote = f"{target_dir}/{f.name}"
        w.files.upload(remote, f.read_bytes(), overwrite=True)
        uploaded.append(remote)
    return uploaded


def _trigger_pipeline(session_id: str) -> int:
    """Trigger quality_de_pipeline with the given session_id. Returns run_id."""
    w = _workspace_client()
    job_id = _resolve_pipeline_job_id()
    run = w.jobs.run_now(
        job_id=job_id,
        notebook_params={"session_id": session_id},
    )
    return run.run_id


def _poll_run(run_id: int) -> dict:
    """Single poll: returns a dict with overall + per-task status."""
    w = _workspace_client()
    run = w.jobs.get_run(run_id=run_id)
    return {
        "life_cycle_state": run.state.life_cycle_state.value if run.state and run.state.life_cycle_state else "UNKNOWN",
        "result_state": run.state.result_state.value if run.state and run.state.result_state else None,
        "state_message": run.state.state_message if run.state else "",
        "tasks": [
            {
                "task_key": t.task_key,
                "life_cycle_state": t.state.life_cycle_state.value if t.state and t.state.life_cycle_state else "PENDING",
                "result_state": t.state.result_state.value if t.state and t.state.result_state else None,
                "start_time": t.start_time,
                "end_time": t.end_time,
                "state_message": t.state.state_message if t.state else "",
            }
            for t in (run.tasks or [])
        ],
    }


def _clear_session_table_rows(session_id: str) -> None:
    """Delete rows tagged with session_id from all 5 tables. Used by retry
    (so re-triggering the job doesn't duplicate rows) and by full discard."""
    for table in [
        f"{CATALOG}.bronze.raw_workbooks",
        f"{CATALOG}.silver.observations_long",
        f"{CATALOG}.silver.dq_issues",
        f"{CATALOG}.silver.column_mapping_log",
        f"{CATALOG}.gold.fact_observation",
    ]:
        try:
            run_query(f"DELETE FROM {table} WHERE session_id = '{session_id}'")
        except Exception as e:
            print(f"  ({table} cleanup skipped: {e})")


def _discard_session(session_id: str) -> None:
    """Full discard: delete input subfolder AND any rows already written."""
    w = _workspace_client()
    target_dir = f"{INPUT_VOLUME_BASE}/{session_id}"
    try:
        for f in w.files.list_directory_contents(target_dir):
            w.files.delete(f.path)
        w.files.delete_directory(target_dir)
    except Exception as e:
        print(f"  (input cleanup skipped: {e})")
    _clear_session_table_rows(session_id)
```

Also add the missing import at the top of the file:
```python
from datetime import datetime, timezone   # if not already imported
```

(The file already imports `datetime, timezone` per line 31; verify and skip the duplicate if present.)

- [ ] **Step 3: Syntax check**

Run:
```bash
python3 -c "import ast; ast.parse(open('/home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/apps/quality_team_intelligence/app.py').read()); print('OK')"
```

- [ ] **Step 4: Commit**

```bash
git add apps/quality_team_intelligence/app.py
git commit -m "App: add session orchestration helpers (mint, upload, trigger, poll, discard)"
```

---

### Task 5.2: Replace _render_home_empty with slider + Generate

**Files:**
- Modify: `apps/quality_team_intelligence/app.py`

- [ ] **Step 1: Locate current `_render_home_empty`**

Run:
```bash
grep -n "def _render_home_empty\|def _render_home_files_loaded\|def _render_home_processing\|def _render_home_results" /home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/apps/quality_team_intelligence/app.py
```

- [ ] **Step 2: Replace `_render_home_empty` body**

Replace the entire `_render_home_empty()` function with:

```python
def _render_home_empty():
    st.title("Process a Quality team workbook")
    st.markdown(
        "Generate synthetic Quality team workbooks and run them through the "
        "Databricks medallion pipeline. Each session runs in isolation."
    )

    with st.container(border=True):
        st.markdown("### Generate session files")
        st.markdown(
            "<div style='opacity:0.65;font-size:0.9rem;margin-bottom:0.6rem;'>"
            "How many synthetic workbooks should this session produce? "
            "Files alternate between API, KSM, and Intermediates types."
            "</div>",
            unsafe_allow_html=True,
        )
        n_files = st.slider(
            "Number of files",
            min_value=1, max_value=25, value=20, step=1,
            key="gen_count",
        )
        if st.button("Generate", type="primary",
                     use_container_width=True, key="btn_generate"):
            session_id = _mint_session_id()
            with st.spinner(f"Generating {n_files} workbooks…"):
                local_files = _generate_session_files(session_id, n_files)
            with st.spinner(f"Uploading {n_files} workbooks to volume…"):
                _upload_session_files(session_id, local_files)
            st.session_state.session_id = session_id
            st.session_state.session_n_files = n_files
            st.session_state.session_files = [f.name for f in local_files]
            st.session_state.stage = "staged"
            st.rerun()
```

- [ ] **Step 3: Add `_generate_session_files` helper**

Replace the existing `_generate_demo_files()` function with:

```python
def _generate_session_files(session_id: str, n_files: int) -> list[Path]:
    """Generate N synthetic workbooks locally for this session.
    Files alternate API/KSM/Intermediates with varied seeds."""
    from generate_quality_data import build_workbook, workbook_specs
    local_dir = Path("/tmp/qde_session") / session_id
    local_dir.mkdir(parents=True, exist_ok=True)
    for old in local_dir.glob("*.xlsx"):
        old.unlink()
    specs = workbook_specs()
    keys = list(specs.keys())
    written: list[Path] = []
    for i in range(n_files):
        spec = specs[keys[i % len(keys)]]
        wb = build_workbook(spec, seed=43 + i)
        base = spec.filename.replace(".xlsx", "")
        target = local_dir / f"{base}_{i:03d}.xlsx"
        wb.save(target)
        written.append(target)
    return written
```

- [ ] **Step 4: Syntax check**

Run:
```bash
python3 -c "import ast; ast.parse(open('/home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/apps/quality_team_intelligence/app.py').read()); print('OK')"
```

- [ ] **Step 5: Commit**

```bash
git add apps/quality_team_intelligence/app.py
git commit -m "App: rewrite Home empty stage with slider + Generate flow"
```

---

### Task 5.3: Add `_render_home_staged`

**Files:**
- Modify: `apps/quality_team_intelligence/app.py`

- [ ] **Step 1: Locate `_render_home_files_loaded` and replace with new staged stage**

Replace the entire `_render_home_files_loaded()` function with:

```python
def _render_home_staged():
    st.title("Session ready")
    sid = st.session_state.get("session_id")
    n = st.session_state.get("session_n_files", 0)
    files = st.session_state.get("session_files", [])

    st.markdown(
        f"Session **`{sid}`** has **{n} files** uploaded to "
        f"`{INPUT_VOLUME_BASE}/{sid}`. "
        "Click **Run pipeline** to trigger the medallion job, or **Discard session** to clean up."
    )

    with st.expander(f"Files in this session ({n})", expanded=False):
        for fn in files:
            st.markdown(f"- `{fn}`")

    c1, c2 = st.columns([3, 1])
    with c1:
        if st.button("Run pipeline", type="primary",
                     use_container_width=True, key="btn_run"):
            with st.spinner("Triggering pipeline…"):
                run_id = _trigger_pipeline(sid)
            st.session_state.run_id = run_id
            st.session_state.run_started_at = time.time()
            st.session_state.stage = "running"
            st.rerun()
    with c2:
        if st.button("Discard session", use_container_width=True, key="btn_discard"):
            with st.spinner("Discarding session…"):
                _discard_session(sid)
            for k in ("session_id", "session_n_files", "session_files",
                      "run_id", "run_started_at"):
                st.session_state.pop(k, None)
            st.session_state.stage = "empty"
            st.rerun()
```

- [ ] **Step 2: Syntax check + commit**

```bash
python3 -c "import ast; ast.parse(open('/home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/apps/quality_team_intelligence/app.py').read()); print('OK')"
git add apps/quality_team_intelligence/app.py
git commit -m "App: add Home staged stage with Run pipeline + Discard buttons"
```

---

### Task 5.4: Replace processing stage with live polling `_render_home_running`

**Files:**
- Modify: `apps/quality_team_intelligence/app.py`

- [ ] **Step 1: Replace `_render_home_processing` with new `_render_home_running`**

Replace the entire `_render_home_processing()` function body with:

```python
def _render_home_running():
    st.title("Pipeline running")
    sid = st.session_state.get("session_id")
    run_id = st.session_state.get("run_id")
    started_at = st.session_state.get("run_started_at", time.time())

    if not (sid and run_id):
        st.error("No active run.")
        st.session_state.stage = "empty"
        st.rerun()

    header = st.empty()
    body = st.empty()

    POLL_INTERVAL_S = 3
    TASK_ORDER = ["setup", "bronze_ingest", "silver_ai_cleaning",
                  "gold_curated", "export_sharepoint"]

    while True:
        elapsed = int(time.time() - started_at)
        try:
            poll = _poll_run(run_id)
        except Exception as e:
            header.error(f"Polling failed: {e}")
            time.sleep(POLL_INTERVAL_S)
            continue

        # Header
        header.markdown(
            f"Session **`{sid}`** · pipeline run `{run_id}` · "
            f"**{elapsed // 60}m {elapsed % 60}s** elapsed"
        )

        # Per-task list
        tasks_by_key = {t["task_key"]: t for t in poll["tasks"]}
        rows = []
        for tk in TASK_ORDER:
            t = tasks_by_key.get(tk, {"life_cycle_state": "PENDING",
                                       "result_state": None,
                                       "start_time": None,
                                       "end_time": None})
            lcs = t.get("life_cycle_state", "PENDING")
            rs = t.get("result_state")
            if lcs == "TERMINATED" and rs == "SUCCESS":
                marker, status_text = "✓", "SUCCESS"
            elif lcs == "TERMINATED" and rs in ("FAILED", "TIMEDOUT", "CANCELED"):
                marker, status_text = "✗", rs
            elif lcs == "RUNNING":
                marker, status_text = "●", "RUNNING"
            elif lcs == "PENDING":
                marker, status_text = "○", "PENDING"
            else:
                marker, status_text = "○", lcs
            # Per-task elapsed
            if t.get("start_time"):
                start_ms = t["start_time"]
                end_ms = t.get("end_time") or int(time.time() * 1000)
                t_secs = max(0, (end_ms - start_ms) // 1000)
                t_elapsed = f"{t_secs // 60}m {t_secs % 60}s" if t_secs >= 60 else f"{t_secs}s"
            else:
                t_elapsed = "—"
            rows.append(f"`{marker}`  **{tk:<22}** {status_text:<12}  {t_elapsed}")

        body.markdown("\n\n".join(rows))

        # Termination
        lcs = poll["life_cycle_state"]
        if lcs == "TERMINATED":
            rs = poll["result_state"]
            if rs == "SUCCESS":
                st.session_state.stage = "results"
                time.sleep(0.5)
                st.rerun()
            else:
                st.error(f"Pipeline {rs}: {poll.get('state_message', '')}")
                cR, cD = st.columns(2)
                with cR:
                    if st.button("Retry this session", type="primary", key="btn_retry"):
                        with st.spinner("Cleaning prior rows + re-triggering…"):
                            _clear_session_table_rows(sid)  # files stay
                            run_id2 = _trigger_pipeline(sid)
                        st.session_state.run_id = run_id2
                        st.session_state.run_started_at = time.time()
                        st.rerun()
                with cD:
                    if st.button("Discard and start over", key="btn_fail_discard"):
                        with st.spinner("Discarding session…"):
                            _discard_session(sid)
                        for k in ("session_id", "session_n_files", "session_files",
                                  "run_id", "run_started_at"):
                            st.session_state.pop(k, None)
                        st.session_state.stage = "empty"
                        st.rerun()
                return
        elif lcs in ("INTERNAL_ERROR", "SKIPPED"):
            st.error(f"Pipeline {lcs}: {poll.get('state_message', '')}")
            return
        time.sleep(POLL_INTERVAL_S)
```

- [ ] **Step 2: Syntax check + commit**

```bash
python3 -c "import ast; ast.parse(open('/home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/apps/quality_team_intelligence/app.py').read()); print('OK')"
git add apps/quality_team_intelligence/app.py
git commit -m "App: live polling stage with per-task status + retry/discard"
```

---

### Task 5.5: Update results stage to read from session-scoped UC volume

**Files:**
- Modify: `apps/quality_team_intelligence/app.py`

- [ ] **Step 1: Read `_render_home_results` and locate where it loads `run` data**

Run:
```bash
grep -n "def _render_home_results\|run\[\"dir\"\]\|run\[\"id\"\]\|_load_run\|RUNS_DIR" /home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/apps/quality_team_intelligence/app.py | head -10
```

- [ ] **Step 2: Replace `_render_home_results` body**

Replace the function with:

```python
def _render_home_results():
    sid = st.session_state.get("session_id")
    if not sid:
        st.error("No session in state.")
        st.session_state.stage = "empty"
        st.rerun()

    out_clean = f"{OUTPUT_VOLUME_BASE}/{sid}/cleaned"
    out_tidy = f"{OUTPUT_VOLUME_BASE}/{sid}/transformed"

    # Load session-scoped tables once for the tabs
    obs = _load_session_observations(sid)
    dq = _load_session_dq_issues(sid)
    mp = _load_session_mappings(sid)

    a1, a2 = st.columns([4, 1])
    with a1:
        st.title("Results")
        st.caption(
            f"Session **`{sid}`** · {len(obs):,} observations · "
            f"{len(dq):,} fixes · {len(mp):,} mapping decisions"
        )
    with a2:
        if st.button("Run another", use_container_width=True, key="run_another"):
            for k in ("session_id", "session_n_files", "session_files",
                      "run_id", "run_started_at"):
                st.session_state.pop(k, None)
            st.session_state.stage = "empty"
            st.rerun()

    tab_outputs, tab_clean, tab_ai, tab_analytics = st.tabs([
        "Deliverables", "DQ Audit", "Column Resolution", "Compliance Metrics",
    ])
    with tab_outputs:
        _tab_outputs_session(out_clean, out_tidy)
    with tab_clean:
        _tab_cleaning(dq)
    with tab_ai:
        _tab_ai_mapping(mp)
    with tab_analytics:
        _tab_analytics(obs)
```

- [ ] **Step 3: Add session-scoped data loaders**

Add these helpers near the other helpers:

```python
def _load_session_observations(session_id: str) -> pd.DataFrame:
    try:
        return run_query(
            f"SELECT * FROM {CATALOG}.gold.fact_observation "
            f"WHERE session_id = '{session_id}'"
        )
    except Exception as e:
        st.warning(f"observations load failed: {e}")
        return pd.DataFrame()


def _load_session_dq_issues(session_id: str) -> pd.DataFrame:
    try:
        return run_query(
            f"SELECT * FROM {CATALOG}.silver.dq_issues "
            f"WHERE session_id = '{session_id}'"
        )
    except Exception as e:
        st.warning(f"dq_issues load failed: {e}")
        return pd.DataFrame()


def _load_session_mappings(session_id: str) -> pd.DataFrame:
    try:
        return run_query(
            f"SELECT * FROM {CATALOG}.silver.column_mapping_log "
            f"WHERE session_id = '{session_id}'"
        )
    except Exception as e:
        st.warning(f"mappings load failed: {e}")
        return pd.DataFrame()
```

- [ ] **Step 4: Add `_tab_outputs_session` (replaces `_tab_outputs`)**

Replace the existing `_tab_outputs` function with a session-scoped version that lists files from the volume. Keep the original signature but switch source:

```python
def _tab_outputs_session(cleaned_dir: str, tidy_dir: str):
    st.markdown(
        "**Both cleaned views are saved to this session's output folder.** "
        "Same-format mirrors the input shape; tidy is the long-form analytics view."
    )
    sub_same, sub_tidy = st.tabs([
        "Same-format (mirrors input)",
        "Tidy long-form (analytics)",
    ])

    w = _workspace_client()

    def _list_volume_xlsx(vol_dir: str) -> list[dict]:
        out = []
        try:
            for f in w.files.list_directory_contents(vol_dir):
                if f.path.endswith(".xlsx"):
                    out.append({"name": Path(f.path).name, "path": f.path,
                                "size": f.file_size})
        except Exception as e:
            st.warning(f"Volume listing failed for `{vol_dir}`: {e}")
        return out

    with sub_same:
        st.caption(f"Folder: `{cleaned_dir}`")
        files = _list_volume_xlsx(cleaned_dir)
        if not files:
            st.info("No same-format outputs found.")
        else:
            choice = st.selectbox("File", [f["name"] for f in files],
                                  key="same_pick")
            sel = next(f for f in files if f["name"] == choice)
            data = w.files.download(sel["path"]).contents.read()
            st.download_button("Download", data=data, file_name=sel["name"],
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               use_container_width=True, key="dl_same",
                               type="primary")
            tmp = Path("/tmp/qde_preview") / sel["name"]
            tmp.parent.mkdir(exist_ok=True)
            tmp.write_bytes(data)
            try:
                sheets = _list_sheets(tmp)
                sheet = st.selectbox("Sheet", sheets, key="same_sheet")
                _render_xlsx_full(tmp, sheet, height=620)
            except Exception as e:
                st.warning(f"Preview unavailable: {e}")

    with sub_tidy:
        st.caption(f"Folder: `{tidy_dir}`")
        files = _list_volume_xlsx(tidy_dir)
        if not files:
            st.info("No tidy outputs found.")
        else:
            choice = st.selectbox("File", [f["name"] for f in files],
                                  key="tidy_pick")
            sel = next(f for f in files if f["name"] == choice)
            data = w.files.download(sel["path"]).contents.read()
            st.download_button("Download", data=data, file_name=sel["name"],
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               use_container_width=True, key="dl_tidy",
                               type="primary")
            tmp = Path("/tmp/qde_preview") / sel["name"]
            tmp.parent.mkdir(exist_ok=True)
            tmp.write_bytes(data)
            try:
                sheets = _list_sheets(tmp)
                sheet = st.selectbox("Sheet", sheets, key="tidy_sheet")
                _render_xlsx_full(tmp, sheet, height=620)
            except Exception as e:
                st.warning(f"Preview unavailable: {e}")
```

- [ ] **Step 5: Syntax check + commit**

```bash
python3 -c "import ast; ast.parse(open('/home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/apps/quality_team_intelligence/app.py').read()); print('OK')"
git add apps/quality_team_intelligence/app.py
git commit -m "App: results stage reads from session-scoped UC volume + tables"
```

---

### Task 5.6: Update view router for new stages

**Files:**
- Modify: `apps/quality_team_intelligence/app.py`

- [ ] **Step 1: Locate `render_home()` view router**

Run:
```bash
grep -n "def render_home\|stage ==" /home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/apps/quality_team_intelligence/app.py | head -10
```

- [ ] **Step 2: Update render_home() to dispatch on the new stages**

Replace the body of `render_home()` with:

```python
def render_home():
    stage = st.session_state.get("stage", "empty")
    if stage == "empty":
        _render_home_empty()
    elif stage == "staged":
        _render_home_staged()
    elif stage == "running":
        _render_home_running()
    elif stage == "results":
        _render_home_results()
    else:
        st.session_state.stage = "empty"
        st.rerun()
```

- [ ] **Step 3: Update session-state defaults in `_init_state`**

Find `_init_state()` and update the defaults dict:

```python
defaults = {
    "view":               "home",
    "stage":              "empty",     # empty | staged | running | results
    "session_id":         None,
    "session_n_files":    0,
    "session_files":      [],
    "run_id":             None,
    "run_started_at":     None,
    "log_lines":          [],
}
```

Remove the obsolete keys (`available_files`, `selected_file`, `current_run_id`, `last_generated_file`).

- [ ] **Step 4: Syntax check + commit**

```bash
python3 -c "import ast; ast.parse(open('/home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/apps/quality_team_intelligence/app.py').read()); print('OK')"
git add apps/quality_team_intelligence/app.py
git commit -m "App: route Home stages to new empty/staged/running/results renderers"
```

---

### Task 5.7: Update History view to query gold for sessions

**Files:**
- Modify: `apps/quality_team_intelligence/app.py`

- [ ] **Step 1: Locate `render_history`**

Run:
```bash
grep -n "def render_history\|RUNS_DIR\|_list_runs" /home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/apps/quality_team_intelligence/app.py | head -10
```

- [ ] **Step 2: Replace `_list_runs` with `_list_sessions`**

Replace `_list_runs()` with:

```python
def _list_sessions() -> pd.DataFrame:
    """Return all sessions visible in gold.fact_observation, newest first.
    Excludes the legacy_main_pipeline session_id."""
    try:
        df = run_query(f"""
            SELECT
              session_id,
              MIN(workbook)            AS first_workbook,
              COUNT(DISTINCT workbook) AS n_workbooks,
              COUNT(*)                 AS n_observations,
              ROUND(100.0 * SUM(CASE WHEN pass = true THEN 1 ELSE 0 END)
                  / NULLIF(SUM(CASE WHEN pass IS NOT NULL THEN 1 ELSE 0 END), 0), 1) AS pass_rate_pct,
              MIN(ingested_at)         AS earliest_ingest
            FROM {CATALOG}.gold.fact_observation
            WHERE session_id <> 'legacy_main_pipeline'
            GROUP BY session_id
            ORDER BY earliest_ingest DESC
        """)
        return df
    except Exception as e:
        st.warning(f"Sessions query failed: {e}")
        return pd.DataFrame()
```

- [ ] **Step 3: Update `render_history` to use the new schema**

Replace `render_history()` body with:

```python
def render_history():
    st.title("History")
    st.markdown(
        "<div style='opacity:0.75;font-size:1.0rem;margin-bottom:1rem;'>"
        "Every session ever processed by the app. Click <strong>Open</strong> "
        "to load that session's results."
        "</div>",
        unsafe_allow_html=True,
    )
    df = _list_sessions()
    if df.empty:
        st.info("No sessions yet. Head to Home and process a workbook.")
        return

    st.dataframe(df, use_container_width=True, hide_index=True, height=420)

    sel = st.selectbox("Select a session_id to open", df["session_id"].tolist(),
                       key="hist_sel")
    if st.button("Open this session", type="primary",
                 use_container_width=True, key="hist_open"):
        st.session_state.session_id = sel
        st.session_state.view = "home"
        st.session_state.stage = "results"
        st.rerun()
```

- [ ] **Step 4: Syntax check + commit**

```bash
python3 -c "import ast; ast.parse(open('/home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/apps/quality_team_intelligence/app.py').read()); print('OK')"
git add apps/quality_team_intelligence/app.py
git commit -m "App: History view queries gold.fact_observation for sessions"
```

---

### Task 5.8: Update Dashboard's Section A to query session tables

**Files:**
- Modify: `apps/quality_team_intelligence/app.py`

- [ ] **Step 1: Locate Section A in `render_dashboard`**

Run:
```bash
grep -n "## App runs\|## Batch pipeline\|_list_runs\|n_observations" /home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/apps/quality_team_intelligence/app.py | head -10
```

- [ ] **Step 2: Replace Section A's data source**

Locate the block under `st.markdown("## App runs (interactive)")` (approximately line 1325). Replace:

```python
runs = _list_runs()
st.markdown("## App runs (interactive)")
if not runs:
    st.info("No interactive runs yet.")
else:
    df = pd.DataFrame(runs)
    df["started"] = pd.to_datetime(df["started_at"])
    a1, a2, a3, a4, a5 = st.columns(5)
    a1.metric("Runs", f"{len(df):,}")
    a2.metric("Files cleaned", f"{df['file_name'].count():,}")
    a3.metric("Total obs", f"{df['n_observations'].sum():,}")
    a4.metric("Total fixes", f"{df['n_dq_issues'].sum():,}")
    a5.metric("Avg pass %", f"{df['pass_rate_pct'].mean():.1f}%")
    # ... charts ...
```

with:

```python
sessions = _list_sessions()
st.markdown("## App sessions")
if sessions.empty:
    st.info("No app sessions yet.")
else:
    df = sessions.copy()
    df["earliest_ingest"] = pd.to_datetime(df["earliest_ingest"])
    a1, a2, a3, a4 = st.columns(4)
    a1.metric("Sessions", f"{len(df):,}")
    a2.metric("Total workbooks", f"{int(df['n_workbooks'].sum()):,}")
    a3.metric("Total obs", f"{int(df['n_observations'].sum()):,}")
    a4.metric("Avg pass %", f"{df['pass_rate_pct'].mean():.1f}%")

    st.markdown("&nbsp;")
    c1, c2 = st.columns(2, gap="large")
    with c1:
        st.markdown("**Sessions over time**")
        by_day = (df.set_index("earliest_ingest")
                    .resample("D").size()
                    .rename("sessions"))
        if not by_day.empty:
            st.bar_chart(by_day, color=PRIMARY, height=260)
    with c2:
        st.markdown("**Pass rate per session**")
        ser = df.set_index("earliest_ingest")["pass_rate_pct"].sort_index()
        if not ser.empty:
            st.line_chart(ser, color=ACCENT_GREEN, height=260)
```

- [ ] **Step 3: Syntax check + commit**

```bash
python3 -c "import ast; ast.parse(open('/home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/apps/quality_team_intelligence/app.py').read()); print('OK')"
git add apps/quality_team_intelligence/app.py
git commit -m "App: Dashboard Section A queries session-scoped tables"
```

---

### Task 5.9: Remove obsolete code paths (uploader, in-process clean, /tmp/qde_runs)

**Files:**
- Modify: `apps/quality_team_intelligence/app.py`

- [ ] **Step 1: Remove the old `_save_uploaded` function and uploader-related session state**

```bash
grep -n "_save_uploaded\|file_uploader\|st.file_uploader\|RUNS_DIR\|process_workbook(" /home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/apps/quality_team_intelligence/app.py | head -20
```

Delete:
- `_save_uploaded()` function (no longer used — Generate is the only entry point)
- All references to `_render_home_processing` from the route table
- `RUNS_DIR` and the directory-based run helpers `_save_run`, `_load_run` (replaced by table queries)
- The `process_workbook` import and call site (only used in the old in-process path)

Be conservative: leave `_render_xlsx_full`, `_list_sheets`, `_xlsx_sheet_to_html` — these are still needed by the new `_tab_outputs_session`.

- [ ] **Step 2: Remove `_generate_demo_files` (replaced by `_generate_session_files`)**

Verify no callers remain:
```bash
grep -n "_generate_demo_files" /home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/apps/quality_team_intelligence/app.py
```
If only the function definition remains, delete it.

- [ ] **Step 3: Update docstring at top of file**

Find the module docstring (lines 1–20) and update the "Home" description to reflect the new flow:

```
  Home      — generate N synthetic Quality team workbooks, upload them
              to a session-scoped subfolder in the input volume, trigger
              the medallion pipeline job, stream live status, then render
              the cleaned outputs filtered to that session.
```

- [ ] **Step 4: Syntax check + commit**

```bash
python3 -c "import ast; ast.parse(open('/home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc/apps/quality_team_intelligence/app.py').read()); print('OK')"
git add apps/quality_team_intelligence/app.py
git commit -m "App: remove obsolete uploader and in-process cleaning paths"
```

---

## Phase 6 — Vendor + deploy + manual smoke test

### Task 6.1: Vendor + bundle deploy

- [ ] **Step 1: Refresh vendored deps in the app folder**

Run:
```bash
cd /home/jellyfish/Desktop/sharepoint_de/primeinsurance-poc
apps/quality_team_intelligence/vendor.sh
```
Expected output: `vendored into apps/quality_team_intelligence/: ... 7 python files ... 1 file ... generate_quality_data.py`

- [ ] **Step 2: Bundle validate + deploy (no-run)**

Run:
```bash
./deploy.sh --no-run 2>&1 | tail -20
```
Expected: `Validation OK!` then `Deployment complete!`

If validation fails, read the error, fix, re-run.

---

### Task 6.2: Run the schema reset notebook once

- [ ] **Step 1: Trigger 00a_reset_tables.py via the workspace UI**

Navigate to the Workspace UI:
- Workspace → `.bundle/quality_de/dev/files/notebooks/00a_reset_tables.py`
- Click **Run all**
- Wait for completion (typically 30 seconds)

Or via CLI:
```bash
/usr/local/bin/databricks bundle run quality_de_pipeline --target dev -p newaccount --notebook-task-key=setup --no-wait
```

(Adjust if the bundle exposes a notebook task for 00a; otherwise UI is fine for this one-shot.)

- [ ] **Step 2: Verify all 5 tables are gone**

Run a quick SQL via the CLI:
```bash
/usr/local/bin/databricks sql query "SHOW TABLES IN quality_de.bronze" -p newaccount 2>&1
/usr/local/bin/databricks sql query "SHOW TABLES IN quality_de.silver" -p newaccount 2>&1
/usr/local/bin/databricks sql query "SHOW TABLES IN quality_de.gold" -p newaccount 2>&1
```
Expected: tables `raw_workbooks`, `observations_long`, `dq_issues`, `column_mapping_log`, `fact_observation` should not be listed.

(MVs in gold may or may not still exist depending on how they were created; that's fine — they get rebuilt by 03_gold_curated.)

---

### Task 6.3: Deploy the app

- [ ] **Step 1: Deploy the app**

```bash
/usr/local/bin/databricks apps deploy quality-team-intelligence \
  --source-code-path "/Workspace/Users/sugandhi.gupta@jellyfishtechnologies.com/.bundle/quality_de/dev/files/apps/quality_team_intelligence" \
  -p newaccount 2>&1 | tail -10
```
Expected: `"state": "SUCCEEDED"`, `"message": "App started successfully"`.

- [ ] **Step 2: Get app URL**

Run:
```bash
/usr/local/bin/databricks apps get quality-team-intelligence -p newaccount --output json | python3 -c "import json,sys; print(json.load(sys.stdin).get('url',''))"
```

---

### Task 6.4: Manual smoke test — happy path

- [ ] **Step 1: Open the app URL in a browser**

- [ ] **Step 2: Click Generate (default 20 files)**

Expected:
- Status spinner during generation + upload
- Transitions to "Session ready" page
- Session ID visible in format `2026-05-08-hhmmss-<6hex>`
- File list shows 20 files

- [ ] **Step 3: Click Run pipeline**

Expected:
- Transitions to "Pipeline running" page
- 5 tasks listed with markers
- Live elapsed time updating
- Tasks transition `○` → `●` → `✓` over ~5 minutes

- [ ] **Step 4: Wait for completion**

Expected:
- All 5 tasks show `✓ SUCCESS`
- Auto-transitions to Results page
- 4 tabs render (Deliverables, DQ Audit, Column Resolution, Compliance Metrics)
- Deliverables shows two sub-tabs with file lists matching the session

- [ ] **Step 5: Verify session-scoped data in tabs**

- DQ Audit: shows `repaired` and `unparseable` severity rows from this session only
- Column Resolution: shows >= 0.9 confidence mappings, all with this session's workbooks
- Compliance Metrics: pass rate / violations / trend show only this session

- [ ] **Step 6: Verify volume contents**

Run:
```bash
/usr/local/bin/databricks fs ls dbfs:/Volumes/quality_de/bronze/sharepoint_input/sessions/ -p newaccount 2>&1
/usr/local/bin/databricks fs ls dbfs:/Volumes/quality_de/bronze/sharepoint_output/sessions/ -p newaccount 2>&1
```
Expected: at least one `<session_id>/` subfolder in each.

---

### Task 6.5: Manual smoke test — discard path

- [ ] **Step 1: Generate a fresh session, then click Discard before Run pipeline**

Expected:
- Returns to "empty" stage
- Volume subfolder `/sessions/<id>/` is deleted (verify via CLI)

---

### Task 6.6: Manual smoke test — retry path (if rate-limit fires naturally)

- [ ] **Step 1: Generate 25 files (max slider value)**
- [ ] **Step 2: Run pipeline**
- [ ] **Step 3: If `silver_ai_cleaning` fails with 429, the cleaner's fallback should kick in and the run should still complete via `mock_synonyms`. Watch the Column Resolution tab — many mappings will show `source = mock_synonyms` instead of `llm`.**

If the run fails entirely (i.e. fallback didn't fire):
- Revisit Task 3.1, ensure the `RateLimitError` import + except branch is correct
- Optionally swap to provisioned-throughput endpoint per `databricks.yml` comment

---

### Task 6.7: Push & merge to prod

Once all smoke tests pass:

- [ ] **Step 1: Push branch**

```bash
git push origin feat/session-scoped-pipeline
```

- [ ] **Step 2: Open PR or merge via merge commit (per existing convention)**

```bash
git checkout prod
git pull origin prod
git merge --no-ff feat/session-scoped-pipeline -m "Merge feat/session-scoped-pipeline: session-isolated medallion pipeline orchestration from app"
git push origin prod
git checkout feat/session-scoped-pipeline
```

---

## Self-review checklist (run before declaring complete)

- [ ] All 5 tables have `session_id STRING NOT NULL` + `PARTITIONED BY (session_id)`
- [ ] All 5 notebooks have a `session_id` widget with default `"legacy_main_pipeline"`
- [ ] `databricks.yml` task `base_parameters` carry `session_id` (not job-level)
- [ ] All Delta writes use `mode("append").option("mergeSchema", "true").partitionBy("session_id")`
- [ ] `quality_core/mapping.py` catches `RateLimitError` explicitly + falls back to synonyms
- [ ] `app.yaml` has `WRITE_VOLUME` on input volume + `CAN_MANAGE_RUN` on the job
- [ ] App's 4 stages render correctly (empty / staged / running / results)
- [ ] Polling shows per-task state with markers
- [ ] Discard button deletes both volume subfolder + table rows
- [ ] Retry button deletes prior table rows then re-triggers same session_id
- [ ] History + Dashboard query session-scoped tables (no `RUNS_DIR` references remain)
- [ ] Vendored `apps/.../quality_core/` matches `quality_core/` after `vendor.sh`
- [ ] Manual smoke test (happy path + discard + retry) passes

---

## Rollback plan (if something goes wrong post-merge)

If the merged change breaks prod:

```bash
git checkout prod
git revert -m 1 <merge_commit_sha>
git push origin prod
./deploy.sh --no-run
/usr/local/bin/databricks apps deploy quality-team-intelligence ...
```

The reset notebook (`00a_reset_tables.py`) is **not** auto-reversible — manual table recreation from prior schemas would be needed. Since the 103-file run was already failed, this isn't a concern for this rollout.
