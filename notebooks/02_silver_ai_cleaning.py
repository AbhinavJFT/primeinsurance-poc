# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver: AI-driven cleaning (showpiece)
# MAGIC
# MAGIC For each workbook in bronze:
# MAGIC   1. **Schema inference** — locate header band + meta vs impurity columns
# MAGIC      (handles both quality_standard and misc_flat layouts).
# MAGIC   2. **AI column mapping** — fuzzy-map raw column labels to the canonical
# MAGIC      schema using `databricks-gpt-oss-20b`, with confidence + rationale.
# MAGIC      Falls back to deterministic synonym matching if `MOCK_LLM=true`.
# MAGIC   3. **Cleaning** — type coercion, normalization, value repair; every fix
# MAGIC      is logged in `silver.dq_issues`.
# MAGIC   4. **Pivot wide → long** — emit one row per (batch, sheet, analyte) into
# MAGIC      `silver.observations_long`.
# MAGIC
# MAGIC Mirrors `primeinsurance-poc/notebooks/silver/02_silver_dlt_pipeline.py` in
# MAGIC spirit: harmonize, validate, log every issue, route bad rows to quarantine.

# COMMAND ----------

dbutils.widgets.text("catalog", "quality_de")
dbutils.widgets.text("volume_input", "sharepoint_input")
dbutils.widgets.text("llm_endpoint", "databricks-gpt-oss-20b")
dbutils.widgets.text("session_id", "legacy_main_pipeline")
# Switch to "false" to skip the LLM mapper entirely and use the fast
# deterministic synonym matcher (`mock_synonyms`) for every sheet.
# Useful when the FM endpoint is rate-limiting or when you want quick runs.
dbutils.widgets.text("use_llm", "false")

CATALOG = dbutils.widgets.get("catalog")
VOL_IN = dbutils.widgets.get("volume_input")
ENDPOINT = dbutils.widgets.get("llm_endpoint")
SESSION_ID = dbutils.widgets.get("session_id")
USE_LLM = dbutils.widgets.get("use_llm").strip().lower() in ("1", "true", "yes")

# COMMAND ----------

import os
import sys
from pathlib import Path

sys.path.insert(0, "../")

from quality_core import process_workbook

# Build the LLM client — Databricks provides DATABRICKS_HOST / TOKEN automatically inside notebooks.
# Skipped entirely when use_llm widget is "false" (fast mode using the
# deterministic synonym matcher).
llm_client = None
if not USE_LLM:
    print("use_llm=false — skipping LLM mapper, using mock_synonyms for every sheet.")
else:
    try:
        from openai import OpenAI
        host = (spark.conf.get("spark.databricks.workspaceUrl", None)
                or os.environ.get("DATABRICKS_HOST"))
        token = (dbutils.notebook.entry_point.getDbutils().notebook().getContext()
                 .apiToken().get())
        if host and token and os.environ.get("MOCK_LLM", "").lower() not in ("1", "true", "yes"):
            host = host if host.startswith("http") else f"https://{host}"
            llm_client = OpenAI(base_url=f"{host}/serving-endpoints", api_key=token)
            print(f"LLM mapper enabled — endpoint={ENDPOINT}")
    except Exception as e:
        print(f"LLM client unavailable, falling back to mock_synonyms: {e}")

# COMMAND ----------

# Process only the files registered for this session in bronze. The legacy
# main pipeline still scans the volume root via the bronze rows it produced.
bronze_for_session = (
    spark.table(f"{CATALOG}.bronze.raw_workbooks")
         .filter(f"session_id = '{SESSION_ID}'")
)
input_paths = [Path(r.source_path) for r in bronze_for_session.collect()]

results = []
for p in input_paths:
    print(f"processing {p.name}…")
    res = process_workbook(p, llm_client=llm_client)
    print(f"  {len(res.observations)} obs | {len(res.dq_issues)} dq | {len(res.mappings)} mappings")
    results.append(res)

# COMMAND ----------

# Materialize the three core silver tables.
obs_rows = [
    {"workbook": o.workbook, "sheet": o.sheet, "row_seq": o.row_seq,
     "sample_date": o.sample_date, "sample_time": str(o.sample_time) if o.sample_time else None,
     "report_time": str(o.report_time) if o.report_time else None,
     "batch_no": o.batch_no, "instrument_id": o.instrument_id, "stage": o.stage,
     "sample_form": o.sample_form, "appearance": o.appearance,
     "appearance_solution": o.appearance_solution,
     "analyte": o.analyte, "analyte_canonical": o.analyte_canonical,
     "column_index": o.column_index,
     "rt": o.rt, "rrt": o.rrt, "value": o.value, "unit": o.unit,
     "spec_min": o.spec_min, "spec_max": o.spec_max,
     "spec_internal_min": o.spec_internal_min, "spec_internal_max": o.spec_internal_max,
     "pass": o.pass_, "raw_value": o.raw_value,
     "mapping_confidence": o.mapping_confidence,
     "session_id": SESSION_ID}
    for r in results for o in r.observations
]
dq_rows = [
    {"workbook": d.workbook, "sheet": d.sheet, "row_seq": d.row_seq,
     "column": d.column, "rule": d.rule, "severity": d.severity,
     "raw_value": str(d.raw_value), "repaired_value": str(d.repaired_value),
     "note": d.note,
     "session_id": SESSION_ID}
    for r in results for d in r.dq_issues
]
map_rows = [
    {"workbook": m.workbook, "sheet": m.sheet, "column_index": m.column_index,
     "raw_label": m.raw_label, "role": m.role, "canonical": m.canonical,
     "confidence": m.confidence, "rationale": m.rationale, "source": m.source,
     "session_id": SESSION_ID}
    for r in results for m in r.mappings
]

# Append + mergeSchema + partition by session_id so multiple sessions
# coexist in the same tables. App-side queries filter by session_id.
(spark.createDataFrame(obs_rows)
   .write.format("delta").mode("append").option("mergeSchema", "true")
   .partitionBy("session_id")
   .saveAsTable(f"{CATALOG}.silver.observations_long"))
(spark.createDataFrame(dq_rows)
   .write.format("delta").mode("append").option("mergeSchema", "true")
   .partitionBy("session_id")
   .saveAsTable(f"{CATALOG}.silver.dq_issues"))
(spark.createDataFrame(map_rows)
   .write.format("delta").mode("append").option("mergeSchema", "true")
   .partitionBy("session_id")
   .saveAsTable(f"{CATALOG}.silver.column_mapping_log"))

print(f"silver.observations_long  rows={len(obs_rows)}")
print(f"silver.dq_issues          rows={len(dq_rows)}")
print(f"silver.column_mapping_log rows={len(map_rows)}")

# COMMAND ----------

# Quarantine view: low-confidence mappings AND error-severity DQ issues.
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.silver.quarantine_review AS
SELECT 'mapping_low_confidence' AS reason, workbook, sheet,
       CAST(NULL AS BIGINT) AS row_seq, raw_label AS column,
       confidence AS metric, rationale AS note
FROM {CATALOG}.silver.column_mapping_log
WHERE confidence < 0.5 AND role <> 'ignored'
UNION ALL
SELECT 'dq_unparseable' AS reason, workbook, sheet, row_seq, column,
       NULL AS metric, rule || ': ' || note AS note
FROM {CATALOG}.silver.dq_issues
WHERE severity = 'unparseable'
""")
