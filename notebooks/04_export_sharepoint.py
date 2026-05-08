# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Export cleaned workbooks to SharePoint /output (ADF mock)
# MAGIC
# MAGIC Thin orchestrator over the self-contained ADF runner. Runs **two**
# MAGIC export pipelines back-to-back, one per output view:
# MAGIC
# MAGIC | Pipeline | What it produces | Lands in |
# MAGIC |---|---|---|
# MAGIC | `pl_export_gold_to_sp`  | tidy long-form 3-sheet `.xlsx` (observations / dq_issues / column_mapping_log) | `/output/<workbook>_CLEAN.xlsx` |
# MAGIC | `pl_export_clean_to_sp` | same-format cleaned `.xlsx` — preserves the original 7 batch tabs, merged headers, header band; only data cells are rewritten with cleaned values | `/output/cleaned/<workbook>` |
# MAGIC
# MAGIC Both pipelines are mock-implemented; production ADF would chain a
# MAGIC Databricks notebook activity in place of the Script handlers.

# COMMAND ----------

dbutils.widgets.text("catalog", "quality_de")
dbutils.widgets.text("volume_input", "sharepoint_input")
dbutils.widgets.text("volume_output", "sharepoint_output")
dbutils.widgets.text("session_id", "legacy_main_pipeline")
CATALOG = dbutils.widgets.get("catalog")
VOL_IN = dbutils.widgets.get("volume_input")
VOL_OUT = dbutils.widgets.get("volume_output")
SESSION_ID = dbutils.widgets.get("session_id")

# COMMAND ----------

import os
import sys
sys.path.insert(0, "../")

# When invoked for an app session, the original input files live in
# /input/sessions/<sid>/ — the same-format export needs to read from there.
# Outputs land in /output/sessions/<sid>/{transformed,cleaned}/.
if SESSION_ID == "legacy_main_pipeline":
    INPUT_ALIAS = VOL_IN
    TIDY_SUBFOLDER = ""              # legacy: write tidy to /output root
    CLEAN_SUBFOLDER = "cleaned"      # legacy: /output/cleaned/
else:
    INPUT_ALIAS = f"{VOL_IN}/sessions/{SESSION_ID}"
    TIDY_SUBFOLDER = f"sessions/{SESSION_ID}/transformed"
    CLEAN_SUBFOLDER = f"sessions/{SESSION_ID}/cleaned"

# Bind the SharePointMock root to the UC Volume tree on Databricks; the
# dataset folderPath "input"/"output" aliases map to the actual volume names.
os.environ["SHAREPOINT_MOCK_ROOT"] = f"/Volumes/{CATALOG}/bronze"
os.environ["SHAREPOINT_FOLDER_ALIAS_input"] = INPUT_ALIAS
os.environ["SHAREPOINT_FOLDER_ALIAS_output"] = VOL_OUT

from connectors.adf import run_pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Tidy long-form export

# COMMAND ----------

result_tidy = run_pipeline(
    "pl_export_gold_to_sp",
    parameters={
        "catalog": CATALOG, "site": "QualityTeam",
        "session_id": SESSION_ID,
        "subfolder": TIDY_SUBFOLDER,
    },
    spark=spark,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Same-format cleaned export

# COMMAND ----------

result_clean = run_pipeline(
    "pl_export_clean_to_sp",
    parameters={
        "catalog": CATALOG, "site": "QualityTeam",
        "session_id": SESSION_ID,
        "subfolder": CLEAN_SUBFOLDER,
    },
    spark=spark,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where the files landed

# COMMAND ----------

from pathlib import Path

output_path = Path(f"/Volumes/{CATALOG}/bronze/{VOL_OUT}")
if SESSION_ID == "legacy_main_pipeline":
    print(f"=== /output/ (tidy long-form) ===")
    for p in sorted(output_path.glob("*_CLEAN.xlsx")):
        print(f"  {p.name:<48} {p.stat().st_size:>10,} bytes")
    print(f"\n=== /output/cleaned/ (same-format) ===")
    for p in sorted((output_path / "cleaned").glob("*.xlsx")):
        print(f"  {p.name:<48} {p.stat().st_size:>10,} bytes")
else:
    tidy_dir = output_path / "sessions" / SESSION_ID / "transformed"
    clean_dir = output_path / "sessions" / SESSION_ID / "cleaned"
    print(f"=== {tidy_dir} (tidy long-form) ===")
    for p in sorted(tidy_dir.glob("*.xlsx")):
        print(f"  {p.name:<48} {p.stat().st_size:>10,} bytes")
    print(f"\n=== {clean_dir} (same-format) ===")
    for p in sorted(clean_dir.glob("*.xlsx")):
        print(f"  {p.name:<48} {p.stat().st_size:>10,} bytes")
