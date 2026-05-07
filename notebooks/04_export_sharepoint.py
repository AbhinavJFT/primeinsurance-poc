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
CATALOG = dbutils.widgets.get("catalog")
VOL_IN = dbutils.widgets.get("volume_input")
VOL_OUT = dbutils.widgets.get("volume_output")

# COMMAND ----------

import os
import sys
sys.path.insert(0, "../")

# Bind the SharePointMock root to the UC Volume tree on Databricks; the
# dataset folderPath "input"/"output" aliases map to the actual volume names.
os.environ["SHAREPOINT_MOCK_ROOT"] = f"/Volumes/{CATALOG}/bronze"
os.environ["SHAREPOINT_FOLDER_ALIAS_input"] = VOL_IN
os.environ["SHAREPOINT_FOLDER_ALIAS_output"] = VOL_OUT

from connectors.adf import run_pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Tidy long-form export (existing behaviour)

# COMMAND ----------

result_tidy = run_pipeline(
    "pl_export_gold_to_sp",
    parameters={"catalog": CATALOG, "site": "QualityTeam"},
    spark=spark,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Same-format cleaned export (new)

# COMMAND ----------

result_clean = run_pipeline(
    "pl_export_clean_to_sp",
    parameters={"catalog": CATALOG, "site": "QualityTeam", "subfolder": "cleaned"},
    spark=spark,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where the files landed

# COMMAND ----------

from pathlib import Path

output_path = Path(f"/Volumes/{CATALOG}/bronze/{VOL_OUT}")
print(f"=== /output/ (tidy long-form) ===")
for p in sorted(output_path.glob("*_CLEAN.xlsx")):
    print(f"  {p.name:<48} {p.stat().st_size:>10,} bytes")
print(f"\n=== /output/cleaned/ (same-format) ===")
for p in sorted((output_path / "cleaned").glob("*.xlsx")):
    print(f"  {p.name:<48} {p.stat().st_size:>10,} bytes")
