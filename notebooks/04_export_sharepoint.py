# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Export cleaned workbooks to SharePoint /output (ADF mock)
# MAGIC
# MAGIC Thin orchestrator over the self-contained ADF runner. Reads the pipeline
# MAGIC definition from `adf/pipelines/pl_export_gold_to_sp.json` and executes
# MAGIC the activities (Lookup → ForEach[Copy]) against the gold/silver Delta
# MAGIC tables and the SharePoint mock /output folder.
# MAGIC
# MAGIC End state, identical to the previous hand-rolled notebook:
# MAGIC * one tidy 3-sheet `.xlsx` (`<workbook>_CLEAN.xlsx`) per source file
# MAGIC   in `/Volumes/{catalog}/bronze/{volume_output}/`
# MAGIC * sheets: `observations`, `dq_issues`, `column_mapping_log`
# MAGIC
# MAGIC The Copy activity that materializes the xlsx is mock-only — production
# MAGIC ADF would chain a Databricks notebook activity for the openpyxl step.

# COMMAND ----------

dbutils.widgets.text("catalog", "quality_de")
dbutils.widgets.text("volume_output", "sharepoint_output")
CATALOG = dbutils.widgets.get("catalog")
VOL_OUT = dbutils.widgets.get("volume_output")

# COMMAND ----------

import os
import sys
sys.path.insert(0, "../")

# Bind the SharePointMock root to the UC Volume tree on Databricks; the
# dataset folderPath "output" is then aliased to the actual volume name.
os.environ["SHAREPOINT_MOCK_ROOT"] = f"/Volumes/{CATALOG}/bronze"
os.environ["SHAREPOINT_FOLDER_ALIAS_output"] = VOL_OUT

from connectors.adf import run_pipeline

result = run_pipeline(
    "pl_export_gold_to_sp",
    parameters={"catalog": CATALOG, "site": "QualityTeam"},
    spark=spark,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where the files landed

# COMMAND ----------

from pathlib import Path

output_path = Path(f"/Volumes/{CATALOG}/bronze/{VOL_OUT}")
for p in sorted(output_path.glob("*_CLEAN.xlsx")):
    print(f"  {p.name:<48} {p.stat().st_size:>10,} bytes")
