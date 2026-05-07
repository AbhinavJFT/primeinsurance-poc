# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze: SharePoint ingest (ADF mock)
# MAGIC
# MAGIC Thin orchestrator over the self-contained ADF runner. Reads the pipeline
# MAGIC definition from `adf/pipelines/pl_ingest_sp_to_bronze.json` and executes
# MAGIC the activities (GetMetadata → ForEach[Copy + Script]) against the
# MAGIC SharePoint mock and a UC Volume.
# MAGIC
# MAGIC End state, identical to the previous hand-rolled notebook:
# MAGIC * source `.xlsx` files copied into `/Volumes/{catalog}/bronze/{volume}/`
# MAGIC * one row per file in `bronze.raw_workbooks` (Delta)
# MAGIC
# MAGIC The ADF JSON shape is what the customer would deploy in production —
# MAGIC swap the `_mock` block on `ls_sharepoint_online` for a real Microsoft
# MAGIC Graph linked service and the runner can be replaced with the ADF
# MAGIC runtime without touching the pipeline / dataset definitions.

# COMMAND ----------

dbutils.widgets.text("catalog", "quality_de")
dbutils.widgets.text("volume_input", "sharepoint_input")
CATALOG = dbutils.widgets.get("catalog")
VOL_IN = dbutils.widgets.get("volume_input")

# COMMAND ----------

import os
import sys
sys.path.insert(0, "../")

# Bind the SharePointMock root to the UC Volume tree on Databricks; the
# dataset folderPath "input" is then aliased to the actual volume name.
os.environ["SHAREPOINT_MOCK_ROOT"] = f"/Volumes/{CATALOG}/bronze"
os.environ["SHAREPOINT_FOLDER_ALIAS_input"] = VOL_IN

from connectors.adf import run_pipeline

result = run_pipeline(
    "pl_ingest_sp_to_bronze",
    parameters={"catalog": CATALOG, "volume": VOL_IN, "site": "QualityTeam"},
    spark=spark,
)

# COMMAND ----------

display(spark.table(f"{CATALOG}.bronze.raw_workbooks"))
