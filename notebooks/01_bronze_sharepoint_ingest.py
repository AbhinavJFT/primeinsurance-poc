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
dbutils.widgets.text("session_id", "legacy_main_pipeline")
CATALOG = dbutils.widgets.get("catalog")
VOL_IN = dbutils.widgets.get("volume_input")
SESSION_ID = dbutils.widgets.get("session_id")

# COMMAND ----------

import os
import sys
sys.path.insert(0, "../")

# When invoked for an app session, scope ingest to the session subfolder
# under the input volume. The legacy main pipeline keeps reading from the
# volume root (back-compat — sessions/ subfolder is invisible to it).
if SESSION_ID == "legacy_main_pipeline":
    INGEST_VOLUME = VOL_IN
else:
    INGEST_VOLUME = f"{VOL_IN}/sessions/{SESSION_ID}"

# Bind the SharePointMock root to the UC Volume tree on Databricks; the
# dataset folderPath "input" is then aliased to the (possibly session-
# scoped) volume name.
os.environ["SHAREPOINT_MOCK_ROOT"] = f"/Volumes/{CATALOG}/bronze"
os.environ["SHAREPOINT_FOLDER_ALIAS_input"] = INGEST_VOLUME

from connectors.adf import run_pipeline

result = run_pipeline(
    "pl_ingest_sp_to_bronze",
    parameters={
        "catalog": CATALOG,
        "volume": INGEST_VOLUME,
        "site": "QualityTeam",
        "session_id": SESSION_ID,
    },
    spark=spark,
)
