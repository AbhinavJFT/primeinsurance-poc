# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup
# MAGIC
# MAGIC Creates the Unity Catalog catalog, schemas, and Volume used by this POC,
# MAGIC and seeds the mock SharePoint /input folder with synthetic Quality team
# MAGIC workbooks. Run this once per workspace before the Bronze pipeline.
# MAGIC
# MAGIC Pattern mirrors `primeinsurance-poc/notebooks/setup/00_create_catalog_and_upload.py`.

# COMMAND ----------

dbutils.widgets.text("catalog", "quality_de")
dbutils.widgets.text("volume_input", "sharepoint_input")
dbutils.widgets.text("volume_output", "sharepoint_output")
dbutils.widgets.text("session_id", "legacy_main_pipeline")
# Service principal that backs the Streamlit app. When set, this notebook
# grants it all UC privileges it needs to upload session files, query the
# medallion tables, and clean up sessions. Empty = skip grants (e.g. when
# running locally or before the app exists).
dbutils.widgets.text("app_service_principal", "")

CATALOG = dbutils.widgets.get("catalog")
VOL_IN = dbutils.widgets.get("volume_input")
VOL_OUT = dbutils.widgets.get("volume_output")
SESSION_ID = dbutils.widgets.get("session_id")
APP_SP = dbutils.widgets.get("app_service_principal").strip()

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.{VOL_IN}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.{VOL_OUT}")

print(f"catalog: {CATALOG}")
print(f"volumes: {CATALOG}.bronze.{VOL_IN}  /  {CATALOG}.bronze.{VOL_OUT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seed the mock SharePoint /input volume
# MAGIC
# MAGIC On Databricks the "mock SharePoint" is just a UC Volume. The synthetic
# MAGIC generator writes into it directly; production would replace this with
# MAGIC a Microsoft Graph download into the same volume.

# COMMAND ----------

import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, "../")

from generate_quality_data import generate

# Demo seeding only applies to the legacy main pipeline. App-driven
# session runs upload their files directly to /sessions/<session_id>/ via
# the Streamlit app — no seeding needed here.
if SESSION_ID == "legacy_main_pipeline":
    # Generate to a local temp dir first — UC Volumes are FUSE-mounted and
    # don't support the random seek() openpyxl needs while writing .xlsx
    # files. We then stream-copy each finished file into the Volume.
    input_root = Path(f"/Volumes/{CATALOG}/bronze/{VOL_IN}")
    input_root.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as tmp:
        written = generate(tmp, seed=43)
        for p in written:
            target = input_root / p.name
            shutil.copy(p, target)
            print(f"  wrote {target}  ({target.stat().st_size:,} bytes)")
else:
    print(f"  (session-scoped run for {SESSION_ID}; skipping demo seeding)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze table is created on first write
# MAGIC
# MAGIC We deliberately don't pre-create `bronze.raw_workbooks` here — letting
# MAGIC `01_bronze_sharepoint_ingest.py` define the schema from the actual
# MAGIC DataFrame avoids INT-vs-BIGINT mismatches between hand-written DDL and
# MAGIC inferred types under Spark Connect.

# COMMAND ----------

print("setup complete — catalog, schemas, volumes ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant app service principal the privileges it needs
# MAGIC
# MAGIC Skipped when `app_service_principal` widget is empty. Idempotent —
# MAGIC GRANT is a no-op if the privilege is already held.

# COMMAND ----------

if APP_SP:
    grants = [
        f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{APP_SP}`",
        f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.bronze TO `{APP_SP}`",
        f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.silver TO `{APP_SP}`",
        f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.gold TO `{APP_SP}`",
        f"GRANT SELECT ON SCHEMA {CATALOG}.bronze TO `{APP_SP}`",
        f"GRANT SELECT ON SCHEMA {CATALOG}.silver TO `{APP_SP}`",
        f"GRANT SELECT ON SCHEMA {CATALOG}.gold TO `{APP_SP}`",
        f"GRANT MODIFY ON SCHEMA {CATALOG}.bronze TO `{APP_SP}`",
        f"GRANT MODIFY ON SCHEMA {CATALOG}.silver TO `{APP_SP}`",
        f"GRANT MODIFY ON SCHEMA {CATALOG}.gold TO `{APP_SP}`",
        f"GRANT WRITE VOLUME ON VOLUME {CATALOG}.bronze.{VOL_IN} TO `{APP_SP}`",
        f"GRANT READ VOLUME  ON VOLUME {CATALOG}.bronze.{VOL_IN} TO `{APP_SP}`",
        f"GRANT READ VOLUME  ON VOLUME {CATALOG}.bronze.{VOL_OUT} TO `{APP_SP}`",
    ]
    for sql in grants:
        try:
            spark.sql(sql)
            print(f"  ✓ {sql}")
        except Exception as e:
            print(f"  ! {sql}  →  {e}")
    print(f"\nApp SP `{APP_SP}` granted UC privileges on {CATALOG}.")
    print("NOTE: CAN_MANAGE_RUN on the pipeline job is not a UC privilege —")
    print("      grant it via: databricks api patch /api/2.0/permissions/jobs/<id> ...")
    print("      or the Jobs UI → Permissions → add the SP with 'Can Manage Run'.")
else:
    print("app_service_principal widget empty — skipping grants.")
