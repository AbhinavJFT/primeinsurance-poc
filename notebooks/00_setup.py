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

CATALOG = dbutils.widgets.get("catalog")
VOL_IN = dbutils.widgets.get("volume_input")
VOL_OUT = dbutils.widgets.get("volume_output")

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

# Generate to a local temp dir first — UC Volumes are FUSE-mounted and don't
# support the random seek() openpyxl needs while writing .xlsx files. We then
# stream-copy each finished file into the Volume.
input_root = Path(f"/Volumes/{CATALOG}/bronze/{VOL_IN}")
input_root.mkdir(parents=True, exist_ok=True)

with tempfile.TemporaryDirectory() as tmp:
    written = generate(tmp, seed=43)
    for p in written:
        target = input_root / p.name
        shutil.copy(p, target)
        print(f"  wrote {target}  ({target.stat().st_size:,} bytes)")

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
