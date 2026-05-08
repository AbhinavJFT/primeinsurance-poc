# Databricks notebook source
# MAGIC %md
# MAGIC # 00a — Reset session-scoped tables (one-time migration)
# MAGIC
# MAGIC Drops the five core tables so they get recreated by `00_setup.py` with
# MAGIC the new schema (`session_id STRING NOT NULL`, partitioned by
# MAGIC `session_id`, append-mode writes). Idempotent — safe to re-run.
# MAGIC
# MAGIC Run this notebook ONCE before the first deploy of the session-scoped
# MAGIC pipeline. Any prior data in these tables is wiped — we start fresh
# MAGIC with the new schema, and every subsequent run tags rows with a
# MAGIC `session_id` so app sessions and main pipeline runs stay separate.

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
