# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold: curated star schema
# MAGIC
# MAGIC Builds the dashboard-facing layer:
# MAGIC   - `gold.dim_batch`         — one row per batch (all sessions)
# MAGIC   - `gold.dim_analyte`       — canonical analyte catalog (all sessions)
# MAGIC   - `gold.fact_observation`  — long-form fact, partitioned by session_id (append)
# MAGIC   - `gold.mv_batch_pass_rate`, `mv_impurity_trend`, `mv_spec_violations`
# MAGIC      — global rollups across all sessions (rebuilt from fact_observation)
# MAGIC
# MAGIC The fact table is append-mode + partitioned by `session_id` so multiple
# MAGIC app sessions accumulate alongside legacy main-pipeline data without
# MAGIC stomping each other. Dim tables and MVs are rebuilt cross-session
# MAGIC every run so the global Dashboard view stays current.

# COMMAND ----------

dbutils.widgets.text("catalog", "quality_de")
dbutils.widgets.text("session_id", "legacy_main_pipeline")
CATALOG = dbutils.widgets.get("catalog")
SESSION_ID = dbutils.widgets.get("session_id")

# COMMAND ----------

# fact_observation: append THIS session's rows. Partitioned by session_id so
# the table coexists with prior sessions and the legacy main pipeline.
fact_for_session = spark.sql(f"""
SELECT
  workbook, sheet, row_seq,
  sample_date, batch_no, instrument_id, stage, sample_form,
  appearance, appearance_solution,
  analyte, analyte_canonical,
  rt, rrt, value, unit,
  spec_min, spec_max, spec_internal_min, spec_internal_max,
  pass,
  mapping_confidence,
  session_id
FROM {CATALOG}.silver.observations_long
WHERE session_id = '{SESSION_ID}'
""")

(fact_for_session.write
   .format("delta")
   .mode("append")
   .option("mergeSchema", "true")
   .partitionBy("session_id")
   .saveAsTable(f"{CATALOG}.gold.fact_observation"))

# COMMAND ----------

# dim_batch / dim_analyte: rebuilt cross-session from the full fact table
# so they reflect the global vocabulary.
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.dim_batch AS
SELECT DISTINCT
  batch_no,
  workbook,
  sheet,
  FIRST(stage)        AS stage,
  FIRST(instrument_id) AS instrument_id,
  MIN(sample_date)    AS first_seen,
  MAX(sample_date)    AS last_seen
FROM {CATALOG}.gold.fact_observation
WHERE batch_no IS NOT NULL
GROUP BY batch_no, workbook, sheet
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.dim_analyte AS
SELECT
  analyte_canonical,
  COLLECT_SET(analyte) AS raw_label_variants,
  COLLECT_SET(unit)    AS units_seen,
  AVG(rt)              AS avg_rt,
  AVG(rrt)             AS avg_rrt,
  AVG(spec_max)        AS avg_spec_max,
  AVG(spec_min)        AS avg_spec_min
FROM {CATALOG}.gold.fact_observation
WHERE analyte_canonical IS NOT NULL
GROUP BY analyte_canonical
""")

# COMMAND ----------

# Cross-session rollups. CREATE OR REPLACE intentional — they mirror the
# current state of fact_observation across all sessions.

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.mv_batch_pass_rate AS
SELECT
  workbook,
  sheet,
  batch_no,
  COUNT(*)                                         AS observations,
  SUM(CASE WHEN pass = true  THEN 1 ELSE 0 END)    AS passing,
  SUM(CASE WHEN pass = false THEN 1 ELSE 0 END)    AS failing,
  ROUND(AVG(CASE WHEN pass = true THEN 1.0 WHEN pass = false THEN 0.0 END), 3)
                                                   AS pass_rate
FROM {CATALOG}.gold.fact_observation
WHERE pass IS NOT NULL
GROUP BY workbook, sheet, batch_no
ORDER BY pass_rate ASC
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.mv_impurity_trend AS
SELECT
  analyte_canonical,
  sample_date,
  COUNT(*)         AS n_observations,
  AVG(value)       AS avg_value,
  MAX(value)       AS max_value,
  MIN(spec_max)    AS spec_max
FROM {CATALOG}.gold.fact_observation
WHERE analyte_canonical IS NOT NULL
  AND sample_date IS NOT NULL
GROUP BY analyte_canonical, sample_date
ORDER BY sample_date
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.mv_spec_violations AS
SELECT
  analyte_canonical,
  workbook,
  sheet,
  COUNT(*) AS violation_count,
  AVG(value)              AS avg_violating_value,
  AVG(value - spec_max)   AS avg_overshoot
FROM {CATALOG}.gold.fact_observation
WHERE pass = false
GROUP BY analyte_canonical, workbook, sheet
ORDER BY violation_count DESC
""")

# COMMAND ----------

display(spark.table(f"{CATALOG}.gold.mv_batch_pass_rate"))
