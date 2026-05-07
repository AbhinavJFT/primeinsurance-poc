# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold: curated star schema
# MAGIC
# MAGIC Builds the dashboard-facing layer:
# MAGIC   - `gold.dim_batch`         — one row per batch
# MAGIC   - `gold.dim_analyte`       — canonical analyte catalog
# MAGIC   - `gold.fact_observation`  — long-form fact table for every measurement
# MAGIC   - `gold.mv_batch_pass_rate`, `mv_impurity_trend`, `mv_spec_violations`
# MAGIC
# MAGIC Mirrors `primeinsurance-poc/notebooks/gold/03_gold_dlt_pipeline.py`.

# COMMAND ----------

dbutils.widgets.text("catalog", "quality_de")
CATALOG = dbutils.widgets.get("catalog")

# COMMAND ----------

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
FROM {CATALOG}.silver.observations_long
WHERE batch_no IS NOT NULL
GROUP BY batch_no, workbook, sheet
""")

# COMMAND ----------

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
FROM {CATALOG}.silver.observations_long
WHERE analyte_canonical IS NOT NULL
GROUP BY analyte_canonical
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.fact_observation AS
SELECT
  workbook, sheet, row_seq,
  sample_date, batch_no, instrument_id, stage, sample_form,
  appearance, appearance_solution,
  analyte, analyte_canonical,
  rt, rrt, value, unit,
  spec_min, spec_max, spec_internal_min, spec_internal_max,
  pass,
  mapping_confidence
FROM {CATALOG}.silver.observations_long
""")

# COMMAND ----------

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

# COMMAND ----------

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

# COMMAND ----------

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
