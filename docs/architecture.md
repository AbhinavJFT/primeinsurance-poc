# Architecture — SharePoint Quality team cleaning POC

## Problem

Quality team data lives in semi-structured Excel workbooks: multiple tabs per
batch, multi-row merged headers, units and specifications stacked above the
data, free-text fields with typos and casing variants, malformed date/time
values, and occasional negative or out-of-range readings. None of this can be
queried, joined, or charted directly.

## Goal

Read workbooks from a SharePoint /input folder, clean them with AI assistance,
and write back:
1. A tidy single-sheet `.xlsx` per workbook into the SharePoint /output folder.
2. Curated Delta tables (`gold.fact_observation`, dimensional + materialized
   views) so dashboards can query the same data.

## Flow

```
                                 SharePoint mock
                               (UC Volume on Databricks /
                                local folder during dev)
                                        │
                                        │  list / download
                                        ▼
                            ┌───────────────────────────┐
                            │  Bronze                   │
                            │  bronze.raw_workbooks     │  one row per file
                            └────────────┬──────────────┘
                                         │
                                         ▼
              ┌────────────────────────────────────────────────────┐
              │  Silver — AI-driven cleaning  (showpiece)          │
              │                                                    │
              │   inference  ─►  detect header band, meta vs       │
              │                  impurity columns, data start row  │
              │                                                    │
              │   mapping    ─►  databricks-gpt-oss-20b fuzzy-maps │
              │                  raw labels to canonical schema    │
              │                  (mock_synonyms fallback)          │
              │                                                    │
              │   cleaning   ─►  type coercion, null sentinels,    │
              │                  case normalization, time/date     │
              │                  repair, negative-value handling   │
              │                                                    │
              │   pivot      ─►  wide ► long observation rows      │
              │                                                    │
              │   silver.observations_long                         │
              │   silver.dq_issues                                 │
              │   silver.column_mapping_log                        │
              │   silver.quarantine_review (view)                  │
              └────────────────────────┬───────────────────────────┘
                                       │
                                       ▼
                       ┌───────────────────────────────────┐
                       │  Gold (star schema)               │
                       │  gold.fact_observation            │
                       │  gold.dim_batch                   │
                       │  gold.dim_analyte                 │
                       │  gold.mv_batch_pass_rate          │
                       │  gold.mv_impurity_trend           │
                       │  gold.mv_spec_violations          │
                       └────────────┬──────────────────────┘
                                    │
                ┌───────────────────┴────────────────────┐
                ▼                                        ▼
   ┌────────────────────────┐               ┌──────────────────────────┐
   │  SQL dashboard         │               │  Export                  │
   │  (4 tiles + 2 tables   │               │  one tidy .xlsx per      │
   │   per page; 2 pages)   │               │  source workbook with    │
   └────────────────────────┘               │  observations / dq_issues│
                                            │  / column_mapping_log    │
                                            │  sheets                  │
                                            └─────────┬────────────────┘
                                                      ▼
                                       SharePoint mock /output
```

## Component map

| Layer | Path | Notes |
|---|---|---|
| Mock SharePoint | `connectors/sharepoint_mock.py` | Surface mirrors Microsoft Graph. Replace with a Graph-backed client to flip to real SharePoint. |
| Synthetic data | `generate_quality_data.py` | Deterministic seed; produces 3 workbooks shaped like the real Quality team workbooks. |
| Cleaning library | `quality_core/` | `inference.py`, `mapping.py`, `cleaning.py`, `pipeline.py`, `models.py`. Pure Python — runs locally and on Databricks. |
| Schema | `schemas/canonical_quality_schema.yaml` | Target tidy schema, synonyms, analyte aliases. |
| Local runner | `scripts/run_local_demo.py` | Full pipeline without Databricks; writes Gold-style CSVs to `data/gold_local/`. |
| Notebooks | `notebooks/00_setup.py` … `05_demo_walkthrough.py` | Thin orchestration over `quality_core`. |
| DAB | `databricks.yml` | One job, five tasks (setup → bronze → silver → gold → export). |
| Dashboard | `dashboards/quality_team_dashboard.json` | Two pages: Overview (KPIs + pass rate + trend) and Data quality (DQ rules, mapping confidence, violations). |

## What "AI-driven" means here

Two LLM-backed steps:

1. **Column mapping** (the showpiece).
   The fuzzy mapping from raw column labels (`Imp-A`, `2-chloro benzamide`,
   `Appearance of 30% solution in methanol (after 12 hrs settling)`) to the
   canonical schema is done by an LLM call against `databricks-gpt-oss-20b` via
   the OpenAI SDK, with a structured-output JSON contract and a confidence
   score per column. Low-confidence mappings land in `quarantine_review` for
   human approval.

2. **Mock fallback.**
   When `MOCK_LLM=true` or no Databricks credentials are available, a
   deterministic synonym matcher takes the LLM's place. Same output schema, so
   the demo still tells the same story end-to-end.

Future extensions (not built — listed for awareness):
- LLM-generated DQ explanations on the `dq_issues` table (mirrors
  `primeinsurance-poc/notebooks/genai/04_uc1_dq_explanations.py`).
- LLM-generated executive summary on the dashboard.

## What lives where in the corresponding cousin POCs

| In this repo | Cousin in primeinsurance-poc | Cousin in dpdp |
|---|---|---|
| `connectors/sharepoint_mock.py` | (no analogue — primeins reads from a UC Volume) | `generate_salesforce_data.py` (synthetic stand-in for an external system) |
| `generate_quality_data.py` | `data/autoinsurancedata/` (CSV/JSON files committed to the repo) | `generate_salesforce_data.py` |
| `notebooks/01_bronze_sharepoint_ingest.py` | `notebooks/bronze/01_bronze_ingestion_dlt.py` | `pipelines/phase1_bootstrap.py` |
| `notebooks/02_silver_ai_cleaning.py` | `notebooks/silver/02_silver_dlt_pipeline.py` + `notebooks/genai/04_uc1_dq_explanations.py` | `pipelines/classification_dlt.py` |
| `notebooks/03_gold_curated.py` | `notebooks/gold/03_gold_dlt_pipeline.py` | `pipelines/redaction_dlt.py` (different purpose, similar layering) |
| `dashboards/quality_team_dashboard.json` | `dashboards/primeinsurance_dashboard.json` | (none) |
| `databricks.yml` | `databricks.yml` | `databricks.yml` |
