# Quality Team SharePoint Data Engineering POC

AI-driven cleaning of semi-structured Quality team Excel workbooks.

**Use case:** Quality team data exists in semi-structured Excel formats (multi-tab, merged headers, units mixed with values, free-text fields, irregular spec metadata). This POC reads workbooks from a SharePoint input folder, cleans and standardizes them with AI assistance, and writes both a tidy Excel back to a SharePoint output folder and curated Delta tables for analytics.

## Flow

```
SharePoint /input  →  Bronze (raw landing)
                  →  Silver (AI column mapping + cleaning + DQ)
                  →  Gold   (star schema, curated)
                  →  SharePoint /output (cleaned .xlsx)
                  →  Dashboard
```

## Repo layout

| Path | Purpose |
|---|---|
| `generate_quality_data.py` | Synthetic xlsx generator (deterministic seed) — mimics real Quality team workbooks |
| `connectors/sharepoint_mock.py` | Mock SharePoint client; same surface as Microsoft Graph so swapping later is one class |
| `connectors/adf/` | Self-contained ADF runner — interprets `adf/*.json` against the SharePoint mock and Delta backends |
| `adf/` | ADF JSON artifacts (linkedServices / datasets / pipelines / triggers) — production-shaped, drop-in to a real ADF instance |
| `quality_core/` | Importable cleaning library (header detection, LLM column mapping, type coercion, pivot) |
| `notebooks/` | Databricks notebooks orchestrating the medallion pipeline |
| `scripts/seed_mock_sharepoint.py` | Generate synthetic xlsx and drop into mock input folder |
| `scripts/run_local_demo.py` | Run the full pipeline locally without Databricks (for dev/CI) |
| `schemas/canonical_quality_schema.yaml` | Target tidy schema |
| `dashboards/` | Databricks SQL dashboard JSON |
| `data/mock_sharepoint/{input,output}/` | Local stand-in for SharePoint |

## Quick demo

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python scripts/seed_mock_sharepoint.py        # produce 3 messy workbooks in data/mock_sharepoint/input/
python scripts/run_local_demo.py              # run the full pipeline locally
ls data/mock_sharepoint/output/               # cleaned .xlsx files appear here
```

### Demo via the ADF mock

The same I/O — SharePoint → bronze landing, gold → SharePoint — can be
driven through the self-contained ADF runner. The artifacts under
`adf/` are real ADF JSON shapes (linkedServices / datasets / pipelines)
that can be dropped into a production ADF instance with only the
`_mock` blocks swapped out. See `connectors/adf/README.md` for details.

```bash
python scripts/seed_mock_sharepoint.py
python -m connectors.adf.runner pl_ingest_sp_to_bronze     # SharePoint → bronze landing + manifest
python scripts/run_local_demo.py                            # populates data/gold_local/*.csv
python -m connectors.adf.runner pl_export_gold_to_sp        # gold → SharePoint /output (3-sheet xlsx)
ls data/mock_sharepoint/output/
```

Open one input file (messy, 7 tabs, merged headers) and one output file (single tidy sheet) side-by-side to see the transformation.

## Databricks deploy

```bash
databricks bundle deploy --target dev
databricks bundle run quality_de_pipeline --target dev
```
