# `connectors/adf` — Self-contained Azure Data Factory mock

A small JSON interpreter that executes the artifacts under `adf/`
(linkedServices, datasets, pipelines, triggers) the way Azure Data
Factory would — but against a local `SharePointMock` and either Spark
Delta tables or local CSV/JSON files. Same role as the
`generate_salesforce_data.py` + `seed_salesforce_data.py` pair in the
dpdp POC: a self-contained stand-in for the production ingestion path.

## What's mock vs. real

| Layer | Mock today | Real production |
|---|---|---|
| `adf/linkedServices/ls_sharepoint_online.json` | `_mock.backend = SharePointMock`, reading `data/mock_sharepoint/` | `SharePointOnlineList` (or HTTP + Graph) with KeyVault-backed service principal |
| `adf/linkedServices/ls_databricks_delta.json` | local files when no Spark, Delta tables when Spark is present | `AzureDatabricksDeltaLake` linked service with workspace MSI |
| `adf/datasets/ds_*` | unchanged — already in real ADF JSON shape | unchanged |
| `adf/pipelines/pl_*` | unchanged — already in real ADF JSON shape | unchanged |
| `adf/pipelines/.../Script[_mock.handler=...]` | runner intercepts and runs Python | replaced with a `DatabricksNotebook` activity that does the same work |
| `connectors/adf/` runner | this package | discarded — ADF runtime executes the JSON directly |

To go to production: drop the `_mock` blocks from the linkedServices,
fill in the real connection details, replace the `Script` activity in
`pl_ingest_sp_to_bronze.json` with a `DatabricksNotebook` activity that
writes `bronze.raw_workbooks`, and delete `connectors/adf/`. The
datasets and pipelines do not change.

## Pipelines

### `pl_ingest_sp_to_bronze`

```
GetMetadata(ds_sp_xlsx_input, childItems)
   → ForEach(@activity('GetMetadata').output.childItems)
        Copy(ds_sp_xlsx_input → ds_uc_volume_bronze)
        Script(register_workbook_manifest)
   → flush manifest → bronze.raw_workbooks  (Delta on Databricks; JSON locally)
```

Replaces `notebooks/01_bronze_sharepoint_ingest.py` and is invoked from it.

### `pl_export_gold_to_sp`

```
Lookup(gold.fact_observation distinct workbooks)
   → ForEach(workbooks)
        Copy(gold/silver delta query → ds_sp_xlsx_output)
            builds 3-sheet xlsx: observations, dq_issues, column_mapping_log
            uploads to SharePoint /output
```

Replaces `notebooks/04_export_sharepoint.py` and is invoked from it.

## Running

### From the CLI (local)

```bash
python scripts/seed_mock_sharepoint.py        # populate data/mock_sharepoint/input/
python -m connectors.adf.runner pl_ingest_sp_to_bronze
python scripts/run_local_demo.py              # generates data/gold_local/*.csv
python -m connectors.adf.runner pl_export_gold_to_sp
ls data/mock_sharepoint/output/
```

### From a Databricks notebook

```python
import os
os.environ["SHAREPOINT_MOCK_ROOT"] = f"/Volumes/{CATALOG}/bronze"
os.environ["SHAREPOINT_FOLDER_ALIAS_input"] = "sharepoint_input"

from connectors.adf import run_pipeline
run_pipeline("pl_ingest_sp_to_bronze",
             parameters={"catalog": CATALOG, "volume": "sharepoint_input"},
             spark=spark)
```

The two env vars rewire the SharePoint backend so the same JSON
artifacts work against UC Volumes:

* `SHAREPOINT_MOCK_ROOT` — overrides the linked-service `_mock.rootHint`
  (the parent dir under which folders live).
* `SHAREPOINT_FOLDER_ALIAS_<folder>` — rewrites a dataset's
  `folderPath`. e.g. `_input=sharepoint_input` makes the dataset's
  `"input"` resolve to the `sharepoint_input` volume.

On a fresh local checkout neither variable is set; SharePointMock falls
back to `data/mock_sharepoint/` and folder names are used verbatim.

## Package layout

```
connectors/adf/
├── __init__.py            re-exports run_pipeline
├── runner.py              entry point, DAG, manifest flush, CLI
├── activities.py          GetMetadata / ForEach / Copy / Lookup / Script
├── linked_services.py     SharePointBackend, DeltaBackend, JSON loaders
└── expressions.py         @item(), @activity(...).output.x, @pipeline().parameters.x
```

## Adding a new activity type

1. Write the handler in `activities.py` taking `(activity, ctx) -> dict`.
2. Register it in the `_HANDLERS` table at the bottom of that file.
3. Reference any new dataset / linkedService fields you need from the
   activity JSON via `ctx.expr_ctx()` and `evaluate(...)`.

Keep handlers small and side-effect-only at the backend boundary; if a
new activity needs to talk to a new external system, add a backend
class to `linked_services.py` and resolve it via `resolve_backend()`.
