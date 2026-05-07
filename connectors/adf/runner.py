"""Top-level entry point for the mock ADF runner.

Loads JSON artifacts from ``adf/``, builds a RunContext, executes
top-level activities in topological order, and (for the ingest pipeline)
flushes the accumulated workbook manifest to Delta or a local JSON file.

CLI::

    python -m connectors.adf.runner pl_ingest_sp_to_bronze
    python -m connectors.adf.runner pl_export_gold_to_sp --param catalog=quality_de

Library::

    from connectors.adf import run_pipeline
    run_pipeline("pl_ingest_sp_to_bronze",
                 parameters={"catalog": "quality_de"},
                 spark=spark)
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from connectors.adf.activities import RunContext, _topo_sort, dispatch
from connectors.adf.linked_services import (
    DeltaBackend,
    load_datasets,
    load_linked_services,
    resolve_backend,
)


ADF_ROOT = REPO_ROOT / "adf"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_pipeline(
    pipeline_name: str,
    *,
    parameters: dict[str, Any] | None = None,
    spark: Any | None = None,
    adf_root: Path | None = None,
) -> dict[str, Any]:
    adf_root = adf_root or ADF_ROOT
    pipeline = _load_pipeline(adf_root, pipeline_name)

    # Merge defaults from the pipeline definition with caller overrides.
    declared_params = pipeline["properties"].get("parameters", {}) or {}
    resolved_params: dict[str, Any] = {
        k: v.get("defaultValue") for k, v in declared_params.items()
    }
    resolved_params.update(parameters or {})

    linked_services = load_linked_services(adf_root)
    datasets = load_datasets(adf_root)

    log: list[str] = []
    print(f"[adf] running pipeline {pipeline_name!r}")
    print(f"[adf]   parameters: {resolved_params}")
    print(f"[adf]   activities: "
          f"{[a['name'] for a in pipeline['properties']['activities']]}")

    ctx = RunContext(
        pipeline_parameters=resolved_params,
        activity_outputs={},
        linked_services=linked_services,
        datasets=datasets,
        spark=spark,
        log=log,
    )

    activities = _topo_sort(pipeline["properties"]["activities"])
    for activity in activities:
        print(f"[adf] → {activity['name']}  ({activity['type']})")
        dispatch(activity, ctx)

    # Flush the workbook manifest if the ingest pipeline accumulated one.
    manifest_rows = ctx.activity_outputs.get("_manifest_buffer") or []
    if manifest_rows:
        ls_db = next(
            ls for ls in linked_services.values()
            if ls["properties"]["type"] == "AzureDatabricksDeltaLake"
        )
        backend = resolve_backend(ls_db, spark=spark)
        assert isinstance(backend, DeltaBackend)
        target = backend.register_workbook_manifest(
            manifest_rows, catalog=resolved_params.get("catalog"),
        )
        print(f"[adf] flushed {len(manifest_rows)} manifest row(s) → {target}")

    summary = {
        "pipeline": pipeline_name,
        "activities": {
            name: out for name, out in ctx.activity_outputs.items()
            if not name.startswith("_")
        },
    }
    print(f"[adf] pipeline {pipeline_name!r} complete")
    return summary


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------

def _load_pipeline(adf_root: Path, name: str) -> dict[str, Any]:
    path = adf_root / "pipelines" / f"{name}.json"
    if not path.exists():
        raise FileNotFoundError(f"pipeline JSON not found: {path}")
    return json.loads(path.read_text())


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_kv(items: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in items or []:
        if "=" not in item:
            raise ValueError(f"--param expects key=value, got {item!r}")
        k, v = item.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run an ADF mock pipeline.")
    parser.add_argument("pipeline", help="Pipeline name (file under adf/pipelines/)")
    parser.add_argument("--param", action="append", default=[],
                        help="Override pipeline parameter, e.g. --param catalog=quality_de")
    args = parser.parse_args(argv)

    run_pipeline(args.pipeline, parameters=_parse_kv(args.param))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
