"""Resolve ADF linkedService JSONs to concrete execution backends.

Two backends in this POC:

* ``SharePointMock`` — wraps connectors.sharepoint_mock.SharePointMock against
  data/mock_sharepoint/.  Maps to LS type ``SharePointOnlineList``.

* ``DeltaBackend``  — writes either to UC Delta tables (when a Spark session
  is provided) or to local files under ``data/bronze_landing/`` (manifest)
  and ``data/gold_local/`` (CSV companions).  Maps to LS type
  ``AzureDatabricksDeltaLake``.

To swap to real production, replace the ``SharePointMock`` branch with a
Microsoft Graph SDK client and the local-file branch in ``DeltaBackend``
with workspace SDK calls.  The runner / activities / pipeline JSONs do
not change.
"""

from __future__ import annotations

import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from connectors.sharepoint_mock import SharePointMock  # noqa: E402


# ---------------------------------------------------------------------------
# SharePoint backend
# ---------------------------------------------------------------------------

class SharePointBackend:
    """Thin wrapper around SharePointMock with the surface the runner uses.

    Two env vars let one set of dataset JSONs work both locally and on
    Databricks without editing the artifacts:

    * ``SHAREPOINT_MOCK_ROOT`` overrides the LS ``rootHint`` (e.g. set this
      to ``/Volumes/{catalog}/bronze`` on Databricks).
    * ``SHAREPOINT_FOLDER_ALIAS_<folder>`` rewrites a dataset's folderPath
      at execution time. e.g. setting
      ``SHAREPOINT_FOLDER_ALIAS_input=sharepoint_input`` makes ``"input"``
      in the dataset resolve to the ``sharepoint_input`` volume.
    """

    def __init__(self, ls_json: dict[str, Any]):
        mock = ls_json["properties"].get("_mock") or {}
        site = mock.get("site", "QualityTeam")

        env_root = os.environ.get("SHAREPOINT_MOCK_ROOT")
        if env_root:
            root: Path | None = Path(env_root)
        else:
            root_hint = mock.get("rootHint")
            root = (REPO_ROOT / root_hint) if root_hint else None

        self.client = SharePointMock(root=root, site=site)

    @staticmethod
    def _resolve_folder(folder: str) -> str:
        alias = os.environ.get(f"SHAREPOINT_FOLDER_ALIAS_{folder}")
        return alias or folder

    def list_files(self, folder: str, suffix: str | None = None) -> list[dict[str, Any]]:
        folder = self._resolve_folder(folder)
        return [
            {"name": f.name, "type": "File", "size": f.size_bytes, "path": f.path}
            for f in self.client.list_files(folder, suffix=suffix)
        ]

    def read_bytes(self, folder: str, name: str) -> bytes:
        folder = self._resolve_folder(folder)
        path = Path(self.client.ensure_folder(folder)) / name
        return path.read_bytes()

    def write_bytes(self, folder: str, name: str, payload: bytes) -> dict[str, Any]:
        folder = self._resolve_folder(folder)
        f = self.client.write_bytes(folder, name, payload)
        return {"name": f.name, "path": f.path, "size": f.size_bytes}

    def upload_file(self, folder: str, src: Path, name: str | None = None) -> dict[str, Any]:
        folder = self._resolve_folder(folder)
        f = self.client.upload_file(folder, src, name=name)
        return {"name": f.name, "path": f.path, "size": f.size_bytes}


# ---------------------------------------------------------------------------
# Databricks Delta backend (with local-mode fallback)
# ---------------------------------------------------------------------------

class DeltaBackend:
    """Writes copied files to a UC Volume + manifest rows to Delta.

    On Databricks: receives a SparkSession and writes to real Delta tables.
    Locally: writes a JSON manifest under data/bronze_landing/ and reads
    CSVs from data/gold_local/ for the export pipeline.
    """

    def __init__(self, ls_json: dict[str, Any], spark: Any | None = None):
        mock = ls_json["properties"].get("_mock") or {}
        self.catalog = mock.get("catalog", "quality_de")
        self.volume_root = mock.get("volumeRoot", "/Volumes/quality_de/bronze")
        self.local_root = REPO_ROOT / mock.get("localRoot", "data/bronze_landing")
        self.local_csv_dir = REPO_ROOT / "data" / "gold_local"
        self.spark = spark

    # --- file movement -----------------------------------------------------

    def is_databricks(self) -> bool:
        return self.spark is not None

    def landing_path(self, volume: str, file_name: str) -> Path:
        if self.is_databricks():
            target = Path(self.volume_root) / volume / file_name
        else:
            target = self.local_root / volume / file_name
        target.parent.mkdir(parents=True, exist_ok=True)
        return target

    def write_file(self, volume: str, file_name: str, payload: bytes) -> Path:
        target = self.landing_path(volume, file_name)
        target.write_bytes(payload)
        return target

    # --- manifest ----------------------------------------------------------

    def register_workbook_manifest(
        self, rows: list[dict[str, Any]], catalog: str | None = None,
    ) -> str:
        """Register one row per ingested workbook in bronze.raw_workbooks.

        Append mode + partitioned by session_id so multiple app sessions can
        coexist in the same table. Each row carries a ``session_id`` value
        threaded through from the pipeline parameters (added in
        ``_register_workbook_manifest``)."""
        catalog = catalog or self.catalog
        if self.is_databricks():
            df = self.spark.createDataFrame(rows)
            (df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .partitionBy("session_id")
                .saveAsTable(f"{catalog}.bronze.raw_workbooks"))
            return f"{catalog}.bronze.raw_workbooks"

        # Local: drop a JSON manifest alongside the landed files.
        manifest_path = self.local_root / "raw_workbooks_manifest.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        serialized = [
            {**r, "ingest_ts": r["ingest_ts"].isoformat() if isinstance(r.get("ingest_ts"), datetime) else r.get("ingest_ts")}
            for r in rows
        ]
        manifest_path.write_text(json.dumps(serialized, indent=2))
        return str(manifest_path)

    # --- export-side reads -------------------------------------------------

    def lookup_distinct_workbooks(self, table: str) -> list[dict[str, Any]]:
        """Return [{'workbook': 'X.xlsx'}, ...] from the gold fact table."""
        if self.is_databricks():
            rows = (
                self.spark.table(f"{self.catalog}.{table}")
                .select("workbook")
                .distinct()
                .orderBy("workbook")
                .collect()
            )
            return [{"workbook": r.workbook} for r in rows]

        csv_path = self._gold_csv_for(table)
        seen: list[str] = []
        with csv_path.open() as f:
            for row in csv.DictReader(f):
                if row["workbook"] and row["workbook"] not in seen:
                    seen.append(row["workbook"])
        return [{"workbook": w} for w in sorted(seen)]

    def fetch_for_workbook(self, table: str, workbook: str) -> tuple[list[str], list[list[Any]]]:
        """Return (headers, rows) for ``table`` filtered to ``workbook``.

        ``table`` is in the form 'gold.fact_observation' / 'silver.dq_issues'.
        """
        if self.is_databricks():
            df = (
                self.spark.table(f"{self.catalog}.{table}")
                .where(f"workbook = '{workbook}'")
            )
            if "row_seq" in df.columns:
                df = df.orderBy("sheet" if "sheet" in df.columns else "row_seq", "row_seq")
            pdf = df.toPandas()
            headers = list(pdf.columns)
            rows = [list(r) for r in pdf.itertuples(index=False, name=None)]
            return headers, rows

        csv_path = self._gold_csv_for(table)
        with csv_path.open() as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []
            rows = [
                [row.get(h) for h in headers]
                for row in reader
                if row.get("workbook") == workbook
            ]
        return headers, rows

    def _gold_csv_for(self, table: str) -> Path:
        # 'gold.fact_observation' → data/gold_local/fact_observation.csv
        suffix = table.split(".", 1)[-1]
        return self.local_csv_dir / f"{suffix}.csv"


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

def load_linked_services(adf_root: Path) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for path in sorted((adf_root / "linkedServices").glob("*.json")):
        ls = json.loads(path.read_text())
        out[ls["name"]] = ls
    return out


def load_datasets(adf_root: Path) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for path in sorted((adf_root / "datasets").glob("*.json")):
        ds = json.loads(path.read_text())
        out[ds["name"]] = ds
    return out


def resolve_backend(
    ls_json: dict[str, Any],
    *,
    spark: Any | None = None,
) -> SharePointBackend | DeltaBackend:
    ls_type = ls_json["properties"]["type"]
    if ls_type == "SharePointOnlineList":
        return SharePointBackend(ls_json)
    if ls_type == "AzureDatabricksDeltaLake":
        return DeltaBackend(ls_json, spark=spark)
    raise ValueError(f"unsupported linkedService type: {ls_type!r}")


def now_utc() -> datetime:
    return datetime.now(timezone.utc)
