"""Self-contained Azure Data Factory mock for SharePoint ↔ Databricks.

Reads the JSON artifacts under ``adf/`` (linkedServices, datasets, pipelines)
and executes them against local SharePoint + Delta backends. Same pattern
as the dpdp Lakeflow Connect simulation: production-shaped artifacts,
mock execution backend, single-class swap to go real.

Public surface::

    from connectors.adf import run_pipeline

    run_pipeline("pl_ingest_sp_to_bronze",
                 parameters={"catalog": "quality_de"},
                 spark=spark)        # spark optional — local mode without it
"""

from .runner import run_pipeline

__all__ = ["run_pipeline"]
