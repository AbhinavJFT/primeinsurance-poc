"""Dataclasses for the cleaning pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, time
from typing import Any, Optional


@dataclass
class HeaderBand:
    process_label_row: Optional[int]
    field_label_row: Optional[int]
    rt_row: Optional[int]
    rrt_row: Optional[int]
    unit_row: Optional[int]
    spec_bound_row: Optional[int]
    spec_value_row: Optional[int]
    internal_spec_row: Optional[int]
    data_start_row: int


@dataclass
class ImpurityProfile:
    raw_label: str
    column_index: int
    rt: Optional[float]
    rrt: Optional[float]
    unit: Optional[str]
    sub_unit: Optional[str]
    spec_bound: Optional[str]    # "Max" / "Min"
    spec_value: Optional[float]
    internal_spec_value: Optional[float]


@dataclass
class MetaColumn:
    raw_label: str
    column_index: int


@dataclass
class SheetProfile:
    sheet_name: str
    layout: str                                # "quality_standard" | "misc_flat" | "unknown"
    header: Optional[HeaderBand]
    meta_columns: list[MetaColumn] = field(default_factory=list)
    impurity_columns: list[ImpurityProfile] = field(default_factory=list)
    n_data_rows: int = 0
    notes: list[str] = field(default_factory=list)


@dataclass
class ColumnMapping:
    workbook: str
    sheet: str
    raw_label: str
    column_index: int
    role: str                       # "meta" | "analyte" | "ignored"
    canonical: Optional[str]        # canonical field name or analyte name
    confidence: float               # 0.0–1.0
    rationale: str
    source: str                     # "llm" | "mock_synonyms" | "default"


@dataclass
class DQIssue:
    workbook: str
    sheet: str
    row_seq: Optional[int]
    column: str
    rule: str           # e.g. "null_sentinel", "negative_value", "malformed_time"
    severity: str       # "repaired" | "unparseable" | "error"
    raw_value: Any
    repaired_value: Any
    note: str = ""


@dataclass
class Observation:
    workbook: str
    sheet: str
    row_seq: int
    sample_date: Optional[date]
    sample_time: Optional[time]
    report_time: Optional[time]
    batch_no: Optional[str]
    instrument_id: Optional[str]
    stage: Optional[str]
    sample_form: Optional[str]
    appearance: Optional[str]
    appearance_solution: Optional[str]
    analyte: str
    analyte_canonical: Optional[str]
    column_index: Optional[int]   # 1-based source column; disambiguates duplicate canonical names
    rt: Optional[float]
    rrt: Optional[float]
    value: Optional[float]
    unit: Optional[str]
    spec_min: Optional[float]
    spec_max: Optional[float]
    spec_internal_min: Optional[float]
    spec_internal_max: Optional[float]
    pass_: Optional[bool]
    raw_value: Optional[str]
    mapping_confidence: Optional[float]
