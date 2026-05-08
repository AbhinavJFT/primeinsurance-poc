"""Map raw column labels to canonical schema names.

Two modes:
  - mock_synonyms: deterministic substring/synonym match (no API calls)
  - llm:          calls databricks-gpt-oss-20b (via OpenAI SDK) for fuzzy
                  mapping, with a Pydantic-validated structured response.

Both modes return ColumnMapping records with a confidence score and a
short rationale, so the demo's column_mapping_log table is populated
either way.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Iterable, Optional

import yaml

from .models import ColumnMapping, ImpurityProfile, MetaColumn, SheetProfile

# Import the OpenAI rate-limit class lazily — the library may not be installed
# in environments that only use the deterministic mock mapper. The fallback
# path treats anything that looks like a 429 the same way.
try:
    from openai import RateLimitError as _OpenAIRateLimitError  # type: ignore
except Exception:  # pragma: no cover
    class _OpenAIRateLimitError(Exception):
        pass


SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "canonical_quality_schema.yaml"


def load_schema(path: Path | str = SCHEMA_PATH) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Deterministic mock mapper (no external dependency)
# ---------------------------------------------------------------------------

def _normalize(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())


def _best_meta_match(raw_label: str, synonyms: dict[str, list[str]]
                     ) -> tuple[Optional[str], float, str]:
    """Return (canonical, confidence, rationale) for a meta column.

    Confidence rewards alias specificity (longer alias relative to label),
    so 'Appearance of 30% solution in methanol' beats the bare 'appearance'
    synonym when both could match.
    """
    norm = _normalize(raw_label)
    best: tuple[Optional[str], float, str] = (None, 0.0, "no synonym match")
    for canonical, aliases in synonyms.items():
        for alias in aliases:
            if alias == norm:
                return canonical, 0.95, f"exact synonym match: '{alias}'"
            if alias in norm or norm in alias:
                # Reward specificity: longer aliases that fill more of the label win.
                coverage = len(alias) / max(len(norm), 1)
                conf = 0.4 + min(0.5, coverage * 0.6)
                if conf > best[1]:
                    best = (canonical, conf,
                            f"substring match against '{alias}' "
                            f"(coverage={coverage:.2f})")
    return best


def _best_analyte_match(raw_label: str, analytes: dict[str, list[str]]
                        ) -> tuple[Optional[str], float, str]:
    norm = _normalize(raw_label).replace("-", " ").replace("_", " ")
    norm = re.sub(r"\s+", " ", norm)
    best: tuple[Optional[str], float, str] = (None, 0.0, "no analyte match")
    for canonical, aliases in analytes.items():
        for alias in aliases:
            alias_norm = _normalize(alias).replace("-", " ").replace("_", " ")
            alias_norm = re.sub(r"\s+", " ", alias_norm)
            if alias_norm == norm:
                return canonical, 0.95, f"exact analyte alias: '{alias}'"
            if alias_norm in norm or norm in alias_norm:
                conf = 0.7 if len(alias_norm) >= 5 else 0.55
                if conf > best[1]:
                    best = (canonical, conf,
                            f"substring match against analyte alias '{alias}'")
    return best


def map_columns_mock(profile: SheetProfile, schema: dict, *, workbook: str
                     ) -> list[ColumnMapping]:
    mappings: list[ColumnMapping] = []
    synonyms = schema.get("synonyms", {})
    analytes = schema.get("analytes", {})

    for meta in profile.meta_columns:
        canonical, conf, rationale = _best_meta_match(meta.raw_label, synonyms)
        # Column A "S.No" is meaningful but not in canonical schema -> ignored.
        role = "ignored" if canonical is None and meta.column_index == 1 else (
            "meta" if canonical else "ignored")
        mappings.append(ColumnMapping(
            workbook=workbook, sheet=profile.sheet_name,
            raw_label=meta.raw_label, column_index=meta.column_index,
            role=role, canonical=canonical, confidence=conf,
            rationale=rationale, source="mock_synonyms",
        ))

    for imp in profile.impurity_columns:
        canonical, conf, rationale = _best_analyte_match(imp.raw_label, analytes)
        if canonical is None:
            # fall back to a deterministic slug so each column still pivots
            slug = re.sub(r"[^A-Za-z0-9]+", "_", imp.raw_label.strip()).strip("_")
            canonical = f"unknown_{slug}"
            rationale = "no canonical analyte match — slug fallback"
            conf = 0.30
        mappings.append(ColumnMapping(
            workbook=workbook, sheet=profile.sheet_name,
            raw_label=imp.raw_label, column_index=imp.column_index,
            role="analyte", canonical=canonical, confidence=conf,
            rationale=rationale, source="mock_synonyms",
        ))

    return mappings


# ---------------------------------------------------------------------------
# LLM mapper (Databricks Foundation Model endpoint via OpenAI SDK)
# ---------------------------------------------------------------------------

def _build_llm_prompt(profile: SheetProfile, schema: dict) -> str:
    canonical_meta = list(schema.get("synonyms", {}).keys())
    canonical_analytes = list(schema.get("analytes", {}).keys())

    rows = []
    for meta in profile.meta_columns:
        rows.append({"raw": meta.raw_label, "kind": "meta",
                     "col": meta.column_index})
    for imp in profile.impurity_columns:
        rows.append({"raw": imp.raw_label, "kind": "analyte",
                     "col": imp.column_index, "rt": imp.rt, "rrt": imp.rrt,
                     "spec_bound": imp.spec_bound, "spec": imp.spec_value})

    return (
        "You are a data engineer cleaning pharmaceutical Quality team Excel "
        "headers. Map each raw column label to a canonical name.\n\n"
        f"Sheet name: {profile.sheet_name}\n"
        f"Layout: {profile.layout}\n\n"
        f"Allowed canonical META names (kind=meta): {canonical_meta}\n"
        f"Allowed canonical ANALYTE names (kind=analyte): {canonical_analytes}\n"
        "If no canonical name fits, return canonical=null and role='ignored'.\n\n"
        "Return STRICT JSON: a list of objects with keys "
        "{raw, col, role, canonical, confidence, rationale}. "
        "confidence is a float in [0,1].\n\n"
        f"Columns to map:\n{json.dumps(rows, indent=2)}\n"
    )


def map_columns_llm(profile: SheetProfile, schema: dict, *, workbook: str,
                    client, model: str = "databricks-gpt-oss-20b"
                    ) -> list[ColumnMapping]:
    prompt = _build_llm_prompt(profile, schema)
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system",
             "content": "You return ONLY valid JSON arrays."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.0,
        response_format={"type": "json_object"},
    )
    raw = response.choices[0].message.content
    try:
        data = json.loads(raw)
        items = data["columns"] if isinstance(data, dict) and "columns" in data else data
    except (json.JSONDecodeError, KeyError, TypeError):
        # Fall back gracefully if the model misbehaves.
        return map_columns_mock(profile, schema, workbook=workbook)

    mappings: list[ColumnMapping] = []
    for item in items:
        try:
            mappings.append(ColumnMapping(
                workbook=workbook, sheet=profile.sheet_name,
                raw_label=item["raw"], column_index=int(item["col"]),
                role=item.get("role") or "ignored",
                canonical=item.get("canonical"),
                confidence=float(item.get("confidence", 0.5)),
                rationale=item.get("rationale", ""),
                source="llm",
            ))
        except (KeyError, ValueError, TypeError):
            continue
    return mappings


# ---------------------------------------------------------------------------
# Top-level dispatcher
# ---------------------------------------------------------------------------

def map_columns(profile: SheetProfile, schema: dict, *, workbook: str,
                llm_client=None) -> list[ColumnMapping]:
    """Dispatch to LLM mapper if a client is provided AND MOCK_LLM is not set.

    Falls back to the deterministic synonym matcher when the Foundation Model
    endpoint rate-limits us (HTTP 429) or any other transport error occurs.
    The fallback is per-sheet, so a session of N workbooks can finish even
    if some sheets exceed the workspace pay-per-token throughput cap.
    """
    if llm_client is not None and os.environ.get("MOCK_LLM", "").lower() not in ("1", "true", "yes"):
        try:
            return map_columns_llm(profile, schema, workbook=workbook, client=llm_client)
        except _OpenAIRateLimitError as e:
            print(f"  [mapping] FM rate-limit (429) on sheet "
                  f"{profile.sheet_name!r}; falling back to mock_synonyms: {e}")
            return map_columns_mock(profile, schema, workbook=workbook)
        except Exception as e:
            # Any other LLM-side failure (network blip, bad token, server
            # 5xx) → fall back so the run completes. The mapping_log will
            # show source=mock_synonyms for these sheets.
            print(f"  [mapping] LLM call failed on sheet "
                  f"{profile.sheet_name!r}; falling back to mock_synonyms: {e}")
            return map_columns_mock(profile, schema, workbook=workbook)
    return map_columns_mock(profile, schema, workbook=workbook)
