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
# Deterministic mock mapper — rapidfuzz-based fuzzy matcher
# ---------------------------------------------------------------------------
#
# Strategy: build a flat (alias_normalized → canonical) lookup once per
# sheet, then use rapidfuzz.process.extractOne with WRatio (a weighted
# combination of token-set / token-sort / partial / full ratios). Catches
# typos, abbreviation drift, and word reordering that the previous
# substring matcher missed.
#
# Falls back gracefully if rapidfuzz isn't installed (keeps the library
# importable in environments without the dep).

try:
    from rapidfuzz import fuzz, process
    _RAPIDFUZZ_AVAILABLE = True
except Exception:
    _RAPIDFUZZ_AVAILABLE = False

# Score thresholds (rapidfuzz returns 0–100). Calibrated for pharma column
# names; higher = stricter. Final ColumnMapping.confidence is rescaled to 0–1.
META_ACCEPT_THRESHOLD = 60
ANALYTE_ACCEPT_THRESHOLD = 65
EXACT_MATCH_SCORE = 100


def _normalize(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())


def _normalize_analyte(s: str) -> str:
    """Same as _normalize, plus collapse hyphens/underscores → spaces so
    'Imp-A' and 'imp_a' compare cleanly to 'imp a'."""
    return re.sub(r"\s+", " ", s.replace("-", " ").replace("_", " ").strip().lower())


def _build_alias_index(
    aliases_by_canonical: dict[str, list[str]],
    *,
    analyte_mode: bool = False,
) -> tuple[list[str], dict[str, str]]:
    """Flatten {canonical: [alias, ...]} into a (choices_list, alias→canonical)
    lookup, normalized identically to the query side."""
    norm_fn = _normalize_analyte if analyte_mode else _normalize
    choices: list[str] = []
    alias_to_canonical: dict[str, str] = {}
    for canonical, aliases in aliases_by_canonical.items():
        # Include the canonical name itself as an alias so 'batch_no' matches
        # 'batch no' even when the YAML doesn't list it.
        for alias in [canonical] + list(aliases):
            norm = norm_fn(alias)
            if norm and norm not in alias_to_canonical:
                choices.append(norm)
                alias_to_canonical[norm] = canonical
    return choices, alias_to_canonical


def _best_match_fuzz(
    raw_label: str,
    aliases_by_canonical: dict[str, list[str]],
    *,
    analyte_mode: bool,
    accept_threshold: int,
) -> tuple[Optional[str], float, str]:
    """Use rapidfuzz.WRatio over a flat alias index. Returns
    (canonical, confidence_0_1, rationale)."""
    norm_fn = _normalize_analyte if analyte_mode else _normalize
    norm = norm_fn(raw_label)
    if not norm:
        return None, 0.0, "empty raw label"

    choices, alias_to_canonical = _build_alias_index(
        aliases_by_canonical, analyte_mode=analyte_mode,
    )
    if not choices:
        return None, 0.0, "no aliases configured"

    if not _RAPIDFUZZ_AVAILABLE:
        # Substring fallback when rapidfuzz isn't installed
        for alias in choices:
            if alias == norm:
                return alias_to_canonical[alias], 0.95, f"exact match: '{alias}'"
            if alias in norm or norm in alias:
                cov = len(alias) / max(len(norm), 1)
                return (alias_to_canonical[alias],
                        0.4 + min(0.5, cov * 0.6),
                        f"substring (rapidfuzz unavailable) against '{alias}'")
        return None, 0.0, "no match (rapidfuzz unavailable)"

    # WRatio: weighted blend of partial/token-sort/token-set ratios.
    # Returns the best (choice, score, index).
    best = process.extractOne(norm, choices, scorer=fuzz.WRatio)
    if best is None:
        return None, 0.0, "no match"
    matched_alias, score, _ = best
    if score < accept_threshold:
        return None, score / 100.0, (
            f"best match '{matched_alias}' scored {score:.1f}, "
            f"below threshold {accept_threshold}"
        )

    canonical = alias_to_canonical[matched_alias]
    if score >= EXACT_MATCH_SCORE:
        rationale = f"exact match against '{matched_alias}'"
    else:
        rationale = (
            f"fuzzy match (rapidfuzz WRatio={score:.1f}) against '{matched_alias}'"
        )
    # Rescale 0–100 → 0–1
    return canonical, score / 100.0, rationale


def _best_meta_match(raw_label: str, synonyms: dict[str, list[str]]
                     ) -> tuple[Optional[str], float, str]:
    """Map a meta column's raw label to a canonical schema name."""
    return _best_match_fuzz(
        raw_label, synonyms,
        analyte_mode=False,
        accept_threshold=META_ACCEPT_THRESHOLD,
    )


def _best_analyte_match(raw_label: str, analytes: dict[str, list[str]]
                        ) -> tuple[Optional[str], float, str]:
    """Map an impurity/analyte column's raw label to a canonical analyte."""
    return _best_match_fuzz(
        raw_label, analytes,
        analyte_mode=True,
        accept_threshold=ANALYTE_ACCEPT_THRESHOLD,
    )


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
