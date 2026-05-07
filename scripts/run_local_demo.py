"""Run the full Quality team pipeline locally (no Databricks required).

Flow:
  1. List Excel files in mock SharePoint /input.
  2. For each: download -> profile + clean -> write tidy .xlsx -> upload to /output.
  3. Aggregate observations, dq_issues, column_mapping_log across all workbooks
     into CSVs under data/gold_local/  (stand-in for Gold Delta tables).
  4. Print a one-screen summary that's easy to demo.

Toggle the LLM mapper by setting DATABRICKS_HOST + DATABRICKS_TOKEN AND
omitting MOCK_LLM. When MOCK_LLM=true (default behaviour without those
secrets), the deterministic synonym mapper runs instead.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import tempfile
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from connectors.sharepoint_mock import SharePointMock  # noqa: E402
from quality_core import process_workbook, write_tidy_workbook  # noqa: E402
from quality_core.pipeline import (  # noqa: E402
    _DQ_HEADERS, _MAP_HEADERS, _OBS_HEADERS,
)


def _build_llm_client():
    if os.environ.get("MOCK_LLM", "").lower() in ("1", "true", "yes"):
        return None
    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    if not host or not token:
        return None
    try:
        from openai import OpenAI
    except ImportError:
        return None
    return OpenAI(
        base_url=f"{host.rstrip('/')}/serving-endpoints",
        api_key=token,
    )


def _serialize(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _write_csv(path: Path, headers: list[str], rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(_serialize(v) for v in row)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--site", default="QualityTeam")
    parser.add_argument("--input-folder", default="input")
    parser.add_argument("--output-folder", default="output")
    parser.add_argument("--gold-dir", default="data/gold_local")
    args = parser.parse_args()

    sp = SharePointMock(site=args.site)
    files = sp.list_files(args.input_folder, suffix=".xlsx")
    if not files:
        print(f"[demo] no .xlsx files in /{args.input_folder}; "
              "run scripts/seed_mock_sharepoint.py first.")
        return

    llm_client = _build_llm_client()
    llm_mode = "llm (databricks-gpt-oss-20b)" if llm_client else "mock_synonyms"
    print(f"[demo] mapper mode: {llm_mode}")
    print(f"[demo] processing {len(files)} workbook(s) from "
          f"site={args.site!r} folder={args.input_folder!r}\n")

    all_obs: list[list] = []
    all_dq: list[list] = []
    all_map: list[list] = []
    all_misc: list[dict] = []
    per_file_summary: list[tuple[str, int, int, int]] = []

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        for f in files:
            local = tmp_dir / f.name
            sp.download_file(args.input_folder, f.name, local)

            result = process_workbook(local, llm_client=llm_client)
            tidy_name = f.name.replace(".xlsx", "_CLEAN.xlsx")
            tidy_path = tmp_dir / tidy_name
            write_tidy_workbook(result, tidy_path)
            sp.upload_file(args.output_folder, tidy_path, name=tidy_name)

            for o in result.observations:
                all_obs.append([
                    o.workbook, o.sheet, o.row_seq, o.sample_date, o.sample_time,
                    o.report_time, o.batch_no, o.instrument_id, o.stage,
                    o.sample_form, o.appearance, o.appearance_solution,
                    o.analyte, o.analyte_canonical, o.rt, o.rrt, o.value, o.unit,
                    o.spec_min, o.spec_max, o.spec_internal_min, o.spec_internal_max,
                    o.pass_, o.raw_value, o.mapping_confidence,
                ])
            for d in result.dq_issues:
                all_dq.append([d.workbook, d.sheet, d.row_seq, d.column, d.rule,
                               d.severity, d.raw_value, d.repaired_value, d.note])
            for m in result.mappings:
                all_map.append([m.workbook, m.sheet, m.column_index, m.raw_label,
                                m.role, m.canonical, m.confidence, m.rationale, m.source])
            all_misc.extend(result.misc_rows)

            print(f"  [{f.name}] {len(result.observations):>4} obs "
                  f"| {len(result.dq_issues):>3} dq "
                  f"| {len(result.mappings):>3} mappings "
                  f"-> {tidy_name}")
            per_file_summary.append((f.name, len(result.observations),
                                     len(result.dq_issues), len(result.mappings)))

    # Write Gold-style CSVs
    gold_dir = REPO_ROOT / args.gold_dir
    _write_csv(gold_dir / "fact_observation.csv", _OBS_HEADERS, all_obs)
    _write_csv(gold_dir / "dq_issues.csv", _DQ_HEADERS, all_dq)
    _write_csv(gold_dir / "column_mapping_log.csv", _MAP_HEADERS, all_map)
    if all_misc:
        misc_headers = sorted({k for row in all_misc for k in row.keys()})
        for lead in ["row_seq", "sheet", "workbook"]:
            if lead in misc_headers:
                misc_headers.remove(lead)
                misc_headers.insert(0, lead)
        _write_csv(gold_dir / "misc_log.csv", misc_headers,
                   [[row.get(h) for h in misc_headers] for row in all_misc])

    # ----- Summary -----
    print()
    print("=" * 70)
    print(" DEMO SUMMARY")
    print("=" * 70)
    print(f"  Total observations:   {len(all_obs):>5}")
    print(f"  Total DQ issues:      {len(all_dq):>5}")
    print(f"  Total column mappings: {len(all_map):>4}")

    rule_counts = Counter(row[4] for row in all_dq)
    print("\n  DQ issue breakdown:")
    for rule, count in rule_counts.most_common():
        print(f"    {rule:<22} {count:>4}")

    pass_count = sum(1 for r in all_obs if r[22] is True)
    fail_count = sum(1 for r in all_obs if r[22] is False)
    null_count = sum(1 for r in all_obs if r[22] is None)
    total = len(all_obs) or 1
    print("\n  Specification compliance (observations):")
    print(f"    pass:  {pass_count:>4}  ({pass_count*100/total:.1f}%)")
    print(f"    fail:  {fail_count:>4}  ({fail_count*100/total:.1f}%)")
    print(f"    n/a:   {null_count:>4}  ({null_count*100/total:.1f}%)")

    low_conf = [r for r in all_map if r[6] is not None and r[6] < 0.5
                and r[4] != "ignored"]
    print(f"\n  Column mappings flagged for review (confidence < 0.5): "
          f"{len(low_conf)}")
    for r in low_conf[:5]:
        print(f"    [{r[1][:18]:<18}] raw={r[3][:24]!r:<26} -> {r[5]} "
              f"(conf={r[6]:.2f})")
    if len(low_conf) > 5:
        print(f"    ... +{len(low_conf) - 5} more")

    out_path = sp.ensure_folder(args.output_folder)
    print(f"\n  Cleaned workbooks landed in: {out_path}")
    print(f"  Gold CSVs landed in:         {gold_dir}")
    print()


if __name__ == "__main__":
    main()
