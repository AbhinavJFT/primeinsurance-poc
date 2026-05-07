"""Generate synthetic Quality team workbooks and drop them in mock SharePoint /input.

Mirrors dpdp/scripts/seed_salesforce_data.py: a one-shot CLI that bridges
the synthetic generator and the destination system. In dpdp the destination
is Unity Catalog; here the destination is the mock SharePoint folder.

Usage:
    python scripts/seed_mock_sharepoint.py
    python scripts/seed_mock_sharepoint.py --clean   # wipe input folder first
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from connectors.sharepoint_mock import SharePointMock  # noqa: E402
from generate_quality_data import DEFAULT_SEED, generate, workbook_specs  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--site", default="QualityTeam")
    parser.add_argument("--input-folder", default="input")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--workbook", action="append",
                        choices=list(workbook_specs()))
    parser.add_argument("--clean", action="store_true",
                        help="Remove existing files from the input folder first")
    args = parser.parse_args()

    sp = SharePointMock(site=args.site)
    input_path = Path(sp.ensure_folder(args.input_folder))

    if args.clean:
        for p in input_path.glob("*.xlsx"):
            p.unlink()
            print(f"removed {p.name}")

    staging = REPO_ROOT / ".cache" / "generated"
    staging.mkdir(parents=True, exist_ok=True)
    written = generate(staging, seed=args.seed, only=args.workbook)

    print(f"\n[seed] uploading {len(written)} workbook(s) to "
          f"SharePoint mock site={args.site!r} folder={args.input_folder!r}\n")
    for src in written:
        uploaded = sp.upload_file(args.input_folder, src)
        print(f"  uploaded {uploaded.name:<48} ({uploaded.size_bytes:,} bytes) "
              f"-> {uploaded.path}")

    print(f"\n[seed] done. Inspect files at {input_path}")


if __name__ == "__main__":
    main()
