#!/usr/bin/env bash
# Mirror runtime dependencies into the app dir.
#
# Databricks Apps copies only the source-code-path into the running
# container — it doesn't pull from the bundle root. Without this script
# the app fails with `ModuleNotFoundError: No module named 'quality_core'`.
#
# Run before every `./deploy.sh` invocation. The targets are gitignored
# so we don't commit duplicates.

set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"

rm -rf "$HERE/quality_core" "$HERE/schemas" "$HERE/generate_quality_data.py"
cp -r "$REPO/quality_core" "$HERE/quality_core"
cp -r "$REPO/schemas"      "$HERE/schemas"
cp    "$REPO/generate_quality_data.py" "$HERE/generate_quality_data.py"

# Strip __pycache__ if any
find "$HERE/quality_core" -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

echo "✓ vendored into apps/quality_team_intelligence/:"
echo "    quality_core/ ($(find "$HERE/quality_core" -name '*.py' | wc -l | tr -d ' ') python files)"
echo "    schemas/      ($(find "$HERE/schemas" -type f | wc -l | tr -d ' ') files)"
echo "    generate_quality_data.py"
