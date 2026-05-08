#!/usr/bin/env bash
# One-shot Databricks deploy for the SharePoint Quality team POC.
#
#   ./deploy.sh                # validates, deploys, runs the medallion job
#   ./deploy.sh --no-run       # just validate + deploy, skip running the job
#   ./deploy.sh --destroy      # tear the bundle (and all its resources) down
#
# Env overrides:
#   DATABRICKS_PROFILE=newaccount    profile from ~/.databrickscfg
#   TARGET=dev                       bundle target (dev|prod)

set -euo pipefail

PROFILE="${DATABRICKS_PROFILE:-newaccount}"
TARGET="${TARGET:-dev}"

# Prefer the modern CLI; the legacy v0.18 CLI on $HOME/.local/bin doesn't speak DAB.
if [[ -x /usr/local/bin/databricks ]]; then
  DBX=/usr/local/bin/databricks
else
  DBX="$(command -v databricks || true)"
fi

if [[ -z "${DBX}" ]]; then
  echo "error: databricks CLI not found"
  exit 1
fi

# The Databricks CLI bundle commands download Terraform on first use, but
# the embedded download path can fail when Hashicorp's GPG signing key is
# rotated. If a local terraform is on PATH, point the CLI at it to skip
# the download entirely. (Install with: install_terraform_to_local_bin.)
if [[ -z "${DATABRICKS_TF_EXEC_PATH:-}" ]] && command -v terraform >/dev/null 2>&1; then
  export DATABRICKS_TF_EXEC_PATH="$(command -v terraform)"
  export DATABRICKS_TF_VERSION="$(terraform --version | head -1 | awk '{print $2}' | tr -d 'v')"
  export DATABRICKS_TF_CLI_CONFIG_FILE="${TMPDIR:-/tmp}/.dbx_tf_cli_config.tfrc"
  : > "$DATABRICKS_TF_CLI_CONFIG_FILE"
fi

cyan()  { printf "\033[1;36m%s\033[0m\n" "$1"; }
green() { printf "\033[1;32m%s\033[0m\n" "$1"; }
red()   { printf "\033[1;31m%s\033[0m\n" "$1"; }
step()  { echo; cyan "── $1 ─────────────────────────────────────────────"; }

DESTROY=0
RUN=1
for arg in "$@"; do
  case "$arg" in
    --no-run)  RUN=0 ;;
    --destroy) DESTROY=1 ;;
    -h|--help)
      sed -n '2,9p' "$0"
      exit 0
      ;;
    *) red "unknown arg: $arg"; exit 2 ;;
  esac
done

step "Preflight"
echo "CLI:     $($DBX --version)"
echo "Profile: $PROFILE"
echo "Target:  $TARGET"

CALLER_USER=$($DBX current-user me -p "$PROFILE" --output json \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('userName') or d['emails'][0]['value'])")
echo "User:    $CALLER_USER"

if [[ "$DESTROY" -eq 1 ]]; then
  step "Destroying bundle (jobs, pipelines, files)"
  "$DBX" bundle destroy --target "$TARGET" -p "$PROFILE" --auto-approve
  green "✓ destroyed"
  exit 0
fi

step "Vendor app deps"
if [[ -x apps/quality_team_intelligence/vendor.sh ]]; then
  apps/quality_team_intelligence/vendor.sh
fi

step "Validate bundle"
"$DBX" bundle validate --target "$TARGET" -p "$PROFILE"

step "Deploy bundle"
"$DBX" bundle deploy --target "$TARGET" -p "$PROFILE"

step "Resources after deploy"
"$DBX" bundle summary --target "$TARGET" -p "$PROFILE" 2>&1 \
  | grep -E '^\s+(Job|Pipeline|Dashboard|URL|Name)' || true

if [[ "$RUN" -eq 1 ]]; then
  step "Running quality_de_pipeline (setup → bronze → silver → gold → export)"
  "$DBX" bundle run quality_de_pipeline --target "$TARGET" -p "$PROFILE"
fi

step "Done"
HOST=$($DBX bundle summary --target "$TARGET" -p "$PROFILE" --output json 2>/dev/null \
       | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['workspace']['host'])" 2>/dev/null \
       || echo "")
green "✓ deployed to $HOST"
echo
echo "Next steps:"
echo "  • Open the workspace UI:  ${HOST}"
echo "  • Inspect Gold tables:    SELECT * FROM quality_de.gold.fact_observation LIMIT 100"
echo "  • Inspect cleaned files:  /Volumes/quality_de/bronze/sharepoint_output/"
echo "  • Tear it all down:       ./deploy.sh --destroy"
