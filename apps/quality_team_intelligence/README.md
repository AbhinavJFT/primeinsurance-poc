# Quality Team Intelligence — Streamlit demo app

Single-page dashboard that tells the SharePoint Quality team cleaning story
end-to-end. Built to be the focal point of a 5–7 minute demo.

## What it shows

| Tab | Audience question it answers |
|---|---|
| **📥 Input — the mess** | What does the Quality team actually upload? |
| **🧹 Cleaning — every fix** | What did the pipeline change, and why? |
| **🤖 AI Mapping** | How did the LLM decide column meanings? |
| **📤 Output — round-trip** | Where did the cleaned files land, and can I download them? |
| **📊 Analytics** | What's the current spec compliance? |

A header strip shows live KPI tiles (files / observations / DQ fixes /
violations / pass-rate) and the last pipeline run state with a one-click
re-run button.

## Architecture

```
Streamlit app  ─►  databricks-sql-connector ─►  SQL Warehouse
            └──►  WorkspaceClient.jobs.run_now()
            └──►  /Volumes/quality_de/bronze/{sharepoint_input,sharepoint_output}/
                  (read directly via openpyxl)
```

Authentication is OAuth via the Databricks App's executing identity — same
pattern as `apps/primeins_intelligence/`.

## Local dev

You can run it on your laptop against the workspace if you have a personal
access token configured:

```bash
cd apps/quality_team_intelligence
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Tell the SDK how to authenticate (any one of these works):
export DATABRICKS_CONFIG_PROFILE=newaccount   # uses ~/.databrickscfg
# or:
export DATABRICKS_HOST=https://dbc-43bf0133-31e2.cloud.databricks.com
export DATABRICKS_TOKEN=...

streamlit run app.py
```

Note: file previews require local access to the UC Volume paths, which
only works inside Databricks. Locally the file-listing tabs will show empty
state; the SQL-driven tabs (Cleaning / AI Mapping / Analytics) work fine.

## Deploy as a Databricks App

```bash
# Sync the app code into the workspace (uses the bundle's deploy path)
databricks bundle deploy --target dev -p newaccount

# Create / update the app
databricks apps deploy quality-team-intelligence \
  --source-code-path "/Workspace/Users/$(whoami)/.bundle/quality_de/dev/files/apps/quality_team_intelligence" \
  -p newaccount

# Open it
databricks apps get quality-team-intelligence -p newaccount --output json | jq -r '.url'
```

The first deploy provisions the app's compute (a small autoscaling cluster
managed by Databricks) and installs `requirements.txt`. Cold start is
~30–60 seconds; warm reload of cached queries is instant.

## Configuration

Override via env vars in `app.yaml`:

| Variable | Default | What it controls |
|---|---|---|
| `QDE_CATALOG` | `quality_de` | Which catalog to query |
| `QDE_VOLUME_INPUT` | `sharepoint_input` | Volume name under bronze |
| `QDE_VOLUME_OUTPUT` | `sharepoint_output` | Volume name under bronze |
| `QDE_JOB_NAME_HINT` | `quality_de` | Substring used to find the pipeline job |
| `DATABRICKS_WAREHOUSE_ID` | `2de6a251cf2870eb` | SQL warehouse for queries |

If the app's identity (a service principal in production, or the deploying
user's identity in dev) lacks `CAN_USE` on the warehouse or `SELECT` on the
catalog, the top-bar query fails fast with a clear error message.
