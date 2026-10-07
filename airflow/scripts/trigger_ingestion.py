"""Start the Databricks ingestion pipeline (Neon -> walmart.bronze) and wait for it to finish."""
import os
import sys
import time

import yaml
from databricks.sdk import WorkspaceClient

PROFILES = "/opt/airflow/walmart_dbt/profiles.yml"
pipeline_id = os.environ["DATABRICKS_PIPELINE_ID"]

# Reuse the host/token already stored in dbt's profiles.yml
with open(PROFILES, encoding="utf-8") as f:
    profile = yaml.safe_load(f)["walmart_dbt"]["outputs"]["dev"]

w = WorkspaceClient(host="https://" + profile["host"], token=profile["token"])

update_id = w.pipelines.start_update(pipeline_id=pipeline_id).update_id
print(f"Started pipeline {pipeline_id}, update {update_id}")

while True:
    state = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id).update.state.value
    print(f"State: {state}")
    if state == "COMPLETED":
        break
    if state in ("FAILED", "CANCELED"):
        sys.exit(f"Pipeline update ended with state {state}")
    time.sleep(30)
