#!/usr/bin/env bash
set -euo pipefail

# Install dependencies
echo "Installing dependencies from requirements.txt..."
pip3 install -r requirements.txt

# Firehose: run Companies House bulk ingestion.
echo "Starting bulk data download..."
python3 Data-injestion-workflows/Bulk-request-workflow/bulk-main.py

# Enrichment: call Companies House API.
echo "Starting API enrichment..."
python3 Data-injestion-workflows/Api-request-workflow/api-main.py

# Collation: financial data extraction (guarded by config flag).
echo "Starting data collation (financials)..."
python3 Data-collation-process/financials_collation.py

# Final step: remove enriched_data.json after it has been merged into financials
echo "Cleaning up enriched_data.json..."
python3 - <<'PY'
from pathlib import Path
from ingestion_config.loader import load_config

root = Path.cwd()
config = load_config()
search_name = config.search_criteria.get("name", "default_search")
base_path = Path(config.technical_config.get("storage", {}).get("base_path", "project_data/default"))
if not base_path.is_absolute():
    base_path = root / base_path
base_path = base_path.with_name(base_path.name.format(name=search_name))
enriched_path = base_path / "json" / "enriched_data.json"
try:
    if enriched_path.exists():
        enriched_path.unlink()
        print(f"  - removed: {enriched_path}")
    else:
        print(f"  - nothing to remove at: {enriched_path}")
except Exception as exc:
    print(f"  ! failed to remove enriched_data.json: {exc}")
PY

echo "Workflow complete."
