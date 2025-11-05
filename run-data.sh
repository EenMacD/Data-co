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

echo "Workflow complete."
