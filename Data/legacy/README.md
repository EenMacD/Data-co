# Legacy Data Ingestion System

This directory contains the **old data ingestion process** that has been replaced by the new bulk ingestion system.

## Contents

### workflows/
Old data ingestion workflows:
- **Api-request-workflow/** - Old API-based enrichment process
- **Bulk-request-workflow/** - Old bulk download process

### collation/
Old financial data collation process:
- **financials_collation.py** - Legacy XBRL/iXBRL parser

### config/
Old configuration system:
- **loader.py** - Legacy YAML config loader

### scripts/
Old run scripts and utilities:
- **run-data.sh** - Old data ingestion runner
- **run-financials.sh** - Old financials runner
- **run.sh** - Old general runner
- **verify_*.py** - Old verification scripts
- **app.py.backup** - Backup of old Flask UI
- **preview_helper.py** - Old preview functionality
- **OLD_UI_README.md** - Documentation for old UI

## Status

**ARCHIVED** - This code is no longer actively used. Kept for reference purposes only.

The new system can be found in `../bulk_ingestion/`

## Migration Notes

The old process had these limitations that the new system addresses:
1. ❌ Required downloading entire bulk dataset then enriching via API (slow)
2. ❌ No UI for selecting specific monthly snapshots
3. ❌ Row-by-row database inserts (very slow)
4. ❌ No change detection or deduplication
5. ❌ No stop/resume capability
6. ❌ Complex YAML configuration

The new system:
1. ✅ UI-driven selection of specific monthly files
2. ✅ PostgreSQL COPY bulk inserts (100x faster)
3. ✅ Hash-based change detection and auto-update
4. ✅ Stop/resume with checkpoint saving
5. ✅ Simple, intuitive web UI
6. ✅ Real-time progress tracking via SSE
