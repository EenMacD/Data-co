# Bulk Data Ingestion System

Modern UI-driven system for ingesting Companies House monthly bulk data snapshots.

## Architecture

```
bulk_ingestion/
├── parsers/          # Data parsers for each product type
├── services/         # Core business logic services
├── web/              # Flask UI application
└── requirements.txt  # Python dependencies
```

## Components

### parsers/
Parsers for Companies House bulk data products:
- **base_parser.py** - Abstract base class with chunked ZIP/CSV reading
- **company_parser.py** - CSV parser for BasicCompanyData files
- **psc_parser.py** - JSON parser for PSC (People with Significant Control) data
- **accounts_parser.py** - XBRL/iXBRL parser for financial accounts data

All parsers use pandas with 100k row chunks for memory efficiency.

### services/
Core services:
- **file_discovery.py** - Scrapes Companies House website for available monthly files
- **download_manager.py** - Concurrent file downloads with progress tracking
- **bulk_loader.py** - PostgreSQL COPY-based bulk insert with UPSERT and change detection
- **ingestion_worker.py** - Background processing thread with stop/resume capability

### web/
Flask web UI:
- **app.py** - Main Flask application with API endpoints
- **templates/index.html** - Single-page dashboard UI
- **requirements.txt** - Python package dependencies

## Key Features

✅ **UI-Driven Selection** - Select specific monthly snapshot files via web interface  
✅ **Monthly Snapshots Only** - PSC and Accounts show first-of-month files only  
✅ **Batch Processing** - Add multiple files to list, process all at once  
✅ **Background Processing** - Non-blocking with real-time progress updates  
✅ **Stop/Resume** - Pause after current file, resume from checkpoint  
✅ **Change Detection** - Hash-based detection, auto-update changed records  
✅ **Bulk Loading** - PostgreSQL COPY for 100x faster inserts  
✅ **Auto-Cleanup** - Deletes downloaded files after successful ingestion  
✅ **Real-time Logs** - SSE (Server-Sent Events) for live log streaming  

## Data Flow

1. **Discovery** - Scrape Companies House website for available files
2. **Selection** - User selects files from UI panels (Company/PSC/Accounts)
3. **Download** - Concurrent download with progress tracking
4. **Parse** - Chunked parsing (ZIP → CSV/JSON → pandas DataFrame)
5. **Load** - PostgreSQL COPY to temp table → UPSERT with change detection
6. **Cleanup** - Delete downloaded file
7. **Repeat** - Process next file in list

## Database Schema

### One-Record-Per-Company Model
Each company has exactly ONE record in `staging_companies`. Updates overwrite with new data:
- `UNIQUE(company_number)` - One record per company
- `data_hash` - MD5 hash for change detection
- `change_detected` - Flag indicating if data changed during last update
- `last_updated` - Timestamp of last update

### Change Detection
```sql
ON CONFLICT (company_number) DO UPDATE SET
    company_name = EXCLUDED.company_name,
    ...
    change_detected = (staging_companies.data_hash IS DISTINCT FROM EXCLUDED.data_hash),
    last_updated = NOW()
WHERE staging_companies.data_hash IS DISTINCT FROM EXCLUDED.data_hash
```

Only updates if hash changed → efficient deduplication.

## API Endpoints

### File Discovery
- `POST /api/discover-files` - Discover available files for date range

### Ingestion Control
- `POST /api/ingestion/start` - Start processing files
- `POST /api/ingestion/stop` - Stop after current file
- `POST /api/ingestion/resume` - Resume from checkpoint

### Status & Monitoring
- `GET /api/ingestion/status` - Get current progress
- `GET /api/status` - Get database stats
- `GET /api/logs` - SSE stream for real-time logs

## Usage

### Start Services
```bash
# From project root
docker-compose up db-staging data-ui -d
```

### Apply Database Migration
```bash
cd Data
python database/apply_migrations.py
```

### Access UI
```
http://localhost:{DATA_UI_PORT}
```

### Workflow
1. Select date range (default: last 3 months)
2. Click "Discover Files"
3. Check files from Company/PSC/Accounts panels
4. Click "Add Selected to Ingestion List"
5. Click "Start Ingestion"
6. Monitor progress via progress bar and live logs

## Performance

- **File Discovery:** 10-30 seconds
- **Small file (< 100 MB):** 30 seconds - 2 minutes
- **Medium file (100-500 MB):** 2-5 minutes
- **Large file (> 500 MB):** 5-15 minutes

PostgreSQL COPY is ~100x faster than row-by-row inserts.

## Configuration

All configuration via environment variables (`.env` file):
- `STAGING_DB_HOST` - PostgreSQL host
- `STAGING_DB_PORT` - PostgreSQL port (configured in root `.env`)
- `STAGING_DB_NAME` - Database name
- `STAGING_DB_USER` - Database user
- `STAGING_DB_PASSWORD` - Database password

## Dependencies

See [requirements.txt](web/requirements.txt):
- Flask - Web framework
- psycopg2-binary - PostgreSQL adapter
- pandas - Data manipulation
- beautifulsoup4 - HTML parsing for file discovery
- requests - HTTP client
- lxml - XML/XBRL parsing

## Error Handling

- **Download failures** - Retried up to 3 times
- **Parse errors** - Logged, skipped, continue to next file
- **Database errors** - Logged, stop ingestion, allow resume
- **Network timeouts** - Exponential backoff retry

## Testing

See [TESTING.md](../TESTING.md) for full testing guide.

Quick test:
```bash
# Select 1-2 small company files
# Click "Start Ingestion"
# Verify progress and logs
# Check database for loaded data
```

## Monitoring

Watch logs in real-time:
- UI: Live logs panel
- Docker: `docker logs -f data-co-ui-flask`
- Database: Query `staging_companies`, `staging_officers`, `staging_financials`

## Troubleshooting

### Import Errors
**Fix:** Check sys.path setup in app.py and services

### Database Connection Failed
**Fix:** Verify `.env` file and docker-compose services

### File Discovery Timeout
**Fix:** Companies House site may be slow, retry after 60s

### Port Already in Use
**Fix:** `docker-compose down && docker-compose up db-staging data-ui -d`

## Future Improvements

- [ ] Parallel file processing (multiple workers)
- [ ] Smart resume (skip already processed files)
- [ ] File caching (reuse recent downloads)
- [ ] Email notifications on completion/failure
- [ ] Scheduled automatic monthly ingestion
- [ ] Export progress reports to CSV
- [ ] API authentication for multi-user access

## Migration from Legacy

The old process (in `../legacy/`) is no longer maintained. All new development uses this system.

To migrate old data:
1. Existing data in staging database is compatible
2. Apply migration: `python database/apply_migrations.py`
3. Old records will be auto-updated on next ingestion
