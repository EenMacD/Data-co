# Data Ingestion System

Modern UI-driven system for ingesting Companies House monthly bulk data snapshots.

## Architecture

```
staging/
├── common/             # Common resources
│   └── web/            # (HERE) Flask UI application
│       ├── run.sh
│       └── app.py
│   └── services/       # IngestionWorker, FileDiscovery, DownloadManager
├── companies/          # Company specific
│   ├── services/       # CompanyLoader
│   └── parsers/        # CompanyDataParser
├── psc/                # PSC specific (Loader, Parser)
└── accounts/           # Accounts specific (Loader, Parser)
```

## Components

### web/
Flask web UI:
- **app.py** - Main Flask application with API endpoints
- **templates/index.html** - Single-page dashboard UI
- **run.sh** - Helper script to run the UI

### services/ & parsers/
Core logic is distributed by domain:
- **File Discovery & Download**: `staging/common/services/`
- **Orchestration**: `staging/common/services/ingestion_worker.py`
- **Parsers**: `staging/*/parsers/`
- **Loaders**: `staging/*/services/` (PostgreSQL COPY-based bulk insert)

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

### Initialize Databases
```bash
cd Data
bash setup_databases.sh
```

### Access UI
```
http://localhost:{DATA_UI_PORT}
```

## Configuration

All configuration via environment variables (`.env` file):
- `STAGING_DB_HOST` - PostgreSQL host
- `STAGING_DB_PORT` - PostgreSQL port
- `STAGING_DB_NAME` - Database name
- `STAGING_DB_USER` - Database user
- `STAGING_DB_PASSWORD` - Database password

## Dependencies

Listed in `Data/requirements.txt`:
- Flask - Web framework
- psycopg2-binary - PostgreSQL adapter
- pandas - Data manipulation
- beautifulsoup4 - HTML parsing for file discovery
- requests - HTTP client
- lxml - XML/XBRL parsing

## Troubleshooting

### Import Errors
**Fix:** Check sys.path setup in app.py (should include Data root)

### Database Connection Failed
**Fix:** Verify `.env` file and docker-compose services matches `connection.py` settings.

### Port Already in Use
**Fix:** `docker-compose down && docker-compose up db-staging data-ui -d`
