# Data Service

Data ingestion and processing service for Companies House data.

## Directory Structure

```
Data/
├── bulk_ingestion/     # NEW: Modern bulk data ingestion system
│   ├── parsers/        # Data parsers (Company, PSC, Accounts)
│   ├── services/       # Core services (discovery, download, loading)
│   ├── web/            # Flask UI and API
│   └── README.md       # Full documentation
│
├── legacy/             # ARCHIVED: Old ingestion workflows (deprecated)
│   ├── workflows/      # Old API and bulk workflows
│   ├── collation/      # Old financials collation
│   ├── config/         # Old YAML config system
│   ├── scripts/        # Old run scripts
│   └── README.md       # Legacy documentation
│
├── database/           # SHARED: Database schema and utilities
│   ├── connection.py   # PostgreSQL connection pooling
│   ├── inserters.py    # Database insert utilities
│   ├── validators.py   # Data validation
│   ├── migrations/     # Schema migrations
│   └── apply_migrations.py  # Migration runner
│
├── config/             # SHARED: Configuration files
├── docs/               # SHARED: Documentation
├── Dockerfile          # Docker build configuration
├── requirements.txt    # Python dependencies
└── setup_databases.sh  # Database initialization script
```

## Quick Start

### Using the Bulk Ingestion System (Recommended)

1. **Start services:**
   ```bash
   docker-compose up db-staging data-ui -d
   ```

2. **Apply database migration:**
   ```bash
   cd Data
   python database/apply_migrations.py
   ```

3. **Access UI:**
   ```
   http://localhost:5001
   ```

4. **Follow workflow:**
   - Select date range → Discover Files
   - Select files from panels → Add to Ingestion List
   - Start Ingestion → Monitor progress

See [bulk_ingestion/README.md](bulk_ingestion/README.md) for full documentation.

## What Changed?

### ✅ New System (bulk_ingestion/)
**Modern UI-driven bulk data ingestion with:**
- Web UI for file selection
- Monthly snapshot selection (first-of-month for PSC/Accounts)
- PostgreSQL COPY bulk loading (100x faster)
- Hash-based change detection and auto-update
- Stop/resume capability
- Real-time progress tracking via SSE
- Auto-cleanup of downloaded files

### ❌ Legacy System (legacy/)
**Old workflow (ARCHIVED):**
- Download entire bulk dataset
- Enrich via API requests
- Row-by-row database inserts (slow)
- Complex YAML configuration
- No UI, no progress tracking
- No change detection

## Components

### Active Components

#### bulk_ingestion/ (Primary System)
Modern bulk data ingestion system. **Use this for all new work.**

Features:
- UI-driven file selection
- Background processing with stop/resume
- Real-time progress and logs
- Efficient bulk loading with change detection

[Full Documentation →](bulk_ingestion/README.md)

#### database/ (Shared)
Database connection and schema management used by all systems.

Key files:
- `connection.py` - Connection pooling for staging/production DBs
- `migrations/` - Schema version control
- `apply_migrations.py` - Migration runner

### Archived Components

#### legacy/ (Deprecated)
Old ingestion workflows. **Do not use for new development.**

Kept for reference purposes only.

[Legacy Documentation →](legacy/README.md)

## Database Schema

### Staging Tables
- `staging_companies` - Company master data (one record per company)
- `staging_officers` - Officers and PSC data
- `staging_financials` - Financial accounts data
- `staging_ingestion_log` - Ingestion audit trail

### Production Tables
Same schema as staging, populated via `database/merge_to_production.py`

## Environment Variables

Configure via `.env` file (copy from `.env.example`):

```bash
# Database Configuration
STAGING_DB_HOST=localhost
STAGING_DB_PORT=5432
STAGING_DB_NAME=staging
STAGING_DB_USER=postgres
STAGING_DB_PASSWORD=your_password

PRODUCTION_DB_HOST=localhost
PRODUCTION_DB_PORT=5433
PRODUCTION_DB_NAME=production
PRODUCTION_DB_USER=postgres
PRODUCTION_DB_PASSWORD=your_password

# Flask UI
DATA_UI_PORT=5001
```

## Docker Services

Defined in `docker-compose.yml`:

- **db-staging** - PostgreSQL staging database (port 5432)
- **db-production** - PostgreSQL production database (port 5433)
- **data-ui** - Flask bulk ingestion UI (port 5001)
- **data-worker** - Background data processing worker

Start services:
```bash
docker-compose up db-staging data-ui -d
```

## Development

### Install Dependencies

```bash
cd Data/bulk_ingestion/web
pip install -r requirements.txt
```

### Run Tests

```bash
# Test database connection
python database/connection.py

# Test file discovery
python -m bulk_ingestion.services.file_discovery

# Apply migrations
python database/apply_migrations.py
```

### Local Development (Without Docker)

1. Start local PostgreSQL
2. Set environment variables in `.env`
3. Run Flask app:
   ```bash
   cd Data
   python bulk_ingestion/web/app.py
   ```

## Monitoring

### View Logs

```bash
# Flask UI logs
docker logs -f data-co-ui-flask

# Database logs
docker logs -f data-co-db-staging
```

### Query Database

```bash
# Connect to staging DB
docker exec -it data-co-db-staging psql -U postgres -d staging

# Check record counts
SELECT COUNT(*) FROM staging_companies;
SELECT COUNT(*) FROM staging_officers;
SELECT COUNT(*) FROM staging_financials;
```

## Troubleshooting

### Services Won't Start
```bash
docker-compose down
docker-compose up db-staging data-ui -d
```

### Database Connection Error
- Check `.env` file exists with correct credentials
- Verify database container is running: `docker ps`

### Import Errors
- Verify `sys.path` setup in Python files
- Check relative imports are correct after reorganization

### Port Conflicts
- Check ports 5432, 5433, 5001 are not in use
- Modify ports in `.env` if needed

## Migration from Legacy

If you have existing code using the legacy system:

1. **Review** [legacy/README.md](legacy/README.md) for migration notes
2. **Update** to use new bulk_ingestion system
3. **Apply** database migration: `python database/apply_migrations.py`
4. **Test** with small dataset first

The new system is backwards-compatible with existing staging data.

## Contributing

When adding new features:

1. Add to `bulk_ingestion/` (not legacy/)
2. Update relevant README files
3. Add database migrations to `database/migrations/`
4. Update `bulk_ingestion/web/requirements.txt` if new dependencies
5. Test with Docker Compose setup

## Support

- **Bulk Ingestion Issues** - See [bulk_ingestion/README.md](bulk_ingestion/README.md)
- **Database Issues** - Check [database/README.md](database/README.md)
- **Legacy Code Questions** - See [legacy/README.md](legacy/README.md)

## License

Internal project - not for public distribution.
