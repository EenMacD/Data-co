# Data Service

Data ingestion and processing service for Companies House data.

## Directory Structure

```
Data/
├── staging/            # Staging environment resources (Companies, PSC, Accounts)
│   ├── common/         # Common utilities (connection, parsers, Web UI)
│   │   ├── services/   # Shared services and base classes
│   │   ├── web/        # Flask Ingestion UI
│   │   └── schemas/    # Common SQL schemas
│   ├── companies/      # Company-specific logic
│   ├── psc/            # PSC-specific logic
│   └── accounts/       # Accounts-specific logic
│
├── production/         # Production environment resources
│   ├── common/         # Merge logic and production schemas
│   └── ...             # Mirrored structure for schemas
│
├── .env.example        # Environment variables template
├── Dockerfile          # Docker build configuration
├── requirements.txt    # Python dependencies
└── setup_databases.sh  # Database initialization script
```

## Quick Start

### Using the Data Ingestion System (Recommended)

1. **Start services:**
   ```bash
   docker-compose up db-staging data-ui -d
   ```

2. **Initialize Databases (if first time):**
   ```bash
   cd Data
   bash setup_databases.sh
   ```

3. **Access UI:**
   ```
   http://localhost:{DATA_UI_PORT}
   ```

4. **Follow workflow:**
   - Select date range → Discover Files
   - Select files from panels → Add to Ingestion List
   - Start Ingestion → Monitor progress

See [staging/common/web/README.md](staging/common/web/README.md) for full documentation.

## Components

### Active Components

#### staging/common/web/ (Primary System)
Modern bulk data ingestion system. **Use this for all new work.**

Features:
- UI-driven file selection
- Background processing with stop/resume
- Real-time progress and logs
- Efficient bulk loading with change detection

[Full Documentation →](staging/common/web/README.md)

#### staging/ & production/ (Core Logic)
Organized by business domain (Companies, PSC, Accounts) and environment.

- **Loaders**: `staging/*/services/loader.py` - Handles ETL for specific data types.
- **Parsers**: `staging/*/parsers/*_parser.py` - specialized data parsing.
- **Connection**: `staging/common/services/connection.py` - Database connection management.
- **Merging**: `production/common/services/merge_to_production.py` - Promotes data from Staging to Production.

## Database Schema

### Staging Tables
- `staging_companies` - Company master data (one record per company)
- `staging_officers` - Officers and PSC data
- `staging_financials` - Financial accounts data
- `staging_ingestion_log` - Ingestion audit trail

### Production Tables
- `production_companies` - Company master data (one record per company)
- `production_officers` - Officers and PSC data
- `production_financials` - Financial accounts data
- `production_merge_log` - Merge audit trail
Same schema structure as staging, populated via `production/common/services/merge_to_production.py`

## Environment Variables

Configure via `.env` file (copy from `.env.example`):

```bash
# Database Configuration
STAGING_DB_HOST=localhost
STAGING_DB_PORT={STAGING_DB_PORT}
STAGING_DB_NAME=staging
STAGING_DB_USER=postgres
STAGING_DB_PASSWORD=your_password

PRODUCTION_DB_HOST=localhost
PRODUCTION_DB_PORT={PRODUCTION_DB_PORT}
PRODUCTION_DB_NAME=production
PRODUCTION_DB_USER=postgres
PRODUCTION_DB_PASSWORD=your_password

# Flask UI
DATA_UI_PORT={DATA_UI_PORT}
```

## Docker Services

Defined in `docker-compose.yml`:

- **db-staging** - PostgreSQL staging database (port `{STAGING_DB_PORT}`)
- **db-production** - PostgreSQL production database (port `{PRODUCTION_DB_PORT}`)
- **data-ui** - Flask bulk ingestion UI (port `{DATA_UI_PORT}`)
- **data-worker** - Background data processing worker

Start services:
```bash
docker-compose up db-staging data-ui -d
```

## Development

### Install Dependencies

```bash
cd Data
pip install -r requirements.txt
```

### Run Tests

```bash
# Test database connection
python staging/common/services/connection.py

# Test merge script (dry run)
python production/common/services/merge_to_production.py --list
```

### Local Development (Without Docker)

1. Start local PostgreSQL
2. Set environment variables in `.env`
3. Run Flask app:
   ```bash
   cd Data
   python staging/common/web/app.py
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
```
