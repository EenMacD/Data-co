# Two-Database Architecture Guide

This directory contains the PostgreSQL database setup for the Companies House data platform.

## Architecture Overview

```
┌─────────────────────────┐
│   STAGING_DB (dev)      │  ← Raw, messy, changing data
│   - Ingest from API     │
│   - Test queries        │
│   - Clean & validate    │
│   - Model & transform   │
└───────────┬─────────────┘
            │
            │ merge_to_production.py
            │ (validates & transforms)
            ▼
┌─────────────────────────┐
│   PRODUCTION_DB (main)  │  ← Clean, validated, stable
│   - Frontend reads here │
│   - Reporting           │
│   - High quality only   │
└─────────────────────────┘
```

## Quick Start

### 1. Set up PostgreSQL databases

```bash
# Create the two databases
createdb companies_house_staging
createdb companies_house_production

# Apply schemas
psql companies_house_staging < database/schema_staging.sql
psql companies_house_production < database/schema_production.sql
```

### 2. Configure environment

```bash
# Copy example env file
cp .env.example .env

# Edit .env and add your database credentials
nano .env
```

Required environment variables:
```
# Staging database
STAGING_DB_HOST=localhost
STAGING_DB_PORT={STAGING_DB_PORT}
STAGING_DB_NAME=companies_house_staging
STAGING_DB_USER=your_username
STAGING_DB_PASSWORD=your_password

# Production database
PRODUCTION_DB_HOST=localhost
PRODUCTION_DB_PORT={PRODUCTION_DB_PORT}
PRODUCTION_DB_NAME=companies_house_production
PRODUCTION_DB_USER=your_username
PRODUCTION_DB_PASSWORD=your_password
```

### 3. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 4. Test connections

```bash
python database/connection.py
```

## Workflow

### Step 1: Ingest data into STAGING

Run the new database-enabled API workflow:

```bash
python Data-injestion-workflows/Api-request-workflow/api-main-db.py
```

This will:
- Fetch data from Companies House API
- Insert raw data into `staging_companies` and `staging_officers` tables
- Create a batch record for tracking

### Step 2: Review data quality in STAGING

Check what data was ingested:

```sql
-- View latest batch
SELECT * FROM staging_ingestion_log ORDER BY started_at DESC LIMIT 5;

-- View data quality summary
SELECT * FROM staging_data_quality;

-- View records needing review
SELECT * FROM staging_review_queue;
```

Or use the validator:

```bash
python database/validators.py
```

### Step 3: Validate and merge to PRODUCTION

List available batches:

```bash
python database/merge_to_production.py --list
```

Run a dry-run first to see what would happen:

```bash
python database/merge_to_production.py --batch-id <batch_id> --dry-run
```

If everything looks good, merge for real:

```bash
python database/merge_to_production.py --batch-id <batch_id>
```

This will:
- Validate data quality (must be >70% score)
- Transform and normalize data
- Insert/update companies in production
- Insert/update officers in production
- Log the merge operation

### Step 4: Query PRODUCTION data

Your frontend should query the production database:

```sql
-- Active companies with financials
SELECT * FROM active_companies_with_financials
WHERE locality = 'Edinburgh'
LIMIT 10;

-- Officers with contacts
SELECT * FROM officers_with_contacts
WHERE company_number = '12345678';

-- Company overview (for detail page)
SELECT * FROM company_overview
WHERE company_number = '12345678';
```

## File Structure

```
database/
├── README.md                    # This file
├── schema_staging.sql           # Staging database schema
├── schema_production.sql        # Production database schema
├── connection.py                # Database connection manager
├── inserters.py                 # Insert data into staging
├── validators.py                # Validate and transform data
└── merge_to_production.py       # Merge staging → production
```

## Key Concepts

### Staging Database

- **Purpose**: Ingest raw, potentially messy data
- **Quality**: Allows duplicates, missing data, errors
- **Usage**: Development, testing, data exploration
- **Tables**:
  - `staging_companies` - Raw company data
  - `staging_officers` - Raw officer/PSC data
  - `staging_financials` - Raw financial data
  - `staging_ingestion_log` - Batch tracking
  - `staging_contact_enrichments` - Third-party contacts

### Production Database

- **Purpose**: Clean, validated data for applications
- **Quality**: High quality only (>70% score required)
- **Usage**: Frontend, reporting, public APIs
- **Tables**:
  - `companies` - Normalized company data
  - `officers` - Normalized officer data with deduplication
  - `financials` - Validated financial records
  - `contact_enrichments` - Verified contacts only
  - `merge_log` - Audit trail of merges

### Data Quality Scores

Each company gets a quality score (0.0 to 1.0):
- **1.0**: Perfect - all fields populated
- **0.7**: Good enough for production
- **<0.7**: Needs review before merging

Score calculation:
- Required fields (company_number, company_name): 3 points each
- Important fields (status, locality, SIC codes): 1 point each
- Nice to have (postal_code, region): 0.5 points each

### Batch System

Every ingestion creates a batch:
- Unique batch ID: `{search_name}_{timestamp}_{uuid}`
- Tracks companies ingested, status, errors
- Allows incremental merges to production
- Full audit trail

## Advanced Usage

### Manual data review

Mark companies for review in staging:

```sql
UPDATE staging_companies
SET needs_review = true,
    review_notes = 'Missing SIC codes'
WHERE sic_codes IS NULL;
```

### Customize validation rules

Edit [validators.py](validators.py) to add your own checks:

```python
def _validate_companies(self) -> dict:
    # Add custom validation logic
    pass
```

### Query raw API responses

All raw API responses are stored as JSONB:

```sql
-- Find companies with specific API data
SELECT company_number, company_name, raw_data->'officers'
FROM staging_companies
WHERE raw_data->'officers' IS NOT NULL;

-- Search within nested JSON
SELECT * FROM companies
WHERE raw_data @> '{"company_status": "active"}';
```

### Re-run failed batches

If a batch fails, fix the data and re-run:

```sql
-- Reset batch status
UPDATE staging_ingestion_log
SET status = 'ready_for_retry'
WHERE batch_id = 'your_batch_id';

-- Delete partial data if needed
DELETE FROM staging_companies WHERE batch_id = 'your_batch_id';
```

## Production Views

The production database includes views optimized for frontend queries:

### `active_companies_with_financials`
Latest financial snapshot for each active company

### `officers_with_contacts`
Officers with verified contact information

### `company_overview`
Summary stats for company detail pages

These views are optimized with indexes and can be queried directly from your frontend.

## Troubleshooting

### Connection errors

```bash
# Test database connections
python database/connection.py

# Check PostgreSQL is running
pg_isready

# View connection details
psql -l
```

### Schema issues

```bash
# Recreate staging schema
psql companies_house_staging < database/schema_staging.sql

# Recreate production schema
psql companies_house_production < database/schema_production.sql
```

### Data quality issues

```bash
# Run validator to see issues
python database/validators.py

# Check staging data quality view
psql companies_house_staging -c "SELECT * FROM staging_data_quality;"
```

### Merge failures

If merge fails due to quality score:
1. Run validator to see issues
2. Fix data in staging
3. Re-run merge

```bash
# Validate first
python database/validators.py

# Then merge
python database/merge_to_production.py --batch-id <batch_id>
```

## Next Steps

1. Set up your databases and configure `.env`
2. Run a test ingestion with `api-main-db.py`
3. Validate the data in staging
4. Merge to production
5. Query production views for your frontend

For more details on the overall system, see the main [README.md](../README.md).
