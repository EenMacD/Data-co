# Quick Reference - Database Commands

## Setup (One Time)

```bash
# 1. Edit credentials
nano .env

# 2. Run setup
./setup_databases.sh

# 3. Test connection
python database/connection.py

# 4. Set up pgAdmin (graphical interface)
# See PGADMIN_SETUP.md
```

## Daily Workflow

```bash
# 1. Ingest data to STAGING
python Data-injestion-workflows/Api-request-workflow/api-main-db.py

# 2. List batches
python database/merge_to_production.py --list

# 3. Validate quality
python database/validators.py

# 4. Merge to PRODUCTION (dry run first)
python database/merge_to_production.py --batch-id <batch_id> --dry-run
python database/merge_to_production.py --batch-id <batch_id>
```

## Useful SQL Queries

### Staging Database

```sql
# Connect
psql companies_house_staging

# What batches do I have?
SELECT batch_id, search_name, companies_count, status
FROM staging_ingestion_log
ORDER BY started_at DESC;

# Data quality summary
SELECT * FROM staging_data_quality;

# What needs review?
SELECT * FROM staging_review_queue LIMIT 10;

# Browse companies
SELECT company_number, company_name, locality, company_status
FROM staging_companies
WHERE batch_id = 'your_batch_id'
LIMIT 10;

# Browse officers
SELECT officer_name, officer_role, appointed_on
FROM staging_officers
WHERE company_number = '12345678';

# Fix bad data
UPDATE staging_companies
SET company_name = 'Corrected Name Ltd'
WHERE company_number = '12345678' AND batch_id = 'your_batch_id';

# Mark for review
UPDATE staging_companies
SET needs_review = true, review_notes = 'Check this'
WHERE company_name LIKE '%TEST%';
```

### Production Database

```sql
# Connect
psql companies_house_production

# Active companies with latest financials
SELECT *
FROM active_companies_with_financials
WHERE locality = 'Edinburgh'
LIMIT 10;

# Officers for a company
SELECT *
FROM officers_with_contacts
WHERE company_number = '12345678';

# Company overview (for detail page)
SELECT *
FROM company_overview
WHERE company_number = '12345678';

# Search companies by name
SELECT company_number, company_name, locality
FROM companies
WHERE company_name ILIKE '%bakery%'
LIMIT 10;

# Companies by SIC code
SELECT company_number, company_name, primary_sic_code
FROM companies
WHERE primary_sic_code = '1071'
LIMIT 10;

# Active officers across all companies
SELECT officer_name, COUNT(*) as company_count
FROM officers
WHERE is_active = true
GROUP BY officer_name
HAVING COUNT(*) > 1
ORDER BY company_count DESC;

# Merge history
SELECT *
FROM merge_log
ORDER BY merged_at DESC
LIMIT 10;
```

## Database Management

```bash
# List all databases
psql -l

# Connect to staging
psql companies_house_staging

# Connect to production
psql companies_house_production

# Backup staging
pg_dump companies_house_staging > staging_backup.sql

# Backup production
pg_dump companies_house_production > production_backup.sql

# Restore from backup
psql companies_house_staging < staging_backup.sql

# Drop and recreate
dropdb companies_house_staging
createdb companies_house_staging
psql companies_house_staging < database/schema_staging.sql
```

## Troubleshooting

```bash
# PostgreSQL not running
brew services start postgresql  # macOS
sudo service postgresql start   # Linux

# Check PostgreSQL status
pg_isready

# Test connection
python database/connection.py

# See database sizes
psql -c "\l+"

# See table sizes
psql companies_house_staging -c "\dt+"
```

## Common Fixes

### Delete a bad batch

```sql
-- In staging database
DELETE FROM staging_officers
WHERE staging_company_id IN (
    SELECT id FROM staging_companies WHERE batch_id = 'bad_batch_id'
);

DELETE FROM staging_companies WHERE batch_id = 'bad_batch_id';
DELETE FROM staging_ingestion_log WHERE batch_id = 'bad_batch_id';
```

### Clear all staging data

```sql
TRUNCATE staging_officers CASCADE;
TRUNCATE staging_companies CASCADE;
TRUNCATE staging_ingestion_log CASCADE;
```

### Reset production (DANGEROUS!)

```sql
-- Only do this if you're sure!
TRUNCATE contact_enrichments CASCADE;
TRUNCATE financials CASCADE;
TRUNCATE officers CASCADE;
TRUNCATE companies CASCADE;
TRUNCATE merge_log CASCADE;
```

## Environment Variables

```bash
# Required in .env file:
COMPANIES_HOUSE_API_KEY=your_api_key

STAGING_DB_HOST=localhost
STAGING_DB_PORT=5432
STAGING_DB_NAME=companies_house_staging
STAGING_DB_USER=your_username
STAGING_DB_PASSWORD=your_password

PRODUCTION_DB_HOST=localhost
PRODUCTION_DB_PORT=5432
PRODUCTION_DB_NAME=companies_house_production
PRODUCTION_DB_USER=your_username
PRODUCTION_DB_PASSWORD=your_password
```

## File Locations

```
database/schema_staging.sql      - Staging schema
database/schema_production.sql   - Production schema
database/connection.py           - DB manager
database/inserters.py            - Insert functions
database/validators.py           - Quality checks
database/merge_to_production.py  - Merge script

Data-injestion-workflows/Api-request-workflow/
├── api-main.py                  - Original (JSON)
└── api-main-db.py              - NEW (PostgreSQL)
```

## Quality Score Calculation

```
Maximum score: 10.0 points

Required (3 points each):
- company_number
- company_name

Important (1 point each):
- company_status
- locality
- sic_codes

Nice to have (0.5 points each):
- postal_code
- region

Final score = total_points / 10.0
Minimum for production = 0.70 (70%)
```

## Batch ID Format

```
{search_name}_{timestamp}_{uuid}

Example:
edinburgh_bakeries_20231128_143022_a1b2c3d4
```

## Next Steps After Setup

1. Configure search in `config/filters.yaml`
2. Run ingestion: `python api-main-db.py`
3. Check staging: `psql companies_house_staging`
4. Validate: `python database/validators.py`
5. Merge: `python database/merge_to_production.py --batch-id <id>`
6. Query production for your frontend

## Getting Help

- [SETUP_GUIDE.md](SETUP_GUIDE.md) - Complete setup guide
- [PGADMIN_SETUP.md](PGADMIN_SETUP.md) - Visual database browser setup
- [DATABASE_WORKFLOW.md](DATABASE_WORKFLOW.md) - Visual workflow
- [database/README.md](database/README.md) - Technical details
