# PostgreSQL Two-Database Setup - Complete Guide

You asked for a PostgreSQL database setup with two databases (staging + production) for your Companies House data. This guide will get you up and running.

## What You Got

I've built you a complete two-database architecture:

```
STAGING DB (dev)          →    PRODUCTION DB (main)
- Raw data                     - Clean data
- Test queries                 - Frontend queries
- Fix issues                   - High quality only
```

## Files Created

```
Data/
├── database/
│   ├── README.md                    # Detailed documentation
│   ├── schema_staging.sql           # Staging database schema
│   ├── schema_production.sql        # Production database schema
│   ├── connection.py                # Database manager
│   ├── inserters.py                 # Insert into staging
│   ├── validators.py                # Validate data quality
│   └── merge_to_production.py       # Merge staging → production
│
├── Data-injestion-workflows/
│   └── Api-request-workflow/
│       ├── api-main.py              # Original (JSON files)
│       └── api-main-db.py           # NEW (PostgreSQL)
│
├── setup_databases.sh               # Automated setup script
├── DATABASE_WORKFLOW.md             # Visual workflow guide
├── .env.example                     # Example configuration
└── .env                             # Your configuration (updated)
```

## Quick Start (5 minutes)

### 1. Install PostgreSQL + pgAdmin

**macOS:**
```bash
# PostgreSQL
brew install postgresql
brew services start postgresql

# pgAdmin (graphical interface)
brew install --cask pgadmin4
```

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install postgresql postgresql-contrib
sudo service postgresql start

# pgAdmin
sudo apt install pgadmin4-desktop
```

**Windows:**
Download from https://www.postgresql.org/download/windows/ (includes pgAdmin)

### 2. Configure Credentials

Edit your `.env` file:
```bash
nano .env
```

Update these lines with your PostgreSQL credentials:
```
STAGING_DB_USER=your_username      # Usually your login name
STAGING_DB_PASSWORD=your_password  # Your PostgreSQL password

PRODUCTION_DB_USER=your_username
PRODUCTION_DB_PASSWORD=your_password
```

Save and exit (Ctrl+X, then Y, then Enter)

### 3. Run Setup Script

```bash
./setup_databases.sh
```

This will:
- Create both databases
- Apply schemas
- Create tables, indexes, views
- Test connections

### 4. Install Python Dependencies

```bash
pip install psycopg2-binary
```

Or reinstall all:
```bash
pip install -r requirements.txt
```

### 5. Test It Works

```bash
python database/connection.py
```

You should see:
```
[db] Connected to companies_house_staging on localhost:5432
✓ Staging database connected
  PostgreSQL version: PostgreSQL 14.x ...

[db] Connected to companies_house_production on localhost:5432
✓ Production database connected
  PostgreSQL version: PostgreSQL 14.x ...
```

### 6. Set Up pgAdmin (Visual Interface)

See [PGADMIN_SETUP.md](PGADMIN_SETUP.md) for:
- How to connect pgAdmin to your databases
- Browse tables visually
- Run queries with a graphical interface
- Export data to CSV
- View data quality dashboards

## Usage Workflow

### Step 1: Ingest Data to Staging

```bash
python Data-injestion-workflows/Api-request-workflow/api-main-db.py
```

This fetches data from Companies House and inserts into **staging** database.

### Step 2: Check What You Got

```bash
# List batches
python database/merge_to_production.py --list
```

Or query directly:
```sql
psql companies_house_staging

SELECT * FROM staging_data_quality;
SELECT * FROM staging_review_queue;
```

### Step 3: Validate Quality

```bash
python database/validators.py
```

### Step 4: Merge to Production

```bash
# Dry run first (safe, doesn't change anything)
python database/merge_to_production.py --batch-id <batch_id> --dry-run

# If it looks good, merge for real
python database/merge_to_production.py --batch-id <batch_id>
```

### Step 5: Query Production Data

```sql
psql companies_house_production

-- Active companies with financials
SELECT * FROM active_companies_with_financials LIMIT 10;

-- Officers for a company
SELECT * FROM officers_with_contacts WHERE company_number = '12345678';

-- Company overview
SELECT * FROM company_overview WHERE company_number = '12345678';
```

## Understanding the Two-Database Pattern

### Why Two Databases?

**STAGING (dev):**
- Accepts messy, incomplete data
- You test queries here
- You fix data issues here
- Safe to experiment
- Can delete and re-run

**PRODUCTION (main):**
- Only clean, validated data
- Your frontend queries this
- High quality guaranteed (>70% score)
- Stable, reliable
- Never touched by ingestion scripts

### The Flow

```
Companies House API
        ↓
    (ingest)
        ↓
STAGING DATABASE ← You work here
        ↓           - Fix issues
    (validate)      - Test queries
        ↓           - Review data
    (transform)
        ↓
PRODUCTION DATABASE ← Frontend queries here
        ↓              - Clean data only
   Your Web App        - Optimized views
```

## What Each Database Contains

### Staging Tables

```sql
staging_companies            -- Raw company data
staging_officers            -- Raw officer/PSC data
staging_financials          -- Raw financial data (future)
staging_ingestion_log       -- Batch tracking
staging_contact_enrichments -- Third-party contacts (future)

-- Helper views
staging_review_queue        -- Records needing manual review
staging_data_quality        -- Quality metrics per batch
```

### Production Tables

```sql
companies                   -- Clean company data
officers                   -- Normalized officers
financials                 -- Validated financials
contact_enrichments        -- Verified contacts only
merge_log                  -- Audit trail

-- Frontend views (optimized)
active_companies_with_financials
officers_with_contacts
company_overview
```

## Real Example

Let's say you want to get Edinburgh bakeries:

**1. Configure search** (`config/filters.yaml`):
```yaml
search_criteria:
  name: "edinburgh_bakeries"
  selection:
    locality: "Edinburgh"
    industry_codes:
      include: ["1071"]
    limit: 50
```

**2. Run ingestion:**
```bash
python api-main-db.py
```

Output:
```
[api] starting enrichment for search: edinburgh_bakeries
  - Found 50 companies matching criteria
  (1/50) Processing company: 12345678 (Edinburgh Bakery Ltd)
    ✓ Fetched officers
    ✓ Inserted 3 officers/PSCs
  ...
[staging] Batch completed: edinburgh_bakeries_20231128_abc123
  - Companies inserted: 50
  - Officers inserted: 127
```

**3. Check staging:**
```sql
psql companies_house_staging

SELECT * FROM staging_data_quality;
```

Output:
```
 batch_id                  | total | missing_names | needs_review
---------------------------+-------+---------------+-------------
 edinburgh_bakeries_...    |    50 |             2 |            7
```

**4. Fix issues:**
```sql
-- Review the problems
SELECT * FROM staging_review_queue;

-- Fix manually if needed
UPDATE staging_companies
SET company_name = 'Corrected Name Ltd'
WHERE company_number = '12345678';
```

**5. Validate:**
```bash
python database/validators.py
```

Output:
```
[validator] Validating batch: edinburgh_bakeries_...
  Quality score: 85.00%
  Issues found: 3
    [warning] 2 companies missing name
    [info] 12 companies missing SIC codes
```

**6. Merge to production:**
```bash
python database/merge_to_production.py \
    --batch-id edinburgh_bakeries_20231128_abc123
```

Output:
```
[merge] Merging batch: edinburgh_bakeries_...

[1/5] Validating data quality...
  ✓ Quality score: 85.00%

[2/5] Merging companies...
  ✓ Merged 48 new companies
  ✓ Updated 0 existing companies

[3/5] Merging officers...
  ✓ Merged 115 officers

[4/5] Merging financials...
  Found 0 financial records to merge

[5/5] Logging merge...
  ✓ Logged merge to production

✓ Merge completed successfully
```

**7. Query production:**
```sql
psql companies_house_production

SELECT company_name, locality, active_officers_count
FROM active_companies_with_financials
WHERE locality = 'Edinburgh'
LIMIT 5;
```

Output:
```
       company_name        | locality  | active_officers
--------------------------+-----------+----------------
 Edinburgh Bakery Ltd     | Edinburgh |              3
 Shortbread Scotland Ltd  | Edinburgh |              2
 ...
```

## Troubleshooting

### "Connection refused" Error

PostgreSQL not running:
```bash
# macOS
brew services start postgresql

# Linux
sudo service postgresql start
```

### "Database does not exist" Error

Run setup script:
```bash
./setup_databases.sh
```

### "Authentication failed" Error

Wrong credentials in `.env`. Check your PostgreSQL user:
```bash
psql -l  # This shows current user

# Then update .env with that username
```

### "Quality score too low" During Merge

Data has too many issues. Fix in staging first:
```sql
-- See what's wrong
SELECT * FROM staging_data_quality;

-- Fix the issues
UPDATE staging_companies
SET company_name = 'Fixed Name'
WHERE company_name IS NULL;

-- Then retry merge
```

### Want to Start Fresh?

```bash
# Drop everything
dropdb companies_house_staging
dropdb companies_house_production

# Run setup again
./setup_databases.sh
```

## Next Steps

1. **Configure your database credentials** in `.env`
2. **Run the setup script** `./setup_databases.sh`
3. **Test ingestion** with a small batch (use `limit: 10` in config)
4. **Validate and merge** to get comfortable with the workflow
5. **Build your frontend** to query the production views

## Documentation

- [database/README.md](database/README.md) - Detailed technical docs
- [DATABASE_WORKFLOW.md](DATABASE_WORKFLOW.md) - Visual workflow guide
- Original README still applies for bulk downloads and configuration

## Questions?

The architecture is intuitive:
1. **Staging** = your workspace (messy, safe to experiment)
2. **Production** = your product (clean, stable, for frontend)
3. **Merge script** = the quality gate between them

You control when data moves from staging → production.
You can test everything in staging first.
Production stays clean and reliable.

Good luck! The system is ready to use.
