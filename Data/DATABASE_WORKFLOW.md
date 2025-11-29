# Database Workflow - Visual Guide

This document shows you exactly how the two-database system works, step by step.

## The Big Picture

```
┌──────────────────┐
│  Companies House │
│       API        │
└────────┬─────────┘
         │
         │ api-main-db.py
         │ (raw ingestion)
         ▼
┌─────────────────────────────────────────────────────────┐
│              STAGING DATABASE (dev)                     │
│  ┌──────────────────────────────────────────────────┐   │
│  │  staging_companies                               │   │
│  │  - Raw data from API                             │   │
│  │  - May have duplicates                           │   │
│  │  - May have missing data                         │   │
│  │  - Store everything as JSONB                     │   │
│  └──────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────┐   │
│  │  staging_officers                                │   │
│  │  - Raw officer/PSC data                          │   │
│  │  - Linked to staging_companies                   │   │
│  └──────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────┐   │
│  │  staging_ingestion_log                           │   │
│  │  - Track each batch                              │   │
│  │  - Batch ID, timestamp, status                   │   │
│  └──────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
         │
         │ YOU WORK HERE
         │ - Test queries
         │ - Fix data
         │ - Review quality
         ▼
┌─────────────────────────────────────────────────────────┐
│          validators.py (quality checks)                 │
│  - Calculate quality score                              │
│  - Check for missing data                               │
│  - Validate formats                                     │
│  - Mark records for review                              │
└─────────────────────────────────────────────────────────┘
         │
         │ Quality score > 70%?
         │
         ▼ YES
┌─────────────────────────────────────────────────────────┐
│     merge_to_production.py (transformation)             │
│  - Normalize officer names                              │
│  - Deduplicate companies                                │
│  - Calculate derived fields                             │
│  - Transform & clean                                    │
└─────────────────────────────────────────────────────────┘
         │
         │ INSERT/UPDATE
         ▼
┌─────────────────────────────────────────────────────────┐
│           PRODUCTION DATABASE (main)                    │
│  ┌──────────────────────────────────────────────────┐   │
│  │  companies                                       │   │
│  │  - Clean, validated data                         │   │
│  │  - No duplicates                                 │   │
│  │  - Quality score tracked                         │   │
│  │  - Unique company_number                         │   │
│  └──────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────┐   │
│  │  officers                                        │   │
│  │  - Normalized names                              │   │
│  │  - Deduplicated                                  │   │
│  │  - Active status computed                        │   │
│  └──────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────┐   │
│  │  financials                                      │   │
│  │  - Validated figures                             │   │
│  │  - Calculated ratios                             │   │
│  └──────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────┐   │
│  │  Views (optimized for frontend)                  │   │
│  │  - active_companies_with_financials              │   │
│  │  - officers_with_contacts                        │   │
│  │  - company_overview                              │   │
│  └──────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
         │
         │ Frontend queries here
         ▼
┌──────────────────┐
│   Your Web App   │
│   (React, etc)   │
└──────────────────┘
```

## Step-by-Step Workflow

### Step 1: Ingest Raw Data to Staging

```bash
python Data-injestion-workflows/Api-request-workflow/api-main-db.py
```

**What happens:**
1. Reads your filter configuration (locality, SIC codes, etc.)
2. Calls Companies House API for each company
3. Stores everything in `staging_companies` and `staging_officers`
4. Creates a batch record with unique ID
5. All raw API responses saved as JSONB

**Result:**
```sql
-- New batch in staging
SELECT * FROM staging_ingestion_log ORDER BY started_at DESC LIMIT 1;

-- batch_id              | search_name       | companies_count | status
-- edinburgh_bakeries... | edinburgh_bakeries| 50              | completed
```

### Step 2: Explore & Fix Data in Staging

```sql
-- What did we get?
SELECT * FROM staging_data_quality;

-- total_companies | missing_names | missing_locality | needs_review
-- 50              | 2             | 5                | 7

-- Look at specific issues
SELECT * FROM staging_review_queue;

-- Fix issues manually
UPDATE staging_companies
SET company_name = 'Fixed Name Ltd'
WHERE company_number = '12345678' AND batch_id = 'your_batch_id';

-- Or mark for later review
UPDATE staging_companies
SET needs_review = true,
    review_notes = 'Name looks suspicious'
WHERE company_name LIKE '%TEST%';
```

### Step 3: Validate Data Quality

```bash
python database/validators.py
```

**What happens:**
1. Checks for missing required fields
2. Validates data formats
3. Calculates quality score (0.0 to 1.0)
4. Identifies issues to fix

**Output:**
```
[validator] Validating batch: edinburgh_bakeries_20231128_...
  Companies checked: 50
  Officers checked: 127
  Quality score: 85.00%

Issues:
  [warning] 2 companies missing name
  [warning] 5 companies missing locality
  [info] 12 companies missing SIC codes
```

### Step 4: Merge to Production (Dry Run First!)

```bash
# See what batches are available
python database/merge_to_production.py --list

# Test the merge (doesn't actually do it)
python database/merge_to_production.py --batch-id <batch_id> --dry-run
```

**Output:**
```
[merge] DRY RUN: Merging batch: edinburgh_bakeries_20231128_...

[1/5] Validating data quality...
  ✓ Quality score: 85.00%

[2/5] Merging companies...
  Found 48 companies to merge  # (2 marked for review)

[3/5] Merging officers...
  Found 115 officers to merge  # (12 had issues, filtered out)

[4/5] Merging financials...
  Found 0 financial records to merge

[5/5] Logging merge...
  ✓ Logged merge to production

DRY RUN Summary:
  Companies merged: 48
  Companies updated: 0
  Officers merged: 115
  Financials merged: 0
```

### Step 5: Merge for Real

```bash
# If dry-run looked good, do it for real
python database/merge_to_production.py --batch-id <batch_id>
```

**What happens:**
1. Validates quality score (must be >70%)
2. Transforms data (normalize names, calculate scores)
3. Upserts companies (insert new, update existing)
4. Upserts officers (deduplicated)
5. Logs the merge in `merge_log`

**Result:**
```sql
-- New companies in production
SELECT COUNT(*) FROM companies;  -- 48 new companies

-- New officers in production
SELECT COUNT(*) FROM officers;   -- 115 new officers

-- Merge was logged
SELECT * FROM merge_log ORDER BY merged_at DESC LIMIT 1;

-- batch_id            | companies_merged | officers_merged | merged_at
-- edinburgh_bakeries...| 48               | 115             | 2023-11-28 14:30:00
```

### Step 6: Query Production Data

```sql
-- Active companies with their latest financials
SELECT * FROM active_companies_with_financials
WHERE locality = 'Edinburgh'
LIMIT 10;

-- Officers for a specific company
SELECT * FROM officers_with_contacts
WHERE company_number = '12345678';

-- Company detail view
SELECT * FROM company_overview
WHERE company_number = '12345678';
```

## Common Scenarios

### Scenario 1: Daily Update

```bash
# 1. Ingest new data
python api-main-db.py

# 2. Quick validation
python database/validators.py

# 3. List batches
python database/merge_to_production.py --list

# 4. Merge latest batch
python database/merge_to_production.py --batch-id <latest_batch> --dry-run
python database/merge_to_production.py --batch-id <latest_batch>
```

### Scenario 2: Bad Data Ingested

```bash
# 1. Check what's wrong
SELECT * FROM staging_data_quality WHERE batch_id = 'bad_batch';

# 2. Option A: Fix in staging
UPDATE staging_companies
SET company_name = 'Fixed Name'
WHERE batch_id = 'bad_batch' AND company_name IS NULL;

# Then validate and merge
python database/validators.py
python database/merge_to_production.py --batch-id bad_batch

# 3. Option B: Delete and re-run
DELETE FROM staging_companies WHERE batch_id = 'bad_batch';
DELETE FROM staging_ingestion_log WHERE batch_id = 'bad_batch';

# Re-run ingestion
python api-main-db.py
```

### Scenario 3: Test Query Before Production

```sql
-- Test complex query in staging first
SELECT
    sc.company_number,
    sc.company_name,
    COUNT(so.id) as officer_count,
    array_agg(so.officer_name) as officers
FROM staging_companies sc
LEFT JOIN staging_officers so ON sc.id = so.staging_company_id
WHERE sc.locality = 'Edinburgh'
GROUP BY sc.company_number, sc.company_name;

-- If it works, merge to production and run same query there
-- Production query will be faster (indexed, optimized)
```

### Scenario 4: Incremental Updates

```bash
# Day 1: Initial load
python api-main-db.py  # 1000 companies
python database/merge_to_production.py --batch-id batch_1

# Day 2: Update changed companies
python api-main-db.py  # 50 companies (updated only)
python database/merge_to_production.py --batch-id batch_2

# Production will have:
# - 1000 original companies
# - 50 updated companies (merged, not duplicated)
# - Total: still 1000 companies, but with fresh data
```

## Data Flow Examples

### Example 1: New Company

```
API Response:
{
  "company_number": "12345678",
  "company_name": "Edinburgh Bakery Ltd",
  "company_status": "active",
  ...
}

         ↓ api-main-db.py

Staging:
staging_companies
- id: 1
- company_number: "12345678"
- company_name: "Edinburgh Bakery Ltd"
- raw_data: { full API response }
- needs_review: false

         ↓ merge_to_production.py

Production:
companies
- id: 1
- company_number: "12345678"
- company_name: "Edinburgh Bakery Ltd"
- data_quality_score: 0.95
- primary_sic_code: "1071"
- raw_data: { full API response }
```

### Example 2: Updated Company

```
Already in production:
companies
- company_number: "12345678"
- company_name: "Edinburgh Bakery Ltd"
- locality: "Edinburgh"
- last_updated: 2023-11-01

New data in staging:
staging_companies
- company_number: "12345678"
- company_name: "Edinburgh Bakery Ltd"
- locality: "Edinburgh"
- postal_code: "EH1 1AA"  ← NEW

         ↓ merge_to_production.py (UPSERT)

Production (updated):
companies
- company_number: "12345678"
- company_name: "Edinburgh Bakery Ltd"
- locality: "Edinburgh"
- postal_code: "EH1 1AA"  ← UPDATED
- last_updated: 2023-11-28  ← UPDATED
```

## Quality Gates

The merge process has quality gates to protect production:

```
┌─────────────────────────────────────┐
│ Quality Gate 1: Data Validation     │
│ - Required fields present?          │
│ - Valid formats?                    │
│ - Reasonable values?                │
└─────────────────────────────────────┘
         │
         ├─ PASS → Continue
         └─ FAIL → Mark for review, don't merge

┌─────────────────────────────────────┐
│ Quality Gate 2: Quality Score       │
│ - Overall score > 70%?              │
│ - Too many missing fields?          │
└─────────────────────────────────────┘
         │
         ├─ PASS → Merge to production
         └─ FAIL → Block merge, fix data first

┌─────────────────────────────────────┐
│ Quality Gate 3: Merge Success       │
│ - Data transformed correctly?       │
│ - No conflicts?                     │
│ - Logged successfully?              │
└─────────────────────────────────────┘
         │
         └─ SUCCESS → Data in production!
```

## Summary

**The workflow is:**

1. **Ingest** → Staging (messy, raw, everything)
2. **Explore** → Fix issues, test queries
3. **Validate** → Check quality, calculate scores
4. **Transform** → Normalize, deduplicate, clean
5. **Merge** → Production (clean, validated, stable)
6. **Query** → Frontend uses production views

**Key benefits:**

- **Safety**: Never mess up production with bad data
- **Flexibility**: Test and fix data before committing
- **Audit trail**: Every batch is tracked and logged
- **Quality**: Only high-quality data reaches production
- **Incremental**: Merge batches as they're ready

For implementation details, see [database/README.md](database/README.md)
