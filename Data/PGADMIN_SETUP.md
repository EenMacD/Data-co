# pgAdmin Setup Guide

pgAdmin is a graphical interface for PostgreSQL that lets you browse tables, run queries, and visualize your data.

## Installation

### macOS

```bash
# Using Homebrew
brew install --cask pgadmin4

# Or download from:
# https://www.pgadmin.org/download/pgadmin-4-macos/
```

### Windows

Download installer from:
https://www.pgadmin.org/download/pgadmin-4-windows/

### Linux (Ubuntu/Debian)

```bash
# Install the public key for the repository
curl -fsS https://www.pgadmin.org/static/packages_pgadmin_org.pub | sudo gpg --dearmor -o /usr/share/keyrings/packages-pgadmin-org.gpg

# Create the repository configuration file
sudo sh -c 'echo "deb [signed-by=/usr/share/keyrings/packages-pgadmin-org.gpg] https://ftp.postgresql.org/pub/pgadmin/pgadmin4/apt/$(lsb_release -cs) pgadmin4 main" > /etc/apt/sources.list.d/pgadmin4.list'

# Update the package list
sudo apt update

# Install pgAdmin
sudo apt install pgadmin4-desktop

# Or for web version:
sudo apt install pgadmin4-web
```

## Initial Setup

### 1. Launch pgAdmin

- Open pgAdmin 4 from your Applications
- First time will ask you to set a **Master Password** (remember this!)

### 2. Add Server Connection - STAGING Database

**Right-click "Servers" → Register → Server**

**General Tab:**
- Name: `Companies House - STAGING`

**Connection Tab:**
- Host name/address: `localhost`
- Port: `5432`
- Maintenance database: `postgres`
- Username: `your_username` (from .env: `STAGING_DB_USER`)
- Password: `your_password` (from .env: `STAGING_DB_PASSWORD`)
- ✅ Save password

**Advanced Tab:**
- DB restriction: `companies_house_staging`

Click **Save**

### 3. Add Server Connection - PRODUCTION Database

**Right-click "Servers" → Register → Server**

**General Tab:**
- Name: `Companies House - PRODUCTION`

**Connection Tab:**
- Host name/address: `localhost`
- Port: `5432`
- Maintenance database: `postgres`
- Username: `your_username` (from .env: `PRODUCTION_DB_USER`)
- Password: `your_password` (from .env: `PRODUCTION_DB_PASSWORD`)
- ✅ Save password

**Advanced Tab:**
- DB restriction: `companies_house_production`

Click **Save**

## Navigating pgAdmin

### Tree Structure

```
Servers
├── Companies House - STAGING
│   └── Databases
│       └── companies_house_staging
│           ├── Schemas
│           │   └── public
│           │       ├── Tables
│           │       │   ├── staging_companies
│           │       │   ├── staging_officers
│           │       │   ├── staging_financials
│           │       │   └── staging_ingestion_log
│           │       └── Views
│           │           ├── staging_review_queue
│           │           └── staging_data_quality
│
└── Companies House - PRODUCTION
    └── Databases
        └── companies_house_production
            ├── Schemas
            │   └── public
            │       ├── Tables
            │       │   ├── companies
            │       │   ├── officers
            │       │   ├── financials
            │       │   └── merge_log
            │       └── Views
            │           ├── active_companies_with_financials
            │           ├── officers_with_contacts
            │           └── company_overview
```

## Common Tasks in pgAdmin

### View Table Data

1. Navigate to: `Servers → Companies House - STAGING → Databases → companies_house_staging → Schemas → public → Tables`
2. **Right-click** on `staging_companies`
3. Select **View/Edit Data → All Rows**

### Run a Query

1. Click on database name (e.g., `companies_house_staging`)
2. Click the **Query Tool** icon (or Tools → Query Tool)
3. Type your SQL:
   ```sql
   SELECT * FROM staging_data_quality;
   ```
4. Click **Execute** (F5) or the ▶ button

### View Table Structure

1. Navigate to table in tree
2. Right-click → **Properties**
3. See:
   - **Columns** tab - field names and types
   - **Constraints** tab - primary keys, foreign keys
   - **Indexes** tab - indexes for performance
   - **SQL** tab - CREATE TABLE statement

### Export Data to CSV

1. View table data (View/Edit Data → All Rows)
2. Click **Download as CSV** icon (looks like a file with arrow)
3. Choose location and save

### Visual Query Builder

1. Open Query Tool
2. Click **View → Graphical Query Builder**
3. Drag tables onto canvas
4. Select columns, add filters visually
5. Click **Generate Query** to see SQL

## Useful Queries to Try

### In STAGING Database

Click on `companies_house_staging` → Query Tool, then try:

**See all batches:**
```sql
SELECT
    batch_id,
    search_name,
    started_at,
    completed_at,
    companies_count,
    status
FROM staging_ingestion_log
ORDER BY started_at DESC;
```

**Data quality overview:**
```sql
SELECT * FROM staging_data_quality;
```

**Browse companies:**
```sql
SELECT
    company_number,
    company_name,
    locality,
    company_status,
    array_length(sic_codes, 1) as sic_count
FROM staging_companies
LIMIT 20;
```

**Companies needing review:**
```sql
SELECT * FROM staging_review_queue;
```

**Officers for a company:**
```sql
SELECT
    o.officer_name,
    o.officer_role,
    o.appointed_on,
    o.nationality,
    o.occupation
FROM staging_officers o
JOIN staging_companies c ON o.staging_company_id = c.id
WHERE c.company_number = '12345678';
```

### In PRODUCTION Database

Click on `companies_house_production` → Query Tool, then try:

**Active companies with financials:**
```sql
SELECT *
FROM active_companies_with_financials
WHERE locality = 'Edinburgh'
LIMIT 10;
```

**Company overview:**
```sql
SELECT *
FROM company_overview
WHERE company_number = '12345678';
```

**Search by name:**
```sql
SELECT
    company_number,
    company_name,
    locality,
    company_status,
    data_quality_score
FROM companies
WHERE company_name ILIKE '%bakery%'
ORDER BY data_quality_score DESC
LIMIT 20;
```

**Officers with contacts:**
```sql
SELECT *
FROM officers_with_contacts
WHERE company_number = '12345678';
```

**Merge history:**
```sql
SELECT
    batch_id,
    merged_at,
    companies_merged,
    officers_merged,
    notes
FROM merge_log
ORDER BY merged_at DESC;
```

## Tips & Tricks

### Color-Code Your Servers

1. Right-click server → **Properties**
2. Go to **General** tab
3. Set **Background color** (e.g., yellow for staging, green for production)
4. This helps avoid mixing them up!

### Save Frequently Used Queries

1. Write your query in Query Tool
2. Click **File → Save As**
3. Give it a name like "Edinburgh Bakeries Query"
4. Access later from **File → Open**

### Auto-Refresh Data

When viewing table data:
1. Click **View/Edit Data → All Rows**
2. Click the **⟳ refresh** icon to see latest data
3. Or set **Auto-refresh** in settings

### View Query Execution Plan

1. Write your query
2. Click **Explain → Explain** (F7)
3. See how PostgreSQL will execute it
4. Useful for optimizing slow queries

### Compare Staging vs Production

1. Open two Query Tool windows (one for each database)
2. Run same query in both
3. Compare results side-by-side

## Dashboard Widgets

### Create a Quick Stats Dashboard

1. Click on database → **Dashboard** tab
2. You'll see:
   - Database size
   - Number of connections
   - Transaction stats
   - Table/index statistics

### Customize Dashboard

1. Click **Edit** (pencil icon)
2. Add/remove widgets
3. Arrange to your liking

## Common Issues

### "Server doesn't listen"

PostgreSQL not running. Start it:
```bash
# macOS
brew services start postgresql

# Linux
sudo service postgresql start
```

### "Password authentication failed"

Wrong credentials. Check your `.env` file and update pgAdmin connection.

### "Database does not exist"

Run setup script:
```bash
./setup_databases.sh
```

### Slow Query Performance

1. Click on slow query in Query Tool
2. Click **Explain → Explain Analyze**
3. Look for "Seq Scan" on large tables
4. Add indexes if needed

## Visual Data Exploration

### View JSONB Data

The `raw_data` columns contain JSON. To view nicely:

```sql
SELECT
    company_number,
    company_name,
    jsonb_pretty(raw_data) as formatted_data
FROM staging_companies
LIMIT 1;
```

Or in pgAdmin data viewer, click on the JSONB cell and it shows formatted.

### Charts (pgAdmin 4.30+)

After running a query:
1. Click **Visualize** tab
2. Choose chart type (bar, line, pie)
3. Select X and Y axis
4. See visual representation

Example query for chart:
```sql
SELECT
    locality,
    COUNT(*) as company_count
FROM companies
WHERE locality IS NOT NULL
GROUP BY locality
ORDER BY company_count DESC
LIMIT 10;
```

Then visualize as a bar chart!

## Recommended Setup

### For Daily Use:

**Favorite Queries** (save these):

1. **Check Latest Batch** (staging):
   ```sql
   SELECT * FROM staging_ingestion_log
   ORDER BY started_at DESC LIMIT 1;
   ```

2. **Data Quality** (staging):
   ```sql
   SELECT * FROM staging_data_quality;
   ```

3. **Search Companies** (production):
   ```sql
   SELECT company_number, company_name, locality, company_status
   FROM companies
   WHERE company_name ILIKE '%search_term%'
   LIMIT 50;
   ```

4. **Company Details** (production):
   ```sql
   SELECT * FROM company_overview
   WHERE company_number = '12345678';
   ```

### Keyboard Shortcuts

- **F5** - Execute query
- **F7** - Explain query plan
- **F8** - Execute query with EXPLAIN ANALYZE
- **Ctrl+Space** - Auto-complete
- **Ctrl+Shift+C** - Comment/uncomment
- **Ctrl+L** - Clear query editor

## Next Steps

1. **Install pgAdmin** using instructions above
2. **Add both server connections** (staging + production)
3. **Color-code them** (staging = yellow, production = green)
4. **Try the sample queries** in each database
5. **Save your favorite queries**
6. **Explore the data visually**

You now have full visual access to both databases!

## Screenshots Guide

When you open pgAdmin, you'll see:

**Left Panel**: Server tree
- Expand to navigate databases, schemas, tables, views

**Top Toolbar**: Common actions
- Query Tool, View Data, Refresh, etc.

**Main Area**:
- Dashboard (when database selected)
- Data viewer (when viewing table)
- Query editor (when using Query Tool)

**Bottom Panel**: Messages, notifications, query history

The interface is intuitive - just click around and explore!
