# Installation Checklist

Use this checklist to set up your complete Companies House database system.

## Prerequisites

- [ ] Python 3.8 or higher installed
- [ ] macOS, Linux, or Windows with admin access
- [ ] Internet connection
- [ ] Companies House API key ([get one here](https://developer.company-information.service.gov.uk/user/register))

## Installation Steps

### 1. PostgreSQL Installation

**macOS:**
```bash
brew install postgresql
brew services start postgresql
```
- [ ] PostgreSQL installed
- [ ] PostgreSQL service running (`pg_isready` shows "accepting connections")

**Linux (Ubuntu/Debian):**
```bash
sudo apt-get update
sudo apt-get install postgresql postgresql-contrib
sudo service postgresql start
```
- [ ] PostgreSQL installed
- [ ] PostgreSQL service running

**Windows:**
- [ ] Downloaded installer from https://www.postgresql.org/download/windows/
- [ ] Installed PostgreSQL
- [ ] Remember the postgres user password you set
- [ ] PostgreSQL service running

### 2. pgAdmin Installation (Visual Database Browser)

**macOS:**
```bash
brew install --cask pgadmin4
```
- [ ] pgAdmin installed
- [ ] Can launch pgAdmin from Applications

**Linux:**
```bash
sudo apt install pgadmin4-desktop
```
- [ ] pgAdmin installed
- [ ] Can launch pgAdmin

**Windows:**
- [ ] Included with PostgreSQL installer, or
- [ ] Download from https://www.pgadmin.org/download/

### 3. Project Setup

```bash
# Navigate to the Data folder
cd /Users/iainmcdulling/Acacia/data-co/Data

# Install Python dependencies
pip install -r requirements.txt
```

- [ ] Navigated to Data folder
- [ ] Dependencies installed (especially `psycopg2-binary`)

### 4. Configuration

```bash
# Edit .env file with your credentials
nano .env
```

Update these values:
```env
COMPANIES_HOUSE_API_KEY=your_actual_api_key

STAGING_DB_USER=your_postgresql_username
STAGING_DB_PASSWORD=your_postgresql_password

PRODUCTION_DB_USER=your_postgresql_username
PRODUCTION_DB_PASSWORD=your_postgresql_password
```

- [ ] Copied API key from Companies House
- [ ] Set PostgreSQL username (usually your login name)
- [ ] Set PostgreSQL password
- [ ] Saved `.env` file

**How to find your PostgreSQL username:**
```bash
whoami  # This is usually your PostgreSQL username
```

### 5. Database Creation

```bash
# Run the setup script
./setup_databases.sh
```

When prompted:
- [ ] Answered prompts (recreate databases if they exist? Y/N)
- [ ] Both databases created successfully
- [ ] Schemas applied without errors
- [ ] Connection test passed

Expected output:
```
✓ PostgreSQL is installed
✓ Database 'companies_house_staging' ready
✓ Schema applied successfully
✓ Database 'companies_house_production' ready
✓ Schema applied successfully
✓ Staging database connected
✓ Production database connected
```

### 6. Test Python Connections

```bash
python database/connection.py
```

- [ ] Staging database connected
- [ ] Production database connected
- [ ] No errors shown

### 7. pgAdmin Setup

1. Launch pgAdmin 4
2. Set master password (first time)
3. Add STAGING server:
   - [ ] Name: `Companies House - STAGING`
   - [ ] Host: `localhost`
   - [ ] Port: `5432`
   - [ ] Username: from `.env`
   - [ ] Password: from `.env`
   - [ ] Save password: ✓
   - [ ] DB restriction: `companies_house_staging`
   - [ ] Connection successful

4. Add PRODUCTION server:
   - [ ] Name: `Companies House - PRODUCTION`
   - [ ] Host: `localhost`
   - [ ] Port: `5432`
   - [ ] Username: from `.env`
   - [ ] Password: from `.env`
   - [ ] Save password: ✓
   - [ ] DB restriction: `companies_house_production`
   - [ ] Connection successful

5. Color-code servers:
   - [ ] STAGING background = Yellow
   - [ ] PRODUCTION background = Green

See [pgadmin_quickstart.txt](pgadmin_quickstart.txt) for detailed instructions.

### 8. Test Data Ingestion

```bash
# Configure a small test search (edit config/filters.yaml)
# Set limit: 5 for testing
nano config/filters.yaml

# Run ingestion to staging
python Data-injestion-workflows/Api-request-workflow/api-main-db.py
```

- [ ] Configuration updated with test search
- [ ] Ingestion started without errors
- [ ] Data inserted into staging database
- [ ] Batch created in `staging_ingestion_log`

### 9. Verify Data in pgAdmin

In pgAdmin, navigate to STAGING database:
- [ ] Can see `staging_companies` table
- [ ] Can see data when viewing table (View/Edit Data → All Rows)
- [ ] Can see `staging_officers` table
- [ ] Can see batch in `staging_ingestion_log`

### 10. Test Validation

```bash
python database/validators.py
```

- [ ] Validation ran successfully
- [ ] Quality score calculated
- [ ] No errors

### 11. Test Merge to Production

```bash
# List available batches
python database/merge_to_production.py --list

# Dry run first
python database/merge_to_production.py --batch-id <batch_id> --dry-run

# If dry run looks good, merge for real
python database/merge_to_production.py --batch-id <batch_id>
```

- [ ] Batches listed successfully
- [ ] Dry run completed without errors
- [ ] Actual merge completed successfully
- [ ] Data visible in production database

### 12. Verify Production Data in pgAdmin

In pgAdmin, navigate to PRODUCTION database:
- [ ] Can see `companies` table with data
- [ ] Can see `officers` table with data
- [ ] Can query views: `active_companies_with_financials`
- [ ] Can see merge log in `merge_log` table

### 13. Run Test Queries

In pgAdmin Query Tool (PRODUCTION database):

```sql
-- This should return your test companies
SELECT * FROM active_companies_with_financials LIMIT 5;
```

- [ ] Query executes successfully
- [ ] Returns data
- [ ] Data looks correct

## Verification Checklist

Run these final checks:

### Database Structure
```bash
# In staging
psql companies_house_staging -c "\dt"
# Should show: staging_companies, staging_officers, etc.

# In production
psql companies_house_production -c "\dt"
# Should show: companies, officers, financials, etc.
```

- [ ] All staging tables present
- [ ] All production tables present

### Data Flow Test

Complete workflow test:

1. **Ingest** → `python api-main-db.py`
   - [ ] Data appears in staging

2. **Validate** → `python database/validators.py`
   - [ ] Quality score > 70%

3. **Merge** → `python database/merge_to_production.py --batch-id <id>`
   - [ ] Data appears in production

4. **Query** → Run SQL in pgAdmin
   - [ ] Can query production views
   - [ ] Data is correct

### Visual Access Test

In pgAdmin:
- [ ] Can browse staging tables visually
- [ ] Can browse production tables visually
- [ ] Can run queries in Query Tool
- [ ] Can export data to CSV
- [ ] Color coding works (yellow=staging, green=production)

## Common Installation Issues

### Issue: "PostgreSQL not found"
**Fix:**
```bash
# macOS
brew install postgresql
brew services start postgresql

# Linux
sudo apt-get install postgresql
sudo service postgresql start
```

### Issue: "Permission denied" when creating databases
**Fix:**
```bash
# Check if postgres user exists
sudo -u postgres psql

# Or create databases manually
createdb companies_house_staging
createdb companies_house_production
```

### Issue: "Connection refused" in pgAdmin
**Fix:**
```bash
# Check PostgreSQL is running
pg_isready

# If not, start it
brew services start postgresql  # macOS
sudo service postgresql start   # Linux
```

### Issue: "Module psycopg2 not found"
**Fix:**
```bash
pip install psycopg2-binary
```

### Issue: "Authentication failed" in pgAdmin
**Fix:**
- Check username/password in `.env` file
- Try username from `whoami` command
- Try password you set during PostgreSQL installation

### Issue: Setup script fails on schema application
**Fix:**
```bash
# Drop databases and start fresh
dropdb companies_house_staging
dropdb companies_house_production

# Re-run setup
./setup_databases.sh
```

## Post-Installation

After completing all steps:

### Save These Files
- [ ] Bookmark `QUICK_REFERENCE.md` for daily commands
- [ ] Keep `PGADMIN_SETUP.md` handy for pgAdmin reference
- [ ] Review `DATABASE_WORKFLOW.md` to understand the flow

### Configure for Production Use
- [ ] Update `config/filters.yaml` with real search criteria
- [ ] Remove `limit: 5` test limits
- [ ] Consider setting up scheduled jobs for daily ingestion

### Backup Strategy
- [ ] Plan regular backups of production database
- [ ] Document backup/restore procedures

Example backup:
```bash
pg_dump companies_house_production > backup_$(date +%Y%m%d).sql
```

## Success Criteria

You're ready to use the system when:

✓ Both databases created and accessible
✓ Python can connect to both databases
✓ pgAdmin can connect and browse both databases
✓ Test data flows from API → Staging → Production
✓ Can query production views in pgAdmin
✓ Color coding helps distinguish staging (yellow) from production (green)

## Next Steps

1. **Configure Real Search** - Edit `config/filters.yaml` with your actual criteria
2. **Bulk Download** - Run bulk workflow for initial data load
3. **API Enrichment** - Run API workflow to get detailed company data
4. **Build Frontend** - Connect your UI to production database views
5. **Automate** - Set up scheduled jobs for daily updates

## Help & Documentation

- [SETUP_GUIDE.md](SETUP_GUIDE.md) - Complete setup guide
- [PGADMIN_SETUP.md](PGADMIN_SETUP.md) - Detailed pgAdmin guide
- [DATABASE_WORKFLOW.md](DATABASE_WORKFLOW.md) - Visual workflow
- [QUICK_REFERENCE.md](QUICK_REFERENCE.md) - Command cheatsheet
- [database/README.md](database/README.md) - Technical details

## Completion

- [ ] All checkboxes above completed
- [ ] System tested end-to-end
- [ ] Documentation reviewed
- [ ] Ready for production use

**Installation completed on:** _________________

**Notes/Issues encountered:**
_________________________________________
_________________________________________
_________________________________________

Congratulations! Your Companies House database system is ready to use.
