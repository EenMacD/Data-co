# START HERE - PostgreSQL Database Setup

Welcome! You asked for a database solution for your Companies House data. This guide gets you started in the right order.

## What You're Getting

A **two-database PostgreSQL system** with visual interface (pgAdmin):

```
STAGING DB (dev)     →    PRODUCTION DB (main)
- Raw data                - Clean data only
- Fix issues here         - Frontend queries here
- Safe to experiment      - Stable & reliable
```

## Quick Start (15 minutes)

### 1. Install Software

**macOS:**
```bash
brew install postgresql pgadmin4
brew services start postgresql
```

**Windows:**
- Download PostgreSQL from https://www.postgresql.org/download/windows/
- pgAdmin is included

**Linux:**
```bash
sudo apt-get install postgresql postgresql-contrib pgadmin4-desktop
sudo service postgresql start
```

### 2. Configure Credentials

```bash
cd /Users/iainmcdulling/Acacia/data-co/Data
nano .env
```

Update these lines (replace `your_username` and `your_password` with your actual PostgreSQL credentials):
```
STAGING_DB_USER=your_username
STAGING_DB_PASSWORD=your_password
PRODUCTION_DB_USER=your_username
PRODUCTION_DB_PASSWORD=your_password
```

Save and exit (Ctrl+X, Y, Enter)

### 3. Create Databases

```bash
pip install -r requirements.txt
./setup_databases.sh
```

### 4. Test Connection

```bash
python database/connection.py
```

You should see:
```
✓ Staging database connected
✓ Production database connected
```

### 5. Set Up Visual Interface (pgAdmin)

Open the file: [pgadmin_quickstart.txt](pgadmin_quickstart.txt)

Follow the simple instructions to:
- Connect pgAdmin to your databases
- Browse tables visually
- Run queries with a graphical interface

## Your First Data Ingestion

Once setup is complete:

```bash
# 1. Ingest data to STAGING
python Data-injestion-workflows/Api-request-workflow/api-main-db.py

# 2. List batches
python database/merge_to_production.py --list

# 3. Merge to PRODUCTION
python database/merge_to_production.py --batch-id <batch_id>
```

Then open pgAdmin and browse your data visually!

## File Guide - What to Read When

### Right Now (Setup)
1. **[INSTALLATION_CHECKLIST.md](INSTALLATION_CHECKLIST.md)** ⭐ START HERE
   - Step-by-step installation guide
   - Checkbox format - nothing to miss
   - Troubleshooting for common issues

2. **[pgadmin_quickstart.txt](pgadmin_quickstart.txt)**
   - How to set up visual database browser
   - Connect pgAdmin in 5 minutes
   - Browse your data graphically

### After Setup (Learning)
3. **[SETUP_GUIDE.md](SETUP_GUIDE.md)**
   - Complete setup documentation
   - Explains the two-database pattern
   - Usage workflow with examples

4. **[DATABASE_WORKFLOW.md](DATABASE_WORKFLOW.md)**
   - Visual diagrams of data flow
   - Step-by-step workflow examples
   - Real-world scenarios

### Daily Use (Reference)
5. **[QUICK_REFERENCE.md](QUICK_REFERENCE.md)** ⭐ BOOKMARK THIS
   - Command cheatsheet
   - Common SQL queries
   - Database management commands

6. **[PGADMIN_SETUP.md](PGADMIN_SETUP.md)**
   - Detailed pgAdmin guide
   - Advanced features
   - Tips & tricks

### Technical Details
7. **[database/README.md](database/README.md)**
   - Architecture details
   - Schema documentation
   - Advanced configuration

## What Each File/Folder Does

```
Data/
├── START_HERE.md                    ⭐ This file
├── INSTALLATION_CHECKLIST.md        ⭐ Follow this for setup
├── SETUP_GUIDE.md                   Complete guide
├── DATABASE_WORKFLOW.md             Visual workflow
├── QUICK_REFERENCE.md               ⭐ Daily commands
├── PGADMIN_SETUP.md                 Visual browser guide
├── pgadmin_quickstart.txt           Quick pgAdmin setup
│
├── .env                             Your credentials (edit this)
├── .env.example                     Example configuration
├── setup_databases.sh               Automated setup script
│
├── database/
│   ├── README.md                    Technical docs
│   ├── schema_staging.sql           Staging DB schema
│   ├── schema_production.sql        Production DB schema
│   ├── connection.py                DB connection manager
│   ├── inserters.py                 Insert into staging
│   ├── validators.py                Validate data quality
│   └── merge_to_production.py       Merge staging → production
│
└── Data-injestion-workflows/
    └── Api-request-workflow/
        ├── api-main.py              Original (JSON files)
        └── api-main-db.py           NEW (PostgreSQL)
```

## The Workflow (Once Set Up)

```
1. Edit search in config/filters.yaml
   ↓
2. Run: python api-main-db.py
   → Fetches data into STAGING database
   ↓
3. Open pgAdmin and browse the data
   → Fix any issues you see
   ↓
4. Run: python database/validators.py
   → Check data quality
   ↓
5. Run: python database/merge_to_production.py --batch-id <id>
   → Moves clean data to PRODUCTION
   ↓
6. Your frontend queries PRODUCTION database
```

## Why Two Databases?

**STAGING (Yellow in pgAdmin):**
- Accepts messy, incomplete data from API
- You test queries here
- You fix data issues here
- Safe to delete and re-run
- Like a workshop

**PRODUCTION (Green in pgAdmin):**
- Only clean, validated data (70%+ quality score)
- Your frontend reads from here
- Stable and reliable
- Never touched by ingestion scripts
- Like your showroom

## Common Questions

**Q: Do I need to know SQL?**
A: Not really! Use pgAdmin to browse data visually. SQL queries are provided in the docs when you need them.

**Q: What if I make a mistake in staging?**
A: That's the point! Fix it in staging, then merge to production. Production stays safe.

**Q: Can I still use the JSON file workflow?**
A: Yes! Your original `api-main.py` still works. The new `api-main-db.py` is for PostgreSQL.

**Q: How do I know if data quality is good enough?**
A: Run `python database/validators.py` - it calculates a quality score. Above 70% is good for production.

**Q: What's the easiest way to see my data?**
A: pgAdmin! It's a visual interface where you can click around and browse tables like a spreadsheet.

## Help & Support

**Installation issues?**
→ See [INSTALLATION_CHECKLIST.md](INSTALLATION_CHECKLIST.md) troubleshooting section

**Don't understand the workflow?**
→ Read [DATABASE_WORKFLOW.md](DATABASE_WORKFLOW.md) with visual diagrams

**Need specific commands?**
→ Use [QUICK_REFERENCE.md](QUICK_REFERENCE.md) cheatsheet

**Want to browse data visually?**
→ Follow [pgadmin_quickstart.txt](pgadmin_quickstart.txt)

## Recommended Path

**First Time Setup:**
1. Read this file (you're here!)
2. Follow [INSTALLATION_CHECKLIST.md](INSTALLATION_CHECKLIST.md)
3. Set up pgAdmin with [pgadmin_quickstart.txt](pgadmin_quickstart.txt)
4. Run a test ingestion (5 companies)
5. Browse the data in pgAdmin
6. Merge to production
7. Celebrate! 🎉

**After Setup:**
- Bookmark [QUICK_REFERENCE.md](QUICK_REFERENCE.md)
- Read [DATABASE_WORKFLOW.md](DATABASE_WORKFLOW.md) to understand the flow
- Use pgAdmin for daily work

## What Makes This System Good?

✓ **Intuitive** - Two databases match how you think: workspace vs. product
✓ **Safe** - Never corrupt production with bad data
✓ **Visual** - pgAdmin lets you see everything
✓ **Flexible** - Test in staging, deploy to production when ready
✓ **Quality-focused** - Automatic validation before merging
✓ **Auditable** - Every batch tracked and logged
✓ **PostgreSQL** - Best of both worlds: SQL + JSONB for flexibility

## Next Steps

1. **[ ]** Complete installation using [INSTALLATION_CHECKLIST.md](INSTALLATION_CHECKLIST.md)
2. **[ ]** Set up pgAdmin visual interface
3. **[ ]** Run test ingestion (5 companies)
4. **[ ]** Browse data in pgAdmin
5. **[ ]** Read [DATABASE_WORKFLOW.md](DATABASE_WORKFLOW.md)
6. **[ ]** Configure real search criteria
7. **[ ]** Start building your frontend

## Summary

**You have:**
- Complete PostgreSQL setup for Companies House data
- Two-database architecture (staging + production)
- Visual database browser (pgAdmin)
- Automated validation and merging
- Full documentation

**To get started:**
1. Install PostgreSQL and pgAdmin
2. Run `./setup_databases.sh`
3. Follow [pgadmin_quickstart.txt](pgadmin_quickstart.txt)
4. Ingest your first batch

**You're ready when:**
- Both databases created ✓
- pgAdmin connected ✓
- Test data flows staging → production ✓
- You can browse data visually ✓

---

**Ready?** → Open [INSTALLATION_CHECKLIST.md](INSTALLATION_CHECKLIST.md) and let's go!

**Questions?** → Everything is documented. Use the "File Guide" section above to find what you need.

Good luck! The system is designed to be intuitive - staging is your workspace, production is your product, and pgAdmin lets you see it all visually.
