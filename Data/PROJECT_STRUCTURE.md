# Data Project Structure

This document outlines the directory structure of the Data component after refactoring to separate concerns by table and environment.

## Directory Tree

```
Data/
├── .env.example                # Environment variables template
├── README.md                   # Project documentation
├── run.sh                      # Master run script for the Data component
├── setup_databases.sh          # Database setup script (Staging & Production)
│
├── database/                   # Shared database utilities
│   ├── services/
│   │   ├── connection.py       # Database connection management
│   │   ├── merge_to_production.py # Script to merge staging to production of 
│   │   └── validators.py       # Data validation logic
│   └── migrations/             # Database migration scripts (if any)
│
├── staging/                    # Staging environment resources
│   ├── common/                 # Common staging resources
│   │   ├── parsers/
│   │   │   └── base_parser.py  # Base parser class
│   │   ├── schemas/
│   │   │   ├── 00_setup.sql    # Initial setup (functions, etc.)
│   │   │   ├── 01_ingestion_log.sql
│   │   │   └── apply_staging.sh # Script to apply all staging schemas
│   │   ├── services/
│   │   │   ├── base_loader.py  # Base loader class
│   │   │   ├── download_manager.py
│   │   │   ├── file_discovery.py
│   │   │   └── ingestion_worker.py # Main orchestration worker
│   │   └── web/                # Ingestion UI (Flask App)
│   │       ├── run.sh          # UI runner script
│   │       ├── app.py          # Flask application
│   │       ├── templates/      # HTML templates
│   │       └── static/         # CSS/JS assets
│   │
│   ├── companies/              # Resources for Company data
│   │   ├── parsers/
│   │   │   └── company_parser.py
│   │   ├── schemas/
│   │   │   └── 02_companies.sql
│   │   └── services/
│   │       └── loader.py       # CompanyLoader class
│   │
│   ├── psc/                    # Resources for PSC (Officers) data
│   │   ├── parsers/
│   │   │   └── psc_parser.py
│   │   ├── schemas/
│   │   │   └── 03_officers.sql
│   │   └── services/
│   │       └── loader.py       # PSCLoader class
│   │
│   └── accounts/               # Resources for Accounts (Financials) data
│       ├── parsers/
│       │   ├── accounts_parser.py
│       │   ├── accounts_tags/  # XBRL tag configs
│       │   └── tag_manager.py
│       ├── schemas/
│       │   └── 04_financials.sql
│       └── services/
│           └── loader.py       # AccountsLoader class
│
└── production/                 # Production environment resources
    ├── common/
    │   └── schemas/
    │       ├── 00_setup.sql
    │       ├── 01_merge_log.sql
    │       └── apply_production.sh # Script to apply all production schemas
    ├── companies/
    │   └── schemas/
    │       └── 02_companies.sql
    ├── psc/
    │   └── schemas/
    │       └── 03_officers.sql
    └── accounts/
        └── schemas/
            └── 04_financials.sql
```

## Key Modules

### Services
- **IngestionWorker** (`staging/common/services/ingestion_worker.py`): Orchestrates the download and loading process. Now uses specific loaders for each data type.
- **Loaders** (`staging/*/services/loader.py`): Responsible for loading parsed data into the staging database. Inherit from `BaseLoader`.
- **MergeService** (`database/services/merge_to_production.py`): Handles the logic for merging data from Staging to Production.

### Parsers
- **Parsers** (`staging/*/parsers/*_parser.py`): specialized parsers for Companies, PSC, and Accounts data. Inherit from `BaseParser`.

### Database
- **Schemas**: SQL files are now co-located with their respective modules in `schemas/` directories.
- **Apply Scripts**: `apply_staging.sh` and `apply_production.sh` iterate through these modular schema folders to set up the databases.
