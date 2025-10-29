# Ingestion Workflow Configuration

This repository now uses a single configuration file to drive both the Companies House bulk downloads and the API-based enrichment jobs. The goal is to keep orchestration declarative so you can switch cohorts (e.g. Scotland, insurance, construction) by editing YAML rather than touching code.

## Key Directories
- `config/filters.yaml` — editable configuration containing defaults plus one entry per ingestion profile.
- `ingestion_config/loader.py` — loader and validation helpers that resolve inheritance, merge defaults, and expose strongly typed profile objects.
- `Data-injestion-workflows/Bulk-request-workflow/bulk-main.py` — bulk ingestion skeleton that reads enabled profiles and prints the actions it would take. Replace the TODO block with the real download/extract/transform logic.
- `Data-injestion-workflows/Api-request-workflow/api-main.py` — Companies House API enrichment skeleton; add the real HTTP client, rate limiter, and persistence inside the TODO block.

> Install the `PyYAML` dependency once with `pip install pyyaml` so the loader can parse the YAML file.

## Configuration Schema
`config/filters.yaml` contains two top-level keys:

- `defaults` — baseline values applied to every profile (jurisdictions, dataset lists, storage roots, scheduling hints, etc.).
- `profiles` — named configurations that can inherit from one another by adding `extends: <other-profile-name>`.

Example excerpt:

```yaml
defaults:
  selection:
    company_status: ["active"]
    company_types: ["ltd", "plc", "llp"]
  sources:
    bulk_download:
      include:
        - name: company_profile
          url: "https://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile.zip"
profiles:
  scotland_baseline:
    enabled: true
    selection:
      jurisdictions: ["scotland"]
    sources:
      bulk_download:
        include:
          - name: company_profile
            url_template: "https://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile-{date}.zip"
            date_format: "%Y-%m-%d"
            date_override: "2025-10-20"  # set to actual available snapshot date
          - name: persons_with_significant_control
            url_template: "https://download.companieshouse.gov.uk/PeopleWithSignificantControl-{date}.zip"
            date_format: "%Y-%m-%d"
            date_override: "2025-10-20"
  scotland_insurance:
    enabled: false
    extends: scotland_baseline
    selection:
      sic_codes:
        include: ["65120", "65202", "66220"]
```

### Profile Fields
- `enabled` — set to `true` to activate a profile for the next run; leave `false` to keep it documented but inactive.
- `description` — free-text notes that appear in the console output for quick context.
- `selection` — filters passed down to both bulk and API jobs (jurisdictions, SIC include/exclude lists, status flags, incorporation date window).
- `sources.bulk_download` — dataset list with download URLs/templates for the Companies House bulk dumps. Supports `url:` (single link) or `url_template:` with `date_format`, optional `date_override`, and optional `lookback_days`.
- `sources.api_enrichment` — endpoint list, delta window, and rate limits used by the API job. Set `enabled: false` if a profile only needs bulk data.
- `sources.third_party_enrichments` — placeholder list for additional enrichers (Apollo, OpenCorporates, custom scrapers).
- `storage` — output roots for raw, staging, and warehouse targets.
- `schedule` — cron/zone hints the orchestrator can use; they are not executed automatically by the scripts but let Airflow/Prefect pick up the settings.
- Successful downloads are zipped into `<raw_root>/<profile>_bundle.zip`, giving you one archive per run.

### How Inheritance Works
- Every profile starts with a deep copy of `defaults`.
- If `extends` is present, values are copied from the parent profile.
- The current profile’s keys override the inherited values.
- Cycles are detected and will raise a `ConfigError`.

## Loading Profiles in Code
Use the helpers from `ingestion_config`:

```python
from ingestion_config import iter_enabled_profiles

for profile in iter_enabled_profiles():
    print(profile.name, profile.selection)
```

Each `ProfileConfig` exposes the resolved dictionaries through `.selection`, `.sources`, `.storage`, and `.schedule`.

## Running the Workflows
1. Ensure `pyyaml` is installed in your virtual environment.
2. Toggle profiles in `config/filters.yaml` by setting `enabled: true/false`.
3. Run the bulk job skeleton:
   ```bash
   python3 Data-injestion-workflows/Bulk-request-workflow/bulk-main.py
   ```
4. Run the API enrichment skeleton:
   ```bash
   python3 Data-injestion-workflows/Api-request-workflow/api-main.py
   ```
5. Replace the TODO notes with real implementations (downloaders, S3/GCS clients, transformation steps) as you build out the pipelines.

## Extending the System
Follow this checklist when adding a new cohort or enrichment:
1. Add a new profile entry to `config/filters.yaml`. Use `extends` to inherit common defaults.
2. Set `enabled: true` only after you are ready for it to run.
3. Update the bulk workflow implementation to interpret any new selection knobs (e.g. SIC codes, custom filters).
4. Update the API workflow to handle additional endpoints or rate limits as defined in the profile.
5. When adding third-party enrichments, augment the profile with a block under `sources.third_party_enrichments`, then hook the new source into your orchestration code.

## Roadmap for Data Linking
Use the following high-level process to reach the linked-company vision:
1. **Bulk ingest** Companies House datasets into raw storage (zip → CSV/JSON → object store).
2. **Stage & normalise** into a warehouse (DuckDB, BigQuery, Snowflake) using repeatable SQL/transform scripts.
3. **Enrich** with the API for deltas (company profile, officer links, PSC, filing history).
4. **Augment** with third-party contact/financial data, storing provenance and confidence scores per source.
5. **Model** the warehouse into `company`, `person`, `company_person`, and `financial_snapshot` tables.
6. **Publish** materialised views that power your application search and profiles.

Every step above can reference the same configuration so that new regions or sectors only require edits to the YAML, keeping the code paths stable and easier to improve over time.
