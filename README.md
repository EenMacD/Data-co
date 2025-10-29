# Data-co Ingestion & Linking Playbook

This repository scaffolds a data platform that links people to companies using Companies House bulk data, Companies House APIs, and optional third-party enrichments. Everything is driven from a single YAML configuration so you can customise cohorts (Scotland, insurance, construction, etc.) without touching code.

Use this README as your living manual. Every instruction is written so you can copy/paste it into prompts or runbooks, then adjust the placeholders to fit new targets.

---

## How to Run a Search (Simple Guide)

Follow these steps to get a list of companies based on your own criteria.

### Step 1: Get Your API Key

1.  You need an API key from Companies House to run this project. You can register for one for free on the [Companies House developer site](https://developer.company-information.service.gov.uk/user/register).
2.  Once you have your key, open the `.env` file in the main project folder.
3.  Paste your key into the file, replacing `YOUR_API_KEY_HERE`. It should look like this:
    ```
    COMPANIES_HOUSE_API_KEY="abc123xyz456"
    ```

### Step 2: Define Your Search Criteria

1.  Open the `config/filters.yaml` file. This is where you tell the script what to look for.
2.  Find the `search_criteria` section at the top and edit it:

    *   `name`: Give your search a unique, descriptive name (e.g., `edinburgh_bakeries` or `manchester_logistics`). This name will be used for the output folder.
    *   `locality`: The city or town you want to search in (e.g., `Edinburgh`).
    *   `company_status`: It's best to leave this as `active`.
    *   `industry_codes`: A list of [SIC codes](https://resources.companieshouse.gov.uk/sic/) that define the industry you're interested in. For example, for bakeries you might use `["1071"]`.
    *   `limit`: (Optional) Add this line to limit how many companies are returned. This is useful for testing. For example: `limit: 50`.

    **Example:** To find 50 active bakeries in Edinburgh, your `search_criteria` would look like this:
    ```yaml
    search_criteria:
      name: "edinburgh_bakeries"
      selection:
        locality: "Edinburgh"
        company_status: "active"
        industry_codes:
          include: ["1071"] # Code for "manufacture of bread"
        limit: 50
    ```

### Step 3: Run the Script

1.  Open your terminal in the project folder.
2.  Run the following command:
    ```bash
    bash run-data.sh
    ```
    The script will start downloading the main data file and then begin enriching it with API calls.

### Step 4: Get Your Results

1.  Once the script is finished, look in the `project_data/` folder.
2.  You will find a new folder named after the `name` you set in Step 2 (e.g., `project_data/edinburgh_bakeries/`).
3.  Inside that, open the `json/` folder.
4.  Your results will be in a single file named `enriched_data.json`. This file contains a list of all the companies that matched your search.

---

## 1. Quick Start Checklist
1. **Clone & activate environment**
   ```bash
   git clone <repo-url>
   cd Data-co
   source bin/activate    # or your preferred virtualenv command
   pip install -r requirements.txt  # ensure PyYAML is included
   ```
2. **Review configuration** — open `config/filters.yaml`; note defaults and existing profiles.
3. **Toggle desired profiles** — set `enabled: true` for cohorts you want to run.
4. **Dry-run workflows**
   ```bash
   python Data-injestion-workflows/Bulk-request-workflow/bulk-main.py
   python Data-injestion-workflows/Api-request-workflow/api-main.py
   ```
5. **Fill in TODO logic** — add real download, storage, and API calls where indicated.
6. **Check `docs/config.md`** — use as a reference for schema, naming, and extension guidelines.

---

## 2. Architecture Overview
- **Configuration layer** (`config/filters.yaml`, `ingestion_config/`): declarative control over cohorts, data sources, and storage targets. Profiles inherit defaults and can compose additional filters.
- **Bulk ingestion workflow** (`Bulk-request-workflow/bulk-main.py`): reads enabled profiles, then orchestrates download → extract → staging (currently stubbed).
- **API enrichment workflow** (`Api-request-workflow/api-main.py`): enriches staged companies via Companies House API delta scans and prepares them for third-party augmentations.
- **Warehouse modelling** (planned): tables for `company`, `person`, `company_person_bridge`, `financial_snapshot`, and `contact_enrichment` views.

Use this architecture to power an app that lets users search a company, see its financials, and contact key individuals with confidence scores.

---

## 3. Configuration Anatomy (`config/filters.yaml`)

### Defaults block
Applied to every profile:
- `selection`: geography, status, company type, SIC filters, incorporation windows.
- `sources`: datasets to fetch from bulk dumps, API behaviour (endpoints, rate limits, delta windows), third-party enrichers.
- `storage`: output directories for raw and staging layers, plus target warehouse schema.
- `schedule`: cron/timezone hints for orchestrators (Airflow, Prefect, etc.).

### Profiles block
Each profile contains:
- `enabled`: toggle on/off.
- `description`: short human-readable summary (printed by workflows).
- `extends`: inherit from another profile; the loader performs deep merging and catches loops.
- Overrides under `selection`, `sources`, `storage`, `schedule`.

### Copy-paste profile template
```yaml
profiles:
  <profile_name>:
    enabled: true             # set false until ready
    description: >
      <one-line summary of target>
    extends: <parent_profile> # optional
    selection:
      jurisdictions: ["<country_or_region_code>"]
      sic_codes:
        include: ["<sic_code>"]
        exclude: []
      company_status: ["active", "dissolved"]
      company_types: ["ltd", "llp"]
      incorporation_date:
        min: "2018-01-01"
        max: null
    sources:
      bulk_download:
        include: ["company_profile", "officers", "persons_with_significant_control"]
      api_enrichment:
        enabled: true
        endpoints: ["company_profile", "officers", "filing_history"]
        request_rate_per_minute: 30
        delta_window_days: 5
      third_party_enrichments:
        - name: apollo
          enabled: false
          config:
            api_key_env: APOLLO_API_KEY
            search_mode: "bulk"
    storage:
      raw_root: "data/raw/<profile>"
      staging_root: "data/staging/<profile>"
      warehouse_schema: "analytics_<profile>"
    schedule:
      cron: "0 4 * * *"
      timezone: "Europe/London"
```

---

## 4. Workflow Execution Prompts

Use these prompt templates to drive conversations with Codex or write runbooks. Replace placeholders in ALL CAPS.

### 4.1 Bulk Ingestion
- *Prompt*: “Download Companies House bulk data for PROFILE_NAME, storing raw files under RAW_ROOT and extracting to STAGING_ROOT. Only include datasets DATASET_LIST and filter to jurisdictions JURISDICTIONS.”
- *Manual run*:
  ```bash
  PROFILE_NAME=scotland_baseline
  python Data-injestion-workflows/Bulk-request-workflow/bulk-main.py
  ```
- *Implementation TODOs*:
  - Build downloader: stream zip/tar files, verify checksums, log versions.
  - Extract to staging: convert to Parquet or CSV, ensure column typing.
  - Apply filters: load `profile.selection` into SQL/transform scripts.

### 4.2 API Enrichment
- *Prompt*: “For PROFILE_NAME, call Companies House endpoints ENDPOINTS for records updated in the last DELTA_DAYS days, observing RATE_LIMIT req/min. Persist responses to STAGING_ROOT/api.”
- *Manual run*:
  ```bash
  PROFILE_NAME=scotland_baseline
  python Data-injestion-workflows/Api-request-workflow/api-main.py
  ```
- *Implementation TODOs*:
  - Rate limiter keyed by `request_rate_per_minute`.
  - Delta logic using `window_start` timestamp.
  - Persistence module writing to raw JSON + processed tables.

### 4.3 Third-Party Enrichment (Future)
- *Prompt*: “Augment PROFILE_NAME with APOLLO contacts, matching officer names to search results, capturing emails/phones and confidence scores. Store provenance under `contact_enrichment`.”
- Implementation steps:
  1. Extend profile `sources.third_party_enrichments` with provider config.
  2. Implement provider adapter map (name → callable).
  3. Merge contact details into person dimension with dedupe heuristics.

---

## 5. Building the Linked Data Model

### 5.1 Core Tables
| Table | Key | Purpose |
| --- | --- | --- |
| `company` | `company_number` | Canonical company metadata, location, status, SIC hierarchy |
| `person` | `person_id` | Normalised officers/beneficial owners with deduped identities |
| `company_person` | `(company_number, person_id, role, start_date)` | Relationship history between people and companies |
| `financial_snapshot` | `(company_number, period_end)` | Financials and KPIs sourced from filings and third-party data |
| `contact_enrichment` | `(person_id, source)` | Email/phone pairs, confidence scores, last verification date |

### 5.2 Recommended Steps
1. Stage raw Companies House data into object storage (`raw_root`).
2. Use DuckDB/Spark to normalise into schema tables (`staging_root` → warehouse).
3. Apply matching routines to deduplicate officers and align them with companies.
4. Derive analytics views (`company_profile_view`, `person_activity_view`) for the application layer.

---

## 6. Operating the System

### 6.1 Local Development
- Always work from a copy of `config/filters.yaml`; commit changes with descriptive messages.
- Run workflow scripts in “dry-run” mode first (print statements). Add `--execute` flag later if you implement it.
- Keep raw dumps out of git; use `.gitignore` to protect `data/` directories.

### 6.2 Orchestrator Integration
1. Create an Airflow/Prefect/Dagster DAG that imports `ingestion_config.iter_enabled_profiles()`.
2. Iterate enabled profiles; enqueue bulk job followed by API job.
3. Pass config dictionaries downstream to tasks so they know where to read/write.
4. Log results per profile for auditing (e.g., counts of companies processed, API requests used).

### 6.3 Quality Assurance
- Add dbt or Great Expectations tests for referential integrity and duplicate detection.
- Maintain source-to-target lineage docs for each dataset.
- Implement anomaly checks (e.g., sudden drop in active companies, missing SIC codes).

---

## 7. Prompt Customisation Cookbook

Use these patterns to build new prompts quickly:

| Goal | Prompt Template |
| --- | --- |
| Create new region profile | “Add profile PROFILE_NAME extending PARENT_PROFILE, restrict to jurisdictions JURISDICTIONS, include SIC codes SIC_LIST, store raw data in RAW_PATH.” |
| Adjust API cadence | “Update PROFILE_NAME so `sources.api_enrichment.delta_window_days` = DAYS and rate limit = RATE.” |
| Prepare sector-specific outreach | “Generate prompt to extract officer contacts for PROFILE_NAME, prioritise roles ROLE_LIST, export to CRM_PATH.” |
| Trigger incremental reload | “Design workflow steps to re-ingest PROFILE_NAME since DATE, including bulk re-download and API delta sweep.” |
| Integrate new enrichment provider | “Outline config and code changes needed to add PROVIDER_NAME under `third_party_enrichments` for PROFILE_NAME.” |

Feel free to drop any of these directly into Codex, swapping placeholders for real values.

---

## 8. Roadmap & Enhancements
- **Automated delta detection** — track `last_updated` per dataset and skip unchanged companies.
- **Officer identity resolution** — build probabilistic matching for name/address variations.
- **Confidence scoring** — combine Companies House, third-party, and internal signals.
- **Search API** — expose `/companies`, `/people`, `/profiles` endpoints hitting warehouse views.
- **UI prototyping** — wire a simple React/Next.js frontend to browse companies, directors, and contact info.

---

## 9. Reference
- Companies House bulk data docs: <https://developer.company-information.service.gov.uk/documentation/bulk>
- Companies House REST API: <https://developer.company-information.service.gov.uk/>
- SIC codes reference: <https://resources.companieshouse.gov.uk/sic/>

---

### Need a new instruction?
When you need to customise behaviour, articulate it as:
> “For PROFILE_NAME, do ACTION with PARAMETERS, storing results at TARGET.”

Then update `config/filters.yaml`, run the relevant workflow, and confirm outputs. Repeat to expand coverage across the UK and your target industries. This README is intentionally verbose—edit sections or add new templates as you refine the process.***
