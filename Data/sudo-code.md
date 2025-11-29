# sudo-code.md – Beginner Runbook

Goal: download Companies House data, focus it on Scotland (or any cohort you choose), and prepare for later enrichments.

---

## 1. One-time setup
1. Open a terminal inside the project folder (`Data-co`).
2. Activate the project’s Python environment:
   ````bash
   source bin/activate
   ````
3. Make sure the YAML parser is installed:
   ````bash
   pip install pyyaml
   ````

---

## 2. Pick the cohort (Scotland by default)
1. Open the file `config/filters.yaml`.
2. Scroll until you see the heading `profiles:` — this is where each dataset lives.
3. Under that heading, find the chunk that starts with `scotland_baseline:`.
4. Inside that chunk:
   - Make sure the line `enabled: true` is present (this turns the Scotland run on).
   - Make sure the line `jurisdictions: ["scotland"]` is there (this is the Scotland filter).
5. Want a different group? Copy the whole `scotland_baseline` chunk, paste it below, give it a new name, change the filters you care about (SIC codes, regions, etc.), and set `enabled: true` on the new chunk.

---

## 3. Run the bulk download script
Purpose: grab the Companies House bulk files and stage them locally.

Command:
````bash
python3 Data-injestion-workflows/Bulk-request-workflow/bulk-main.py
````

What happens:
- Reads all profiles with `enabled: true`.
- For each profile, prints the key settings and downloads the datasets listed under `sources.bulk_download.include`.
- Files land under `storage.raw_root` (defaults to `project_data/companies_house/raw`). For dated files, set `date_override` in `config/filters.yaml` to the snapshot date you want (e.g., `2025-10-20`) so it only requests that specific file. If `date_override` is blank, the script will fall back to trying the most recent few days.
- After the downloads finish, the script creates a single zip archive for quick sharing at `<storage.raw_root>/<profile>_bundle.zip`.

---

## 4. Run the API enrichment script
Purpose: fill in extra details (directors, filings, etc.) that the bulk dumps don’t contain.

Command:
````bash
python3 Data-injestion-workflows/Api-request-workflow/api-main.py
````

What happens:
- Looks at `sources.api_enrichment` for each enabled profile.
- Prints the endpoints, date window, and rate limits it will use.
- Once the TODOs are implemented, it should call the Companies House API and save responses near your staging data.

---

## 5. Where the Scotland filter comes into play
- The config loader passes `selection.jurisdictions = ["scotland"]` into both scripts.
- When you implement the data-handling code, filter rows where the company is registered in Scotland (e.g., `registered_office_address.region == "Scotland"`).
- Store the filtered results in the staging folder for that profile. Example from the default config:
  - Raw files: `project_data/companies_house_scotland/raw`
  - Staging files: `project_data/companies_house_scotland/staging`

---

## 6. Check the results
1. After running the scripts, inspect the staging directory defined in `storage.staging_root`.
2. Confirm the files include only the companies you expect (e.g., all in Scotland).
3. If something looks wrong, adjust filters in `config/filters.yaml` and rerun the scripts.

---

## 7. Make your own query (example)
Want Scottish construction companies?

1. Duplicate the block `scotland_baseline` in `config/filters.yaml`.
2. Rename it to `scotland_construction`.
3. Set `enabled: true`.
4. Add SIC codes that represent construction under `selection.sic_codes.include`.
5. Run the bulk/API scripts again. That new cohort will now be included.

---

## 8. Need more cohorts?
- **New region**: change `selection.jurisdictions` to another area (e.g., `["england"]`).
- **New industry**: update the SIC include/exclude lists.
- **Pause a cohort**: set `enabled: false`.
- **Add third-party sources**: extend `sources.third_party_enrichments` with provider info (for future use).

---

## 9. Quick troubleshooting
- Forgot to activate the environment? Scripts can’t find dependencies. Run `source bin/activate`.
- YAML typo? Scripts will throw a `ConfigError`. Double-check spacing/indentation.
- No data downloaded? Check your internet access and update the `date_override` (or `lookback_days`) inside `config/filters.yaml` so the URLs match the actual snapshot names.

---

You’re ready to customise the config, run the scripts, and shape the pipeline for any region or sector you need.
