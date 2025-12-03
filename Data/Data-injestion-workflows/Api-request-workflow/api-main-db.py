from __future__ import annotations

import csv
import json
import os
import sys
import time
import threading
from pathlib import Path
from zipfile import ZipFile
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from ingestion_config.loader import load_config
from database.inserters import (
    StagingInserter,
    parse_companies_house_officer,
    parse_companies_house_psc,
)
from incremental_enrichment import get_incremental_work_plan, print_work_plan


class RateLimiter:
    """Thread-safe rate limiter for concurrent API requests."""

    def __init__(self, max_per_minute):
        self.max_per_minute = max_per_minute
        self.timestamps = deque()
        self.lock = threading.Lock()
        self.total_requests = 0

    def wait_if_needed(self):
        """Block if rate limit would be exceeded, ensuring thread-safe operation."""
        while True:
            sleep_time = 0
            with self.lock:
                now = time.time()
                # Remove timestamps older than 1 minute
                while self.timestamps and self.timestamps[0] < now - 60:
                    self.timestamps.popleft()

                # If we can make a request, record it and return
                if len(self.timestamps) < self.max_per_minute:
                    self.timestamps.append(now)
                    self.total_requests += 1
                    return

                # Calculate wait time - need to wait until oldest request expires
                if self.timestamps:
                    sleep_time = max(0, 60 - (now - self.timestamps[0]) + 0.1)

            # Sleep OUTSIDE the lock to allow other threads to proceed
            if sleep_time > 0:
                time.sleep(sleep_time)
            else:
                # Small sleep to prevent busy-waiting
                time.sleep(0.01)
            
            if sleep_time > 1.0:
                print(f"[RateLimiter] Pausing for {sleep_time:.1f} seconds to respect rate limit...")


def fetch_company_data(company_info, api_key, endpoints, rate_limiter):
    """
    Fetch API data for a single company with rate limiting.

    Args:
        company_info: Dict with company data (must have CompanyNumber and CompanyName keys)
        api_key: Companies House API key
        endpoints: List of API endpoints to call
        rate_limiter: RateLimiter instance for coordinating requests across threads

    Returns:
        Tuple of (company_info, api_responses) where api_responses is a dict of endpoint -> response
    """
    company_number = company_info["CompanyNumber"]
    company_name = company_info["CompanyName"]

    session = requests.Session()
    session.auth = (api_key, "")

    api_responses = {}
    for endpoint in endpoints:
        rate_limiter.wait_if_needed()
        url = f"https://api.companieshouse.gov.uk/company/{company_number}/{endpoint}"
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            api_responses[endpoint] = response.json()
        except requests.exceptions.RequestException as e:
            api_responses[endpoint] = {"error": str(e)}

    session.close()  # Clean up session
    return company_info, api_responses


def run_api_enrichment_to_db() -> None:
    """
    Execute the Companies House API enrichment workflow.
    Now inserts data into staging PostgreSQL database instead of JSON files.
    """
    load_dotenv(ROOT / ".env")
    api_key = os.getenv("COMPANIES_HOUSE_API_KEY")
    if not api_key or api_key == "YOUR_API_KEY_HERE":
        print("! API key not found. Please set COMPANIES_HOUSE_API_KEY in .env")
        sys.exit(1)

    config = load_config()
    criteria = config.search_criteria
    tech_config = config.technical_config
    search_name = criteria.get("name", "default_search")

    print(f"[api] starting enrichment for search: {search_name}")

    companies_df = _find_and_filter_companies(criteria, tech_config)
    if companies_df.empty:
        print("  - No companies matched the filter criteria. Nothing to do.")
        print(f"[api] completed search: {search_name}")
        return

    api_config = tech_config.get("api_enrichment", {})
    endpoints = api_config.get("endpoints", [])
    rate_limit = api_config.get("request_rate_per_minute", 120)
    incremental_mode = api_config.get("incremental_mode", False)
    max_workers = 60  # Number of concurrent workers

    # Initialize database inserter and rate limiter
    inserter = StagingInserter(search_name)
    rate_limiter = RateLimiter(rate_limit)

    # Extract SIC codes BEFORE converting to dict to preserve list type
    companies_df["sic_codes_extracted"] = companies_df.apply(_extract_sic_codes, axis=1)

    # Convert DataFrame to list of dicts for parallel processing
    companies = companies_df.to_dict('records')

    # Handle incremental mode
    if incremental_mode:
        print("\n[api] INCREMENTAL MODE ENABLED")
        enabled_features = {
            'fetch_officers': 'officers' in endpoints or 'persons-with-significant-control' in endpoints,
            'fetch_financials': False  # Will be enabled when financial workflow is ready
        }

        work_plan = get_incremental_work_plan(companies, enabled_features)
        print_work_plan(work_plan)

        # Only process new companies + companies that need updates
        companies_to_process = work_plan['new_companies']

        # For companies needing updates, we'll fetch only missing endpoints
        print(f"  - Skipping {work_plan['stats']['up_to_date']} companies that are up-to-date")

    else:
        companies_to_process = companies
        work_plan = None

    total_companies = len(companies_to_process)

    print(f"  - Processing {total_companies} companies with {max_workers} concurrent workers")
    print(f"  - Rate limit: {rate_limit} requests/minute")

    try:
        # Use ThreadPoolExecutor for concurrent processing
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all fetch tasks
            futures = {
                executor.submit(
                    fetch_company_data,
                    company,
                    api_key,
                    endpoints,
                    rate_limiter
                ): idx for idx, company in enumerate(companies_to_process)
            }

            # Process results as they complete
            completed = 0
            start_time = time.time()
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    company_info, api_responses = future.result()
                    company_number = company_info["CompanyNumber"]
                    company_name = company_info["CompanyName"]

                    completed += 1
                    elapsed = time.time() - start_time
                    rate = completed / elapsed if elapsed > 0 else 0
                    eta = (total_companies - completed) / rate if rate > 0 else 0

                    print(
                        f"  ({completed}/{total_companies}) Completed API fetch: {company_number} ({company_name}) "
                        f"[{rate:.1f} companies/sec, ETA: {eta/60:.1f} min]"
                    )

                    # Insert company into staging database
                    company_data = {
                        "company_number": company_number,
                        "company_name": company_name,
                        "company_status": company_info.get("CompanyStatus", "unknown"),
                        "locality": company_info.get("RegAddress.PostTown"),
                        "postal_code": company_info.get("RegAddress.PostCode"),
                        "region": company_info.get("RegAddress.County"),
                        "country": company_info.get("RegAddress.Country"),
                        "sic_codes": company_info.get("sic_codes_extracted", []),
                        "raw_data": api_responses,  # Store all API responses as JSONB
                    }

                    staging_company_id = inserter.insert_company(company_data)

                    if not staging_company_id:
                        print(f"    ! Company {company_number} already exists in this batch")
                        continue

                    # Parse and insert officers
                    officers = []
                    if "officers" in api_responses and "error" not in api_responses["officers"]:
                        officers = parse_companies_house_officer(
                            api_responses["officers"], company_number
                        )

                    # Parse and insert PSCs
                    if (
                        "persons-with-significant-control" in api_responses
                        and "error" not in api_responses["persons-with-significant-control"]
                    ):
                        pscs = parse_companies_house_psc(
                            api_responses["persons-with-significant-control"], company_number
                        )
                        officers.extend(pscs)

                    if officers:
                        officer_count = inserter.insert_officers(staging_company_id, officers)
                        print(f"    ✓ Inserted {officer_count} officers/PSCs")

                except Exception as e:
                    print(f"    ✗ Error processing company at index {idx}: {e}")

        # Mark batch as complete
        inserter.complete_batch(status="completed")

        # Show batch statistics
        stats = inserter.get_batch_stats()
        print(f"\n[api] Batch statistics:")
        print(f"  - Total companies: {stats.get('total_companies', 0)}")
        print(f"  - Needs review: {stats.get('needs_review', 0)}")
        print(f"  - Missing names: {stats.get('missing_names', 0)}")

    except Exception as e:
        print(f"\n[api] Error during ingestion: {e}")
        inserter.complete_batch(status="failed", error_message=str(e))
        raise

    print(f"\n[api] completed search: {search_name}")


def _find_and_filter_companies(criteria: dict, tech_config: dict) -> pd.DataFrame:
    """
    Finds the bulk company snapshot, filters it, and returns a DataFrame
    with CompanyNumber and CompanyName.
    """
    search_name = criteria.get("name", "default_search")
    base_path = _resolve_path(
        tech_config.get("storage", {}).get("base_path", "project_data/default")
    )
    raw_root = base_path.with_name(base_path.name.format(name=search_name)) / "raw"
    company_profile_dir = raw_root / "company_profile"

    if not company_profile_dir.is_dir():
        print(f"  ! Bulk company data not found at {company_profile_dir}")
        print("  ! Please run the bulk download workflow first.")
        return pd.DataFrame()

    try:
        latest_zip = max(company_profile_dir.glob("*.zip"), key=os.path.getmtime)
        print(f"  - Reading bulk data from: {latest_zip.name}")
    except ValueError:
        print(f"  ! No bulk ZIP file found in {company_profile_dir}")
        return pd.DataFrame()

    selection = criteria.get("selection", {})
    locality = selection.get("locality")
    status = selection.get("company_status")
    sic_include = selection.get("industry_codes", {}).get("include", [])
    limit = selection.get("limit")

    print(
        f"  - Applying filters: locality='{locality or 'any'}', status='{status or 'any'}', sic_codes='{sic_include or 'any'}'"
    )

    all_chunks = []
    with ZipFile(latest_zip, "r") as archive:
        csv_filename = next(
            (name for name in archive.namelist() if name.lower().endswith(".csv")), None
        )
        if not csv_filename:
            print(f"  ! No CSV file found in {latest_zip.name}")
            return pd.DataFrame()

        with archive.open(csv_filename) as csv_file:
            chunk_iterator = pd.read_csv(
                csv_file, chunksize=100000, dtype=str, low_memory=False
            )

            for chunk in chunk_iterator:
                chunk.columns = chunk.columns.str.strip()
                filtered_chunk = chunk.copy()
                if locality:
                    if "RegAddress.PostTown" in filtered_chunk.columns:
                        filtered_chunk = filtered_chunk[
                            filtered_chunk["RegAddress.PostTown"].str.contains(
                                locality, case=False, na=False
                            )
                        ]
                if status:
                    if "CompanyStatus" in filtered_chunk.columns:
                        filtered_chunk = filtered_chunk[
                            filtered_chunk["CompanyStatus"].str.lower() == status.lower()
                        ]
                if sic_include:
                    sic_cols = [
                        f"SICCode.SicText_{i}"
                        for i in range(1, 5)
                        if f"SICCode.SicText_{i}" in filtered_chunk.columns
                    ]
                    if sic_cols:
                        mask = filtered_chunk[sic_cols].apply(
                            lambda row: any(
                                str(cell).startswith(tuple(sic_include)) for cell in row
                            ),
                            axis=1,
                        )
                        filtered_chunk = filtered_chunk[mask]
                if not filtered_chunk.empty:
                    all_chunks.append(filtered_chunk)

    if not all_chunks:
        return pd.DataFrame()

    final_df = pd.concat(all_chunks)
    final_df.columns = final_df.columns.str.strip()
    print(f"  - Found {len(final_df)} companies matching criteria.")

    if limit:
        print(f"  - Applying limit of {limit} companies.")
        final_df = final_df.head(limit)
    else:
        print(f"  - No limit configured (processing all {len(final_df)} matches)")

    # Return more columns for database insertion
    return_cols = ["CompanyNumber", "CompanyName"]
    optional_cols = [
        "CompanyStatus",
        "RegAddress.PostTown",
        "RegAddress.PostCode",
        "RegAddress.County",
        "RegAddress.Country",
    ]

    # Add SIC code columns
    for i in range(1, 5):
        col = f"SICCode.SicText_{i}"
        if col in final_df.columns:
            optional_cols.append(col)

    # Include columns that exist
    for col in optional_cols:
        if col in final_df.columns:
            return_cols.append(col)

    return final_df[return_cols]


def _extract_sic_codes(row: pd.Series) -> list[str]:
    """Extract SIC codes from CSV row into array."""
    sic_codes = []
    for i in range(1, 5):
        col = f"SICCode.SicText_{i}"
        if col in row and pd.notna(row[col]):
            sic_code = str(row[col]).split(" - ")[0].strip()  # Extract code before description
            if sic_code and sic_code != "nan":
                sic_codes.append(sic_code)
    return sic_codes


def _resolve_path(path_value) -> Path:
    path = Path(path_value)
    return path if path.is_absolute() else ROOT / path


def main() -> None:
    run_api_enrichment_to_db()


if __name__ == "__main__":
    main()
