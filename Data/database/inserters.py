"""
Data insertion utilities for staging database.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from psycopg2.extras import execute_values, Json

from database.connection import get_staging_db


class StagingInserter:
    """Handle data insertion into staging database."""

    def __init__(self, search_name: str):
        """
        Initialize inserter with a new batch.

        Args:
            search_name: Name of the search/cohort being ingested
        """
        self.db = get_staging_db()
        self.batch_id = f"{search_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        self.search_name = search_name
        self.companies_inserted = 0
        self.officers_inserted = 0

        # Start the ingestion log
        self._start_batch()

    def _start_batch(self) -> None:
        """Create ingestion log entry for this batch."""
        query = """
            INSERT INTO staging_ingestion_log (batch_id, search_name, status)
            VALUES (%(batch_id)s, %(search_name)s, 'running')
        """
        self.db.execute(query, {
            "batch_id": self.batch_id,
            "search_name": self.search_name,
        })
        print(f"[staging] Started batch: {self.batch_id}")

    def insert_company(self, company_data: dict) -> int | None:
        """
        Insert a company into staging.

        Args:
            company_data: Dict with company_number, company_name, and raw_data

        Returns:
            Staging company ID if successful, None otherwise
        """
        query = """
            INSERT INTO staging_companies (
                batch_id,
                company_number,
                company_name,
                company_status,
                locality,
                postal_code,
                region,
                country,
                sic_codes,
                raw_data
            ) VALUES (
                %(batch_id)s,
                %(company_number)s,
                %(company_name)s,
                %(company_status)s,
                %(locality)s,
                %(postal_code)s,
                %(region)s,
                %(country)s,
                %(sic_codes)s,
                %(raw_data)s
            )
            ON CONFLICT (batch_id, company_number) DO NOTHING
            RETURNING id
        """

        # Extract fields from raw_data (if provided)
        raw = company_data.get("raw_data", {})

        params = {
            "batch_id": self.batch_id,
            "company_number": company_data.get("company_number"),
            "company_name": company_data.get("company_name"),
            "company_status": company_data.get("company_status"),
            "locality": company_data.get("locality"),
            "postal_code": company_data.get("postal_code"),
            "region": company_data.get("region"),
            "country": company_data.get("country"),
            "sic_codes": company_data.get("sic_codes", []),
            "raw_data": Json(raw),  # Explicitly convert dict to JSONB
        }

        result = self.db.execute(query, params, fetch=True)
        if result:
            self.companies_inserted += 1
            return result[0]["id"]
        return None

    def insert_officers(self, staging_company_id: int, officers_data: list[dict]) -> int:
        """
        Insert officers for a company.

        Args:
            staging_company_id: ID from staging_companies table
            officers_data: List of officer dicts from API

        Returns:
            Number of officers inserted
        """
        if not officers_data:
            return 0

        # Prepare data for bulk insert
        values = []
        for officer in officers_data:
            # Extract address if present
            address = officer.get("address", {})

            values.append((
                staging_company_id,
                officer.get("company_number"),
                officer.get("name"),
                officer.get("officer_role"),
                officer.get("appointed_on"),
                officer.get("resigned_on"),
                officer.get("nationality"),
                officer.get("occupation"),
                address.get("address_line_1"),
                address.get("address_line_2"),
                address.get("locality"),
                address.get("postal_code"),
                address.get("country"),
                Json(officer),  # Explicitly convert dict to JSONB
            ))

        query = """
            INSERT INTO staging_officers (
                staging_company_id,
                company_number,
                officer_name,
                officer_role,
                appointed_on,
                resigned_on,
                nationality,
                occupation,
                address_line_1,
                address_line_2,
                locality,
                postal_code,
                country,
                raw_data
            ) VALUES %s
        """

        with self.db.get_cursor(dict_cursor=False) as cur:
            execute_values(cur, query, values)
            inserted_count = cur.rowcount

        self.officers_inserted += inserted_count
        return inserted_count

    def complete_batch(self, status: str = "completed", error_message: str | None = None) -> None:
        """
        Mark batch as complete in ingestion log.

        Args:
            status: 'completed' or 'failed'
            error_message: Error message if status='failed'
        """
        query = """
            UPDATE staging_ingestion_log
            SET completed_at = NOW(),
                companies_count = %(companies_count)s,
                status = %(status)s,
                error_message = %(error_message)s
            WHERE batch_id = %(batch_id)s
        """

        self.db.execute(query, {
            "batch_id": self.batch_id,
            "companies_count": self.companies_inserted,
            "status": status,
            "error_message": error_message,
        })

        print(f"[staging] Batch {status}: {self.batch_id}")
        print(f"  - Companies inserted: {self.companies_inserted}")
        print(f"  - Officers inserted: {self.officers_inserted}")

    def get_batch_stats(self) -> dict:
        """Get statistics for current batch."""
        query = """
            SELECT
                COUNT(*) as total_companies,
                COUNT(*) FILTER (WHERE needs_review = true) as needs_review,
                COUNT(*) FILTER (WHERE company_name IS NULL) as missing_names
            FROM staging_companies
            WHERE batch_id = %(batch_id)s
        """

        result = self.db.execute(query, {"batch_id": self.batch_id}, fetch=True)
        return result[0] if result else {}


def parse_companies_house_officer(api_response: dict, company_number: str) -> list[dict]:
    """
    Parse Companies House officers API response into list of officer dicts.

    Args:
        api_response: Response from /company/{number}/officers endpoint
        company_number: The company number for reference

    Returns:
        List of normalized officer dicts
    """
    if "error" in api_response:
        return []

    officers = []
    for item in api_response.get("items", []):
        officer = {
            "company_number": company_number,
            "name": item.get("name"),
            "officer_role": item.get("officer_role"),
            "appointed_on": item.get("appointed_on"),
            "resigned_on": item.get("resigned_on"),
            "nationality": item.get("nationality"),
            "occupation": item.get("occupation"),
            "address": item.get("address", {}),
        }
        officers.append(officer)

    return officers


def parse_companies_house_psc(api_response: dict, company_number: str) -> list[dict]:
    """
    Parse Companies House PSC (Persons with Significant Control) API response.

    Args:
        api_response: Response from /company/{number}/persons-with-significant-control
        company_number: The company number for reference

    Returns:
        List of PSC dicts formatted as officers
    """
    if "error" in api_response:
        return []

    pscs = []
    for item in api_response.get("items", []):
        psc = {
            "company_number": company_number,
            "name": item.get("name"),
            "officer_role": "person-with-significant-control",
            "appointed_on": item.get("notified_on"),
            "resigned_on": item.get("ceased_on"),
            "nationality": item.get("nationality"),
            "address": item.get("address", {}),
            "natures_of_control": item.get("natures_of_control", []),
        }
        pscs.append(psc)

    return pscs


if __name__ == "__main__":
    # Test inserter
    print("Testing staging inserter...")

    try:
        inserter = StagingInserter("test_search")

        # Test company insertion
        test_company = {
            "company_number": "12345678",
            "company_name": "Test Company Ltd",
            "company_status": "active",
            "locality": "Edinburgh",
            "raw_data": {"test": "data"},
        }

        company_id = inserter.insert_company(test_company)
        print(f"✓ Inserted test company, ID: {company_id}")

        # Test officer insertion
        test_officers = [
            {
                "company_number": "12345678",
                "name": "John Smith",
                "officer_role": "director",
                "appointed_on": "2020-01-01",
                "address": {"locality": "Edinburgh"},
            }
        ]

        officer_count = inserter.insert_officers(company_id, test_officers)
        print(f"✓ Inserted {officer_count} test officers")

        # Complete batch
        inserter.complete_batch()

        # Get stats
        stats = inserter.get_batch_stats()
        print(f"✓ Batch stats: {stats}")

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
