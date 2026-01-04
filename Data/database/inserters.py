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

    def insert_company(self, company_data: dict) -> str | None:
        """
        Insert a company into staging.

        Args:
            company_data: Dict with company_number, company_name, and raw_data

        Returns:
            Company Number if successful, None otherwise
        """
        query = """
            INSERT INTO staging_companies (
                company_number,
                company_name,
                company_type,
                company_status,
                locality,
                postal_code,
                address_line_1,
                address_line_2,
                region,
                country,
                sic_codes,
                incorporation_date,
                accounts_last_made_up_date,
                accounts_ref_date,
                accounts_next_due_date,
                account_category,
                returns_next_due_date,
                returns_last_made_up_date,
                num_mort_charges,
                num_mort_outstanding,
                num_mort_part_satisfied,
                previous_names,
                conf_stm_next_due_date,
                conf_stm_last_made_up_date,
                raw_data,
                batch_id
            ) VALUES (
                %(company_number)s,
                %(company_name)s,
                %(company_type)s,
                %(company_status)s,
                %(locality)s,
                %(postal_code)s,
                %(address_line_1)s,
                %(address_line_2)s,
                %(region)s,
                %(country)s,
                %(sic_codes)s,
                %(incorporation_date)s,
                %(accounts_last_made_up_date)s,
                %(accounts_ref_date)s,
                %(accounts_next_due_date)s,
                %(account_category)s,
                %(returns_next_due_date)s,
                %(returns_last_made_up_date)s,
                %(num_mort_charges)s,
                %(num_mort_outstanding)s,
                %(num_mort_part_satisfied)s,
                %(previous_names)s,
                %(conf_stm_next_due_date)s,
                %(conf_stm_last_made_up_date)s,
                %(raw_data)s,
                %(batch_id)s
            )
            ON CONFLICT (company_number) DO UPDATE SET
                company_name = EXCLUDED.company_name,
                company_type = EXCLUDED.company_type,
                company_status = EXCLUDED.company_status,
                locality = EXCLUDED.locality,
                postal_code = EXCLUDED.postal_code,
                address_line_1 = EXCLUDED.address_line_1,
                address_line_2 = EXCLUDED.address_line_2,
                region = EXCLUDED.region,
                country = EXCLUDED.country,
                sic_codes = EXCLUDED.sic_codes,
                incorporation_date = EXCLUDED.incorporation_date,
                accounts_last_made_up_date = EXCLUDED.accounts_last_made_up_date,
                accounts_ref_date = EXCLUDED.accounts_ref_date,
                accounts_next_due_date = EXCLUDED.accounts_next_due_date,
                account_category = EXCLUDED.account_category,
                returns_next_due_date = EXCLUDED.returns_next_due_date,
                returns_last_made_up_date = EXCLUDED.returns_last_made_up_date,
                num_mort_charges = EXCLUDED.num_mort_charges,
                num_mort_outstanding = EXCLUDED.num_mort_outstanding,
                num_mort_part_satisfied = EXCLUDED.num_mort_part_satisfied,
                previous_names = EXCLUDED.previous_names,
                conf_stm_next_due_date = EXCLUDED.conf_stm_next_due_date,
                conf_stm_last_made_up_date = EXCLUDED.conf_stm_last_made_up_date,
                raw_data = EXCLUDED.raw_data,
                batch_id = EXCLUDED.batch_id,
                last_updated = NOW()
            RETURNING company_number
        """

        # Extract fields from raw_data (if provided)
        raw = company_data.get("raw_data", {})

        params = {
            "company_number": company_data.get("company_number"),
            "company_name": company_data.get("company_name"),
            "company_type": company_data.get("company_type"),
            "company_status": company_data.get("company_status"),
            "locality": company_data.get("locality"),
            "postal_code": company_data.get("postal_code"),
            "address_line_1": company_data.get("address_line_1"),
            "address_line_2": company_data.get("address_line_2"),
            "region": company_data.get("region"),
            "country": company_data.get("country"),
            "sic_codes": company_data.get("sic_codes", []),
            "incorporation_date": company_data.get("incorporation_date"),
            "accounts_last_made_up_date": company_data.get("accounts_last_made_up_date"),
            "accounts_ref_date": company_data.get("accounts_ref_date"),
            "accounts_next_due_date": company_data.get("accounts_next_due_date"),
            "account_category": company_data.get("account_category"),
            "returns_next_due_date": company_data.get("returns_next_due_date"),
            "returns_last_made_up_date": company_data.get("returns_last_made_up_date"),
            "num_mort_charges": company_data.get("num_mort_charges"),
            "num_mort_outstanding": company_data.get("num_mort_outstanding"),
            "num_mort_part_satisfied": company_data.get("num_mort_part_satisfied"),
            "previous_names": company_data.get("previous_names"),
            "conf_stm_next_due_date": company_data.get("conf_stm_next_due_date"),
            "conf_stm_last_made_up_date": company_data.get("conf_stm_last_made_up_date"),
            "raw_data": Json(raw),
            "batch_id": self.batch_id,
        }

        result = self.db.execute(query, params, fetch=True)
        if result:
            self.companies_inserted += 1
            return result[0]["company_number"]
        return None

    def insert_officers(self, company_number: str, officers_data: list[dict]) -> int:
        """
        Insert officers for a company.

        Args:
            company_number: Company Number from staging_companies table
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
                company_number,
                officer.get("name"),
                officer.get("officer_role"),
                officer.get("appointed_on"),
                officer.get("resigned_on"),
                officer.get("nationality"),
                officer.get("nature_of_control"),
                address.get("address_line_1"),
                address.get("address_line_2"),
                address.get("locality"),
                address.get("postal_code"),
                address.get("country"),
                Json(officer),  # Explicitly convert dict to JSONB
            ))

        query = """
            INSERT INTO staging_officers (
                company_number,
                officer_name,
                officer_role,
                appointed_on,
                resigned_on,
                nationality,
                nature_of_control,
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
            "nature_of_control": None,
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
        natures = item.get("natures_of_control", [])
        nature_of_control = '|'.join(natures) if natures else None
        
        psc = {
            "company_number": company_number,
            "name": item.get("name"),
            "officer_role": "person-with-significant-control",
            "appointed_on": item.get("notified_on"),
            "resigned_on": item.get("ceased_on"),
            "nationality": item.get("nationality"),
            "address": item.get("address", {}),
            "nature_of_control": nature_of_control,
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

        if company_id:
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
