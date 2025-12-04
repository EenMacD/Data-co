"""
Incremental enrichment workflow.
Only fetches missing data for existing companies to save time and avoid duplicates.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List, Set

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from database.connection import get_staging_db


def get_existing_companies() -> Dict[str, dict]:
    """
    Get all existing companies from staging database.

    Returns:
        Dict mapping company_number to company data with flags for what data exists
    """
    db = get_staging_db()

    query = """
        SELECT
            sc.id,
            sc.company_number,
            sc.company_name,
            sc.batch_id,
            (SELECT COUNT(*) FROM staging_officers WHERE staging_company_id = sc.id) as officer_count,
            (SELECT COUNT(*) FROM staging_financials WHERE staging_company_id = sc.id) as financial_count
        FROM staging_companies sc
    """

    results = db.execute(query, fetch=True)

    companies_map = {}
    for row in results:
        companies_map[row['company_number']] = {
            'id': row['id'],
            'company_number': row['company_number'],
            'company_name': row['company_name'],
            'batch_id': row['batch_id'],
            'has_officers': row['officer_count'] > 0,
            'has_financials': row['financial_count'] > 0,
        }

    return companies_map


def determine_missing_data(existing_companies: Dict[str, dict], enabled_features: dict) -> Dict[str, List[str]]:
    """
    Determine what data is missing for each company.

    Args:
        existing_companies: Map of company_number to existing data
        enabled_features: Dict with 'fetch_officers', 'fetch_financials' flags

    Returns:
        Dict mapping company_number to list of missing endpoints
    """
    missing_data = {}

    for company_number, data in existing_companies.items():
        missing_endpoints = []

        # Check if we need officers data
        if enabled_features.get('fetch_officers') and not data['has_officers']:
            missing_endpoints.extend(['officers', 'persons-with-significant-control'])

        # Check if we need financials data
        if enabled_features.get('fetch_financials') and not data['has_financials']:
            missing_endpoints.append('filing-history')

        if missing_endpoints:
            missing_data[company_number] = missing_endpoints

    return missing_data


def filter_new_companies(all_companies: List[dict], existing_companies: Dict[str, dict]) -> List[dict]:
    """
    Filter out companies that already exist in the database.

    Args:
        all_companies: List of all companies from bulk data
        existing_companies: Map of existing company numbers

    Returns:
        List of new companies that don't exist yet
    """
    new_companies = []
    for company in all_companies:
        company_number = company.get('CompanyNumber')
        if company_number and company_number not in existing_companies:
            new_companies.append(company)

    return new_companies


def get_incremental_work_plan(all_companies: List[dict], enabled_features: dict) -> dict:
    """
    Create a work plan for incremental enrichment.

    Args:
        all_companies: List of all companies from filters
        enabled_features: Dict with feature flags

    Returns:
        Dict with:
            - new_companies: Companies to add completely
            - update_companies: Existing companies needing data updates
            - missing_data: Map of company_number to missing endpoints
    """
    existing = get_existing_companies()

    # Split into new vs existing
    new_companies = filter_new_companies(all_companies, existing)

    # For existing companies, determine what's missing
    existing_numbers = {c.get('CompanyNumber') for c in all_companies}
    companies_to_update = {num: data for num, data in existing.items() if num in existing_numbers}

    missing_data = determine_missing_data(companies_to_update, enabled_features)

    return {
        'new_companies': new_companies,
        'update_companies': companies_to_update,
        'missing_data': missing_data,
        'stats': {
            'total_in_filter': len(all_companies),
            'existing_in_db': len(existing),
            'new_to_add': len(new_companies),
            'existing_to_update': len(missing_data),
            'up_to_date': len(companies_to_update) - len(missing_data)
        }
    }


def print_work_plan(plan: dict):
    """Print the incremental work plan."""
    stats = plan['stats']

    print("\n" + "="*60)
    print("INCREMENTAL UPDATE WORK PLAN")
    print("="*60)
    print(f"Total companies in filter:     {stats['total_in_filter']}")
    print(f"Already in database:           {stats['existing_in_db']}")
    print(f"  - Up to date:                {stats['up_to_date']}")
    print(f"  - Need updates:              {stats['existing_to_update']}")
    print(f"New companies to add:          {stats['new_to_add']}")
    print("="*60)

    if plan['missing_data']:
        print("\nCompanies needing updates:")
        for company_num, endpoints in list(plan['missing_data'].items())[:10]:
            company_data = plan['update_companies'][company_num]
            print(f"  {company_num} ({company_data['company_name']})")
            print(f"    Missing: {', '.join(endpoints)}")

        if len(plan['missing_data']) > 10:
            print(f"  ... and {len(plan['missing_data']) - 10} more")

    print()


if __name__ == "__main__":
    # Test the incremental logic
    print("Testing incremental enrichment logic...")

    try:
        existing = get_existing_companies()
        print(f"\nFound {len(existing)} existing companies in database")

        # Show sample
        for i, (num, data) in enumerate(list(existing.items())[:5]):
            print(f"  {num}: officers={data['has_officers']}, financials={data['has_financials']}")

        # Determine missing data
        missing = determine_missing_data(existing, {
            'fetch_officers': True,
            'fetch_financials': True
        })

        print(f"\n{len(missing)} companies need data updates")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
