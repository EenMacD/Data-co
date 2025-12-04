"""
Isolated preview functionality - doesn't touch database or save configs.
"""
import os
import sys
import tempfile
from pathlib import Path

import pandas as pd
import requests
import yaml
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parents[1]


def preview_filter_results(filters: dict, api_key: str = None, fetch_sample_data: bool = True):
    """
    Preview what companies would be scraped with given filters.

    Args:
        filters: Dict with locality, company_status, sic_codes, limit
        api_key: Companies House API key (optional, for fetching sample data)
        fetch_sample_data: Whether to fetch actual API data for first company

    Returns:
        Dict with:
            - total_matches: Total companies matching filters
            - sample_companies: List of first 10 companies
            - sample_api_data: Actual API data for first company (if api_key provided)
            - statistics: Breakdown by status, locality, etc.
    """
    # Find bulk data file
    data_dir = ROOT / 'project_data'

    # Look for most recent company profile zip
    company_profile_dirs = list(data_dir.glob('*/raw/company_profile'))

    if not company_profile_dirs:
        return {
            'success': False,
            'error': 'No bulk company data found. Please run bulk download first.'
        }

    # Get most recent
    latest_dir = max(company_profile_dirs, key=lambda p: p.stat().st_mtime)

    try:
        latest_zip = max(latest_dir.glob('*.zip'), key=os.path.getmtime)
    except ValueError:
        return {
            'success': False,
            'error': f'No ZIP file found in {latest_dir}'
        }

    # Extract filters
    locality = filters.get('locality', '')
    status = filters.get('company_status', '')
    sic_codes = filters.get('sic_codes', '')
    limit = filters.get('limit')

    # Parse SIC codes
    sic_include = []
    if sic_codes:
        sic_include = [s.strip() for s in sic_codes.split(',') if s.strip()]

    # Read and filter companies
    print(f"Reading bulk data from: {latest_zip.name}")

    all_chunks = []
    with ZipFile(latest_zip, 'r') as archive:
        csv_filename = next(
            (name for name in archive.namelist() if name.lower().endswith('.csv')), None
        )

        if not csv_filename:
            return {'success': False, 'error': 'No CSV found in ZIP'}

        with archive.open(csv_filename) as csv_file:
            # Read in chunks
            chunk_iterator = pd.read_csv(csv_file, chunksize=100000, dtype=str, low_memory=False)

            for chunk in chunk_iterator:
                chunk.columns = chunk.columns.str.strip()
                filtered_chunk = chunk.copy()

                # Apply filters
                if locality:
                    if 'RegAddress.PostTown' in filtered_chunk.columns:
                        filtered_chunk = filtered_chunk[
                            filtered_chunk['RegAddress.PostTown'].str.contains(
                                locality, case=False, na=False
                            )
                        ]

                if status:
                    if 'CompanyStatus' in filtered_chunk.columns:
                        filtered_chunk = filtered_chunk[
                            filtered_chunk['CompanyStatus'].str.lower() == status.lower()
                        ]

                if sic_include:
                    sic_cols = [
                        f'SICCode.SicText_{i}'
                        for i in range(1, 5)
                        if f'SICCode.SicText_{i}' in filtered_chunk.columns
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

                    # Early exit if we have enough for preview
                    if limit and sum(len(c) for c in all_chunks) >= limit:
                        break

    if not all_chunks:
        return {
            'success': True,
            'total_matches': 0,
            'sample_companies': [],
            'sample_api_data': None,
            'statistics': {},
            'message': 'No companies match your filters'
        }

    # Combine chunks
    final_df = pd.concat(all_chunks)

    # Apply limit
    if limit:
        final_df = final_df.head(limit)

    total_matches = len(final_df)

    # Get sample companies (first 10)
    sample_df = final_df.head(10)
    sample_companies = []

    for _, row in sample_df.iterrows():
        # Convert row to dict and replace NaN with None or 'N/A'
        row_dict = row.to_dict()

        def get_value(key, default='N/A'):
            val = row_dict.get(key, default)
            # Check if value is NaN (pandas NaN != NaN returns True)
            if pd.isna(val):
                return default
            return val

        sample_companies.append({
            'company_number': get_value('CompanyNumber'),
            'company_name': get_value('CompanyName'),
            'status': get_value('CompanyStatus'),
            'locality': get_value('RegAddress.PostTown'),
            'postcode': get_value('RegAddress.PostCode'),
            'country': get_value('RegAddress.Country')
        })

    # Fetch sample API data for first company
    sample_api_data = None
    if api_key and fetch_sample_data and len(sample_companies) > 0:
        first_company = sample_companies[0]
        company_number = first_company['company_number']

        print(f"Fetching sample API data for: {company_number}")

        try:
            api_responses = {}
            session = requests.Session()
            session.auth = (api_key, "")

            # Fetch officers, PSCs, and filing history
            endpoints_to_preview = ['officers', 'persons-with-significant-control', 'filing-history']

            for endpoint in endpoints_to_preview:
                url = f"https://api.companieshouse.gov.uk/company/{company_number}/{endpoint}"
                try:
                    response = session.get(url, timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        # Limit to first 3 items for preview
                        if 'items' in data:
                            data['items'] = data['items'][:3]
                            data['preview_note'] = f'Showing first 3 of {data.get("total_results", "?")} total'
                        api_responses[endpoint] = data
                    else:
                        api_responses[endpoint] = {'error': f'HTTP {response.status_code}'}
                except Exception as e:
                    api_responses[endpoint] = {'error': str(e)}

            # Extract financial documents info from filing history
            financial_docs = []
            if 'filing-history' in api_responses and 'items' in api_responses['filing-history']:
                for filing in api_responses['filing-history']['items']:
                    category = filing.get('category', '')
                    description = filing.get('description', '')
                    if 'accounts' in category.lower() or 'accounts' in description.lower():
                        financial_docs.append({
                            'date': filing.get('date'),
                            'category': filing.get('category'),
                            'description': filing.get('description'),
                            'type': filing.get('type'),
                            'links': filing.get('links', {})
                        })

            api_responses['financial_documents_preview'] = {
                'total_found': len(financial_docs),
                'sample': financial_docs[:5],
                'note': 'These are accounts filings that would be processed for financial data extraction'
            }

            session.close()

            sample_api_data = {
                'company': first_company,
                'api_data': api_responses,
                'note': 'This is sample data for the first matching company. All companies will be fetched with similar data.'
            }

        except Exception as e:
            sample_api_data = {'error': f'Could not fetch sample data: {str(e)}'}

    # Calculate statistics
    status_counts = {}
    if 'CompanyStatus' in final_df.columns:
        status_counts = final_df['CompanyStatus'].value_counts().to_dict()
        # Replace NaN keys with 'Unknown'
        status_counts = {(k if pd.notna(k) else 'Unknown'): v for k, v in status_counts.items()}

    locality_counts = {}
    if 'RegAddress.PostTown' in final_df.columns:
        locality_counts = final_df['RegAddress.PostTown'].value_counts().head(5).to_dict()
        # Replace NaN keys with 'Unknown'
        locality_counts = {(k if pd.notna(k) else 'Unknown'): v for k, v in locality_counts.items()}

    return {
        'success': True,
        'total_matches': total_matches,
        'sample_companies': sample_companies,
        'sample_api_data': sample_api_data,
        'statistics': {
            'by_status': status_counts,
            'top_localities': locality_counts
        },
        'message': f'Found {total_matches} companies matching your filters'
    }
