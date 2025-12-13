"""
Parser for Companies House Company Data bulk CSV files.
"""
from __future__ import annotations

import pandas as pd

from .base_parser import BulkDataParser


class CompanyDataParser(BulkDataParser):
    """
    Parser for BasicCompanyData CSV files from Companies House.

    Source format: BasicCompanyData-YYYY-MM-DD-partX_Y.zip
    Contains columns like CompanyNumber, CompanyName, CompanyStatus, etc.
    """

    # Map source CSV columns to staging_companies columns
    FIELD_MAPPINGS = {
        'CompanyNumber': 'company_number',
        'CompanyName': 'company_name',
        'CompanyStatus': 'company_status',
        'RegAddress.PostTown': 'locality',
        'RegAddress.PostCode': 'postal_code',
        'RegAddress.AddressLine1': 'address_line_1',
        'RegAddress.AddressLine2': 'address_line_2',
        'RegAddress.County': 'region',
        'RegAddress.Country': 'country',
        # SIC codes handled separately in transform()
        'SICCode.SicText_1': 'sic_code_1',
        'SICCode.SicText_2': 'sic_code_2',
        'SICCode.SicText_3': 'sic_code_3',
        'SICCode.SicText_4': 'sic_code_4',
        # Additional useful fields
        'IncorporationDate': 'incorporation_date',
        'DissolutionDate': 'dissolution_date',
        'CompanyCategory': 'company_category',
        'CountryOfOrigin': 'country_of_origin',
    }

    TARGET_TABLE = 'staging_companies'

    # Fields used to compute the change detection hash
    HASH_FIELDS = [
        'company_number',
        'company_name',
        'company_status',
        'locality',
        'postal_code',
        'address_line_1',
        'address_line_2',
        'region',
        'country',
        'sic_codes',
    ]

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply company-specific transformations.

        - Extract SIC codes into an array
        - Clean up company status values
        - Handle empty strings
        """
        # Extract SIC codes into array column
        df['sic_codes'] = df.apply(self._extract_sic_codes, axis=1)

        # Drop individual SIC code columns after extraction
        sic_cols = ['sic_code_1', 'sic_code_2', 'sic_code_3', 'sic_code_4']
        df = df.drop(columns=[c for c in sic_cols if c in df.columns], errors='ignore')

        # Normalize company status to lowercase
        if 'company_status' in df.columns:
            df['company_status'] = df['company_status'].str.lower().str.strip()

        # Replace empty strings with None for cleaner data
        df = df.replace({'': None, 'nan': None, 'NaN': None})

        return df

    def _extract_sic_codes(self, row: pd.Series) -> list[str]:
        """
        Extract SIC codes from individual columns into a list.

        SIC codes in the CSV are formatted as "12345 - Description"
        We extract just the numeric code.

        Args:
            row: A single row from the DataFrame

        Returns:
            List of SIC code strings
        """
        sic_codes = []
        for i in range(1, 5):
            col = f'sic_code_{i}'
            if col in row and pd.notna(row[col]):
                val = str(row[col])
                # Extract code before " - " description
                code = val.split(' - ')[0].strip()
                if code and code != 'nan':
                    sic_codes.append(code)
        return sic_codes
