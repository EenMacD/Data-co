"""
Parser for Companies House Company Data bulk CSV files.
"""
from __future__ import annotations

import pandas as pd

from staging.common.parsers.base_parser import BulkDataParser


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
        'CompanyCategory': 'company_type',
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
        
        # New Mappings
        'IncorporationDate': 'incorporation_date',
        'Accounts.LastMadeUpDate': 'accounts_last_made_up_date',
        # Accounts.AccountRefDay + Month handled in transform
        'Accounts.NextDueDate': 'accounts_next_due_date',
        'Accounts.AccountCategory': 'account_category',
        'Returns.NextDueDate': 'returns_next_due_date',
        'Returns.LastMadeUpDate': 'returns_last_made_up_date',
        'Mortgages.NumMortCharges': 'num_mort_charges',
        'Mortgages.NumMortOutstanding': 'num_mort_outstanding',
        'Mortgages.NumMortPartSatisfied': 'num_mort_part_satisfied',
        'ConfStmtNextDueDate': 'conf_stm_next_due_date',
        'ConfStmtLastMadeUpDate': 'conf_stm_last_made_up_date',
        
        # Source columns needed for transformation but not direct mapping
        'Accounts.AccountRefDay': 'temp_acc_ref_day',
        'Accounts.AccountRefMonth': 'temp_acc_ref_month',
    }
    
    # Generate mappings for Previous Names (1-10)
    for i in range(1, 11):
        FIELD_MAPPINGS[f'PreviousName_{i}.CompanyName'] = f'temp_prev_name_{i}'

    TARGET_TABLE = 'staging_companies'

    # Fields used to compute the change detection hash
    HASH_FIELDS = [
        'company_number',
        'company_name',
        'company_status',
        'company_type',
        'locality',
        'postal_code',
        'address_line_1',
        'address_line_2',
        'region',
        'country',
        'sic_codes',
        'incorporation_date',
        'accounts_next_due_date',
        'num_mort_charges',
        'previous_names'
    ]

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply company-specific transformations.

        - Extract SIC codes into an array
        - Clean up company status values
        - Handle empty strings
        - Combine Account Ref Day/Month
        - Combine Previous Names
        - Normalize Date Formats (DD/MM/YYYY -> YYYY-MM-DD)
        """
        # Extract SIC codes into array column
        df['sic_codes'] = df.apply(self._extract_sic_codes, axis=1)

        # Drop individual SIC code columns after extraction
        sic_cols = ['sic_code_1', 'sic_code_2', 'sic_code_3', 'sic_code_4']
        df = df.drop(columns=[c for c in sic_cols if c in df.columns], errors='ignore')

        # Normalize company status to lowercase
        if 'company_status' in df.columns:
            df['company_status'] = df['company_status'].str.lower().str.strip()

        # Combine Account Ref Day/Month -> "MM-DD"
        if 'temp_acc_ref_day' in df.columns and 'temp_acc_ref_month' in df.columns:
            df['accounts_ref_date'] = df.apply(self._format_ref_date, axis=1)
            df = df.drop(columns=['temp_acc_ref_day', 'temp_acc_ref_month'], errors='ignore')

        # Combine Previous Names -> "Name1|Name2|..."
        df['previous_names'] = df.apply(self._combine_previous_names, axis=1)
        # Drop temp previous name colums
        prev_cols = [f'temp_prev_name_{i}' for i in range(1, 11)]
        df = df.drop(columns=[c for c in prev_cols if c in df.columns], errors='ignore')

        # Fix Date Formats (DD/MM/YYYY -> YYYY-MM-DD)
        date_cols = [
            'incorporation_date',
            'accounts_last_made_up_date',
            'accounts_next_due_date',
            'returns_next_due_date',
            'returns_last_made_up_date',
            'conf_stm_next_due_date',
            'conf_stm_last_made_up_date'
        ]
        
        for col in date_cols:
            if col in df.columns:
                # Convert to datetime, coercing errors to NaT, then format as YYYY-MM-DD
                # Companies House dates are typically DD/MM/YYYY
                df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')

        # Replace empty strings with None for cleaner data
        df = df.replace({'': None, 'nan': None, 'NaN': None})

        return df

    def _extract_sic_codes(self, row: pd.Series) -> list[str]:
        """Extract SIC codes from individual columns into a list."""
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

    def _format_ref_date(self, row: pd.Series) -> str | None:
        """Format Account Ref Day/Month as MM-DD."""
        try:
            day = row.get('temp_acc_ref_day')
            month = row.get('temp_acc_ref_month')
            
            if pd.isna(day) or pd.isna(month):
                return None
                
            day_str = str(int(float(day))).zfill(2)
            month_str = str(int(float(month))).zfill(2)
            
            return f"{month_str}-{day_str}"
        except (ValueError, TypeError):
            return None

    def _combine_previous_names(self, row: pd.Series) -> str | None:
        """Combine previous names into a pipe-separated string."""
        names = []
        for i in range(1, 11):
            col = f'temp_prev_name_{i}'
            if col in row and pd.notna(row[col]) and str(row[col]).strip() != '':
                names.append(str(row[col]).strip())
        
        return "|".join(names) if names else None
