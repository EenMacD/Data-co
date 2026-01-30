"""
Company loader service.
"""
from __future__ import annotations

import sys
from io import StringIO
from pathlib import Path
from typing import Optional, Any

import pandas as pd

# Add Data root to path
DATA_ROOT = Path(__file__).resolve().parents[4]
if str(DATA_ROOT) not in sys.path:
    sys.path.append(str(DATA_ROOT))

from staging.common.services.base_loader import BaseLoader

class CompanyLoader(BaseLoader):
    """
    Handles bulk loading of company data into PostgreSQL staging tables.
    """

    def __init__(self, batch_id: str):
        super().__init__(batch_id)
        # Specific stats for companies if needed, or stick to generic 'inserted', 'skipped'
        self.stats = {
            'companies_inserted': 0,
            'companies_updated': 0,
            'companies_skipped': 0
        }

    def load_companies(self, df: pd.DataFrame) -> dict:
        """
        Bulk load companies with change detection.

        Uses a temp table + UPSERT pattern:
        1. COPY data to temp table
        2. UPSERT to staging_companies with change detection

        Args:
            df: DataFrame with company data (must have 'company_number' column)

        Returns:
            Stats dict with inserted, updated, skipped counts
        """
        if df.empty:
            return {'inserted': 0, 'updated': 0, 'skipped': 0}

        # Ensure required columns exist
        required = ['company_number', 'data_hash']
        for col in required:
            if col not in df.columns:
                raise ValueError(f"DataFrame missing required column: {col}")

        # Add batch_id
        df = df.copy()
        df['batch_id'] = self.batch_id
        df['last_updated'] = pd.Timestamp.now()

        # Create temp table
        temp_table = f"temp_companies_{self.batch_id.replace('-', '_')[:20]}"

        with self.db.get_cursor() as cur:
            # Create temp table matching staging_companies structure
            cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
            cur.execute(f'''
                CREATE TEMP TABLE {temp_table} (
                    company_number VARCHAR(8) NOT NULL,
                    company_name VARCHAR(500),
                    company_status VARCHAR(50),
                    company_type VARCHAR(100),
                    locality VARCHAR(200),
                    postal_code VARCHAR(20),
                    address_line_1 VARCHAR(500),
                    address_line_2 VARCHAR(500),
                    region VARCHAR(100),
                    country VARCHAR(100),
                    sic_codes TEXT[],
                    incorporation_date DATE,
                    accounts_last_made_up_date DATE,
                    accounts_ref_date CHAR(5),
                    accounts_next_due_date DATE,
                    account_category VARCHAR(30),
                    returns_next_due_date DATE,
                    returns_last_made_up_date DATE,
                    num_mort_charges INTEGER,
                    num_mort_outstanding INTEGER,
                    num_mort_part_satisfied INTEGER,
                    previous_names TEXT,
                    conf_stm_next_due_date DATE,
                    conf_stm_last_made_up_date DATE,
                    data_hash VARCHAR(32),
                    batch_id VARCHAR(50),
                    last_updated TIMESTAMP
                )
            ''')

            # Prepare data for COPY
            columns = [
                'company_number', 'company_name', 'company_status', 'company_type',
                'locality', 'postal_code', 'address_line_1', 'address_line_2',
                'region', 'country', 'sic_codes',
                'incorporation_date', 'accounts_last_made_up_date', 'accounts_ref_date',
                'accounts_next_due_date', 'account_category',
                'returns_next_due_date', 'returns_last_made_up_date',
                'num_mort_charges', 'num_mort_outstanding', 'num_mort_part_satisfied',
                'previous_names', 'conf_stm_next_due_date', 'conf_stm_last_made_up_date',
                'data_hash', 'batch_id', 'last_updated'
            ]

            # Ensure all columns exist, fill missing with None
            for col in columns:
                if col not in df.columns:
                    df[col] = None

            # Convert sic_codes list to PostgreSQL array format
            if 'sic_codes' in df.columns:
                df['sic_codes'] = df['sic_codes'].apply(
                    lambda x: '{' + ','.join(x) + '}' if isinstance(x, list) else None
                )

            # Create CSV buffer for COPY
            buffer = StringIO()
            df[columns].to_csv(buffer, index=False, header=False, na_rep='\\N')
            buffer.seek(0)

            # COPY to temp table
            cur.copy_expert(
                f"COPY {temp_table} ({','.join(columns)}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                buffer
            )

            # UPSERT with change detection
            cur.execute(f'''
                INSERT INTO staging_companies (
                    company_number, company_name, company_status, company_type,
                    locality, postal_code, address_line_1, address_line_2,
                    region, country, sic_codes,
                    incorporation_date, accounts_last_made_up_date, accounts_ref_date,
                    accounts_next_due_date, account_category,
                    returns_next_due_date, returns_last_made_up_date,
                    num_mort_charges, num_mort_outstanding, num_mort_part_satisfied,
                    previous_names, conf_stm_next_due_date, conf_stm_last_made_up_date,
                    data_hash, last_updated, change_detected, raw_data, batch_id
                )
                SELECT DISTINCT ON (t.company_number)
                    t.company_number, t.company_name, t.company_status, t.company_type,
                    t.locality, t.postal_code, t.address_line_1, t.address_line_2,
                    t.region, t.country, t.sic_codes,
                    t.incorporation_date, t.accounts_last_made_up_date, t.accounts_ref_date,
                    t.accounts_next_due_date, t.account_category,
                    t.returns_next_due_date, t.returns_last_made_up_date,
                    t.num_mort_charges, t.num_mort_outstanding, t.num_mort_part_satisfied,
                    t.previous_names, t.conf_stm_next_due_date, t.conf_stm_last_made_up_date,
                    t.data_hash, t.last_updated, FALSE, '{{}}'::jsonb, t.batch_id
                FROM {temp_table} t
                ORDER BY t.company_number
                ON CONFLICT (company_number) DO UPDATE SET
                    company_name = EXCLUDED.company_name,
                    company_status = EXCLUDED.company_status,
                    company_type = EXCLUDED.company_type,
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
                    data_hash = EXCLUDED.data_hash,
                    last_updated = EXCLUDED.last_updated,
                    batch_id = EXCLUDED.batch_id,
                    change_detected = (staging_companies.data_hash IS DISTINCT FROM EXCLUDED.data_hash)
                WHERE staging_companies.data_hash IS DISTINCT FROM EXCLUDED.data_hash
            ''')

            affected_rows = cur.rowcount
            cur.execute(f"DROP TABLE IF EXISTS {temp_table}")

        stats = {
            'inserted': affected_rows,
            'updated': 0,
            'skipped': len(df) - affected_rows,
        }

        self.stats['companies_inserted'] += stats['inserted']
        self.stats['companies_skipped'] += stats['skipped']

        return stats
