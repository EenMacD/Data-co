"""
PSC (Officers) loader service.
"""
from __future__ import annotations

import sys
import json
from io import StringIO
from pathlib import Path
from typing import Optional, Any

import pandas as pd
from psycopg2.extras import Json

# Add Data root to path
DATA_ROOT = Path(__file__).resolve().parents[4]
if str(DATA_ROOT) not in sys.path:
    sys.path.append(str(DATA_ROOT))

from staging.common.services.base_loader import BaseLoader

class PSCLoader(BaseLoader):
    """
    Handles bulk loading of officers/PSC data with change detection.
    """

    def __init__(self, batch_id: str):
        super().__init__(batch_id)
        self.stats = {
            'officers_inserted': 0,
            'officers_updated': 0
        }

    def load_officers(self, df: pd.DataFrame) -> dict:
        """
        Bulk load officers/PSC data with change detection.

        Args:
            df: DataFrame with officer data

        Returns:
            Stats dict with inserted, updated counts
        """
        if df.empty:
            return {'inserted': 0, 'updated': 0, 'skipped': 0}

        # Ensure required columns
        if 'company_number' not in df.columns:
            raise ValueError("DataFrame missing required column: company_number")

        df = df.copy()
        df['batch_id'] = self.batch_id
        df['last_updated'] = pd.Timestamp.now()

        # Create temp table
        temp_table = f"temp_officers_{self.batch_id.replace('-', '_')[:20]}"

        with self.db.get_cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
            cur.execute(f'''
                CREATE TEMP TABLE {temp_table} (
                    company_number VARCHAR(8) NOT NULL,
                    officer_name VARCHAR(500),
                    officer_role VARCHAR(200),
                    appointed_on DATE,
                    resigned_on DATE,
                    nationality VARCHAR(100),
                    nature_of_control TEXT,
                    address_line_1 VARCHAR(500),
                    address_line_2 VARCHAR(500),
                    locality VARCHAR(200),
                    postal_code VARCHAR(20),
                    country VARCHAR(100),
                    date_of_birth DATE,
                    data_hash VARCHAR(32),
                    batch_id VARCHAR(50),
                    last_updated TIMESTAMP,
                    raw_data JSONB
                )
            ''')

            columns = [
                'company_number', 'officer_name', 'officer_role',
                'appointed_on', 'resigned_on', 'nationality', 'nature_of_control',
                'address_line_1', 'address_line_2', 'locality', 'postal_code',
                'country', 'date_of_birth', 'data_hash', 'batch_id', 'last_updated'
            ]

            # Ensure columns exist
            for col in columns:
                if col not in df.columns:
                    df[col] = None

            # Handle raw_data
            if 'raw_data' in df.columns:
                 df['raw_data'] = df['raw_data'].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
            else:
                 df['raw_data'] = '{}'

            export_cols = columns + ['raw_data']

            buffer = StringIO()
            df[export_cols].to_csv(buffer, index=False, header=False, na_rep='\\N')
            buffer.seek(0)

            cur.copy_expert(
                f"COPY {temp_table} ({','.join(export_cols)}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                buffer
            )

            # Insert officers with UPSERT
            cur.execute(f'''
                INSERT INTO staging_officers (
                    company_number, officer_name, officer_role,
                    appointed_on, resigned_on, nationality, nature_of_control,
                    address_line_1, address_line_2, locality, postal_code, country,
                    date_of_birth, raw_data, data_hash, change_detected, last_updated
                )
                SELECT DISTINCT ON (t.company_number, t.officer_name, t.appointed_on::date, t.officer_role, t.date_of_birth::date)
                    t.company_number, t.officer_name, t.officer_role,
                    t.appointed_on::date, t.resigned_on::date, t.nationality, t.nature_of_control,
                    t.address_line_1, t.address_line_2, t.locality, t.postal_code, t.country,
                    t.date_of_birth::date, t.raw_data::jsonb, t.data_hash, FALSE, t.last_updated
                FROM {temp_table} t
                -- Ensure company exists first (FK constraint)
                JOIN staging_companies sc ON sc.company_number = t.company_number
                ORDER BY t.company_number, t.officer_name, t.appointed_on::date, t.officer_role, t.date_of_birth::date
                ON CONFLICT (company_number, officer_name, appointed_on, officer_role, date_of_birth) DO UPDATE SET
                    resigned_on = EXCLUDED.resigned_on,
                    nationality = EXCLUDED.nationality,
                    nature_of_control = EXCLUDED.nature_of_control,
                    address_line_1 = EXCLUDED.address_line_1,
                    address_line_2 = EXCLUDED.address_line_2,
                    locality = EXCLUDED.locality,
                    postal_code = EXCLUDED.postal_code,
                    country = EXCLUDED.country,
                    raw_data = EXCLUDED.raw_data,
                    data_hash = EXCLUDED.data_hash,
                    last_updated = EXCLUDED.last_updated,
                    change_detected = (staging_officers.data_hash IS DISTINCT FROM EXCLUDED.data_hash)
                WHERE staging_officers.data_hash IS DISTINCT FROM EXCLUDED.data_hash
            ''')

            affected_rows = cur.rowcount
            cur.execute(f"DROP TABLE IF EXISTS {temp_table}")

        stats = {
            'inserted': affected_rows,
            'updated': 0,
            'skipped': len(df) - affected_rows
        }
        self.stats['officers_inserted'] += stats['inserted']
        self.stats['officers_updated'] += stats['updated']

        return stats
