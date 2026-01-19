"""
Bulk loader service for PostgreSQL with COPY and UPSERT with change detection.
"""
from __future__ import annotations

import hashlib
import json
import sys
from io import StringIO
from pathlib import Path
from typing import Optional, Any

import pandas as pd
from psycopg2.extras import Json

# Add parent paths for imports
DATA_ROOT = Path(__file__).resolve().parents[2]  # Data/
if str(DATA_ROOT) not in sys.path:
    sys.path.append(str(DATA_ROOT))

from database.connection import get_staging_db


class BulkLoader:
    """
    Handles bulk loading of data into PostgreSQL staging tables.

    Uses PostgreSQL COPY for fast bulk inserts, then UPSERT with
    hash-based change detection for deduplication.
    """

    def __init__(self, batch_id: str):
        """
        Initialize the bulk loader.

        Args:
            batch_id: Batch identifier for this ingestion run
        """
        self.batch_id = batch_id
        self.db = get_staging_db()
        self.stats = {
            'companies_inserted': 0,
            'companies_updated': 0,
            'companies_skipped': 0,
            'officers_inserted': 0,
            'officers_updated': 0,
            'financials_inserted': 0,
            'financials_updated': 0,
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
            cur.execute(f"""
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
            """)

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
                f"""COPY {temp_table} ({','.join(columns)})
                    FROM STDIN WITH (FORMAT CSV, NULL '\\N')""",
                buffer
            )

            # UPSERT with change detection
            cur.execute(f"""
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
            """)

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
            cur.execute(f"""
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
            """)

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
                f"""COPY {temp_table} ({','.join(export_cols)})
                    FROM STDIN WITH (FORMAT CSV, NULL '\\N')""",
                buffer
            )

            # Insert officers with UPSERT
            # Changed insert target to use `company_number` foreign key directly
            cur.execute(f"""
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
            """)

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

    def load_financials(self, df: pd.DataFrame) -> dict:
        """
        Bulk load financial data with change detection.

        Args:
            df: DataFrame with financial data

        Returns:
            Stats dict with inserted, updated counts
        """
        if df.empty:
            return {'inserted': 0, 'updated': 0, 'skipped': 0}

        if 'company_number' not in df.columns:
            raise ValueError("DataFrame missing required column: company_number")

        df = df.copy()
        df['batch_id'] = self.batch_id
        df['last_updated'] = pd.Timestamp.now()

        temp_table = f"temp_financials_{self.batch_id.replace('-', '_')[:20]}"

        with self.db.get_cursor() as cur:
            cur.execute(f"""
                CREATE TEMP TABLE {temp_table} (
                    company_number VARCHAR(8) NOT NULL,
                    period_start DATE,
                    period_end DATE NOT NULL,
                    turnover NUMERIC(15, 2),
                    profit_loss NUMERIC(15, 2),
                    total_assets NUMERIC(15, 2),
                    total_liabilities NUMERIC(15, 2),
                    net_assets_liabilities NUMERIC(15, 2),
                    distribution_costs NUMERIC(15, 2), administrative_expenses NUMERIC(15, 2), other_operating_income NUMERIC(15, 2),
                    cost_sales NUMERIC(15, 2), gross_profit_loss NUMERIC(15, 2), fixed_assets NUMERIC(15, 2), current_assets NUMERIC(15, 2),
                    creditors NUMERIC(15, 2), net_current_assets_liabilities NUMERIC(15, 2), total_assets_less_current_liabilities NUMERIC(15, 2),
                    staff_costs_employee_benefits_expense NUMERIC(15, 2), wages_salaries NUMERIC(15, 2),
                    operating_profit_loss NUMERIC(15, 2), net_finance_income_costs NUMERIC(15, 2),
                    profit_loss_on_ordinary_activities_before_tax NUMERIC(15, 2),
                    profit_loss_on_ordinary_activities_after_tax NUMERIC(15, 2),
                    investments_fixed_assets NUMERIC(15, 2), cash_bank_on_hand NUMERIC(15, 2), debtors NUMERIC(15, 2), total_inventories NUMERIC(15, 2),
                    trade_creditors_trade_payables NUMERIC(15, 2), bank_borrowings_overdrafts NUMERIC(15, 2), current_liabilities NUMERIC(15, 2),
                    income_expense_recognised_directly_in_equity NUMERIC(15, 2), dividends_paid NUMERIC(15, 2),
                    
                    net_cash_flows_from_used_in_operating_activities NUMERIC(15, 2), net_cash_generated_from_operations NUMERIC(15, 2),
                    income_taxes_paid_refund_classified_as_operating_activities NUMERIC(15, 2),
                    net_cash_flows_from_used_in_investing_activities NUMERIC(15, 2),
                    
                    net_cash_flows_from_used_in_financing_activities NUMERIC(15, 2),
                    
                    cash_cash_equivalents_cash_flow_value NUMERIC(15, 2), social_security_costs NUMERIC(15, 2),
                    other_employee_expense NUMERIC(15, 2), director_remuneration NUMERIC(15, 2),
                    
                    production_software_name VARCHAR(255), production_software_version VARCHAR(100),
                    description_body_authorising_financial_statements TEXT, average_number_employees_during_period INTEGER,
                    report_title VARCHAR(255), entity_current_legal_or_registered_name VARCHAR(255), name_entity_officer VARCHAR(255),
                    entity_trading_status VARCHAR(255),
                    date_assumed_position DATE,
                    cash_receipts_from_disposal_non_controlling_interests VARCHAR(255), administration_support_average_number_employees INTEGER, production_average_number_employees INTEGER,
                    sales_marketing_distribution_average_number_employees INTEGER,
                    other_departments_average_number_employees INTEGER,
                    
                    source VARCHAR(20),
                    data_hash VARCHAR(32),
                    batch_id VARCHAR(50),
                    last_updated TIMESTAMP,
                    raw_data JSONB
                )
            """)

            columns = [
                'company_number', 'period_start', 'period_end',
                'turnover', 'profit_loss', 'total_assets', 'total_liabilities', 'net_assets_liabilities',
                'distribution_costs', 'administrative_expenses', 'other_operating_income', 'cost_sales', 'gross_profit_loss',
                'fixed_assets', 'current_assets', 'creditors', 'net_current_assets_liabilities', 'total_assets_less_current_liabilities',
                'staff_costs_employee_benefits_expense', 'wages_salaries',
                'operating_profit_loss', 'net_finance_income_costs',
                'profit_loss_on_ordinary_activities_before_tax',
                'profit_loss_on_ordinary_activities_after_tax',
                'investments_fixed_assets', 'cash_bank_on_hand', 'debtors', 'total_inventories', 'trade_creditors_trade_payables',
                'bank_borrowings_overdrafts', 'current_liabilities',
                'income_expense_recognised_directly_in_equity', 'dividends_paid',
                'net_cash_flows_from_used_in_operating_activities', 'net_cash_generated_from_operations',
                'income_taxes_paid_refund_classified_as_operating_activities',
                'net_cash_flows_from_used_in_investing_activities',
                
                'net_cash_flows_from_used_in_financing_activities',
                'cash_cash_equivalents_cash_flow_value', 'social_security_costs', 'other_employee_expense',
                'director_remuneration', 'production_software_name', 'production_software_version',
                'description_body_authorising_financial_statements',
                'average_number_employees_during_period', 'report_title', 'entity_current_legal_or_registered_name',
                'name_entity_officer', 'entity_trading_status',
                'date_assumed_position',
                'cash_receipts_from_disposal_non_controlling_interests', 'administration_support_average_number_employees',
                'production_average_number_employees',
                'sales_marketing_distribution_average_number_employees',
                'other_departments_average_number_employees',
                'source', 'data_hash', 'batch_id', 'last_updated'
            ]

            # Ensure all columns exist
            for col in columns:
                if col not in df.columns:
                    df[col] = None

            # Handle raw_data which might be a dict/JSON
            if 'raw_data' in df.columns:
                 df['raw_data'] = df['raw_data'].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
            else:
                 df['raw_data'] = '{}'

            # Reorder df to match columns + raw_data
            export_cols = columns + ['raw_data']
            
            # Cast integer columns to avoid "431.0" format which fails COPY
            integer_cols = [
                'average_number_employees_during_period',
                'production_average_number_employees',
                'sales_marketing_distribution_average_number_employees',
                'other_departments_average_number_employees',
                'administration_support_average_number_employees'
            ]
            for col in integer_cols:
                if col in df.columns:
                     # Convert to numeric first to handle strings safely, then to nullable Int64
                     df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

            buffer = StringIO()
            df[export_cols].to_csv(buffer, index=False, header=False, na_rep='\\N')
            buffer.seek(0)

            cur.copy_expert(
                f"""COPY {temp_table} ({','.join(export_cols)})
                    FROM STDIN WITH (FORMAT CSV, NULL '\\N')""",
                buffer
            )

            # UPSERT with change detection
            # Changed to use company_number FK directly
            # Build dynamic column lists for SQL
            # We already have `columns` list which matches DB columns exactly.
            # Exclude metadata for the insert list? No, we include them in temp table.
            
            # Target columns for INSERT
            target_cols_str = ', '.join(columns)
            
            # Source columns for SELECT (prefix with t.)
            source_cols_str = ', '.join([f"t.{c}" if c != 'raw_data' else 't.raw_data::jsonb' for c in columns])
            
            # Update assignments for ON CONFLICT DO UPDATE
            # Exclude PK/Unique keys (company_number, period_end) from update
            update_cols = [c for c in columns if c not in ['company_number', 'period_end']]
            update_set_str = ', '.join([f"{c} = EXCLUDED.{c}" for c in update_cols])
            
            cur.execute(f"""
                INSERT INTO staging_financials (
                    {target_cols_str},
                    change_detected
                )
                SELECT DISTINCT ON (t.company_number, t.period_end::date)
                    {source_cols_str},
                    FALSE
                FROM {temp_table} t
                -- Ensure company exists first (FK constraint)
                JOIN staging_companies sc ON sc.company_number = t.company_number
                ORDER BY t.company_number, t.period_end::date
                ON CONFLICT (company_number, period_end) DO UPDATE SET
                    {update_set_str},
                    change_detected = (staging_financials.data_hash IS DISTINCT FROM EXCLUDED.data_hash)
                WHERE staging_financials.data_hash IS DISTINCT FROM EXCLUDED.data_hash
            """)

            affected_rows = cur.rowcount
            cur.execute(f"DROP TABLE IF EXISTS {temp_table}")

        stats = {
            'inserted': affected_rows, # Approximate (inserts + updates)
            'updated': 0,
            'skipped': len(df) - affected_rows
        }
        self.stats['financials_inserted'] += stats['inserted']
        self.stats['financials_updated'] += stats['updated']

        return stats

    def get_stats(self) -> dict:
        """Get cumulative statistics for this loader instance."""
        return dict(self.stats)
