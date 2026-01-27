"""
Accounts (Financials) loader service.
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

class AccountsLoader(BaseLoader):
    """
    Handles bulk loading of financial data with change detection.
    """

    def __init__(self, batch_id: str):
        super().__init__(batch_id)
        self.stats = {
            'financials_inserted': 0,
            'financials_updated': 0,
        }

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
            cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
            cur.execute(f'''
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
                    cash_receipts_from_disposal_non_controlling_interests VARCHAR(255), administration_support_average_number_employees INTEGER, production_average_number_employees INTEGER,
                    sales_marketing_distribution_average_number_employees INTEGER,
                    other_departments_average_number_employees INTEGER,
                    
                    source VARCHAR(20),
                    data_hash VARCHAR(32),
                    batch_id VARCHAR(50),
                    last_updated TIMESTAMP,
                    raw_data JSONB
                )
            ''')

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
                f"COPY {temp_table} ({','.join(export_cols)}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                buffer
            )

            # UPSERT with change detection
            # Build dynamic column lists for SQL
            target_cols_str = ', '.join(columns)
            
            # Source columns for SELECT (prefix with t.)
            source_cols_str = ', '.join([f"t.{c}" if c != 'raw_data' else 't.raw_data::jsonb' for c in columns])
            
            # Update assignments for ON CONFLICT DO UPDATE
            # Exclude PK/Unique keys (company_number, period_end) from update
            update_cols = [c for c in columns if c not in ['company_number', 'period_end']]
            update_set_str = ', '.join([f"{c} = EXCLUDED.{c}" for c in update_cols])
            
            cur.execute(f'''
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
            ''')

            affected_rows = cur.rowcount
            cur.execute(f"DROP TABLE IF EXISTS {temp_table}")

        stats = {
            'inserted': affected_rows,
            'updated': 0,
            'skipped': len(df) - affected_rows
        }
        self.stats['financials_inserted'] += stats['inserted']
        self.stats['financials_updated'] += stats['updated']

        return stats
