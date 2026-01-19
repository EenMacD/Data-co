-- =====================================================
-- Financials (from accounts bulk data)
-- =====================================================
DROP TABLE IF EXISTS staging_financials CASCADE;

CREATE TABLE staging_financials (
    id SERIAL PRIMARY KEY,
    company_number VARCHAR(8) NOT NULL REFERENCES staging_companies(company_number) ON DELETE CASCADE,

    -- Financial period
    period_start DATE,
    period_end DATE NOT NULL,

    -- Dynamic columns from tag_dictionary.json
    turnover NUMERIC(12, 2),
    profit_loss NUMERIC(12, 2),
    total_assets NUMERIC(12, 2),
    total_liabilities NUMERIC(12, 2),
    net_assets_liabilities NUMERIC(12, 2),
    
    distribution_costs NUMERIC(12, 2),
    administrative_expenses NUMERIC(12, 2),
    other_operating_income NUMERIC(12, 2),
    cost_sales NUMERIC(12, 2),
    gross_profit_loss NUMERIC(12, 2),
    
    fixed_assets NUMERIC(12, 2),
    current_assets NUMERIC(12, 2),
    creditors NUMERIC(12, 2),
    net_current_assets_liabilities NUMERIC(12, 2),
    total_assets_less_current_liabilities NUMERIC(12, 2),
    
    staff_costs_employee_benefits_expense NUMERIC(12, 2),
    wages_salaries NUMERIC(12, 2),
    operating_profit_loss NUMERIC(12, 2),
    net_finance_income_costs NUMERIC(12, 2),
    profit_loss_on_ordinary_activities_before_tax NUMERIC(12, 2),
    profit_loss_on_ordinary_activities_after_tax NUMERIC(12, 2),
    
    
    investments_fixed_assets NUMERIC(12, 2),
    cash_bank_on_hand NUMERIC(12, 2),
    debtors NUMERIC(12, 2),
    total_inventories NUMERIC(12, 2),
    trade_creditors_trade_payables NUMERIC(12, 2),
    bank_borrowings_overdrafts NUMERIC(12, 2),
    current_liabilities NUMERIC(12, 2),
    income_expense_recognised_directly_in_equity NUMERIC(12, 2),
    dividends_paid NUMERIC(12, 2),
    
    -- Cash flow
    net_cash_flows_from_used_in_operating_activities NUMERIC(12, 2),
    net_cash_generated_from_operations NUMERIC(12, 2),
    income_taxes_paid_refund_classified_as_operating_activities NUMERIC(12, 2),
    net_cash_flows_from_used_in_investing_activities NUMERIC(12, 2),
    
    net_cash_flows_from_used_in_financing_activities NUMERIC(12, 2),
    
    cash_cash_equivalents_cash_flow_value NUMERIC(12, 2),
    social_security_costs NUMERIC(12, 2),
    other_employee_expense NUMERIC(12, 2),
    director_remuneration NUMERIC(12, 2),

    -- Text/Other fields
    production_software_name VARCHAR(255),
    production_software_version VARCHAR(100),
    description_body_authorising_financial_statements TEXT,
    average_number_employees_during_period INTEGER,
    report_title VARCHAR(255),
    entity_current_legal_or_registered_name VARCHAR(255),
    name_entity_officer VARCHAR(255),
    entity_trading_status VARCHAR(255),
    date_assumed_position DATE,
    cash_receipts_from_disposal_non_controlling_interests VARCHAR(255),
    
    administration_support_average_number_employees INTEGER,
    production_average_number_employees INTEGER,
    sales_marketing_distribution_average_number_employees INTEGER,
    other_departments_average_number_employees INTEGER,

    -- Data source
    source VARCHAR(20), -- 'xbrl', 'ixbrl', 'ocr', 'api', 'bulk_xbrl'

    -- Raw data
    raw_data JSONB,
    
    -- Change detection
    data_hash VARCHAR(32),
    batch_id VARCHAR(50),
    change_detected BOOLEAN DEFAULT FALSE,
    last_updated TIMESTAMP DEFAULT NOW(),

    -- Merge tracking
    merged_at TIMESTAMP,

    -- Metadata
    ingested_at TIMESTAMP DEFAULT NOW(),
    needs_review BOOLEAN DEFAULT false,
    review_notes TEXT,

    UNIQUE(company_number, period_end)
);

-- Indexes
CREATE INDEX idx_staging_financials_company ON staging_financials(company_number);
CREATE INDEX idx_staging_financials_period ON staging_financials(period_end);
-- Change detection indexes
CREATE INDEX idx_staging_financials_hash ON staging_financials(company_number, period_end, data_hash);
CREATE INDEX idx_staging_financials_change_detected ON staging_financials(change_detected) WHERE change_detected = TRUE;
-- Merge tracking index
CREATE INDEX idx_staging_financials_merged_at ON staging_financials(merged_at) WHERE merged_at IS NULL;
