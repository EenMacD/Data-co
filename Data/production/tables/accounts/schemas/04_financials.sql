-- =====================================================
-- Financials (clean, validated)
-- =====================================================
DROP TABLE IF EXISTS production_financials CASCADE;

CREATE TABLE production_financials (
    id SERIAL PRIMARY KEY,
    company_number VARCHAR(8) NOT NULL REFERENCES production_companies(company_number) ON DELETE CASCADE,

    -- Financial period
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,

    -- P&L figures
    turnover NUMERIC(15, 2),
    cost_of_sales NUMERIC(15, 2),
    gross_profit NUMERIC(15, 2),
    operating_profit NUMERIC(15, 2),
    profit_before_tax NUMERIC(15, 2),
    profit_after_tax NUMERIC(15, 2),

    -- Balance sheet
    total_assets NUMERIC(15, 2),
    current_assets NUMERIC(15, 2),
    fixed_assets NUMERIC(15, 2),
    total_liabilities NUMERIC(15, 2),
    current_liabilities NUMERIC(15, 2),
    long_term_liabilities NUMERIC(15, 2),
    net_worth NUMERIC(15, 2),

    -- Calculated metrics
    debt_to_equity_ratio NUMERIC(10, 4),
    current_ratio NUMERIC(10, 4),
    profit_margin NUMERIC(10, 4),

    -- Data source
    source VARCHAR(20) NOT NULL, -- 'xbrl', 'ixbrl', 'ocr', 'api'
    data_quality_score DECIMAL(3,2),

    -- Metadata
    filed_at TIMESTAMP,
    first_seen TIMESTAMP DEFAULT NOW(),
    last_updated TIMESTAMP DEFAULT NOW(),

    -- Raw data reference
    raw_data JSONB,

    -- Data lineage
    source_batch_id VARCHAR(50),

    -- One record per period per company
    UNIQUE(company_number, period_end),

    CHECK (period_end > period_start),
    CHECK (data_quality_score >= 0 AND data_quality_score <= 1)
);

-- Indexes
CREATE INDEX idx_financials_company_number ON production_financials(company_number);
CREATE INDEX idx_financials_period_end ON production_financials(period_end DESC);
CREATE INDEX idx_financials_turnover ON production_financials(turnover);

-- Trigger
CREATE TRIGGER financials_update_timestamp
    BEFORE UPDATE ON production_financials
    FOR EACH ROW
    EXECUTE FUNCTION update_last_updated();

-- Comments
COMMENT ON TABLE production_financials IS 'Production financial data - validated figures with calculated ratios';
