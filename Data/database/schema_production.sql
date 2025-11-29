-- =====================================================
-- PRODUCTION DATABASE SCHEMA
-- Purpose: Clean, validated data for frontend and reporting
-- =====================================================

-- Drop existing tables if recreating
DROP TABLE IF EXISTS contact_enrichments CASCADE;
DROP TABLE IF EXISTS financials CASCADE;
DROP TABLE IF EXISTS officers CASCADE;
DROP TABLE IF EXISTS companies CASCADE;
DROP TABLE IF EXISTS merge_log CASCADE;

-- =====================================================
-- Merge tracking
-- =====================================================
CREATE TABLE merge_log (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR(50) NOT NULL,
    merged_at TIMESTAMP DEFAULT NOW(),
    companies_merged INTEGER DEFAULT 0,
    officers_merged INTEGER DEFAULT 0,
    financials_merged INTEGER DEFAULT 0,
    contacts_merged INTEGER DEFAULT 0,
    merged_by VARCHAR(100),
    notes TEXT
);

-- =====================================================
-- Companies (clean, deduplicated)
-- =====================================================
CREATE TABLE companies (
    id SERIAL PRIMARY KEY,
    company_number VARCHAR(8) UNIQUE NOT NULL,
    company_name VARCHAR(500) NOT NULL,
    company_status VARCHAR(50) NOT NULL,

    -- Address (normalized)
    locality VARCHAR(200),
    postal_code VARCHAR(20),
    address_line_1 VARCHAR(500),
    address_line_2 VARCHAR(500),
    region VARCHAR(100),
    country VARCHAR(100),

    -- Business classification
    sic_codes TEXT[],
    primary_sic_code VARCHAR(5),
    industry_category VARCHAR(200),

    -- Dates
    incorporation_date DATE,
    dissolution_date DATE,

    -- Metadata
    first_seen TIMESTAMP DEFAULT NOW(),
    last_updated TIMESTAMP DEFAULT NOW(),
    data_quality_score DECIMAL(3,2), -- 0.00 to 1.00

    -- Keep raw data for reference
    raw_data JSONB,

    -- Data lineage
    source_batch_id VARCHAR(50),

    CHECK (data_quality_score >= 0 AND data_quality_score <= 1)
);

-- =====================================================
-- Officers (normalized, deduplicated)
-- =====================================================
CREATE TABLE officers (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
    company_number VARCHAR(8) NOT NULL,

    -- Officer identity (normalized)
    officer_name VARCHAR(500) NOT NULL,
    officer_name_normalized VARCHAR(500), -- For matching: lowercase, no punctuation
    officer_role VARCHAR(200) NOT NULL,

    -- Dates
    appointed_on DATE,
    resigned_on DATE,
    is_active BOOLEAN GENERATED ALWAYS AS (resigned_on IS NULL) STORED,

    -- Personal details
    nationality VARCHAR(100),
    occupation VARCHAR(200),
    date_of_birth_month INTEGER,
    date_of_birth_year INTEGER,

    -- Address (normalized)
    address_line_1 VARCHAR(500),
    address_line_2 VARCHAR(500),
    locality VARCHAR(200),
    postal_code VARCHAR(20),
    country VARCHAR(100),

    -- Metadata
    first_seen TIMESTAMP DEFAULT NOW(),
    last_updated TIMESTAMP DEFAULT NOW(),

    -- Raw data reference
    raw_data JSONB,

    -- Data lineage
    source_batch_id VARCHAR(50),

    -- Prevent exact duplicates
    UNIQUE(company_number, officer_name, appointed_on, officer_role)
);

-- =====================================================
-- Financials (clean, validated)
-- =====================================================
CREATE TABLE financials (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
    company_number VARCHAR(8) NOT NULL,

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

-- =====================================================
-- Contact enrichments (verified only)
-- =====================================================
CREATE TABLE contact_enrichments (
    id SERIAL PRIMARY KEY,
    officer_id INTEGER REFERENCES officers(id) ON DELETE CASCADE,

    -- Contact details
    email VARCHAR(255),
    email_verified BOOLEAN DEFAULT false,
    phone VARCHAR(50),
    phone_verified BOOLEAN DEFAULT false,
    linkedin_url VARCHAR(500),

    -- Quality metrics
    source VARCHAR(50) NOT NULL,
    confidence_score DECIMAL(3,2) NOT NULL,

    -- Verification
    enriched_at TIMESTAMP DEFAULT NOW(),
    verified_at TIMESTAMP,
    last_attempted_contact TIMESTAMP,

    -- Raw provider data
    raw_data JSONB,

    -- Data lineage
    source_batch_id VARCHAR(50),

    -- Only high-confidence contacts in production
    CHECK (confidence_score >= 0.5 AND confidence_score <= 1.0),

    -- One contact record per officer per source
    UNIQUE(officer_id, source)
);

-- =====================================================
-- Indexes for production queries
-- =====================================================

-- Companies
CREATE INDEX idx_companies_number ON companies(company_number);
CREATE INDEX idx_companies_name ON companies(company_name);
CREATE INDEX idx_companies_status ON companies(company_status);
CREATE INDEX idx_companies_locality ON companies(locality);
CREATE INDEX idx_companies_region ON companies(region);
CREATE INDEX idx_companies_primary_sic ON companies(primary_sic_code);
CREATE INDEX idx_companies_sic_array ON companies USING GIN(sic_codes);
CREATE INDEX idx_companies_updated ON companies(last_updated);
CREATE INDEX idx_companies_raw_data ON companies USING GIN(raw_data);

-- Full-text search on company name
CREATE INDEX idx_companies_name_trgm ON companies USING gin(company_name gin_trgm_ops);

-- Officers
CREATE INDEX idx_officers_company_id ON officers(company_id);
CREATE INDEX idx_officers_company_number ON officers(company_number);
CREATE INDEX idx_officers_name ON officers(officer_name);
CREATE INDEX idx_officers_normalized_name ON officers(officer_name_normalized);
CREATE INDEX idx_officers_role ON officers(officer_role);
CREATE INDEX idx_officers_active ON officers(is_active);
CREATE INDEX idx_officers_appointed ON officers(appointed_on);

-- Financials
CREATE INDEX idx_financials_company_id ON financials(company_id);
CREATE INDEX idx_financials_company_number ON financials(company_number);
CREATE INDEX idx_financials_period_end ON financials(period_end DESC);
CREATE INDEX idx_financials_turnover ON financials(turnover);

-- Contact enrichments
CREATE INDEX idx_contacts_officer_id ON contact_enrichments(officer_id);
CREATE INDEX idx_contacts_verified ON contact_enrichments(verified_at);
CREATE INDEX idx_contacts_confidence ON contact_enrichments(confidence_score);

-- =====================================================
-- Views for frontend queries
-- =====================================================

-- Active companies with latest financial data
CREATE VIEW active_companies_with_financials AS
SELECT
    c.company_number,
    c.company_name,
    c.company_status,
    c.locality,
    c.region,
    c.primary_sic_code,
    c.industry_category,
    c.incorporation_date,
    f.period_end as latest_accounts_date,
    f.turnover,
    f.profit_after_tax,
    f.total_assets,
    f.net_worth,
    f.profit_margin,
    f.current_ratio,
    COUNT(o.id) FILTER (WHERE o.is_active) as active_officers_count
FROM companies c
LEFT JOIN LATERAL (
    SELECT * FROM financials
    WHERE company_id = c.id
    ORDER BY period_end DESC
    LIMIT 1
) f ON true
LEFT JOIN officers o ON c.id = o.company_id
WHERE c.company_status = 'active'
GROUP BY c.id, c.company_number, c.company_name, c.company_status, c.locality,
         c.region, c.primary_sic_code, c.industry_category, c.incorporation_date,
         f.period_end, f.turnover, f.profit_after_tax, f.total_assets,
         f.net_worth, f.profit_margin, f.current_ratio;

-- Officers with contact information
CREATE VIEW officers_with_contacts AS
SELECT
    o.id as officer_id,
    o.officer_name,
    o.officer_role,
    o.company_number,
    c.company_name,
    o.appointed_on,
    o.is_active,
    o.nationality,
    o.occupation,
    ce.email,
    ce.email_verified,
    ce.phone,
    ce.phone_verified,
    ce.linkedin_url,
    ce.confidence_score,
    ce.source as contact_source
FROM officers o
JOIN companies c ON o.company_id = c.id
LEFT JOIN contact_enrichments ce ON o.id = ce.officer_id
WHERE o.is_active = true
ORDER BY ce.confidence_score DESC NULLS LAST;

-- Company overview (for detail page)
CREATE VIEW company_overview AS
SELECT
    c.id,
    c.company_number,
    c.company_name,
    c.company_status,
    c.locality,
    c.region,
    c.postal_code,
    c.primary_sic_code,
    c.industry_category,
    c.incorporation_date,
    c.data_quality_score,
    COUNT(DISTINCT o.id) FILTER (WHERE o.is_active) as active_officers,
    COUNT(DISTINCT f.id) as financial_periods_available,
    MAX(f.period_end) as latest_accounts_date,
    COUNT(DISTINCT ce.id) as verified_contacts_count
FROM companies c
LEFT JOIN officers o ON c.id = o.company_id
LEFT JOIN financials f ON c.id = f.company_id
LEFT JOIN contact_enrichments ce ON o.id = ce.officer_id AND ce.verified_at IS NOT NULL
GROUP BY c.id, c.company_number, c.company_name, c.company_status, c.locality,
         c.region, c.postal_code, c.primary_sic_code, c.industry_category,
         c.incorporation_date, c.data_quality_score;

-- =====================================================
-- Triggers for automatic updates
-- =====================================================

-- Update last_updated timestamp
CREATE OR REPLACE FUNCTION update_last_updated()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_updated = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER companies_update_timestamp
    BEFORE UPDATE ON companies
    FOR EACH ROW
    EXECUTE FUNCTION update_last_updated();

CREATE TRIGGER officers_update_timestamp
    BEFORE UPDATE ON officers
    FOR EACH ROW
    EXECUTE FUNCTION update_last_updated();

CREATE TRIGGER financials_update_timestamp
    BEFORE UPDATE ON financials
    FOR EACH ROW
    EXECUTE FUNCTION update_last_updated();

-- =====================================================
-- Comments for documentation
-- =====================================================
COMMENT ON TABLE companies IS 'Production company data - clean, deduplicated, validated';
COMMENT ON TABLE officers IS 'Production officer data - normalized names, active status computed';
COMMENT ON TABLE financials IS 'Production financial data - validated figures with calculated ratios';
COMMENT ON TABLE contact_enrichments IS 'Verified contact information - minimum 50% confidence required';
COMMENT ON VIEW active_companies_with_financials IS 'Frontend view: active companies with their latest financial snapshot';
COMMENT ON VIEW officers_with_contacts IS 'Frontend view: officers with verified contact details';
COMMENT ON VIEW company_overview IS 'Frontend view: company summary with counts for detail page';
