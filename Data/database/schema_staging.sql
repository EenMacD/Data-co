-- =====================================================
-- STAGING DATABASE SCHEMA
-- Purpose: Ingest raw Companies House data for cleaning
-- =====================================================

-- Drop existing tables if recreating
DROP TABLE IF EXISTS staging_contact_enrichments CASCADE;
DROP TABLE IF EXISTS staging_financials CASCADE;
DROP TABLE IF EXISTS staging_officers CASCADE;
DROP TABLE IF EXISTS staging_companies CASCADE;
DROP TABLE IF EXISTS staging_ingestion_log CASCADE;

-- =====================================================
-- Ingestion tracking
-- =====================================================
CREATE TABLE staging_ingestion_log (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR(50) UNIQUE NOT NULL,
    search_name VARCHAR(100),
    started_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP,
    companies_count INTEGER DEFAULT 0,
    status VARCHAR(20) DEFAULT 'running', -- 'running', 'completed', 'failed'
    error_message TEXT,
    metadata JSONB
);

-- =====================================================
-- Companies (raw from API)
-- =====================================================
CREATE TABLE staging_companies (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR(50) REFERENCES staging_ingestion_log(batch_id),
    company_number VARCHAR(8) NOT NULL,
    company_name VARCHAR(500),
    company_status VARCHAR(50),

    -- Address fields from CSV
    locality VARCHAR(200),
    postal_code VARCHAR(20),
    address_line_1 VARCHAR(500),
    address_line_2 VARCHAR(500),
    region VARCHAR(100),
    country VARCHAR(100),

    -- SIC codes (stored as array)
    sic_codes TEXT[],

    -- Raw API response (everything else goes here)
    raw_data JSONB NOT NULL,

    -- Metadata
    ingested_at TIMESTAMP DEFAULT NOW(),
    needs_review BOOLEAN DEFAULT false,
    review_notes TEXT,

    -- Allow duplicates in staging (we'll dedupe when merging)
    UNIQUE(batch_id, company_number)
);

-- =====================================================
-- Officers (raw from API)
-- =====================================================
CREATE TABLE staging_officers (
    id SERIAL PRIMARY KEY,
    staging_company_id INTEGER REFERENCES staging_companies(id) ON DELETE CASCADE,
    company_number VARCHAR(8) NOT NULL,

    -- Basic officer info
    officer_name VARCHAR(500),
    officer_role VARCHAR(200),
    appointed_on DATE,
    resigned_on DATE,
    nationality VARCHAR(100),
    occupation VARCHAR(200),

    -- Address
    address_line_1 VARCHAR(500),
    address_line_2 VARCHAR(500),
    locality VARCHAR(200),
    postal_code VARCHAR(20),
    country VARCHAR(100),

    -- Raw API response
    raw_data JSONB NOT NULL,

    -- Metadata
    ingested_at TIMESTAMP DEFAULT NOW(),
    needs_review BOOLEAN DEFAULT false,
    review_notes TEXT
);

-- =====================================================
-- Financials (will be populated later)
-- =====================================================
CREATE TABLE staging_financials (
    id SERIAL PRIMARY KEY,
    staging_company_id INTEGER REFERENCES staging_companies(id) ON DELETE CASCADE,
    company_number VARCHAR(8) NOT NULL,

    -- Financial period
    period_start DATE,
    period_end DATE,

    -- Key figures
    turnover NUMERIC(15, 2),
    profit_loss NUMERIC(15, 2),
    total_assets NUMERIC(15, 2),
    total_liabilities NUMERIC(15, 2),
    net_worth NUMERIC(15, 2),

    -- Data source
    source VARCHAR(20), -- 'xbrl', 'ixbrl', 'ocr', 'api'

    -- Raw data
    raw_data JSONB,

    -- Metadata
    ingested_at TIMESTAMP DEFAULT NOW(),
    needs_review BOOLEAN DEFAULT false,
    review_notes TEXT
);

-- =====================================================
-- Contact enrichments (future third-party data)
-- =====================================================
CREATE TABLE staging_contact_enrichments (
    id SERIAL PRIMARY KEY,
    staging_officer_id INTEGER REFERENCES staging_officers(id) ON DELETE CASCADE,

    -- Contact details
    email VARCHAR(255),
    phone VARCHAR(50),
    linkedin_url VARCHAR(500),

    -- Source tracking
    source VARCHAR(50), -- 'apollo', 'linkedin', 'manual', etc.
    confidence_score DECIMAL(3,2), -- 0.00 to 1.00

    -- Raw provider response
    raw_data JSONB,

    -- Metadata
    enriched_at TIMESTAMP DEFAULT NOW(),
    verified_at TIMESTAMP,
    needs_review BOOLEAN DEFAULT false,
    review_notes TEXT
);

-- =====================================================
-- Indexes for staging queries
-- =====================================================
CREATE INDEX idx_staging_companies_number ON staging_companies(company_number);
CREATE INDEX idx_staging_companies_batch ON staging_companies(batch_id);
CREATE INDEX idx_staging_companies_status ON staging_companies(company_status);
CREATE INDEX idx_staging_companies_locality ON staging_companies(locality);
CREATE INDEX idx_staging_companies_needs_review ON staging_companies(needs_review);
CREATE INDEX idx_staging_companies_raw_data ON staging_companies USING GIN(raw_data);

CREATE INDEX idx_staging_officers_company ON staging_officers(company_number);
CREATE INDEX idx_staging_officers_name ON staging_officers(officer_name);
CREATE INDEX idx_staging_officers_needs_review ON staging_officers(needs_review);

CREATE INDEX idx_staging_financials_company ON staging_financials(company_number);
CREATE INDEX idx_staging_financials_period ON staging_financials(period_end);

-- =====================================================
-- Helper views for data quality checks
-- =====================================================

-- View: Companies needing review
CREATE VIEW staging_review_queue AS
SELECT
    c.id,
    c.company_number,
    c.company_name,
    c.batch_id,
    c.needs_review,
    c.review_notes,
    c.ingested_at,
    COUNT(o.id) as officer_count
FROM staging_companies c
LEFT JOIN staging_officers o ON c.id = o.staging_company_id
WHERE c.needs_review = true
GROUP BY c.id, c.company_number, c.company_name, c.batch_id, c.needs_review, c.review_notes, c.ingested_at
ORDER BY c.ingested_at DESC;

-- View: Data quality summary
CREATE VIEW staging_data_quality AS
SELECT
    batch_id,
    COUNT(*) as total_companies,
    COUNT(*) FILTER (WHERE company_name IS NULL OR company_name = '') as missing_names,
    COUNT(*) FILTER (WHERE locality IS NULL OR locality = '') as missing_locality,
    COUNT(*) FILTER (WHERE sic_codes IS NULL OR array_length(sic_codes, 1) = 0) as missing_sic,
    COUNT(*) FILTER (WHERE needs_review = true) as needs_review,
    MAX(ingested_at) as latest_ingestion
FROM staging_companies
GROUP BY batch_id
ORDER BY latest_ingestion DESC;

-- =====================================================
-- Comments for documentation
-- =====================================================
COMMENT ON TABLE staging_companies IS 'Raw company data from Companies House API - allow duplicates, clean before merging to production';
COMMENT ON TABLE staging_officers IS 'Raw officer/PSC data - may contain duplicates and messy names';
COMMENT ON TABLE staging_ingestion_log IS 'Track each batch of data ingested for auditing';
COMMENT ON COLUMN staging_companies.needs_review IS 'Flag records that need manual review before merging to production';
COMMENT ON COLUMN staging_companies.raw_data IS 'Complete JSON response from Companies House API';
