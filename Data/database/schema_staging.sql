-- =====================================================
-- STAGING DATABASE SCHEMA
-- Purpose: Ingest raw Companies House data for cleaning
-- Model: One record per company, updated in place with change detection
-- =====================================================

-- Drop existing tables if recreating
DROP TABLE IF EXISTS staging_contact_enrichments CASCADE;
DROP TABLE IF EXISTS staging_financials CASCADE;
DROP TABLE IF EXISTS staging_officers CASCADE;
DROP TABLE IF EXISTS staging_companies CASCADE;
DROP TABLE IF EXISTS staging_ingestion_log CASCADE;

-- =====================================================
-- Ingestion tracking (for audit trail and resume)
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
    metadata JSONB,
    -- File tracking for progress/resume
    files_total INTEGER DEFAULT 0,
    files_completed INTEGER DEFAULT 0,
    current_file VARCHAR(500),
    current_file_progress INTEGER DEFAULT 0
);

-- =====================================================
-- Companies (raw from API/bulk data)
-- One record per company, updated in place
-- =====================================================
CREATE TABLE staging_companies (
    id SERIAL PRIMARY KEY,
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
    raw_data JSONB NOT NULL DEFAULT '{}'::jsonb,

    -- Change detection
    data_hash VARCHAR(32),
    change_detected BOOLEAN DEFAULT FALSE,
    last_updated TIMESTAMP DEFAULT NOW(),

    -- Merge tracking
    merged_at TIMESTAMP,

    -- Metadata
    ingested_at TIMESTAMP DEFAULT NOW(),
    needs_review BOOLEAN DEFAULT false,
    review_notes TEXT,

    -- One record per company
    UNIQUE(company_number)
);

-- =====================================================
-- Officers (raw from API/PSC bulk data)
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
    raw_data JSONB NOT NULL DEFAULT '{}'::jsonb,

    -- Change detection
    data_hash VARCHAR(32),
    change_detected BOOLEAN DEFAULT FALSE,
    last_updated TIMESTAMP DEFAULT NOW(),

    -- Metadata
    ingested_at TIMESTAMP DEFAULT NOW(),
    needs_review BOOLEAN DEFAULT false,
    review_notes TEXT
);

-- =====================================================
-- Financials (from accounts bulk data)
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
    source VARCHAR(20), -- 'xbrl', 'ixbrl', 'ocr', 'api', 'bulk_xbrl'

    -- Raw data
    raw_data JSONB,
    
    -- Change detection
    data_hash VARCHAR(32),
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
CREATE INDEX idx_staging_companies_status ON staging_companies(company_status);
CREATE INDEX idx_staging_companies_locality ON staging_companies(locality);
CREATE INDEX idx_staging_companies_needs_review ON staging_companies(needs_review);
CREATE INDEX idx_staging_companies_raw_data ON staging_companies USING GIN(raw_data);
-- Change detection indexes
CREATE INDEX idx_staging_companies_hash ON staging_companies(company_number, data_hash);
CREATE INDEX idx_staging_companies_last_updated ON staging_companies(last_updated);
CREATE INDEX idx_staging_companies_change_detected ON staging_companies(change_detected) WHERE change_detected = TRUE;
-- Merge tracking index
CREATE INDEX idx_staging_companies_merged_at ON staging_companies(merged_at) WHERE merged_at IS NULL;

CREATE INDEX idx_staging_officers_company ON staging_officers(company_number);
CREATE INDEX idx_staging_officers_name ON staging_officers(officer_name);
CREATE INDEX idx_staging_officers_needs_review ON staging_officers(needs_review);
-- Change detection index
CREATE INDEX idx_staging_officers_hash ON staging_officers(company_number, officer_name, data_hash);

CREATE INDEX idx_staging_financials_company ON staging_financials(company_number);
CREATE INDEX idx_staging_financials_period ON staging_financials(period_end);
-- Change detection indexes
CREATE INDEX idx_staging_financials_hash ON staging_financials(company_number, period_end, data_hash);
CREATE INDEX idx_staging_financials_change_detected ON staging_financials(change_detected) WHERE change_detected = TRUE;
-- Merge tracking index
CREATE INDEX idx_staging_financials_merged_at ON staging_financials(merged_at) WHERE merged_at IS NULL;

-- =====================================================
-- Helper views for data quality checks
-- =====================================================

-- View: Companies needing review
CREATE VIEW staging_review_queue AS
SELECT
    c.id,
    c.company_number,
    c.company_name,
    c.needs_review,
    c.review_notes,
    c.ingested_at,
    c.last_updated,
    COUNT(o.id) as officer_count
FROM staging_companies c
LEFT JOIN staging_officers o ON c.id = o.staging_company_id
WHERE c.needs_review = true
GROUP BY c.id, c.company_number, c.company_name, c.needs_review, c.review_notes, c.ingested_at, c.last_updated
ORDER BY c.last_updated DESC;

-- View: Data quality summary
CREATE VIEW staging_data_quality AS
SELECT
    COUNT(*) as total_companies,
    COUNT(*) FILTER (WHERE company_name IS NULL OR company_name = '') as missing_names,
    COUNT(*) FILTER (WHERE locality IS NULL OR locality = '') as missing_locality,
    COUNT(*) FILTER (WHERE sic_codes IS NULL OR array_length(sic_codes, 1) = 0) as missing_sic,
    COUNT(*) FILTER (WHERE needs_review = true) as needs_review,
    COUNT(*) FILTER (WHERE merged_at IS NULL) as pending_merge,
    COUNT(*) FILTER (WHERE change_detected = true) as recently_changed,
    MAX(last_updated) as latest_update
FROM staging_companies;

-- View: Pending merge summary
CREATE VIEW staging_pending_merge AS
SELECT
    'companies' as table_name,
    COUNT(*) as pending_count,
    MIN(ingested_at) as oldest_record,
    MAX(last_updated) as newest_record
FROM staging_companies WHERE merged_at IS NULL
UNION ALL
SELECT
    'financials' as table_name,
    COUNT(*) as pending_count,
    MIN(ingested_at) as oldest_record,
    MAX(ingested_at) as newest_record
FROM staging_financials WHERE merged_at IS NULL;

-- =====================================================
-- Comments for documentation
-- =====================================================
COMMENT ON TABLE staging_companies IS 'Raw company data from Companies House - one record per company, updated in place with change detection';
COMMENT ON TABLE staging_officers IS 'Raw officer/PSC data - may contain duplicates and messy names';
COMMENT ON TABLE staging_ingestion_log IS 'Track each ingestion batch for auditing and resume capability';
COMMENT ON COLUMN staging_companies.data_hash IS 'MD5 hash of key fields for change detection';
COMMENT ON COLUMN staging_companies.change_detected IS 'TRUE if data changed during last ingestion';
COMMENT ON COLUMN staging_companies.last_updated IS 'Timestamp of last update to this record';
COMMENT ON COLUMN staging_companies.merged_at IS 'When this record was merged to production (NULL = pending)';
COMMENT ON COLUMN staging_companies.needs_review IS 'Flag records that need manual review before merging to production';
COMMENT ON COLUMN staging_companies.raw_data IS 'Complete JSON response from Companies House API';
