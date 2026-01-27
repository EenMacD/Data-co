-- =====================================================
-- Companies (raw from API/bulk data)
-- One record per company, updated in place
-- =====================================================
DROP TABLE IF EXISTS staging_companies CASCADE;

CREATE TABLE staging_companies (
    company_number VARCHAR(8) PRIMARY KEY, -- Changed from id to company_number as PK
    company_name VARCHAR(500),
    company_status VARCHAR(50),
    company_type VARCHAR(100), -- New

    -- Address fields from CSV
    locality VARCHAR(200),
    postal_code VARCHAR(20),
    address_line_1 VARCHAR(500),
    address_line_2 VARCHAR(500),
    region VARCHAR(100),
    country VARCHAR(100),

    -- SIC codes (stored as array)
    sic_codes TEXT[],

    -- New columns matching production
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

    -- Raw API response (everything else goes here)
    raw_data JSONB NOT NULL DEFAULT '{}'::jsonb,

    -- Change detection
    data_hash VARCHAR(32),
    change_detected BOOLEAN DEFAULT FALSE,
    last_updated TIMESTAMP DEFAULT NOW(),

    -- Merge tracking
    merged_at TIMESTAMP,
    batch_id VARCHAR(50), -- Tracks which batch last updated this record

    -- Metadata
    ingested_at TIMESTAMP DEFAULT NOW(),
    needs_review BOOLEAN DEFAULT false,
    review_notes TEXT
);

-- Indexes for staging queries
-- PK index exists for company_number
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

-- Comments
COMMENT ON TABLE staging_companies IS 'Raw company data from Companies House - one record per company, updated in place with change detection';
COMMENT ON COLUMN staging_companies.data_hash IS 'MD5 hash of key fields for change detection';
COMMENT ON COLUMN staging_companies.change_detected IS 'TRUE if data changed during last ingestion';
COMMENT ON COLUMN staging_companies.last_updated IS 'Timestamp of last update to this record';
COMMENT ON COLUMN staging_companies.merged_at IS 'When this record was merged to production (NULL = pending)';
COMMENT ON COLUMN staging_companies.needs_review IS 'Flag records that need manual review before merging to production';
COMMENT ON COLUMN staging_companies.raw_data IS 'Complete JSON response from Companies House API';
