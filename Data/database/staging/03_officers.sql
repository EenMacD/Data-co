-- =====================================================
-- Officers (raw from API/PSC bulk data)
-- =====================================================
DROP TABLE IF EXISTS staging_officers CASCADE;

CREATE TABLE staging_officers (
    id SERIAL PRIMARY KEY,
    company_number VARCHAR(8) NOT NULL REFERENCES staging_companies(company_number) ON DELETE CASCADE,

    -- Basic officer info
    officer_name VARCHAR(500),
    officer_role VARCHAR(200),
    appointed_on DATE,
    resigned_on DATE,
    -- Personal details
    date_of_birth DATE,
    nationality VARCHAR(100),
    nature_of_control TEXT,

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
    review_notes TEXT,

    UNIQUE(company_number, officer_name, appointed_on, officer_role, date_of_birth)
);

-- Indexes
CREATE INDEX idx_staging_officers_company ON staging_officers(company_number);
CREATE INDEX idx_staging_officers_name ON staging_officers(officer_name);
CREATE INDEX idx_staging_officers_needs_review ON staging_officers(needs_review);
-- Change detection index
CREATE INDEX idx_staging_officers_hash ON staging_officers(company_number, officer_name, data_hash);
CREATE INDEX idx_staging_officers_nature_of_control ON staging_officers USING gin(nature_of_control gin_trgm_ops);

-- Comments
COMMENT ON TABLE staging_officers IS 'Raw officer/PSC data - may contain duplicates and messy names';
