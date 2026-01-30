-- =====================================================
-- Companies (clean, deduplicated)
-- =====================================================
DROP TABLE IF EXISTS production_companies CASCADE;

CREATE TABLE production_companies (
    company_number VARCHAR(8) PRIMARY KEY, -- Changed from id to company_number as PK
    company_name VARCHAR(500) NOT NULL,
    company_status VARCHAR(50) NOT NULL,
    company_type VARCHAR(100), -- Added

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

    -- Dates & Accounts
    incorporation_date DATE,
    dissolution_date DATE,
    
    accounts_last_made_up_date DATE,
    accounts_ref_date CHAR(5), -- MM-DD
    accounts_next_due_date DATE,
    account_category VARCHAR(30),
    
    returns_next_due_date DATE,
    returns_last_made_up_date DATE,
    
    conf_stm_next_due_date DATE,
    conf_stm_last_made_up_date DATE,

    -- Mortgages
    num_mort_charges INTEGER,
    num_mort_outstanding INTEGER,
    num_mort_part_satisfied INTEGER,

    -- Previous Names
    previous_names TEXT, -- List separated by "|"

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

-- Indexes
-- PK constraint already creates index on company_number
CREATE INDEX idx_companies_name ON production_companies(company_name);
CREATE INDEX idx_companies_status ON production_companies(company_status);
CREATE INDEX idx_companies_locality ON production_companies(locality);
CREATE INDEX idx_companies_region ON production_companies(region);
CREATE INDEX idx_companies_primary_sic ON production_companies(primary_sic_code);
CREATE INDEX idx_companies_sic_array ON production_companies USING GIN(sic_codes);
CREATE INDEX idx_companies_updated ON production_companies(last_updated);
CREATE INDEX idx_companies_raw_data ON production_companies USING GIN(raw_data);
CREATE INDEX idx_companies_type ON production_companies(company_type);

-- Dates indexes
CREATE INDEX idx_companies_inc_date ON production_companies(incorporation_date);
CREATE INDEX idx_companies_accounts_next ON production_companies(accounts_next_due_date);
CREATE INDEX idx_companies_returns_next ON production_companies(returns_next_due_date);
CREATE INDEX idx_companies_conf_next ON production_companies(conf_stm_next_due_date);

-- Full-text search on company name
CREATE INDEX idx_companies_name_trgm ON production_companies USING gin(company_name gin_trgm_ops);

-- Previous names search
CREATE INDEX idx_companies_prev_names_trgm ON production_companies USING gin(previous_names gin_trgm_ops);

-- Trigger
CREATE TRIGGER companies_update_timestamp
    BEFORE UPDATE ON production_companies
    FOR EACH ROW
    EXECUTE FUNCTION update_last_updated();

-- Comments
COMMENT ON TABLE production_companies IS 'Production company data - clean, deduplicated, validated';
