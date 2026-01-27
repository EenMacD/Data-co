-- =====================================================
-- Officers (normalized, deduplicated)
-- =====================================================
DROP TABLE IF EXISTS production_officers CASCADE;

CREATE TABLE production_officers (
    id SERIAL PRIMARY KEY,
    company_number VARCHAR(8) NOT NULL REFERENCES production_companies(company_number) ON DELETE CASCADE,

    -- Officer identity (normalized)
    officer_name VARCHAR(500), -- Nullable as requested
    -- officer_name_normalized removed
    officer_role VARCHAR(200), -- Nullable as requested

    -- Dates
    appointed_on DATE,
    resigned_on DATE,
    -- is_active removed
    is_active BOOLEAN GENERATED ALWAYS AS (resigned_on IS NULL) STORED,

    -- Personal details
    nationality VARCHAR(100),
    -- occupation removed
    date_of_birth DATE,
    -- date_of_birth_month removed
    -- date_of_birth_year removed

    nature_of_control TEXT, -- List separated by |

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
    UNIQUE(company_number, officer_name, appointed_on, officer_role, date_of_birth)
);

-- Indexes
CREATE INDEX idx_officers_company_number ON production_officers(company_number);
CREATE INDEX idx_officers_name ON production_officers(officer_name);
CREATE INDEX idx_officers_role ON production_officers(officer_role);
CREATE INDEX idx_officers_appointed ON production_officers(appointed_on);
CREATE INDEX idx_officers_nature_of_control ON production_officers USING gin(nature_of_control gin_trgm_ops);

-- Trigger
CREATE TRIGGER officers_update_timestamp
    BEFORE UPDATE ON production_officers
    FOR EACH ROW
    EXECUTE FUNCTION update_last_updated();

-- Comments
COMMENT ON TABLE production_officers IS 'Production officer data - normalized names, active status computed';
