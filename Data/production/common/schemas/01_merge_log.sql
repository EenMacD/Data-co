-- =====================================================
-- Merge tracking
-- =====================================================
DROP TABLE IF EXISTS merge_log CASCADE;

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
