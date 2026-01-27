-- =====================================================
-- Contact enrichments (verified only)
-- =====================================================
DROP TABLE IF EXISTS contact_enrichments CASCADE;

CREATE TABLE contact_enrichments (
    id SERIAL PRIMARY KEY,
    officer_id INTEGER REFERENCES production_officers(id) ON DELETE CASCADE,

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

-- Indexes
CREATE INDEX idx_contacts_officer_id ON contact_enrichments(officer_id);
CREATE INDEX idx_contacts_verified ON contact_enrichments(verified_at);
CREATE INDEX idx_contacts_confidence ON contact_enrichments(confidence_score);

-- Comments
COMMENT ON TABLE contact_enrichments IS 'Verified contact information - minimum 50% confidence required';
