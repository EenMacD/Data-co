-- =====================================================
-- Contact enrichments (future third-party data)
-- =====================================================
DROP TABLE IF EXISTS staging_contact_enrichments CASCADE;

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
