-- =====================================================
-- Ingestion tracking (for audit trail and resume)
-- =====================================================
DROP TABLE IF EXISTS staging_ingestion_log CASCADE;

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

COMMENT ON TABLE staging_ingestion_log IS 'Track each ingestion batch for auditing and resume capability';
