-- Migration: Fix staging_financials constraints and add change detection
-- Run this AFTER backing up your database if you have existing data

-- =====================================================
-- 1. Modify staging_financials table
-- =====================================================

-- Add columns for change detection
ALTER TABLE staging_financials ADD COLUMN IF NOT EXISTS data_hash VARCHAR(32);
ALTER TABLE staging_financials ADD COLUMN IF NOT EXISTS change_detected BOOLEAN DEFAULT FALSE;
ALTER TABLE staging_financials ADD COLUMN IF NOT EXISTS last_updated TIMESTAMP DEFAULT NOW();

-- Create unique constraint for UPSERT support
-- We use (company_number, period_end) as the unique key for a financial record
-- Note: If period_end is NULL, standard UNIQUE might allow duplicates depending on PG version/settings,
-- but financial records typically have period_end.
ALTER TABLE staging_financials DROP CONSTRAINT IF EXISTS staging_financials_unique_key;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'staging_financials_unique_key'
    ) THEN
        ALTER TABLE staging_financials ADD CONSTRAINT staging_financials_unique_key UNIQUE (company_number, period_end);
    END IF;
END $$;

-- Create index for efficient hash lookups
CREATE INDEX IF NOT EXISTS idx_staging_financials_hash ON staging_financials(company_number, period_end, data_hash);
CREATE INDEX IF NOT EXISTS idx_staging_financials_change_detected ON staging_financials(change_detected) WHERE change_detected = TRUE;

-- =====================================================
-- 2. Comments
-- =====================================================
COMMENT ON COLUMN staging_financials.data_hash IS 'MD5 hash of key fields for change detection';
COMMENT ON COLUMN staging_financials.change_detected IS 'TRUE if data changed during last ingestion';
COMMENT ON COLUMN staging_financials.last_updated IS 'Timestamp of last update to this record';
