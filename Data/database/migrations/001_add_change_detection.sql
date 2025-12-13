-- Migration: Add change detection support for one-record-per-company model
-- Run this AFTER backing up your database if you have existing data

-- =====================================================
-- 1. Modify staging_companies table
-- =====================================================

-- Change UNIQUE constraint from (batch_id, company_number) to just (company_number)
-- This enables auto-update instead of creating duplicates per batch
ALTER TABLE staging_companies DROP CONSTRAINT IF EXISTS staging_companies_batch_id_company_number_key;

-- Add new UNIQUE constraint on company_number only (if not exists)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'staging_companies_company_number_key'
    ) THEN
        ALTER TABLE staging_companies ADD CONSTRAINT staging_companies_company_number_key UNIQUE (company_number);
    END IF;
END $$;

-- Add columns for change detection
ALTER TABLE staging_companies ADD COLUMN IF NOT EXISTS data_hash VARCHAR(32);
ALTER TABLE staging_companies ADD COLUMN IF NOT EXISTS change_detected BOOLEAN DEFAULT FALSE;
ALTER TABLE staging_companies ADD COLUMN IF NOT EXISTS last_updated TIMESTAMP DEFAULT NOW();

-- Create index for efficient hash lookups
CREATE INDEX IF NOT EXISTS idx_staging_companies_hash ON staging_companies(company_number, data_hash);
CREATE INDEX IF NOT EXISTS idx_staging_companies_last_updated ON staging_companies(last_updated);
CREATE INDEX IF NOT EXISTS idx_staging_companies_change_detected ON staging_companies(change_detected) WHERE change_detected = TRUE;

-- =====================================================
-- 2. Modify staging_ingestion_log table for file tracking
-- =====================================================

ALTER TABLE staging_ingestion_log ADD COLUMN IF NOT EXISTS files_total INTEGER DEFAULT 0;
ALTER TABLE staging_ingestion_log ADD COLUMN IF NOT EXISTS files_completed INTEGER DEFAULT 0;
ALTER TABLE staging_ingestion_log ADD COLUMN IF NOT EXISTS current_file VARCHAR(500);
ALTER TABLE staging_ingestion_log ADD COLUMN IF NOT EXISTS current_file_progress INTEGER DEFAULT 0;

-- =====================================================
-- 3. Add similar columns to staging_officers for PSC data
-- =====================================================

ALTER TABLE staging_officers ADD COLUMN IF NOT EXISTS data_hash VARCHAR(32);
ALTER TABLE staging_officers ADD COLUMN IF NOT EXISTS change_detected BOOLEAN DEFAULT FALSE;
ALTER TABLE staging_officers ADD COLUMN IF NOT EXISTS last_updated TIMESTAMP DEFAULT NOW();

-- Create index for officer deduplication
CREATE INDEX IF NOT EXISTS idx_staging_officers_hash ON staging_officers(company_number, officer_name, data_hash);

-- =====================================================
-- 4. Add merge tracking columns
-- =====================================================

-- Add merged_at to staging_companies for tracking which records have been merged to production
ALTER TABLE staging_companies ADD COLUMN IF NOT EXISTS merged_at TIMESTAMP;
CREATE INDEX IF NOT EXISTS idx_staging_companies_merged_at ON staging_companies(merged_at) WHERE merged_at IS NULL;

-- Add merged_at to staging_financials
ALTER TABLE staging_financials ADD COLUMN IF NOT EXISTS merged_at TIMESTAMP;
CREATE INDEX IF NOT EXISTS idx_staging_financials_merged_at ON staging_financials(merged_at) WHERE merged_at IS NULL;

-- =====================================================
-- 5. Remove batch_id dependency from staging_companies
-- =====================================================

-- Drop the batch_id index if it exists
DROP INDEX IF EXISTS idx_staging_companies_batch;

-- Make batch_id nullable (we're moving away from batch-based model)
-- Note: We can't easily drop the FK constraint without knowing its exact name,
-- so we'll just make it nullable and ignore it
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'staging_companies' 
        AND column_name = 'batch_id'
    ) THEN
        ALTER TABLE staging_companies ALTER COLUMN batch_id DROP NOT NULL;
    END IF;
END $$;

-- =====================================================
-- 6. Comments for documentation
-- =====================================================

COMMENT ON COLUMN staging_companies.data_hash IS 'MD5 hash of key fields for change detection';
COMMENT ON COLUMN staging_companies.change_detected IS 'TRUE if data changed during last ingestion';
COMMENT ON COLUMN staging_companies.last_updated IS 'Timestamp of last update to this record';
COMMENT ON COLUMN staging_companies.merged_at IS 'When this record was merged to production (NULL = pending)';
COMMENT ON COLUMN staging_financials.merged_at IS 'When this record was merged to production (NULL = pending)';
COMMENT ON COLUMN staging_ingestion_log.files_total IS 'Total number of files in this ingestion batch';
COMMENT ON COLUMN staging_ingestion_log.files_completed IS 'Number of files completed so far';
COMMENT ON COLUMN staging_ingestion_log.current_file IS 'URL/name of file currently being processed';
COMMENT ON COLUMN staging_ingestion_log.current_file_progress IS 'Progress percentage of current file (0-100)';
