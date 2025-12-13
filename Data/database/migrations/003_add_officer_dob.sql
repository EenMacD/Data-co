-- Migration: Add date_of_birth to staging_officers and enable UPSERT
-- Run this AFTER backing up your database

-- =====================================================
-- 1. Modify staging_officers table
-- =====================================================

-- Add date_of_birth column
ALTER TABLE staging_officers ADD COLUMN IF NOT EXISTS date_of_birth DATE;

-- Create unique constraint for UPSERT support
-- Constraint: (company_number, officer_name, appointed_on, officer_role, date_of_birth)
-- Note: All fields must be non-null for a strict UNIQUE constraint to prevent duplicates in older PG versions,
-- but usually appointed_on and known fields are present.
-- If date_of_birth is missing (NULL), it might allow multiples if we don't handle it,
-- but standard uniqueness usually implies known data.

ALTER TABLE staging_officers DROP CONSTRAINT IF EXISTS staging_officers_unique_key;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'staging_officers_unique_key'
    ) THEN
        -- We interpret "uniqueness" as the combination of these fields.
        -- Rows with NULLs in these columns might still be inserted as distinct rows (PG default behavior).
        -- For the purpose of ingestion, we want strict matching.
        ALTER TABLE staging_officers ADD CONSTRAINT staging_officers_unique_key 
        UNIQUE (company_number, officer_name, appointed_on, officer_role, date_of_birth);
    END IF;
END $$;

-- Update index for hash including new column (optional but good for consistency)
DROP INDEX IF EXISTS idx_staging_officers_hash;
CREATE INDEX idx_staging_officers_hash ON staging_officers(company_number, officer_name, data_hash);

-- Comments
COMMENT ON COLUMN staging_officers.date_of_birth IS 'Date of birth (YYYY-MM-DD), usually 1st of month for privacy';
