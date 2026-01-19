-- =====================================================
-- Helper views for data quality checks
-- =====================================================

-- View: Companies needing review
CREATE OR REPLACE VIEW staging_review_queue AS
SELECT
    c.company_number,
    c.company_name,
    c.needs_review,
    c.review_notes,
    c.ingested_at,
    c.last_updated,
    COUNT(o.id) as officer_count
FROM staging_companies c
LEFT JOIN staging_officers o ON c.company_number = o.company_number
WHERE c.needs_review = true
GROUP BY c.company_number, c.company_name, c.needs_review, c.review_notes, c.ingested_at, c.last_updated
ORDER BY c.last_updated DESC;

-- View: Data quality summary
CREATE OR REPLACE VIEW staging_data_quality AS
SELECT
    COUNT(*) as total_companies,
    COUNT(*) FILTER (WHERE company_name IS NULL OR company_name = '') as missing_names,
    COUNT(*) FILTER (WHERE locality IS NULL OR locality = '') as missing_locality,
    COUNT(*) FILTER (WHERE sic_codes IS NULL OR array_length(sic_codes, 1) = 0) as missing_sic,
    COUNT(*) FILTER (WHERE needs_review = true) as needs_review,
    COUNT(*) FILTER (WHERE merged_at IS NULL) as pending_merge,
    COUNT(*) FILTER (WHERE change_detected = true) as recently_changed,
    MAX(last_updated) as latest_update
FROM staging_companies;

-- View: Pending merge summary
CREATE OR REPLACE VIEW staging_pending_merge AS
SELECT
    'companies' as table_name,
    COUNT(*) as pending_count,
    MIN(ingested_at) as oldest_record,
    MAX(last_updated) as newest_record
FROM staging_companies WHERE merged_at IS NULL
UNION ALL
SELECT
    'financials' as table_name,
    COUNT(*) as pending_count,
    MIN(ingested_at) as oldest_record,
    MAX(ingested_at) as newest_record
FROM staging_financials WHERE merged_at IS NULL;
