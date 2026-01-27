-- =====================================================
-- Views for frontend queries
-- =====================================================

-- Active companies with latest financial data
CREATE OR REPLACE VIEW active_companies_with_financials AS
SELECT
    c.company_number,
    c.company_name,
    c.company_status,
    c.company_type,
    c.locality,
    c.region,
    c.primary_sic_code,
    c.industry_category,
    c.incorporation_date,
    f.period_end as latest_accounts_date,
    f.turnover,
    f.profit_after_tax,
    f.total_assets,
    f.net_worth,
    f.profit_margin,
    f.current_ratio,
    COUNT(o.id) FILTER (WHERE o.is_active) as active_officers_count
FROM production_companies c
LEFT JOIN LATERAL (
    SELECT * FROM production_financials
    WHERE company_number = c.company_number
    ORDER BY period_end DESC
    LIMIT 1
) f ON true
LEFT JOIN production_officers o ON c.company_number = o.company_number
WHERE c.company_status = 'active'
GROUP BY c.company_number, c.company_name, c.company_status, c.company_type, c.locality,
         c.region, c.primary_sic_code, c.industry_category, c.incorporation_date,
         f.period_end, f.turnover, f.profit_after_tax, f.total_assets,
         f.net_worth, f.profit_margin, f.current_ratio;

-- Officers with contact information
CREATE OR REPLACE VIEW officers_with_contacts AS
SELECT
    o.id as officer_id,
    o.officer_name,
    o.officer_role,
    o.company_number,
    c.company_name,
    o.appointed_on,
    o.is_active,
    o.nationality,
    -- o.occupation, -- removed from table
    ce.email,
    ce.email_verified,
    ce.phone,
    ce.phone_verified,
    ce.linkedin_url,
    ce.confidence_score,
    ce.source as contact_source
FROM production_officers o
JOIN production_companies c ON o.company_number = c.company_number
LEFT JOIN contact_enrichments ce ON o.id = ce.officer_id
WHERE o.is_active = true
ORDER BY ce.confidence_score DESC NULLS LAST;

-- Company overview (for detail page)
CREATE OR REPLACE VIEW company_overview AS
SELECT
    c.company_number,
    c.company_name,
    c.company_status,
    c.company_type,
    c.locality,
    c.region,
    c.postal_code,
    c.primary_sic_code,
    c.industry_category,
    c.incorporation_date,
    c.accounts_next_due_date,
    c.conf_stm_next_due_date,
    c.data_quality_score,
    COUNT(DISTINCT o.id) FILTER (WHERE o.is_active) as active_officers,
    COUNT(DISTINCT f.id) as financial_periods_available,
    MAX(f.period_end) as latest_accounts_date,
    COUNT(DISTINCT ce.id) as verified_contacts_count
FROM production_companies c
LEFT JOIN production_officers o ON c.company_number = o.company_number
LEFT JOIN production_financials f ON c.company_number = f.company_number
LEFT JOIN contact_enrichments ce ON o.id = ce.officer_id AND ce.verified_at IS NOT NULL
GROUP BY c.company_number, c.company_name, c.company_status, c.company_type, c.locality,
         c.region, c.postal_code, c.primary_sic_code, c.industry_category,
         c.incorporation_date, c.accounts_next_due_date, c.conf_stm_next_due_date, c.data_quality_score;

-- Comments
COMMENT ON VIEW active_companies_with_financials IS 'Frontend view: active companies with their latest financial snapshot';
COMMENT ON VIEW officers_with_contacts IS 'Frontend view: officers with verified contact details';
COMMENT ON VIEW company_overview IS 'Frontend view: company summary with counts for detail page';
