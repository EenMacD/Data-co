package database

import (
	"fmt"
	"strings"
	"time"

	"data-co/api/models"
)

// QueryBuilder builds SQL queries based on filter criteria
type QueryBuilder struct {
	conditions []string
	args       []interface{}
	argCount   int
}

// NewQueryBuilder creates a new query builder
func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{
		conditions: make([]string, 0),
		args:       make([]interface{}, 0),
		argCount:   0,
	}
}

// addCondition adds a WHERE condition with a parameter
func (qb *QueryBuilder) addCondition(condition string, value interface{}) {
	qb.argCount++
	qb.conditions = append(qb.conditions, fmt.Sprintf(condition, qb.argCount))
	qb.args = append(qb.args, value)
}

// AddIndustryFilter filters by industry using SIC codes
func (qb *QueryBuilder) AddIndustryFilter(industry string) {
	if industry == "" {
		return
	}

	// Map industry names to SIC code prefixes
	// See: https://resources.companieshouse.gov.uk/sic/
	industryToSicPrefixes := map[string][]string{
		"tech":          {"62", "63"},       // Computer programming, IT services, data processing
		"finance":       {"64", "65", "66"}, // Financial services, insurance
		"retail":        {"47"},             // Retail trade
		"manufacturing": {"10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "33"},
		"professional":  {"69", "70", "71", "72", "73", "74"}, // Professional, scientific and technical
	}

	prefixes, ok := industryToSicPrefixes[industry]
	if !ok {
		// If no mapping found, try to match directly against sic_codes array
		qb.addCondition("$%d = ANY(c.sic_codes)", industry)
		return
	}

	// Build condition to check if any SIC code starts with one of the prefixes
	// Using EXISTS with unnest to check array elements
	conditions := make([]string, len(prefixes))
	for i, prefix := range prefixes {
		qb.argCount++
		qb.args = append(qb.args, prefix+"%")
		conditions[i] = fmt.Sprintf("sic ILIKE $%d", qb.argCount)
	}

	condition := fmt.Sprintf("EXISTS (SELECT 1 FROM unnest(c.sic_codes) AS sic WHERE %s)", strings.Join(conditions, " OR "))
	qb.conditions = append(qb.conditions, condition)
}

// AddLocationFilter filters by location (locality or region)
func (qb *QueryBuilder) AddLocationFilter(location string) {
	if location == "" {
		return
	}

	locationMap := map[string]string{
		"london":     "London",
		"manchester": "Manchester",
		"birmingham": "Birmingham",
		"edinburgh":  "Edinburgh",
		"bristol":    "Bristol",
	}

	dbLocation := locationMap[strings.ToLower(location)]
	if dbLocation == "" {
		dbLocation = strings.Title(location)
	}

	// Add pattern matching with wildcards for ILIKE
	pattern := "%" + dbLocation + "%"

	qb.argCount++
	firstArg := qb.argCount
	qb.args = append(qb.args, pattern)

	qb.argCount++
	secondArg := qb.argCount
	qb.args = append(qb.args, pattern)

	qb.conditions = append(qb.conditions, fmt.Sprintf("(c.locality ILIKE $%d OR c.region ILIKE $%d)", firstArg, secondArg))
}

// AddRevenueFilter filters by revenue range
func (qb *QueryBuilder) AddRevenueFilter(revenueRange string) {
	if revenueRange == "" {
		return
	}

	ranges := map[string]struct{ min, max float64 }{
		"0-1m":      {0, 1_000_000},
		"1m-10m":    {1_000_000, 10_000_000},
		"10m-50m":   {10_000_000, 50_000_000},
		"50m-100m":  {50_000_000, 100_000_000},
		"100m+":     {100_000_000, 0},
		"50m+":      {50_000_000, 0},
	}

	if r, ok := ranges[revenueRange]; ok {
		if r.max == 0 {
			qb.addCondition("latest_fin.turnover >= $%d", r.min)
		} else {
			qb.argCount++
			qb.conditions = append(qb.conditions, fmt.Sprintf("latest_fin.turnover BETWEEN $%d AND $%d", qb.argCount, qb.argCount+1))
			qb.args = append(qb.args, r.min, r.max)
			qb.argCount++
		}
	}
}

// AddEmployeesFilter filters by employee count (using officer count as proxy)
func (qb *QueryBuilder) AddEmployeesFilter(employeesRange string) {
	if employeesRange == "" {
		return
	}

	ranges := map[string]struct{ min, max int }{
		"1-10":   {1, 10},
		"11-50":  {11, 50},
		"51-250": {51, 250},
		"251+":   {251, 0},
	}

	if r, ok := ranges[employeesRange]; ok {
		if r.max == 0 {
			qb.addCondition("officer_counts.active_officers >= $%d", r.min)
		} else {
			qb.argCount++
			qb.conditions = append(qb.conditions, fmt.Sprintf("officer_counts.active_officers BETWEEN $%d AND $%d", qb.argCount, qb.argCount+1))
			qb.args = append(qb.args, r.min, r.max)
			qb.argCount++
		}
	}
}

// AddProfitabilityFilter filters by profitability status
func (qb *QueryBuilder) AddProfitabilityFilter(profitability string) {
	if profitability == "" {
		return
	}

	switch profitability {
	case "profitable":
		qb.conditions = append(qb.conditions, "latest_fin.profit_after_tax > 0")
	case "loss_making":
		qb.conditions = append(qb.conditions, "latest_fin.profit_after_tax < 0")
	case "breakeven":
		qb.argCount++
		qb.conditions = append(qb.conditions, fmt.Sprintf("latest_fin.profit_after_tax BETWEEN $%d AND $%d", qb.argCount, qb.argCount+1))
		qb.args = append(qb.args, -10000, 10000)
		qb.argCount++
	}
}

// AddCompanySizeFilter filters by company size
func (qb *QueryBuilder) AddCompanySizeFilter(size string) {
	if size == "" {
		return
	}

	ranges := map[string]struct{ min, max int }{
		"micro":  {1, 10},
		"small":  {11, 50},
		"medium": {51, 250},
		"large":  {251, 0},
	}

	if r, ok := ranges[size]; ok {
		if r.max == 0 {
			qb.addCondition("officer_counts.active_officers >= $%d", r.min)
		} else {
			qb.argCount++
			qb.conditions = append(qb.conditions, fmt.Sprintf("officer_counts.active_officers BETWEEN $%d AND $%d", qb.argCount, qb.argCount+1))
			qb.args = append(qb.args, r.min, r.max)
			qb.argCount++
		}
	}
}

// AddCompanyAgeFilter filters by company age
func (qb *QueryBuilder) AddCompanyAgeFilter(ageRange string) {
	if ageRange == "" {
		return
	}

	currentYear := time.Now().Year()

	ranges := map[string]struct{ maxYear, minYear int }{
		"0-2":   {currentYear, currentYear - 2},
		"3-5":   {currentYear - 3, currentYear - 5},
		"6-10":  {currentYear - 6, currentYear - 10},
		"11-20": {currentYear - 11, currentYear - 20},
		"21+":   {0, currentYear - 21},
	}

	if r, ok := ranges[ageRange]; ok {
		if r.maxYear == 0 {
			qb.addCondition("c.incorporation_date <= $%d::date", fmt.Sprintf("%d-01-01", r.minYear))
		} else {
			qb.argCount++
			qb.conditions = append(qb.conditions, fmt.Sprintf("c.incorporation_date BETWEEN $%d::date AND $%d::date", qb.argCount, qb.argCount+1))
			qb.args = append(qb.args, fmt.Sprintf("%d-01-01", r.minYear), fmt.Sprintf("%d-12-31", r.maxYear))
			qb.argCount++
		}
	}
}

// AddCompanyStatusFilter filters by company status
func (qb *QueryBuilder) AddCompanyStatusFilter(status string) {
	if status == "" || status == "all" {
		return
	}

	qb.addCondition("LOWER(c.company_status) = LOWER($%d)", status)
}

// AddNetAssetsFilter filters by net assets/net worth
func (qb *QueryBuilder) AddNetAssetsFilter(netAssetsRange string) {
	if netAssetsRange == "" {
		return
	}

	if netAssetsRange == "negative" {
		qb.conditions = append(qb.conditions, "latest_fin.net_worth < 0")
		return
	}

	ranges := map[string]struct{ min, max float64 }{
		"0-100k":   {0, 100_000},
		"100k-1m":  {100_000, 1_000_000},
		"1m-10m":   {1_000_000, 10_000_000},
		"10m+":     {10_000_000, 0},
	}

	if r, ok := ranges[netAssetsRange]; ok {
		if r.max == 0 {
			qb.addCondition("latest_fin.net_worth >= $%d", r.min)
		} else {
			qb.argCount++
			qb.conditions = append(qb.conditions, fmt.Sprintf("latest_fin.net_worth BETWEEN $%d AND $%d", qb.argCount, qb.argCount+1))
			qb.args = append(qb.args, r.min, r.max)
			qb.argCount++
		}
	}
}

// AddDebtLevelFilter filters by debt level as percentage of assets
func (qb *QueryBuilder) AddDebtLevelFilter(debtLevel string) {
	if debtLevel == "" {
		return
	}

	ranges := map[string]struct{ min, max float64 }{
		"none":   {0, 0.01},
		"low":    {0.01, 0.30},
		"medium": {0.30, 0.60},
		"high":   {0.60, 0},
	}

	if r, ok := ranges[debtLevel]; ok {
		if r.max == 0 {
			qb.addCondition("(latest_fin.total_liabilities::numeric / NULLIF(latest_fin.total_assets, 0)) >= $%d", r.min)
		} else {
			qb.argCount++
			qb.conditions = append(qb.conditions, fmt.Sprintf("(latest_fin.total_liabilities::numeric / NULLIF(latest_fin.total_assets, 0)) BETWEEN $%d AND $%d", qb.argCount, qb.argCount+1))
			qb.args = append(qb.args, r.min, r.max)
			qb.argCount++
		}
	}
}

// AddSearchTerm adds full-text search on company name
func (qb *QueryBuilder) AddSearchTerm(searchTerm string) {
	if searchTerm == "" {
		return
	}

	qb.addCondition("c.company_name ILIKE $%d", "%"+searchTerm+"%")
}

// BuildQuery builds the complete SQL query
func (qb *QueryBuilder) BuildQuery(filters models.CompanySearchFilters) string {
	baseQuery := `
	WITH latest_financials AS (
		SELECT DISTINCT ON (staging_company_id)
			staging_company_id as company_id,
			turnover,
			profit_loss as profit_after_tax,
			total_assets,
			total_liabilities,
			net_worth,
			0 as profit_margin,
			0 as current_ratio,
			period_end
		FROM staging_financials
		WHERE period_end IS NOT NULL
		ORDER BY staging_company_id, period_end DESC
	),
	officer_counts AS (
		SELECT
			staging_company_id as company_id,
			COUNT(*) FILTER (WHERE resigned_on IS NULL) as active_officers
		FROM staging_officers
		GROUP BY staging_company_id
	)
	SELECT
		c.id,
		c.company_number,
		c.company_name,
		c.company_status,
		c.locality,
		c.region,
		c.postal_code,
		'' as primary_sic_code,
		'' as industry_category,
		NULL::date as incorporation_date,
		latest_fin.turnover,
		latest_fin.profit_after_tax,
		latest_fin.total_assets,
		latest_fin.net_worth,
		latest_fin.profit_margin,
		latest_fin.period_end as latest_accounts_date,
		COALESCE(officer_counts.active_officers, 0) as active_officers_count
	FROM staging_companies c
	LEFT JOIN latest_financials latest_fin ON c.id = latest_fin.company_id
	LEFT JOIN officer_counts ON c.id = officer_counts.company_id
	`

	if len(qb.conditions) > 0 {
		baseQuery += "\nWHERE " + strings.Join(qb.conditions, " AND ")
	}

	// Safe sort column mapping
	sortMap := map[string]string{
		"company_name":         "c.company_name",
		"company_number":       "c.company_number",
		"incorporation_date":   "c.incorporation_date",
		"latest_accounts_date": "latest_fin.period_end",
		"turnover":             "latest_fin.turnover",
		"net_worth":            "latest_fin.net_worth",
		"employees":            "active_officers_count",
		"relevance":            "c.company_name", // Default to name if no similarity score
	}

	orderBy := "c.company_name"
	if filters.OrderBy != "" {
		if val, ok := sortMap[filters.OrderBy]; ok {
			orderBy = val
		}
	}
	baseQuery += fmt.Sprintf("\nORDER BY %s", orderBy)

	limit := 100
	if filters.Limit > 0 {
		limit = filters.Limit
	}
	offset := 0
	if filters.Offset > 0 {
		offset = filters.Offset
	}

	qb.argCount++
	qb.args = append(qb.args, limit)
	baseQuery += fmt.Sprintf("\nLIMIT $%d", qb.argCount)

	qb.argCount++
	qb.args = append(qb.args, offset)
	baseQuery += fmt.Sprintf(" OFFSET $%d", qb.argCount)

	return baseQuery
}

// BuildCountQuery builds a query to count total matching records
func (qb *QueryBuilder) BuildCountQuery() string {
	baseQuery := `
	WITH latest_financials AS (
		SELECT DISTINCT ON (staging_company_id)
			staging_company_id as company_id,
			turnover,
			profit_loss as profit_after_tax,
			total_assets,
			total_liabilities,
			net_worth
		FROM staging_financials
		WHERE period_end IS NOT NULL
		ORDER BY staging_company_id, period_end DESC
	),
	officer_counts AS (
		SELECT
			staging_company_id as company_id,
			COUNT(*) FILTER (WHERE resigned_on IS NULL) as active_officers
		FROM staging_officers
		GROUP BY staging_company_id
	)
	SELECT COUNT(*) as total
	FROM staging_companies c
	LEFT JOIN latest_financials latest_fin ON c.id = latest_fin.company_id
	LEFT JOIN officer_counts ON c.id = officer_counts.company_id
	`

	if len(qb.conditions) > 0 {
		baseQuery += "\nWHERE " + strings.Join(qb.conditions, " AND ")
	}

	return baseQuery
}

// GetArgs returns the query arguments
func (qb *QueryBuilder) GetArgs() []interface{} {
	return qb.args
}

// BuildCompanyQuery is a convenience function to build a query from filters
func BuildCompanyQuery(filters models.CompanySearchFilters) (string, []interface{}) {
	qb := NewQueryBuilder()

	qb.AddIndustryFilter(filters.Industry)
	qb.AddLocationFilter(filters.Location)
	qb.AddRevenueFilter(filters.Revenue)
	qb.AddEmployeesFilter(filters.Employees)
	qb.AddProfitabilityFilter(filters.Profitability)
	qb.AddCompanySizeFilter(filters.CompanySize)
	qb.AddCompanyStatusFilter(filters.CompanyStatus)
	qb.AddNetAssetsFilter(filters.NetAssets)
	qb.AddDebtLevelFilter(filters.DebtLevel)
	qb.AddSearchTerm(filters.SearchTerm)

	query := qb.BuildQuery(filters)
	return query, qb.GetArgs()
}

// BuildCompanyCountQuery builds a count query from filters
func BuildCompanyCountQuery(filters models.CompanySearchFilters) (string, []interface{}) {
	qb := NewQueryBuilder()

	qb.AddIndustryFilter(filters.Industry)
	qb.AddLocationFilter(filters.Location)
	qb.AddRevenueFilter(filters.Revenue)
	qb.AddEmployeesFilter(filters.Employees)
	qb.AddProfitabilityFilter(filters.Profitability)
	qb.AddCompanySizeFilter(filters.CompanySize)
	qb.AddCompanyStatusFilter(filters.CompanyStatus)
	qb.AddNetAssetsFilter(filters.NetAssets)
	qb.AddDebtLevelFilter(filters.DebtLevel)
	qb.AddSearchTerm(filters.SearchTerm)

	query := qb.BuildCountQuery()
	return query, qb.GetArgs()
}
