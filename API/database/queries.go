package database

import (
	"fmt"
	"strings"

	"data-co/api/models"
)

const companySearchCTEs = `WITH latest_financials AS (
	SELECT DISTINCT ON (company_number)
		company_number,
		turnover,
		total_assets,
		total_liabilities,
		period_end
	FROM production_financials
	WHERE period_end IS NOT NULL
	ORDER BY company_number, period_end DESC
),
officer_counts AS (
	SELECT
		company_number,
		COUNT(*) FILTER (WHERE resigned_on IS NULL) AS active_officers
	FROM production_officers
	GROUP BY company_number
)`

const companySearchFromClause = `FROM production_companies c
LEFT JOIN latest_financials latest_fin ON c.company_number = latest_fin.company_number
LEFT JOIN officer_counts ON c.company_number = officer_counts.company_number`

var companySortMap = map[string]string{
	"company_name":         "c.company_name",
	"company_number":       "c.company_number",
	"incorporation_date":   "c.incorporation_date",
	"latest_accounts_date": "latest_fin.period_end",
	"turnover":             "latest_fin.turnover",
	"net_worth":            "latest_fin.net_worth",
	"employees":            "officer_counts.active_officers",
	"relevance":            "c.company_name", // Default to name if no similarity score
}

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
func (qb *QueryBuilder) AddIndustryFilter(industries []string) {
	if len(industries) == 0 {
		return
	}

	seen := make(map[string]struct{}, len(industries))
	conditions := make([]string, 0, len(industries))
	for _, industry := range industries {
		prefix := strings.TrimSpace(industry)
		if prefix == "" {
			continue
		}
		if _, ok := seen[prefix]; ok {
			continue
		}

		seen[prefix] = struct{}{}
		qb.argCount++
		qb.args = append(qb.args, prefix+"%")
		conditions = append(conditions, fmt.Sprintf("sic ILIKE $%d", qb.argCount))
	}

	if len(conditions) == 0 {
		return
	}

	condition := fmt.Sprintf("EXISTS (SELECT 1 FROM unnest(c.sic_codes) AS sic WHERE %s)", strings.Join(conditions, " OR "))
	qb.conditions = append(qb.conditions, condition)
}

// AddLocationFilter filters by any location in locality, region, or country.
func (qb *QueryBuilder) AddLocationFilter(locations []string) {
	if len(locations) == 0 {
		return
	}

	locationConditions := make([]string, 0, len(locations))
	for _, location := range locations {
		searchTerms := normalizeLocationTerms(location)
		if len(searchTerms) == 0 {
			continue
		}

		termConditions := make([]string, 0, len(searchTerms))
		for _, term := range searchTerms {
			pattern := "%" + term + "%"

			qb.argCount++
			localityArg := qb.argCount
			qb.args = append(qb.args, pattern)

			qb.argCount++
			regionArg := qb.argCount
			qb.args = append(qb.args, pattern)

			qb.argCount++
			countryArg := qb.argCount
			qb.args = append(qb.args, pattern)

			termConditions = append(termConditions, fmt.Sprintf("(c.locality ILIKE $%d OR c.region ILIKE $%d OR c.country ILIKE $%d)", localityArg, regionArg, countryArg))
		}

		if len(termConditions) == 1 {
			locationConditions = append(locationConditions, termConditions[0])
			continue
		}

		locationConditions = append(locationConditions, "("+strings.Join(termConditions, " OR ")+")")
	}

	if len(locationConditions) == 0 {
		return
	}

	qb.conditions = append(qb.conditions, "("+strings.Join(locationConditions, " OR ")+")")
}

func normalizeLocationTerms(location string) []string {
	trimmedLocation := strings.TrimSpace(location)
	if trimmedLocation == "" {
		return nil
	}

	parts := strings.Split(trimmedLocation, "/")
	if len(parts) > 1 {
		parts = parts[1:]
	}

	seen := make(map[string]struct{}, len(parts))
	terms := make([]string, 0, len(parts))
	for _, part := range parts {
		term := strings.TrimSpace(part)
		if term == "" {
			continue
		}

		if _, ok := seen[term]; ok {
			continue
		}

		seen[term] = struct{}{}
		terms = append(terms, term)
	}

	if len(terms) > 0 {
		return terms
	}

	return []string{trimmedLocation}
}

/*
// AddRevenueFilter filters by revenue range
func (qb *QueryBuilder) AddRevenueFilter(revenueRange string) {
	if revenueRange == "" {
		return
	}

	ranges := map[string]struct{ min, max float64 }{
		"0-1m":     {0, 1_000_000},
		"1m-10m":   {1_000_000, 10_000_000},
		"10m-50m":  {10_000_000, 50_000_000},
		"50m-100m": {50_000_000, 100_000_000},
		"100m+":    {100_000_000, 0},
		"50m+":     {50_000_000, 0},
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
*/

/*
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
*/

/*
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
*/

/*
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
*/

/*
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
*/

// AddCompanyStatusFilter filters by company status
func (qb *QueryBuilder) AddCompanyStatusFilter(statuses []string) {
	if len(statuses) == 0 {
		return
	}

	seen := make(map[string]struct{}, len(statuses))
	conditions := make([]string, 0, len(statuses))
	for _, status := range statuses {
		normalizedStatus := strings.ToLower(strings.TrimSpace(status))
		if normalizedStatus == "" || normalizedStatus == "all" {
			continue
		}
		if _, ok := seen[normalizedStatus]; ok {
			continue
		}

		seen[normalizedStatus] = struct{}{}
		qb.argCount++
		qb.args = append(qb.args, normalizedStatus)
		conditions = append(conditions, fmt.Sprintf("LOWER(c.company_status) = $%d", qb.argCount))
	}

	if len(conditions) == 0 {
		return
	}

	qb.conditions = append(qb.conditions, "("+strings.Join(conditions, " OR ")+")")
}

/*
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
		"0-100k":  {0, 100_000},
		"100k-1m": {100_000, 1_000_000},
		"1m-10m":  {1_000_000, 10_000_000},
		"10m+":    {10_000_000, 0},
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
*/

/*
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
*/

/*
// AddSearchTerm adds full-text search on company name
func (qb *QueryBuilder) AddSearchTerm(searchTerm string) {
	if searchTerm == "" {
		return
	}

	qb.addCondition("c.company_name ILIKE $%d", "%"+searchTerm+"%")
}
*/

func buildCompanySearchQuery(projection string) string {
	return companySearchCTEs + "\nSELECT\n\t" + projection + "\n" + companySearchFromClause
}

func (qb *QueryBuilder) appendWhereClause(query string) string {
	if len(qb.conditions) == 0 {
		return query
	}

	return query + "\nWHERE " + strings.Join(qb.conditions, " AND ")
}

// BuildQuery builds the complete SQL query
func (qb *QueryBuilder) BuildQuery(filters models.CompanySearchFilters) string {
	baseQuery := buildCompanySearchQuery(CompanyProjection())
	baseQuery = qb.appendWhereClause(baseQuery)

	orderBy := "c.company_name"
	if filters.OrderBy != "" {
		if val, ok := companySortMap[filters.OrderBy]; ok {
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
	baseQuery := buildCompanySearchQuery("COUNT(*) AS total")
	return qb.appendWhereClause(baseQuery)
}

// GetArgs returns the query arguments
func (qb *QueryBuilder) GetArgs() []interface{} {
	return qb.args
}

func applyCompanyFilters(qb *QueryBuilder, filters models.CompanySearchFilters) {
	qb.AddIndustryFilter(filters.Industry)
	qb.AddLocationFilter(filters.Locations)
	qb.AddCompanyStatusFilter(filters.Status)
	// qb.AddRevenueFilter(filters.Revenue)
	// qb.AddEmployeesFilter(filters.Employees)
	// qb.AddProfitabilityFilter(filters.Profitability)
	// qb.AddCompanySizeFilter(filters.CompanySize)
	// qb.AddNetAssetsFilter(filters.NetAssets)
	// qb.AddDebtLevelFilter(filters.DebtLevel)
	// qb.AddSearchTerm(filters.SearchTerm)
}

// BuildCompanyQuery is a convenience function to build a query from filters
func BuildCompanyQuery(filters models.CompanySearchFilters) (string, []interface{}) {
	qb := NewQueryBuilder()
	applyCompanyFilters(qb, filters)

	query := qb.BuildQuery(filters)
	return query, qb.GetArgs()
}

// BuildCompanyCountQuery builds a count query from filters
func BuildCompanyCountQuery(filters models.CompanySearchFilters) (string, []interface{}) {
	qb := NewQueryBuilder()
	applyCompanyFilters(qb, filters)

	query := qb.BuildCountQuery()
	return query, qb.GetArgs()
}

// BuildCompanyByNumberQuery builds a query to fetch one company by company_number.
func BuildCompanyByNumberQuery() string {
	return "SELECT\n\t" + CompanyProjection() + "\nFROM production_companies c\nWHERE c.company_number = $1"
}
