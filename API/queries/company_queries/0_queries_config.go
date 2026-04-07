package company_queries

import (
	"fmt"
	"strings"
)

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


// GetArgs returns the query arguments
func (qb *QueryBuilder) GetArgs() []interface{} {
	return qb.args
}


// BuildQuery builds the complete SQL query
func (qb *QueryBuilder) BuildQuery(filters CompanySearchFilters) string {
	baseQuery := `
	WITH latest_financials AS (
		SELECT DISTINCT ON (company_number)
			company_number as company_id,
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
		ORDER BY company_number, period_end DESC
	),
	officer_counts AS (
		SELECT
			company_number as company_id,
			COUNT(*) FILTER (WHERE resigned_on IS NULL) as active_officers
		FROM staging_officers
		GROUP BY company_number
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