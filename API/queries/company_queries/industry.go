package company_queries

import (
	"fmt"
	"strings"
)

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
