package company_queries

import (
	"fmt"
	"strings"
)

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
