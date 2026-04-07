package company_queries

import "fmt"

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