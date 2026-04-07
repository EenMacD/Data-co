package company_queries

import (
	"fmt"
	"time"
)

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