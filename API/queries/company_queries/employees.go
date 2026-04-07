package company_queries

import "fmt"

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