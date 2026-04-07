package company_queries

import "fmt"

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