package company_queries

import "fmt"

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