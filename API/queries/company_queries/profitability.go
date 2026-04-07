package company_queries

import "fmt"

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