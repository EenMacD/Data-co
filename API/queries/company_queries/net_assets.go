package company_queries

import "fmt"

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
		"0-100k":   {0, 100_000},
		"100k-1m":  {100_000, 1_000_000},
		"1m-10m":   {1_000_000, 10_000_000},
		"10m+":     {10_000_000, 0},
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