package company_queries

// AddCompanyStatusFilter filters by company status
func (qb *QueryBuilder) AddCompanyStatusFilter(status string) {
	if status == "" || status == "all" {
		return
	}

	qb.addCondition("LOWER(c.company_status) = LOWER($%d)", status)
}
