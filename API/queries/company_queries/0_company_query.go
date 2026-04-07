package company_queries



// AddSearchTerm adds full-text search on company name
func (qb *QueryBuilder) AddSearchTerm(searchTerm string)  {
	if searchTerm == "" {
		return
	}

	qb.addCondition("c.company_name ILIKE $%d", "%"+searchTerm+"%")
}

// BuildCompanyQuery is a convenience function to build a query from filters
func BuildCompanyQuery(filters CompanySearchFilters) (string, []interface{}) {
	qb := NewQueryBuilder()

	qb.AddIndustryFilter(filters.Industry)
	qb.AddLocationFilter(filters.Location)
	qb.AddRevenueFilter(filters.Revenue)
	qb.AddEmployeesFilter(filters.Employees)
	qb.AddProfitabilityFilter(filters.Profitability)
	qb.AddCompanySizeFilter(filters.CompanySize)
	qb.AddCompanyStatusFilter(filters.CompanyStatus)
	qb.AddNetAssetsFilter(filters.NetAssets)
	qb.AddDebtLevelFilter(filters.DebtLevel)
	qb.AddSearchTerm(filters.SearchTerm)

	query := qb.BuildQuery(filters)
	return query, qb.GetArgs()
}