package company_queries

// QueryBuilder builds SQL queries based on filter criteria
type QueryBuilder struct {
	conditions []string
	args       []interface{}
	argCount   int
}

// CompanySearchFilters represents the filter criteria from frontend
//TODO: having this used in two saperate places and it does not entirely make sense for it to be here, figure out a better placement of it whilst avoiding import cycle loops errors
type CompanySearchFilters struct {
	Industry       string `json:"industry"`
	Location       string `json:"location"`
	Revenue        string `json:"revenue"`
	Employees      string `json:"employees"`
	Profitability  string `json:"profitability"`
	CompanySize    string `json:"companySize"`
	CompanyStatus  string `json:"companyStatus"`
	NetAssets      string `json:"netAssets"`
	DebtLevel      string `json:"debtLevel"`
	SearchTerm     string `json:"searchTerm"`
	Limit          int    `json:"limit"`
	Offset         int    `json:"offset"`
	OrderBy        string `json:"orderBy"`
}