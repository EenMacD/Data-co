package models

import (
	"database/sql"
	"time"
)

// Company represents a company record from the database
type Company struct {
	ID                   int             `json:"id"`
	CompanyNumber        string          `json:"company_number"`
	CompanyName          string          `json:"company_name"`
	CompanyStatus        string          `json:"company_status"`
	Locality             sql.NullString  `json:"locality"`
	Region               sql.NullString  `json:"region"`
	PostalCode           sql.NullString  `json:"postal_code"`
	PrimarySICCode       sql.NullString  `json:"primary_sic_code"`
	IndustryCategory     sql.NullString  `json:"industry_category"`
	IncorporationDate    *time.Time      `json:"incorporation_date"`
	Turnover             sql.NullFloat64 `json:"turnover"`
	ProfitAfterTax       sql.NullFloat64 `json:"profit_after_tax"`
	TotalAssets          sql.NullFloat64 `json:"total_assets"`
	NetWorth             sql.NullFloat64 `json:"net_worth"`
	ProfitMargin         sql.NullFloat64 `json:"profit_margin"`
	LatestAccountsDate   *time.Time      `json:"latest_accounts_date"`
	ActiveOfficersCount  int             `json:"active_officers_count"`
}

// CompanySearchFilters represents the filter criteria from frontend
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

// SearchResponse represents the API response for company search
type SearchResponse struct {
	Companies  []Company `json:"companies"`
	Total      int       `json:"total"`
	Limit      int       `json:"limit"`
	Offset     int       `json:"offset"`
	HasMore    bool      `json:"has_more"`
}

// CountResponse represents the API response for count endpoint
type CountResponse struct {
	Total int `json:"total"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}
