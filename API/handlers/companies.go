package handlers

import (
	"database/sql"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"

	"data-co/api/database"
	"data-co/api/models"
)

// CompanyHandler handles company-related HTTP requests
type CompanyHandler struct {
	db *database.DB
}

// NewCompanyHandler creates a new company handler
func NewCompanyHandler(db *database.DB) *CompanyHandler {
	return &CompanyHandler{db: db}
}

// SearchCompanies handles POST /api/companies/search
func (h *CompanyHandler) SearchCompanies(c *gin.Context) {
	// Parse request body
	var filters models.CompanySearchFilters
	if err := c.ShouldBindJSON(&filters); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "Invalid request body",
			Message: err.Error(),
		})
		return
	}

	// Set defaults
	if filters.Limit == 0 {
		filters.Limit = 10
	}
	if filters.CompanyStatus == "" {
		filters.CompanyStatus = "active"
	}
	includeTotal := filters.IncludeTotal == nil || *filters.IncludeTotal
	queryFilters := filters
	if !includeTotal {
		queryFilters.Limit = filters.Limit + 1
	}

	// Build query
	query, args := database.BuildCompanyQuery(queryFilters)

	log.Printf("Executing search query with filters: %+v (includeTotal=%t)", queryFilters, includeTotal)

	// Execute query
	rows, err := h.db.Query(query, args...)
	if err != nil {
		log.Printf("Query error: %v", err)
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Failed to search companies",
			Message: err.Error(),
		})
		return
	}
	defer rows.Close()

	// Parse results
	companies := make([]models.Company, 0, queryFilters.Limit)
	for rows.Next() {
		var c models.Company
		err := rows.Scan(
			&c.CompanyNumber,
			&c.CompanyName,
			&c.CompanyStatus,
			&c.Locality,
			&c.Region,
			&c.PostalCode,
			&c.SICCode,
			// &c.IndustryCategory,
			&c.IncorporationDate,
			// &c.Turnover,
			// &c.ProfitAfterTax,
			// &c.TotalAssets,
			// &c.NetWorth,
			// &c.ProfitMargin,
			// &c.LatestAccountsDate,
			// &c.ActiveOfficersCount,
		)
		if err != nil {
			log.Printf("Row scan error: %v", err)
			continue
		}
		companies = append(companies, c)
	}

	if err := rows.Err(); err != nil {
		log.Printf("Rows iteration error: %v", err)
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Error processing results",
			Message: err.Error(),
		})
		return
	}

	var total int
	hasMore := false
	if !includeTotal && len(companies) > filters.Limit {
		hasMore = true
		companies = companies[:filters.Limit]
	}

	if includeTotal {
		countQuery, countArgs := database.BuildCompanyCountQuery(filters)
		err = h.db.QueryRow(countQuery, countArgs...).Scan(&total)
		if err != nil {
			log.Printf("Count query error: %v", err)
			total = len(companies) // Fallback to returned count
		}
		hasMore = filters.Offset+len(companies) < total
	} else if !hasMore {
		total = filters.Offset + len(companies)
	}

	// Build response
	response := models.SearchResponse{
		Companies: companies,
		Total:     total,
		Limit:     filters.Limit,
		Offset:    filters.Offset,
		HasMore:   hasMore,
	}

	log.Printf("Returning %d companies (total: %d)", len(companies), total)

	c.JSON(http.StatusOK, response)
}

// CountCompanies handles POST /api/companies/count
func (h *CompanyHandler) CountCompanies(c *gin.Context) {
	// Parse request body
	var filters models.CompanySearchFilters
	if err := c.ShouldBindJSON(&filters); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "Invalid request body",
			Message: err.Error(),
		})
		return
	}

	// Set defaults
	if filters.CompanyStatus == "" {
		filters.CompanyStatus = "active"
	}

	// Build count query
	query, args := database.BuildCompanyCountQuery(filters)

	log.Printf("Executing count query with filters: %+v", filters)

	// Execute query
	var total int
	err := h.db.QueryRow(query, args...).Scan(&total)
	if err != nil {
		log.Printf("Count query error: %v", err)
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Failed to count companies",
			Message: err.Error(),
		})
		return
	}

	response := models.CountResponse{
		Total: total,
	}

	log.Printf("Total matching companies: %d", total)

	c.JSON(http.StatusOK, response)
}

// GetCompany handles GET /api/companies/:id
func (h *CompanyHandler) GetCompany(c *gin.Context) {
	// Get company number from URL
	companyNumber := c.Param("id")

	log.Printf("Fetching company with company_number: %s", companyNumber)

	// Query for single company
	query := `
	WITH latest_financial AS (
		SELECT
			turnover,
			profit_loss as profit_after_tax,
			total_assets,
			net_assets_liabilities as net_worth,
			0 as profit_margin,
			period_end
		FROM staging_financials
		WHERE company_number = $1
		ORDER BY period_end DESC
		LIMIT 1
	),
	officer_count AS (
		SELECT COUNT(*) FILTER (WHERE resigned_on IS NULL) as active_officers
		FROM staging_officers
		WHERE company_number = $1
	)
	SELECT
		c.company_number,
		c.company_name,
		c.company_status,
		c.locality,
		c.region,
		c.postal_code,
		array_to_string(c.sic_codes, ',') as sic_code,
		c.incorporation_date
	FROM staging_companies c
	LEFT JOIN latest_financial lf ON true
	LEFT JOIN officer_count oc ON true
	WHERE c.company_number = $1
	`

	var company models.Company
	err := h.db.QueryRow(query, companyNumber).Scan(
		&company.CompanyNumber,
		&company.CompanyName,
		&company.CompanyStatus,
		&company.Locality,
		&company.Region,
		&company.PostalCode,
		&company.SICCode,
		&company.IncorporationDate,
	)

	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, models.ErrorResponse{
			Error:   "Company not found",
			Message: "",
		})
		return
	}
	if err != nil {
		log.Printf("Query error: %v", err)
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Failed to fetch company",
			Message: err.Error(),
		})
		return
	}

	log.Printf("Found company: %s (%s)", company.CompanyName, company.CompanyNumber)

	c.JSON(http.StatusOK, company)
}
