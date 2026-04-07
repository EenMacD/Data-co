package handlers

import (
	"data-co/api/common/helpers"
	"data-co/api/queries/company_queries"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

func (h *CompanyHandler) SearchCompanies(c *gin.Context) {
	var filters company_queries.CompanySearchFilters

	// Parse request body
	err := c.ShouldBindJSON(&filters)
	helpers.HResponseError(http.StatusBadRequest, "Invalid Body Request", err, c)

	// set the default values for filter fields
	setDefault(&filters)

	// Build query
	query, args := company_queries.BuildCompanyQuery(filters)
	log.Printf("Executing search query with filters: %+v", filters)

	// Execute query
	rows, err := h.db.Query(query, args...)
	if err != nil {
			log.Printf("Query error: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{
					"error":   "Failed to search companies",
					"details": err.Error(),
			})
			return
	}
	defer rows.Close()

	// Parse results
	companies := make([]Company, 0)
	for rows.Next() {
		var company Company
		err := rows.Scan(
			&company.CompanyNumber,
			&company.CompanyName,
			&company.CompanyStatus,
			&company.Locality,
			&company.Region,
			&company.PostalCode,
			&company.SICCodes,
			// &company.IndustryCategory,
			&company.IncorporationDate,
			// &company.Turnover,
			// &company.ProfitAfterTax,
			// &company.TotalAssets,
			// &company.NetWorth,
			// &company.ProfitMargin,
			// &company.LatestAccountsDate,
			// &company.ActiveOfficersCount,
		)
		if err != nil {
			log.Printf("Row scan error: %v", err)
			continue
		}
		companies = append(companies, company)
	}

	if err := rows.Err(); err != nil {
    log.Printf("Rows iteration error: %v", err)
    c.JSON(http.StatusInternalServerError, gin.H{
        "error":   "Error processing results",
        "details": err.Error(),
    })
    return
	}

		// Get total count
	countQuery, countArgs := database.BuildCompanyCountQuery(filters)
	var total int
	err = h.db.QueryRow(countQuery, countArgs...).Scan(&total)
	if err != nil {
		log.Printf("Count query error: %v", err)
		total = len(companies) // Fallback to returned count
	}

}

func setDefault(filters *company_queries.CompanySearchFilters) {
	// setting the default value for filter fields
	if filters.Limit == 0 {
		filters.Limit = 10
	}
	if filters.CompanyStatus == "" {
		filters.CompanyStatus = "active"
	}
}