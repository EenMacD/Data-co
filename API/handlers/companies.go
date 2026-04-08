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
	includeTotal := filters.IncludeTotal == nil || *filters.IncludeTotal
	queryFilters := filters
	if !includeTotal {
		queryFilters.Limit = filters.Limit + 1
	}

	// Build query
	query, args := database.BuildCompanyQuery(queryFilters)

	log.Printf("Executing search query with filters: %+v (includeTotal=%t)", queryFilters, includeTotal)

	var companies []models.Company
	err := h.db.Select(&companies, query, args...)
	if err != nil {
		log.Printf("Query error: %v", err)
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Failed to search companies",
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
		err = h.db.Get(&total, countQuery, countArgs...)
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

	// Build count query
	query, args := database.BuildCompanyCountQuery(filters)

	log.Printf("Executing count query with filters: %+v", filters)

	// Execute query
	var total int
	err := h.db.Get(&total, query, args...)
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

	var company models.Company
	err := h.db.Get(&company, database.BuildCompanyByNumberQuery(), companyNumber)

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
