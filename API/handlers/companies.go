package handlers

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"

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
func (h *CompanyHandler) SearchCompanies(w http.ResponseWriter, r *http.Request) {
	// Parse request body
	var filters models.CompanySearchFilters
	if err := json.NewDecoder(r.Body).Decode(&filters); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	// Set defaults
	if filters.Limit == 0 {
		filters.Limit = 100
	}
	if filters.CompanyStatus == "" {
		filters.CompanyStatus = "active"
	}

	// Build query
	query, args := database.BuildCompanyQuery(filters)

	log.Printf("Executing search query with filters: %+v", filters)

	// Execute query
	rows, err := h.db.Query(query, args...)
	if err != nil {
		log.Printf("Query error: %v", err)
		respondWithError(w, http.StatusInternalServerError, "Failed to search companies", err.Error())
		return
	}
	defer rows.Close()

	// Parse results
	companies := make([]models.Company, 0)
	for rows.Next() {
		var c models.Company
		err := rows.Scan(
			&c.ID,
			&c.CompanyNumber,
			&c.CompanyName,
			&c.CompanyStatus,
			&c.Locality,
			&c.Region,
			&c.PostalCode,
			&c.PrimarySICCode,
			&c.IndustryCategory,
			&c.IncorporationDate,
			&c.Turnover,
			&c.ProfitAfterTax,
			&c.TotalAssets,
			&c.NetWorth,
			&c.ProfitMargin,
			&c.LatestAccountsDate,
			&c.ActiveOfficersCount,
		)
		if err != nil {
			log.Printf("Row scan error: %v", err)
			continue
		}
		companies = append(companies, c)
	}

	if err := rows.Err(); err != nil {
		log.Printf("Rows iteration error: %v", err)
		respondWithError(w, http.StatusInternalServerError, "Error processing results", err.Error())
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

	// Build response
	response := models.SearchResponse{
		Companies: companies,
		Total:     total,
		Limit:     filters.Limit,
		Offset:    filters.Offset,
		HasMore:   filters.Offset+len(companies) < total,
	}

	log.Printf("Returning %d companies (total: %d)", len(companies), total)

	respondWithJSON(w, http.StatusOK, response)
}

// CountCompanies handles POST /api/companies/count
func (h *CompanyHandler) CountCompanies(w http.ResponseWriter, r *http.Request) {
	// Parse request body
	var filters models.CompanySearchFilters
	if err := json.NewDecoder(r.Body).Decode(&filters); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request body", err.Error())
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
		respondWithError(w, http.StatusInternalServerError, "Failed to count companies", err.Error())
		return
	}

	response := models.CountResponse{
		Total: total,
	}

	log.Printf("Total matching companies: %d", total)

	respondWithJSON(w, http.StatusOK, response)
}

// GetCompany handles GET /api/companies/:id
func (h *CompanyHandler) GetCompany(w http.ResponseWriter, r *http.Request) {
	// Get company ID from URL
	vars := mux.Vars(r)
	idStr := vars["id"]

	id, err := strconv.Atoi(idStr)
	if err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid company ID", err.Error())
		return
	}

	log.Printf("Fetching company with ID: %d", id)

	// Query for single company
	query := `
	WITH latest_financial AS (
		SELECT
			turnover,
			profit_loss as profit_after_tax,
			total_assets,
			net_worth,
			0 as profit_margin,
			period_end
		FROM staging_financials
		WHERE staging_company_id = $1
		ORDER BY period_end DESC
		LIMIT 1
	),
	officer_count AS (
		SELECT COUNT(*) FILTER (WHERE resigned_on IS NULL) as active_officers
		FROM staging_officers
		WHERE staging_company_id = $1
	)
	SELECT
		c.id,
		c.company_number,
		c.company_name,
		c.company_status,
		c.locality,
		c.region,
		c.postal_code,
		'' as primary_sic_code,
		'' as industry_category,
		NULL::date as incorporation_date,
		lf.turnover,
		lf.profit_after_tax,
		lf.total_assets,
		lf.net_worth,
		lf.profit_margin,
		lf.period_end as latest_accounts_date,
		COALESCE(oc.active_officers, 0) as active_officers_count
	FROM staging_companies c
	LEFT JOIN latest_financial lf ON true
	LEFT JOIN officer_count oc ON true
	WHERE c.id = $1
	`

	var company models.Company
	err = h.db.QueryRow(query, id).Scan(
		&company.ID,
		&company.CompanyNumber,
		&company.CompanyName,
		&company.CompanyStatus,
		&company.Locality,
		&company.Region,
		&company.PostalCode,
		&company.PrimarySICCode,
		&company.IndustryCategory,
		&company.IncorporationDate,
		&company.Turnover,
		&company.ProfitAfterTax,
		&company.TotalAssets,
		&company.NetWorth,
		&company.ProfitMargin,
		&company.LatestAccountsDate,
		&company.ActiveOfficersCount,
	)

	if err == sql.ErrNoRows {
		respondWithError(w, http.StatusNotFound, "Company not found", "")
		return
	}
	if err != nil {
		log.Printf("Query error: %v", err)
		respondWithError(w, http.StatusInternalServerError, "Failed to fetch company", err.Error())
		return
	}

	log.Printf("Found company: %s (%s)", company.CompanyName, company.CompanyNumber)

	respondWithJSON(w, http.StatusOK, company)
}

// Helper functions

func respondWithJSON(w http.ResponseWriter, statusCode int, payload interface{}) {
	response, err := json.Marshal(payload)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"Failed to marshal response"}`))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	w.Write(response)
}

func respondWithError(w http.ResponseWriter, statusCode int, error string, message string) {
	errorResponse := models.ErrorResponse{
		Error:   error,
		Message: message,
	}
	respondWithJSON(w, statusCode, errorResponse)
}
