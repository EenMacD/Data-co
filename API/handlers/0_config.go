package handlers

import (
	"data-co/api/database"
)

// NewCompanyHandler creates a new company handler
func NewCompanyHandler(db *database.DB) *CompanyHandler {
	return &CompanyHandler{db: db}
}