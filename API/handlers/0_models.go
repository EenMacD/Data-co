package handlers

import (
	"data-co/api/database"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/lib/pq"
)

// CompanyHandler handles company-related HTTP requests
type CompanyHandler struct {
	db *database.DB
}

// Company represents a company record from the database
type Company struct {
	CompanyNumber             string          `json:"company_number"`
	CompanyName               sql.NullString  `json:"company_name"`
	CompanyStatus             sql.NullString  `json:"company_status"`
	CompanyType               sql.NullString  `json:"company_type"`
	Locality                  sql.NullString  `json:"locality"`
	PostalCode                sql.NullString  `json:"postal_code"`
	AddressLine1              sql.NullString  `json:"address_line_1"`
	AddressLine2              sql.NullString  `json:"address_line_2"`
	Region                    sql.NullString  `json:"region"`
	Country                   sql.NullString  `json:"country"`
	SICCodes                  pq.StringArray  `json:"sic_codes"`
	IncorporationDate         *time.Time      `json:"incorporation_date"`
	AccountsLastMadeUpDate    *time.Time      `json:"accounts_last_made_up_date"`
	AccountsRefDate           sql.NullString  `json:"accounts_ref_date"`
	AccountsNextDueDate       *time.Time      `json:"accounts_next_due_date"`
	AccountCategory           sql.NullString  `json:"account_category"`
	ReturnsNextDueDate        *time.Time      `json:"returns_next_due_date"`
	ReturnsLastMadeUpDate     *time.Time      `json:"returns_last_made_up_date"`
	NumMortCharges            sql.NullInt64   `json:"num_mort_charges"`
	NumMortOutstanding        sql.NullInt64   `json:"num_mort_outstanding"`
	NumMortPartSatisfied      sql.NullInt64   `json:"num_mort_part_satisfied"`
	PreviousNames             sql.NullString  `json:"previous_names"`
	ConfStmNextDueDate        *time.Time      `json:"conf_stm_next_due_date"`
	ConfStmLastMadeUpDate     *time.Time      `json:"conf_stm_last_made_up_date"`
	RawData                   json.RawMessage `json:"raw_data"`
	DataHash                  sql.NullString  `json:"data_hash"`
	ChangeDetected            sql.NullBool    `json:"change_detected"`
	LastUpdated               *time.Time      `json:"last_updated"`
	MergedAt                  *time.Time      `json:"merged_at"`
	BatchID                   sql.NullString  `json:"batch_id"`
	IngestedAt                *time.Time      `json:"ingested_at"`
	NeedsReview               sql.NullBool    `json:"needs_review"`
	ReviewNotes               sql.NullString  `json:"review_notes"`
}