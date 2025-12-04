package database

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"

	"data-co/api/config"
)

// DB wraps the database connection
type DB struct {
	*sql.DB
}

// NewConnection creates a new database connection
func NewConnection(cfg config.DatabaseConfig) (*DB, error) {
	connStr := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host,
		cfg.Port,
		cfg.User,
		cfg.Password,
		cfg.Name,
		cfg.SSLMode,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)

	return &DB{db}, nil
}
