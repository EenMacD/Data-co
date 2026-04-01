package database

import "database/sql"

// DB wraps the database connection
type DB struct {
	*sql.DB
}

// Config holds all application configuration
type Config struct {
	Database DatabaseConfig
	Server   ServerConfig
}

// ServerConfig holds server settings
type ServerConfig struct {
	Port string
}

// DatabaseConfig holds database connection settings
type DatabaseConfig struct {
	Host     string
	Port     string
	Name     string
	User     string
	Password string
	SSLMode  string
}