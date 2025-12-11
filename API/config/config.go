package config

import (
	"os"
)

// Config holds all application configuration
type Config struct {
	Database DatabaseConfig
	Server   ServerConfig
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

// ServerConfig holds server settings
type ServerConfig struct {
	Port string
}

// LoadConfig loads configuration from environment variables
func LoadConfig() *Config {
	return &Config{
		Database: DatabaseConfig{
			Host:     os.Getenv("STAGING_DB_HOST"),
			Port:     os.Getenv("STAGING_DB_PORT"),
			Name:     os.Getenv("STAGING_DB_NAME"),
			User:     os.Getenv("STAGING_DB_USER"),
			Password: os.Getenv("STAGING_DB_PASSWORD"),
			SSLMode:  getEnv("DB_SSLMODE", "disable"),
		},
		Server: ServerConfig{
			Port: os.Getenv("API_PORT"),
		},
	}
}

// getEnv gets an environment variable with a fallback default value
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
