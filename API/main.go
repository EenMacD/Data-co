package main

import (
	"data-co/api/common/helpers"
	"data-co/api/database"
	"data-co/api/handlers"
	"log"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

func main() {
	// Load environment variables from .env file
	if err := godotenv.Load("../.env"); err != nil {
		log.Println("Warning: No .env file found or unable to load it. Relying on system environment variables.")
	}

	// creating the gin router
	router := gin.Default()

	// Initialize configuration
	cfg := database.LoadConfig()

	// Initialize database connection
	db, err := database.NewConnection(cfg.Database)
	helpers.HError("Failed to connect to database", err)
	defer db.Close()

	// Initialize handlers
	companyHandler := handlers.NewCompanyHandler(db)

	// to group route under companies
	companies := router.Group("/api/companies")

	companies.GET("/:id", companyHandler.FetchCompany)
	companies.GET("/search", companyHandler.SearchCompanies)


 
	router.Run()
}