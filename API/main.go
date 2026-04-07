package main

import (
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"

	"data-co/api/config"
	"data-co/api/database"
	"data-co/api/handlers"
)

func main() {
	// Load environment variables from .env file if it exists
	// In Docker, environment variables are provided via docker-compose.yml
	_ = godotenv.Load("../.env") // Ignore error, env vars may come from docker-compose

	// Initialize configuration
	cfg := config.LoadConfig()

	// Initialize database connection
	db, err := database.NewConnection(cfg.Database)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	log.Printf("Connected to database: %s", cfg.Database.Name)

	// Initialize handlers
	companyHandler := handlers.NewCompanyHandler(db)

	// Setup router
	router := gin.Default()

	// CORS middleware - read allowed origins from environment
	corsOrigins := os.Getenv("CORS_ALLOWED_ORIGINS")
	allowedOrigins := strings.Split(corsOrigins, ",")
	// Trim whitespace from each origin
	for i, origin := range allowedOrigins {
		allowedOrigins[i] = strings.TrimSpace(origin)
	}
	log.Printf("CORS allowed origins: %v", allowedOrigins)

	router.Use(cors.New(cors.Config{
		AllowOrigins:     allowedOrigins,
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Content-Type", "Authorization", "Origin"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
	}))

	// Root route
	router.GET("/", rootHandler)

	// API routes
	api := router.Group("/api")
	{
		api.GET("/health", healthCheck)

		companies := api.Group("/companies")
		{
			companies.POST("/search", companyHandler.SearchCompanies)
			companies.POST("/count", companyHandler.CountCompanies)
			companies.GET("/:id", companyHandler.GetCompany)
		}
	}

	// Start server
	port := os.Getenv("API_PORT")

	
	if err := router.Run(":" + port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

// this function ensures the API is running and healthy to client
func rootHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"service": "Data-Co API",
		"status":  "running",
		"message": "Welcome to the Data-Co API",
	})
}

func healthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":  "ok",
		"service": "data-co-api",
	})
}
