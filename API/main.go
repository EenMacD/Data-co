package main

import (
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	"github.com/rs/cors"

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
	router := mux.NewRouter()

	// Root route
	router.HandleFunc("/", rootHandler).Methods("GET")

	// API routes
	api := router.PathPrefix("/api").Subrouter()
	api.HandleFunc("/companies/search", companyHandler.SearchCompanies).Methods("POST", "OPTIONS")
	api.HandleFunc("/companies/count", companyHandler.CountCompanies).Methods("POST", "OPTIONS")
	api.HandleFunc("/companies/{id}", companyHandler.GetCompany).Methods("GET", "OPTIONS")
	api.HandleFunc("/health", healthCheck).Methods("GET")

	// CORS middleware - read allowed origins from environment
	corsOrigins := os.Getenv("CORS_ALLOWED_ORIGINS")
	allowedOrigins := strings.Split(corsOrigins, ",")
	// Trim whitespace from each origin
	for i, origin := range allowedOrigins {
		allowedOrigins[i] = strings.TrimSpace(origin)
	}
	log.Printf("CORS allowed origins: %v", allowedOrigins)

	corsHandler := cors.New(cors.Options{
		AllowedOrigins:   allowedOrigins,
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Content-Type", "Authorization"},
		AllowCredentials: true,
	})

	// Start server
	port := os.Getenv("API_PORT")

	log.Printf("Starting API server on port %s...", port)
	log.Printf("API endpoints:")
	log.Printf("  POST   http://localhost:%s/api/companies/search", port)
	log.Printf("  POST   http://localhost:%s/api/companies/count", port)
	log.Printf("  GET    http://localhost:%s/api/companies/{id}", port)
	log.Printf("  GET    http://localhost:%s/api/health", port)

	if err := http.ListenAndServe(":"+port, corsHandler.Handler(router)); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

// this function ensures the API is running and healthy to client
func rootHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{
		"service": "Data-Co API",
		"status": "running",
		"message": "Welcome to the Data-Co API"
	}`))
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok","service":"data-co-api"}`))
}
