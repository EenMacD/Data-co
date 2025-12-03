package main

import (
	"log"
	"net/http"
	"os"

	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	"github.com/rs/cors"

	"data-co/api/config"
	"data-co/api/database"
	"data-co/api/handlers"
)

func main() {
	// Load environment variables
	if err := godotenv.Load("../.env"); err != nil {
		log.Printf("Warning: .env file not found: %v", err)
	}

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

	// API routes
	api := router.PathPrefix("/api").Subrouter()
	api.HandleFunc("/companies/search", companyHandler.SearchCompanies).Methods("POST", "OPTIONS")
	api.HandleFunc("/companies/count", companyHandler.CountCompanies).Methods("POST", "OPTIONS")
	api.HandleFunc("/companies/{id}", companyHandler.GetCompany).Methods("GET", "OPTIONS")
	api.HandleFunc("/health", healthCheck).Methods("GET")

	// CORS middleware
	corsHandler := cors.New(cors.Options{
		AllowedOrigins:   []string{"http://localhost:3000", "http://localhost:3001", "http://192.168.1.112:3000"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Content-Type", "Authorization"},
		AllowCredentials: true,
	})

	// Start server
	port := os.Getenv("API_PORT")
	if port == "" {
		port = "8080"
	}

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

func healthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok","service":"data-co-api"}`))
}
