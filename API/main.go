package main

import (
	"data-co/api/handlers/companies"

	"github.com/gin-gonic/gin"
)

func main() {
	// creating the gin router
	router := gin.Default()

	// test api
	router.GET("/companies/fetch-company/:id", companies.FetchCompany)

	router.Run()
}