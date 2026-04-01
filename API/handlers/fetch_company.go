package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func (h *CompanyHandler) FetchCompany(c *gin.Context) {
	companyId := c.Param("id")

	var name string
	err := h.db.QueryRow("SELECT company_name FROM staging_companies WHERE company_number = $1", companyId).Scan(&name)

	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Not found", "error message: ": err.Error() })
		return
	}

	c.JSON(http.StatusOK, gin.H{"id": companyId, "name": name,})
}