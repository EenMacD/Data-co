package companies

import (
	"fmt"

	"github.com/gin-gonic/gin"
)

func FetchCompany(c *gin.Context) {
	companyId := c.Param("id")
	fmt.Println(companyId)
}