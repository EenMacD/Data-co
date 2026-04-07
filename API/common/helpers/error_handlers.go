package helpers

import (
	"log"

	"github.com/gin-gonic/gin"
)

func HError(msg string, err error) {
	if err != nil {
		log.Fatalf("Fatal error: %s:\n %v", msg, err)
	}
}

func HResponseError(errCode int, msg string, err error, c *gin.Context) {
		if err != nil {
		c.JSON(errCode, gin.H{"error": msg, "error message": err.Error() })
		return
	}
}