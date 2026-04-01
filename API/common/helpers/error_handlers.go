package helpers

import "log"

func HandleError(msg string, err error) {
	if err != nil {
		log.Fatalf("Fatal error: %s:\n %v", msg, err)
	}
}